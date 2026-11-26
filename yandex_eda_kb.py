"""Поиск по открытой официальной базе знаний курьеров Яндекс Про / Яндекс Еды.

Загружает только публичные страницы pro.yandex.ru, обнаруженные в sitemap/robots.txt.
Локальный индекс хранится в JSON и обновляется раз в сутки.
"""
from __future__ import annotations

import asyncio
import html
import json
import logging
import os
import re
import time
import xml.etree.ElementTree as ET
from collections import Counter
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlparse, urldefrag

import aiohttp

logger = logging.getLogger("lunapark.yandex_eda_kb")
BASE_DOMAIN = "pro.yandex.ru"
ROBOTS_URL = "https://pro.yandex.ru/robots.txt"
FALLBACK_SITEMAPS = ("https://pro.yandex.ru/sitemap.xml",)
CACHE_PATH = Path(os.getenv("YANDEX_EDA_KB_CACHE", "yandex_eda_kb.json"))
SYNC_INTERVAL_SEC = 24 * 60 * 60
MAX_SITEMAPS = 100
MAX_PAGES = 2500
MAX_TEXT_CHARS = 18000
STOP_WORDS = {
    "это", "как", "что", "где", "когда", "если", "для", "или", "при", "после",
    "перед", "можно", "нужно", "надо", "мой", "моя", "мои", "мне", "меня",
    "есть", "нет", "быть", "будет", "был", "была", "были", "его", "ее", "их",
    "вы", "вам", "вас", "я", "мы", "они", "он", "она", "по", "на", "в", "из",
    "за", "от", "до", "а", "и", "но", "не", "ни", "же", "ли", "то", "к",
}
TOKEN_RE = re.compile(r"[a-zа-яё0-9]{2,}", re.I)
KB_URL_RE = re.compile(r"^/ru-ru/[^/]+/knowledge-base/courier(?:/|$)", re.I)


class _PageParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []
        self.links = []
        self.title_parts = []
        self._skip = 0
        self._in_title = False

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag in ("script", "style", "noscript", "svg"):
            self._skip += 1
        if tag == "title":
            self._in_title = True
        if tag == "a" and attrs.get("href"):
            self.links.append(attrs["href"])
        if tag in ("p", "li", "h1", "h2", "h3", "br", "div"):
            self.parts.append("\n")

    def handle_endtag(self, tag):
        if tag in ("script", "style", "noscript", "svg") and self._skip:
            self._skip -= 1
        if tag == "title":
            self._in_title = False
        if tag in ("p", "li", "h1", "h2", "h3", "div"):
            self.parts.append("\n")

    def handle_data(self, data):
        if self._skip:
            return
        value = data.strip()
        if value:
            self.parts.append(value + " ")
            if self._in_title:
                self.title_parts.append(value)


def _is_kb_url(url: str) -> bool:
    try:
        p = urlparse(url)
        return p.scheme == "https" and p.netloc.lower() == BASE_DOMAIN and bool(KB_URL_RE.match(p.path))
    except Exception:
        return False


def _clean_url(url: str) -> str:
    url, _ = urldefrag(url)
    return url.rstrip("/")


async def _get_text(session: aiohttp.ClientSession, url: str) -> str:
    async with session.get(url, allow_redirects=True) as resp:
        if resp.status != 200:
            return ""
        ctype = resp.headers.get("Content-Type", "").lower()
        if "text/html" not in ctype and "xml" not in ctype and "text/plain" not in ctype:
            return ""
        return await resp.text(errors="ignore")


async def _discover_sitemaps(session: aiohttp.ClientSession) -> list[str]:
    candidates = list(FALLBACK_SITEMAPS)
    try:
        robots = await _get_text(session, ROBOTS_URL)
        for line in robots.splitlines():
            if line.lower().startswith("sitemap:"):
                candidate = line.split(":", 1)[1].strip()
                if urlparse(candidate).netloc.lower() == BASE_DOMAIN:
                    candidates.append(candidate)
    except Exception as exc:
        logger.info("KB: robots.txt недоступен: %s", exc)

    found = []
    seen = set()
    queue = list(dict.fromkeys(candidates))
    while queue and len(seen) < MAX_SITEMAPS:
        sitemap = queue.pop(0)
        if sitemap in seen or urlparse(sitemap).netloc.lower() != BASE_DOMAIN:
            continue
        seen.add(sitemap)
        try:
            raw = await _get_text(session, sitemap)
            if not raw:
                continue
            root = ET.fromstring(raw)
            for loc in root.iter():
                if loc.tag.lower().endswith("loc") and loc.text:
                    target = loc.text.strip()
                    if urlparse(target).netloc.lower() != BASE_DOMAIN:
                        continue
                    if target.lower().endswith(".xml"):
                        if target not in seen:
                            queue.append(target)
                    elif _is_kb_url(target):
                        found.append(_clean_url(target))
        except Exception as exc:
            logger.debug("KB: sitemap %s: %s", sitemap, exc)
    return list(dict.fromkeys(found))[:MAX_PAGES]


async def _fetch_page(session: aiohttp.ClientSession, url: str) -> tuple[dict | None, list[str]]:
    try:
        raw = await _get_text(session, url)
        if not raw:
            return None, []
        parser = _PageParser()
        parser.feed(raw)
        title = " ".join(parser.title_parts).strip() or urlparse(url).path.rstrip("/").split("/")[-1]
        text = html.unescape(" ".join(parser.parts))
        text = re.sub(r"[ \t\xa0]+", " ", text)
        text = re.sub(r"\s*\n\s*", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text).strip()
        text = text[:MAX_TEXT_CHARS]
        if len(text) < 180:
            return None, []
        links = []
        for href in parser.links:
            target = _clean_url(urljoin(url, href))
            if _is_kb_url(target):
                links.append(target)
        return {"url": url, "title": title[:250], "text": text, "city": urlparse(url).path.split("/")[2]}, links
    except Exception as exc:
        logger.debug("KB: страница %s не загружена: %s", url, exc)
        return None, []


def _save_cache(docs: list[dict]) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    temp = CACHE_PATH.with_suffix(CACHE_PATH.suffix + ".tmp")
    temp.write_text(json.dumps({"updated_at": time.time(), "docs": docs}, ensure_ascii=False), encoding="utf-8")
    os.replace(temp, CACHE_PATH)


def _load_cache() -> list[dict]:
    try:
        data = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
        docs = data.get("docs", [])
        return docs if isinstance(docs, list) else []
    except Exception:
        return []


_DOCUMENTS: list[dict] = _load_cache()
_LAST_SYNC = 0.0
_SYNC_LOCK = asyncio.Lock()


async def sync_yandex_eda_knowledge_base(force: bool = False) -> int:
    """Обновляет общий корпус публичных официальных статей по всем регионам из sitemap."""
    global _DOCUMENTS, _LAST_SYNC
    if _SYNC_LOCK.locked():
        return len(_DOCUMENTS)
    async with _SYNC_LOCK:
        if not force and _DOCUMENTS and time.time() - _LAST_SYNC < SYNC_INTERVAL_SEC:
            return len(_DOCUMENTS)
        timeout = aiohttp.ClientTimeout(total=22, connect=8)
        headers = {"User-Agent": "LUNAPARK-EDA-KnowledgeBot/1.0 (public Yandex Pro knowledge base)"}
        connector = aiohttp.TCPConnector(limit=6, ttl_dns_cache=300)
        async with aiohttp.ClientSession(timeout=timeout, headers=headers, connector=connector) as session:
            urls = await _discover_sitemaps(session)
            if not urls:
                logger.warning("KB: sitemap не вернул страницы базы курьеров Яндекс Про")
                return len(_DOCUMENTS)
            queue = list(urls)
            seen = set(queue)
            docs_by_url = {}
            semaphore = asyncio.Semaphore(5)

            async def worker(url: str):
                async with semaphore:
                    return await _fetch_page(session, url)

            while queue and len(seen) <= MAX_PAGES:
                batch = queue[:80]
                queue = queue[80:]
                results = await asyncio.gather(*(worker(url) for url in batch), return_exceptions=True)
                for result in results:
                    if isinstance(result, Exception) or not result:
                        continue
                    doc, links = result
                    if doc:
                        docs_by_url[doc["url"]] = doc
                    for link in links:
                        if link not in seen and len(seen) < MAX_PAGES:
                            seen.add(link)
                            queue.append(link)
                await asyncio.sleep(0.15)

            docs = list(docs_by_url.values())
            if docs:
                _save_cache(docs)
                _DOCUMENTS = docs
                _LAST_SYNC = time.time()
                logger.info("KB: загружено %s официальных страниц Яндекс Про из региональных разделов", len(docs))
            return len(_DOCUMENTS)


def search_yandex_eda_knowledge(query: str, limit: int = 4) -> str:
    """Простой локальный полнотекстовый поиск; возвращает только найденные официальные выдержки."""
    if not _DOCUMENTS:
        return ""
    tokens = [t for t in TOKEN_RE.findall((query or "").lower()) if t not in STOP_WORDS]
    if not tokens:
        return ""
    qset = set(tokens)
    scored = []
    for doc in _DOCUMENTS:
        text = str(doc.get("text") or "")
        title = str(doc.get("title") or "")
        words = TOKEN_RE.findall((title + " " + text).lower())
        if not words:
            continue
        counts = Counter(words)
        score = sum((1.0 + (2.5 if token in title.lower() else 0.0)) * (1 + min(counts[token], 5) * 0.25) for token in qset)
        coverage = sum(1 for token in qset if counts[token]) / max(1, len(qset))
        score *= coverage
        if score > 0 and coverage >= 0.25:
            scored.append((score, doc))
    scored.sort(key=lambda item: item[0], reverse=True)
    chunks = []
    used = set()
    for _, doc in scored:
        key = re.sub(r"/ru-ru/[^/]+/", "/ru-ru/{region}/", doc.get("url", ""))
        if key in used:
            continue
        used.add(key)
        text = str(doc.get("text") or "")
        title = str(doc.get("title") or "Статья Яндекс Про")
        # Берём предложение вокруг первого совпадения, не отправляем целую статью.
        low = text.lower()
        positions = [low.find(token) for token in qset if low.find(token) >= 0]
        start = max(0, min(positions, default=0) - 240)
        end = min(len(text), start + 1100)
        excerpt = text[start:end].strip()
        chunks.append(f"Источник: {title}\nРегион страницы: {doc.get('city', 'не указан')}\nURL: {doc.get('url')}\nФрагмент: {excerpt}")
        if len(chunks) >= limit:
            break
    return "\n\n".join(chunks)
