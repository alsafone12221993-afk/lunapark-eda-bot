import asyncio
import json
import logging
import os
import re
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import aiohttp
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)

# ================= КОНФИГУРАЦИЯ =================
BOT_TOKEN = "9247740629:AAHjKLh1e7cWIznoDS4ogpxfIENiIyx9FvI"
ADMIN_IDS: set[int] = {673430213}

ADMIN_FILE = "admin_chat.json"
USERS_FILE = "users_db.json"
APPS_FILE = "applications.json"
DRIVERS_CACHE_FILE = "drivers_cache.json"
CLIENT_LEADS_FILE = "client_leads.json"

FLEET_API_URL = "https://fleet-api.taxi.yandex.net"

# Основной парк (Карасев Павел Максимович)
PARKS_CONFIG: list[dict[str, str]] = [
    {
        "name": "ЛунапаркЕда",
        "park_id": "1680a13d35084b0688ecab0f925c9d75",
        "client_id": "taxi/park/1680a13d35084b0688ecab0f925c9d75",
        "api_key": "yapi10-E5IuB_Gpfog2E7XMXYxTl1SsS3HyVGbXduM9Jg_d1",
    },
]

# --- НАСТРОЙКИ amoCRM ---
AMOCRM_BASE_URL = "https://lunapark7042.amocrm.ru"
AMOCRM_ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6ImQ3NDM4MThmM2IxMzMxYTE2ZDUwZDRhMDA5ZWI3ZmQ1OTI3YTQ5YTljMDE4OWMwZGY5NjY2YTM2OTU4YjdiYjA1ZjhkNmM2MjkyZDY3MjM2In0.eyJhdWQiOiI3NjNkNmJjNC1mMGVmLTQwY2EtYjg3Mi01MmMzMmY5MWM5MTQiLCJqdGkiOiJkNzQzODE4ZjNiMTMzMWExNmQ1MGQ0YTAwOWViN2ZkNTkyN2E0OWE5YzAxODljMGRmOTY2NmEzNjk1OGI3YmIwNWY4ZDZjNjI5MmQ2NzIzNiIsImlhdCI6MTc5NTMzMDQyNiwibmJmIjoxNzk1MzMwNDI2LCJleHAiOjE5NDc2Mjg4MDAsInN1YiI6IjEyNjQ1NDY2IiwiZ3JhbnRfdHlwZSI6IiIsImFjY291bnRfaWQiOjMxMjkwOTc0LCJiYXNlX2RvbWFpbiI6ImFtb2NybS5ydSIsInZlcnNpb24iOjIsInNjb3BlcyI6WyJwdXNoX25vdGlmaWNhdGlvbnMiLCJmaWxlcyIsImNybSIsImZpbGVzX2RlbGV0ZSIsIm5vdGlmaWNhdGlvbnMiXSwiaGFzaF91dWlkIjoiMDFjMWE3MWEtZWI5Yy00MjJjLTg4ODEtMGQ2Mjg3ZDA2ODA2IiwiYXBpX2RvbWFpbiI6ImFwaS1iLmFtb2NybS5ydSJ9.TyjWbPrNmzDY_yUNQwEFO-8kWSW0MNHyoF_Xd5Vir0TRTfhpLGcpUOAKKPRvLvpESIO1wJ-lUXMcmawGdiLFbKLTqQv3djdWUc2vypbZZVsHKXT2_ZHhaLyxYHNtWB-HJ1Z_F8TB404KqnjlQ2n6opAlu5HMdHpamquMn3AJUcZAqMTknQ8_mlaiMQRUvhD1Dvn6XRtJ9SETH3a78QUQbpj__qc-HOT_b91eLHgjUIV6uLVqRyMFioPjrpY0jpZnAk4CLRoClNZPqml5qbcSnFfJuLBD763O5JoprazMnIWIR_r80Nc7DpLLZk3Eb_yXKTiRqj67lskDtvoM6q19zQ"
AMOCRM_PIPELINE_ID = 10053610  # Яндекс еда
AMOCRM_STATUS_ID = 79731826    # Новый лид
AMOCRM_CLOSED_LOST_ID = 143    # Закрыто и не реализовано
AMOCRM_TELEGRAM_FIELD_ID: int | None = None

GOAL_ORDERS = 5
GOAL_DAYS = 14
GOAL_BONUS = 500

DRIVERS_CACHE: dict[str, dict[str, Any]] = {}
DRIVERS_TTL_SEC = 3600
PAGE_PAUSE_SEC = 0.5

DRIVER_STATS_CACHE: dict[str, dict[str, Any]] = {}
DRIVER_STATS_TTL_SEC = 180

PENDING_PROFILES: dict[int, list[dict[str, Any]]] = {}
_DETECTED_TG_FIELD_CACHE: dict[str, int | None] = {}
_LAST_BOT_MSG_BY_CHAT: dict[int, int] = {}
_FILE_LOCKS: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
_STATS_LOCKS: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

STS_TRANSPORT_KEYWORDS = ("авто", "мото", "груз")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("lunapark_eda_bot")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


# ================= БЕЗОПАСНАЯ РАБОТА С ФАЙЛАМИ И ДАННЫМИ =================
def is_admin_user(user_id: int | None, chat_id: int | None = None) -> bool:
    if user_id is not None and user_id in ADMIN_IDS:
        return True
    if chat_id is not None and chat_id in ADMIN_IDS:
        return True
    return False


def _read_json(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as exc:
        logger.warning("Ошибка чтения JSON %s: %s", path, exc)
        return default


def _write_json(path: str, data: Any) -> None:
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


ADMIN_CHAT_ID: int | None = _read_json(ADMIN_FILE, {}).get("admin_chat_id") or next(iter(ADMIN_IDS), None)


def save_admin_id(chat_id: int) -> None:
    global ADMIN_CHAT_ID
    ADMIN_CHAT_ID = chat_id
    _write_json(ADMIN_FILE, {"admin_chat_id": chat_id})


def normalize_phone(raw_phone: str) -> str:
    digits = re.sub(r"\D", "", raw_phone or "")
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    elif len(digits) == 10:
        digits = "7" + digits
    return "+" + digits if digits else (raw_phone or "").strip()


def format_tg_login(tg_id: int, tg_username: str | None) -> str:
    if tg_username and tg_username != "нет юзернейма":
        clean = tg_username.lstrip("@").strip()
        if clean:
            return f"@{clean}"
    return f"id{tg_id}"


def format_tg_link(tg_id: int, tg_username: str | None) -> str:
    if tg_username and tg_username != "нет юзернейма":
        clean = tg_username.lstrip("@").strip()
        if clean:
            return f"https://t.me/{clean}"
    return f"tg://user?id={tg_id}"


def requires_two_sided_sts(transport_text: str) -> bool:
    low = (transport_text or "").lower()
    return any(k in low for k in STS_TRANSPORT_KEYWORDS)


async def send_ephemeral(
    chat_id: int,
    text: str,
    reply_markup: Any = None,
    parse_mode: str | None = None,
    track: bool = True,
) -> types.Message:
    prev_mid = _LAST_BOT_MSG_BY_CHAT.pop(chat_id, None) if track else None
    if prev_mid:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=prev_mid)
        except Exception:
            pass

    msg = await bot.send_message(
        chat_id=chat_id,
        text=text,
        reply_markup=reply_markup,
        parse_mode=parse_mode,
    )
    if track:
        _LAST_BOT_MSG_BY_CHAT[chat_id] = msg.message_id
    return msg


async def clear_tracked_message(chat_id: int) -> None:
    prev_mid = _LAST_BOT_MSG_BY_CHAT.pop(chat_id, None)
    if prev_mid:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=prev_mid)
        except Exception:
            pass


def save_user_session(tg_id: int, phone: str = "", park_id: str = "", driver_id: str = "", city: str = "") -> dict[str, Any]:
    db = _read_json(USERS_FILE, {})
    key = str(tg_id)
    entry = db.get(key, {})
    if not entry.get("registered_at"):
        entry["registered_at"] = datetime.now(timezone.utc).isoformat()
    if phone:
        entry["phone"] = normalize_phone(phone)
    if park_id:
        entry["park_id"] = park_id
    if driver_id:
        entry["driver_id"] = driver_id
    if city:
        entry["city"] = city
    db[key] = entry
    _write_json(USERS_FILE, db)
    return entry


def get_user_record(tg_id: int) -> dict[str, Any]:
    db = _read_json(USERS_FILE, {})
    return db.get(str(tg_id), {})


def get_registered_dt(tg_id: int) -> datetime:
    rec = get_user_record(tg_id)
    raw = rec.get("registered_at")
    if raw:
        try:
            dt = datetime.fromisoformat(str(raw))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            pass
    entry = save_user_session(tg_id)
    return datetime.fromisoformat(entry["registered_at"])


def save_application(lead_payload: dict[str, Any]) -> None:
    apps = _read_json(APPS_FILE, [])
    lead_payload["created_at"] = datetime.now().strftime("%d.%m.%Y %H:%M")
    lead_payload["status"] = "Принята в работу"
    apps.append(lead_payload)
    _write_json(APPS_FILE, apps)


def load_client_leads() -> dict[str, int]:
    data = _read_json(CLIENT_LEADS_FILE, {})
    if isinstance(data, dict):
        return {str(k): int(v) for k, v in data.items() if str(v).isdigit()}
    return {}


def save_client_lead(tg_id: int, lead_id: int) -> None:
    data = load_client_leads()
    data[str(tg_id)] = int(lead_id)
    _write_json(CLIENT_LEADS_FILE, data)


# ================= ИНТЕГРАЦИЯ С AMOCRM =================
def _amocrm_headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {AMOCRM_ACCESS_TOKEN.strip()}",
        "Content-Type": "application/json",
    }


async def _detect_tg_custom_field_id(
    session: aiohttp.ClientSession,
    base_url: str,
    entity_type: str,
) -> int | None:
    if AMOCRM_TELEGRAM_FIELD_ID:
        return AMOCRM_TELEGRAM_FIELD_ID
    if entity_type in _DETECTED_TG_FIELD_CACHE:
        return _DETECTED_TG_FIELD_CACHE[entity_type]

    try:
        async with session.get(
            f"{base_url}/api/v4/{entity_type}/custom_fields",
            headers=_amocrm_headers(),
            timeout=15,
        ) as resp:
            if resp.status != 200:
                _DETECTED_TG_FIELD_CACHE[entity_type] = None
                return None
            data = await resp.json()
            for f_item in data.get("_embedded", {}).get("custom_fields", []):
                fname = (f_item.get("name") or "").lower().strip()
                if fname in ("telegram", "телеграм", "тг", "tg", "аккаунт telegram", "telegram аккаунт", "логин telegram"):
                    fid = int(f_item["id"])
                    _DETECTED_TG_FIELD_CACHE[entity_type] = fid
                    return fid
    except Exception as exc:
        logger.warning("Не удалось определить поле Telegram для %s: %s", entity_type, exc)

    _DETECTED_TG_FIELD_CACHE[entity_type] = None
    return None


async def _add_amocrm_note(
    session: aiohttp.ClientSession,
    base_url: str,
    lead_id: int,
    text: str,
) -> None:
    notes_url = f"{base_url}/api/v4/leads/{lead_id}/notes"
    notes_payload = [{"note_type": "common", "params": {"text": text}}]
    async with session.post(notes_url, headers=_amocrm_headers(), json=notes_payload, timeout=15) as nresp:
        if nresp.status not in (200, 201, 202):
            logger.warning("amoCRM note warning (%s): %s", nresp.status, await nresp.text())


async def _upload_and_attach_sts(
    session: aiohttp.ClientSession,
    base_url: str,
    lead_id: int | None,
    contact_id: int | None,
    sts_front: Optional[tuple[bytes, str]],
    sts_back: Optional[tuple[bytes, str]],
) -> list[str]:
    uploaded_lines: list[str] = []
    files_to_send: list[tuple[str, bytes, str]] = []
    if sts_front:
        files_to_send.append(("СТС (лицевая сторона)", sts_front[0], sts_front[1]))
    if sts_back:
        files_to_send.append(("СТС (обратная сторона)", sts_back[0], sts_back[1]))

    for label, raw_bytes, fname in files_to_send:
        form = aiohttp.FormData()
        form.add_field("file", raw_bytes, filename=fname, content_type="image/jpeg")
        try:
            async with session.post(
                f"{base_url}/api/v4/files",
                data=form,
                headers={"Authorization": f"Bearer {AMOCRM_ACCESS_TOKEN.strip()}"},
                timeout=25,
            ) as resp:
                txt = await resp.text()
                if resp.status not in (200, 201):
                    uploaded_lines.append(f"{label}: {fname} (ошибка загрузки {resp.status})")
                    continue
                parsed = json.loads(txt) if txt else {}
                item = None
                if isinstance(parsed, list) and parsed:
                    item = parsed[0]
                elif isinstance(parsed, dict):
                    files_list = parsed.get("_embedded", {}).get("files", [])
                    item = files_list[0] if files_list else parsed
                uuid_val = item.get("uuid") if isinstance(item, dict) else None
                dl_url = item.get("_links", {}).get("download") if isinstance(item, dict) else None
                if uuid_val:
                    for ent, eid in [("leads", lead_id), ("contacts", contact_id)]:
                        if eid:
                            await session.put(
                                f"{base_url}/api/v4/{ent}/{eid}/files",
                                headers=_amocrm_headers(),
                                json=[{"file_uuid": uuid_val}],
                                timeout=20,
                            )
                uploaded_lines.append(f"{label}: {fname}" + (f" ({dl_url})" if dl_url else ""))
        except Exception as exc:
            logger.warning("Ошибка загрузки %s в amoCRM: %s", label, exc)
            uploaded_lines.append(f"{label}: {fname} (не удалось загрузить)")

    return uploaded_lines


async def _find_existing_client_lead(
    session: aiohttp.ClientSession,
    base_url: str,
    tg_id: int,
    phone: str = "",
) -> int | None:
    stored = load_client_leads()
    saved_lead_id = stored.get(str(tg_id))
    if saved_lead_id:
        async with session.get(
            f"{base_url}/api/v4/leads/{saved_lead_id}",
            headers=_amocrm_headers(),
            timeout=15,
        ) as resp:
            if resp.status == 200:
                lead_data = await resp.json()
                if lead_data.get("status_id") != AMOCRM_CLOSED_LOST_ID:
                    return int(saved_lead_id)

    queries = [q for q in [normalize_phone(phone) if phone else "", str(tg_id)] if q]
    for query in queries:
        async with session.get(
            f"{base_url}/api/v4/contacts",
            params={"query": query, "with": "leads"},
            headers=_amocrm_headers(),
            timeout=15,
        ) as resp:
            if resp.status != 200:
                continue
            data = await resp.json()
            for contact in data.get("_embedded", {}).get("contacts", []):
                for lead_ref in contact.get("_embedded", {}).get("leads", []):
                    lid = lead_ref.get("id")
                    if not lid:
                        continue
                    async with session.get(
                        f"{base_url}/api/v4/leads/{lid}",
                        headers=_amocrm_headers(),
                        timeout=15,
                    ) as lresp:
                        if lresp.status == 200:
                            ldata = await lresp.json()
                            if (
                                (not AMOCRM_PIPELINE_ID or ldata.get("pipeline_id") == AMOCRM_PIPELINE_ID)
                                and ldata.get("status_id") != AMOCRM_CLOSED_LOST_ID
                            ):
                                save_client_lead(tg_id, int(lid))
                                return int(lid)
    return None


async def send_lead_to_amocrm(
    lead: dict[str, Any],
    sts_front: Optional[tuple[bytes, str]] = None,
    sts_back: Optional[tuple[bytes, str]] = None,
) -> tuple[bool, str]:
    base_url = AMOCRM_BASE_URL.strip().rstrip("/")
    token = AMOCRM_ACCESS_TOKEN.strip()
    if not base_url or not token:
        return False, "не заполнены AMOCRM_BASE_URL / AMOCRM_ACCESS_TOKEN"

    tg_id = int(lead["tg_id"])
    async with _FILE_LOCKS[f"lead_{tg_id}"]:
        tg_username = lead.get("tg_username", "")
        tg_login = format_tg_login(tg_id, tg_username)
        tg_link = format_tg_link(tg_id, tg_username)
        phone = normalize_phone(lead.get("phone", ""))
        lead_title = f"ЛунапаркЕда ({lead['type']}): {lead['name']} — {lead['city']}"

        note_lines = [
            "Парк: ЛунапаркЕда",
            f"Тип заявки: {lead['type']}",
            f"Имя: {lead['name']}",
            f"Город: {lead['city']}",
            f"Формат/транспорт: {lead['transport']}",
            f"Телефон: {phone}",
            f"Telegram: {tg_login}",
            f"Ссылка на чат: {tg_link}",
            f"Telegram ID: {tg_id}",
        ]

        try:
            async with aiohttp.ClientSession() as session:
                contact_tg_fid = await _detect_tg_custom_field_id(session, base_url, "contacts")
                lead_tg_fid = await _detect_tg_custom_field_id(session, base_url, "leads")

                existing_lead_id = await _find_existing_client_lead(session, base_url, tg_id, phone)
                if existing_lead_id:
                    patch_obj: dict[str, Any] = {"name": lead_title}
                    if lead_tg_fid:
                        patch_obj["custom_fields_values"] = [
                            {"field_id": lead_tg_fid, "values": [{"value": tg_login}]}
                        ]
                    async with session.patch(
                        f"{base_url}/api/v4/leads/{existing_lead_id}",
                        headers=_amocrm_headers(),
                        json=patch_obj,
                        timeout=20,
                    ) as presp:
                        if presp.status not in (200, 201, 202):
                            logger.warning("amoCRM patch warning (%s): %s", presp.status, await presp.text())

                    sts_lines = await _upload_and_attach_sts(
                        session, base_url, existing_lead_id, None, sts_front, sts_back
                    )
                    if sts_lines:
                        note_lines.extend(sts_lines)
                    await _add_amocrm_note(session, base_url, existing_lead_id, "\n".join(note_lines))
                    return True, f"обновлена сделка #{existing_lead_id}"

                contact_fields: list[dict[str, Any]] = [
                    {
                        "field_code": "PHONE",
                        "values": [{"value": phone, "enum_code": "MOB"}],
                    }
                ]
                if contact_tg_fid:
                    contact_fields.append({
                        "field_id": contact_tg_fid,
                        "values": [{"value": tg_login}],
                    })

                lead_obj: dict[str, Any] = {"name": lead_title}
                if AMOCRM_PIPELINE_ID:
                    lead_obj["pipeline_id"] = AMOCRM_PIPELINE_ID
                if AMOCRM_STATUS_ID:
                    lead_obj["status_id"] = AMOCRM_STATUS_ID
                if lead_tg_fid:
                    lead_obj["custom_fields_values"] = [
                        {"field_id": lead_tg_fid, "values": [{"value": tg_login}]}
                    ]

                payload = [
                    {
                        **lead_obj,
                        "_embedded": {
                            "contacts": [
                                {
                                    "first_name": lead["name"],
                                    "custom_fields_values": contact_fields,
                                }
                            ]
                        },
                    }
                ]

                url = f"{base_url}/api/v4/leads/complex"
                async with session.post(url, headers=_amocrm_headers(), json=payload, timeout=20) as resp:
                    resp_text = await resp.text()
                    if resp.status not in (200, 201, 202):
                        return False, f"HTTP {resp.status}: {resp_text[:250]}"
                    created = json.loads(resp_text)
                    lead_id = created[0].get("id") if isinstance(created, list) and created else None
                    contact_id = None
                    if isinstance(created, list) and created:
                        c_emb = created[0].get("_embedded", {}).get("contacts", [])
                        if c_emb:
                            contact_id = c_emb[0].get("id")

                if lead_id:
                    save_client_lead(tg_id, int(lead_id))
                    sts_lines = await _upload_and_attach_sts(
                        session,
                        base_url,
                        int(lead_id),
                        int(contact_id) if contact_id else None,
                        sts_front,
                        sts_back,
                    )
                    if sts_lines:
                        note_lines.extend(sts_lines)
                    await _add_amocrm_note(session, base_url, int(lead_id), "\n".join(note_lines))

            return True, f"сделка #{lead_id or 'создана'}"
        except Exception as exc:
            logger.exception("Ошибка отправки заявки в amoCRM")
            return False, str(exc)


async def send_help_to_amocrm(
    problem_text: str,
    tg_id: int,
    tg_username: str | None,
    full_name: str,
) -> tuple[bool, str]:
    base_url = AMOCRM_BASE_URL.strip().rstrip("/")
    token = AMOCRM_ACCESS_TOKEN.strip()
    if not base_url or not token:
        return False, "не заполнены AMOCRM_BASE_URL / AMOCRM_ACCESS_TOKEN"

    async with _FILE_LOCKS[f"lead_{tg_id}"]:
        tg_login = format_tg_login(tg_id, tg_username)
        tg_link = format_tg_link(tg_id, tg_username)
        safe_name = (full_name or "Пользователь").strip()
        user_rec = get_user_record(tg_id)
        phone = user_rec.get("phone", "")

        note_text = (
            "Парк: ЛунапаркЕда\n"
            "Обращение по кнопке «Нужна помощь»\n"
            f"Имя: {safe_name}\n"
            f"Telegram: {tg_login}\n"
            f"Ссылка на чат: {tg_link}\n"
            f"Telegram ID: {tg_id}\n"
            f"Текст проблемы:\n{problem_text.strip()}"
        )

        try:
            async with aiohttp.ClientSession() as session:
                contact_tg_fid = await _detect_tg_custom_field_id(session, base_url, "contacts")
                lead_tg_fid = await _detect_tg_custom_field_id(session, base_url, "leads")

                existing_lead_id = await _find_existing_client_lead(session, base_url, tg_id, phone)
                if existing_lead_id:
                    if lead_tg_fid:
                        await session.patch(
                            f"{base_url}/api/v4/leads/{existing_lead_id}",
                            headers=_amocrm_headers(),
                            json={
                                "custom_fields_values": [
                                    {"field_id": lead_tg_fid, "values": [{"value": tg_login}]}
                                ]
                            },
                            timeout=15,
                        )
                    await _add_amocrm_note(session, base_url, existing_lead_id, note_text)
                    return True, f"добавлено в сделку #{existing_lead_id}"

                contact_fields: list[dict[str, Any]] = []
                if phone:
                    contact_fields.append({
                        "field_code": "PHONE",
                        "values": [{"value": phone, "enum_code": "MOB"}],
                    })
                if contact_tg_fid:
                    contact_fields.append({
                        "field_id": contact_tg_fid,
                        "values": [{"value": tg_login}],
                    })

                lead_obj: dict[str, Any] = {"name": f"ЛунапаркЕда (Помощь): {safe_name} ({tg_login})"}
                if AMOCRM_PIPELINE_ID:
                    lead_obj["pipeline_id"] = AMOCRM_PIPELINE_ID
                if AMOCRM_STATUS_ID:
                    lead_obj["status_id"] = AMOCRM_STATUS_ID
                if lead_tg_fid:
                    lead_obj["custom_fields_values"] = [
                        {"field_id": lead_tg_fid, "values": [{"value": tg_login}]}
                    ]

                contact_payload: dict[str, Any] = {"first_name": safe_name}
                if contact_fields:
                    contact_payload["custom_fields_values"] = contact_fields

                payload = [
                    {
                        **lead_obj,
                        "_embedded": {"contacts": [contact_payload]},
                    }
                ]

                async with session.post(
                    f"{base_url}/api/v4/leads/complex",
                    headers=_amocrm_headers(),
                    json=payload,
                    timeout=20,
                ) as resp:
                    resp_text = await resp.text()
                    if resp.status not in (200, 201, 202):
                        return False, f"HTTP {resp.status}: {resp_text[:250]}"
                    created = json.loads(resp_text)
                    lead_id = created[0].get("id") if isinstance(created, list) and created else None

                if lead_id:
                    save_client_lead(tg_id, int(lead_id))
                    await _add_amocrm_note(session, base_url, int(lead_id), note_text)

            return True, f"сделка #{lead_id or 'создана'}"
        except Exception as exc:
            logger.exception("Ошибка отправки обращения в amoCRM")
            return False, str(exc)


async def append_reply_note_to_amocrm(tg_id: int, reply_text: str) -> None:
    base_url = AMOCRM_BASE_URL.strip().rstrip("/")
    token = AMOCRM_ACCESS_TOKEN.strip()
    if not base_url or not token:
        return
    try:
        async with aiohttp.ClientSession() as session:
            lead_id = await _find_existing_client_lead(session, base_url, tg_id)
            if lead_id:
                await _add_amocrm_note(
                    session,
                    base_url,
                    lead_id,
                    f"Ответ оператора в Telegram (ЛунапаркЕда):\n{reply_text}",
                )
    except Exception as exc:
        logger.warning("Не удалось записать ответ оператора в amoCRM: %s", exc)


# ================= ЯНДЕКС FLEET API И ЗАЩИТА ОТ 429 =================
def _safe_float(val: Any) -> float:
    if val is None:
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, dict):
        for k in ("amount", "value", "total", "price"):
            if k in val:
                return _safe_float(val[k])
        return 0.0
    cleaned = str(val).replace(" ", "").replace(",", ".")
    cleaned = re.sub(r"[^\d.-]", "", cleaned)
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def get_park_by_id(park_id: str) -> dict[str, str]:
    for p in PARKS_CONFIG:
        if p["park_id"] == park_id:
            return p
    return PARKS_CONFIG[0]


def _yandex_headers(park: dict[str, str]) -> dict[str, str]:
    return {
        "X-Park-ID": park["park_id"],
        "X-Client-ID": park["client_id"],
        "X-API-Key": park["api_key"],
        "Content-Type": "application/json",
        "Accept-Language": "ru",
    }


async def _post_fleet_with_retry(
    session: aiohttp.ClientSession,
    park: dict[str, str],
    path: str,
    payload: dict[str, Any],
    max_attempts: int = 4,
) -> dict[str, Any]:
    url = f"{FLEET_API_URL}{path}"
    last_err = ""
    for attempt in range(max_attempts):
        async with session.post(url, headers=_yandex_headers(park), json=payload, timeout=25) as resp:
            text = await resp.text()
            if resp.status == 429:
                retry_after_hdr = resp.headers.get("Retry-After")
                try:
                    wait_sec = max(1.5, float(retry_after_hdr)) if retry_after_hdr else 1.5 * (2 ** attempt)
                except ValueError:
                    wait_sec = 1.5 * (2 ** attempt)
                last_err = "HTTP 429"
                logger.warning("429 на %s (%s), пауза %.1f сек...", path, park["name"], wait_sec)
                await asyncio.sleep(wait_sec)
                continue
            if resp.status != 200:
                raise RuntimeError(f"HTTP {resp.status}: {text[:250]}")
            return json.loads(text)
    raise RuntimeError(last_err or "HTTP 429")


async def _fetch_drivers_page(
    session: aiohttp.ClientSession,
    park: dict[str, str],
    offset: int,
    limit: int = 500,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "query": {"park": {"id": park["park_id"]}},
        "limit": limit,
        "offset": offset,
    }
    return await _post_fleet_with_retry(session, park, "/v1/parks/driver-profiles/list", payload)


async def _fetch_all_drivers_for_park(
    session: aiohttp.ClientSession,
    park: dict[str, str],
    partial_fallback: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    offset = 0
    limit = 500
    result: list[dict[str, Any]] = []

    try:
        while True:
            data = await _fetch_drivers_page(session, park, offset=offset, limit=limit)
            batch = data.get("driver_profiles", []) or []
            if not batch:
                break

            result.extend(batch)
            total = data.get("total")
            offset += len(batch)
            if total is not None and offset >= int(total):
                break

            await asyncio.sleep(PAGE_PAUSE_SEC)
    except RuntimeError as exc:
        if "429" in str(exc) and (result or partial_fallback):
            logger.warning("Частичный кэш по парку %s из-за 429", park["name"])
            return result if len(result) > len(partial_fallback or []) else (partial_fallback or [])
        raise

    return result


async def get_cached_drivers(
    session: aiohttp.ClientSession,
    park: dict[str, str],
    force_refresh: bool = False,
) -> list[dict[str, Any]]:
    pid = park["park_id"]
    now = time.monotonic()
    mem = DRIVERS_CACHE.get(pid)
    if not force_refresh and mem and mem.get("data") and (now - mem.get("ts", 0.0)) < DRIVERS_TTL_SEC:
        return mem["data"]

    disk_all = _read_json(DRIVERS_CACHE_FILE, {})
    disk_entry = disk_all.get(pid, {})
    disk_ts = disk_entry.get("updated_at", 0.0)
    disk_profiles = disk_entry.get("profiles", [])

    if not force_refresh and disk_profiles and (time.time() - disk_ts) < DRIVERS_TTL_SEC:
        DRIVERS_CACHE[pid] = {"ts": now, "data": disk_profiles}
        return disk_profiles

    try:
        drivers = await _fetch_all_drivers_for_park(session, park, partial_fallback=disk_profiles or (mem or {}).get("data"))
    except RuntimeError as exc:
        if "429" in str(exc) and (disk_profiles or (mem or {}).get("data")):
            return (mem or {}).get("data") or disk_profiles
        raise

    if drivers:
        DRIVERS_CACHE[pid] = {"ts": now, "data": drivers}
        disk_all[pid] = {"updated_at": time.time(), "profiles": drivers}
        _write_json(DRIVERS_CACHE_FILE, disk_all)
    return drivers


def _extract_matches_in_park(park: dict[str, str], drivers: list[dict[str, Any]], target_phone: str) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for item in drivers:
        profile = item.get("driver_profile", {})
        phones = profile.get("phones", []) or []
        if any(normalize_phone(str(p)) == target_phone for p in phones):
            accounts = item.get("accounts") or []
            balance_raw = accounts[0].get("balance", "0") if accounts else "0"
            current_status = item.get("current_status") or {}
            car = item.get("car") or {}
            category = car.get("category") or profile.get("car_category") or "Курьер"
            city = profile.get("city") or "—"
            work_status = profile.get("work_status", "unknown")

            matches.append({
                "park_id": park["park_id"],
                "park_name": park["name"],
                "id": profile.get("id"),
                "phone": target_phone,
                "full_name": " ".join(
                    x for x in [
                        profile.get("last_name", ""),
                        profile.get("first_name", ""),
                        profile.get("middle_name", ""),
                    ] if x
                ).strip() or "Курьер",
                "work_status": work_status,
                "line_status": current_status.get("status", "offline"),
                "balance": _safe_float(balance_raw),
                "city": city,
                "category": category,
            })
    return matches


async def _find_all_profiles_by_phone(session: aiohttp.ClientSession, target_phone: str) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    for park in PARKS_CONFIG:
        drivers = await get_cached_drivers(session, park, force_refresh=False)
        park_matches = _extract_matches_in_park(park, drivers, target_phone)
        found.extend(park_matches)
    return found


def _parse_order_dt(order: dict[str, Any]) -> Optional[datetime]:
    for key in ("booked_at", "ended_at", "created_at"):
        raw = order.get(key)
        if not raw:
            continue
        try:
            dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None


async def _get_driver_orders_stats(
    session: aiohttp.ClientSession,
    park: dict[str, str],
    driver_id: str,
    registered_dt: datetime,
    force_refresh: bool = False,
) -> dict[str, Any]:
    cache_key = f"{park['park_id']}:{driver_id}"
    async with _STATS_LOCKS[cache_key]:
        now_mono = time.monotonic()
        cached = DRIVER_STATS_CACHE.get(cache_key)
        if not force_refresh and cached and (now_mono - cached["ts"]) < DRIVER_STATS_TTL_SEC:
            return cached["data"]

        now = datetime.now(timezone.utc)
        week_ago = now - timedelta(days=7)
        reg_end = registered_dt + timedelta(days=GOAL_DAYS)
        window_from = min(week_ago, registered_dt)
        window_to = now

        payload = {
            "query": {
                "park": {
                    "id": park["park_id"],
                    "driver_profile": {"id": driver_id},
                    "order": {
                        "booked_at": {
                            "from": window_from.strftime("%Y-%m-%dT%H:%M:%SZ"),
                            "to": window_to.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        }
                    },
                }
            },
            "limit": 300,
        }

        try:
            data = await _post_fleet_with_retry(session, park, "/v1/parks/orders/list", payload)
        except Exception as exc:
            logger.warning("orders/list ошибка для %s: %s", driver_id, exc)
            if cached and cached.get("data", {}).get("ok"):
                return cached["data"]
            return {
                "ok": False,
                "error": str(exc),
                "completed_7d": 0,
                "completed_14d_from_reg": 0,
                "within_14d_window": now <= reg_end,
                "active_orders": [],
                "earnings_7d": 0.0,
            }

        completed_7d = 0
        completed_14d_from_reg = 0
        active_orders: list[str] = []
        earnings_total = 0.0

        for order in data.get("orders", []):
            status = order.get("status", "")
            order_id = order.get("id", "—")
            price = _safe_float(order.get("price") or order.get("driver_income") or order.get("cost"))
            odt = _parse_order_dt(order)

            if status in ("complete", "finished", "delivered"):
                if odt is None or odt >= week_ago:
                    completed_7d += 1
                    earnings_total += price
                if odt is None or (registered_dt <= odt <= reg_end):
                    completed_14d_from_reg += 1
            elif status not in ("cancelled", "failed"):
                active_orders.append(f"#{order_id} ({status})")

        result = {
            "ok": True,
            "error": "",
            "completed_7d": completed_7d,
            "completed_14d_from_reg": completed_14d_from_reg,
            "within_14d_window": now <= reg_end,
            "active_orders": active_orders,
            "earnings_7d": round(earnings_total, 2),
        }
        DRIVER_STATS_CACHE[cache_key] = {"ts": now_mono, "data": result}
        return result


# ================= СОСТОЯНИЯ И КЛАВИАТУРЫ =================
class RegForm(StatesGroup):
    mode = State()
    transport = State()
    city = State()
    name = State()
    phone = State()
    sts_front = State()
    sts_back = State()
    confirm = State()
    choosing_edit = State()


class CourierAuthForm(StatesGroup):
    phone = State()


class HelpForm(StatesGroup):
    waiting_problem = State()


def start_menu(user_id: int | None = None):
    rows = [
        [KeyboardButton(text="🚀 Подключиться к парку"), KeyboardButton(text="✅ Уже курьер парка")],
        [KeyboardButton(text="❓ Нужна помощь")],
    ]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def courier_cabinet_menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Моя статистика"), KeyboardButton(text="🤔 Я на линии?")],
            [KeyboardButton(text="🎁 Цель"), KeyboardButton(text="❓ Нужна помощь")],
            [KeyboardButton(text="🔄 Сменить номер"), KeyboardButton(text="⬅️ В начало")],
        ],
        resize_keyboard=True,
    )


def phone_request_menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📱 Отправить мой номер телефона", request_contact=True)],
            [KeyboardButton(text="❌ Отмена")],
        ],
        resize_keyboard=True,
    )


def transport_choice_menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🚶 Пеший / Вело"), KeyboardButton(text="🛵 Мото")],
            [KeyboardButton(text="🚗 Авто"), KeyboardButton(text="🚚 Грузовой")],
            [KeyboardButton(text="❌ Отмена")],
        ],
        resize_keyboard=True,
    )


def confirm_application_menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Да"), KeyboardButton(text="Исправить")],
            [KeyboardButton(text="❌ Отмена")],
        ],
        resize_keyboard=True,
    )


def edit_field_choice_menu(data: dict[str, Any]):
    rows: list[list[KeyboardButton]] = [
        [KeyboardButton(text="Изменить формат"), KeyboardButton(text="Изменить город")],
        [KeyboardButton(text="Изменить имя"), KeyboardButton(text="Изменить телефон")],
    ]
    if requires_two_sided_sts(data.get("transport", "")):
        rows.append([KeyboardButton(text="Изменить фото СТС")])
    rows.append([KeyboardButton(text="Назад к проверке"), KeyboardButton(text="❌ Отмена")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def build_profiles_choice_kb(profiles: list[dict[str, Any]]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for idx, prof in enumerate(profiles[:10]):
        rows.append([
            InlineKeyboardButton(
                text=f"{idx + 1}. {prof['park_name']}",
                callback_data=f"pick_profile:{idx}",
            )
        ])
    rows.append([InlineKeyboardButton(text="Хочу ввести другой номер", callback_data="retry_phone")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ================= АДМИН-КОМАНДА ОТВЕТА =================
@dp.message(Command("reply"))
async def cmd_reply_to_user(message: types.Message):
    if not is_admin_user(message.from_user.id if message.from_user else None, message.chat.id):
        return
    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 3 or not parts[1].isdigit():
        await send_ephemeral(
            message.chat.id,
            "Формат:\n/reply <telegram_id> <текст ответа>",
        )
        return

    target_tg_id = int(parts[1])
    reply_body = parts[2].strip()
    try:
        await bot.send_message(
            chat_id=target_tg_id,
            text=f"💬 Ответ поддержки парка ЛунапаркЕда:\n\n{reply_body}",
        )
        await append_reply_note_to_amocrm(target_tg_id, reply_body)
        await send_ephemeral(message.chat.id, f"✅ Ответ отправлен пользователю {target_tg_id}.")
    except Exception as exc:
        await send_ephemeral(message.chat.id, f"⚠️ Не удалось отправить ответ: {exc}")


# ================= ОСНОВНОЕ МЕНЮ И КАБИНЕТ КУРЬЕРА =================
@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    if message.from_user:
        save_user_session(message.from_user.id)
    uid = message.from_user.id if message.from_user else None
    text = (
        f"Привет, {message.from_user.first_name}! 👋\n\n"
        "Добро пожаловать в официальный бот парка ЛунапаркЕда.\n"
        "Выберите нужный пункт в меню ниже 👇"
    )
    await send_ephemeral(message.chat.id, text, reply_markup=start_menu(uid))


@dp.message(F.text == "⬅️ В начало")
async def back_to_start(message: types.Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id if message.from_user else None
    await send_ephemeral(message.chat.id, "Главное меню 👇", reply_markup=start_menu(uid))


@dp.message(F.text == "✅ Уже курьер парка")
async def btn_already_courier(message: types.Message, state: FSMContext):
    await state.clear()
    if message.from_user:
        save_user_session(message.from_user.id)
    await state.set_state(CourierAuthForm.phone)
    await send_ephemeral(
        message.chat.id,
        "Введите номер телефона, привязанный к аккаунту в парке (или нажмите кнопку ниже):",
        reply_markup=phone_request_menu(),
    )


@dp.message(F.text == "🔄 Сменить номер")
async def btn_change_phone(message: types.Message, state: FSMContext):
    await state.clear()
    await state.set_state(CourierAuthForm.phone)
    await send_ephemeral(
        message.chat.id,
        "Отправьте номер телефона для входа в кабинет курьера:",
        reply_markup=phone_request_menu(),
    )


@dp.message(CourierAuthForm.phone)
async def process_courier_auth_phone(message: types.Message, state: FSMContext):
    uid = message.from_user.id if message.from_user else None
    if message.text == "❌ Отмена":
        await state.clear()
        await send_ephemeral(message.chat.id, "Отменено.", reply_markup=start_menu(uid))
        return

    raw_phone = message.contact.phone_number if message.contact else (message.text or "")
    phone = normalize_phone(raw_phone)
    await state.clear()

    await lookup_and_route_phone(message, phone)


@dp.callback_query(F.data == "retry_phone")
async def cb_retry_phone(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    await state.set_state(CourierAuthForm.phone)
    await send_ephemeral(
        callback.message.chat.id,
        "Введите номер телефона, привязанный к аккаунту в парке:",
        reply_markup=phone_request_menu(),
    )


@dp.callback_query(F.data.startswith("pick_profile:"))
async def cb_pick_profile(callback: types.CallbackQuery):
    await callback.answer()
    idx_str = callback.data.split(":", 1)[1]
    profiles = PENDING_PROFILES.get(callback.from_user.id, [])
    if not idx_str.isdigit() or int(idx_str) >= len(profiles):
        await send_ephemeral(
            callback.message.chat.id,
            "Список профилей устарел. Нажмите «✅ Уже курьер парка» и введите номер снова.",
        )
        return

    chosen = profiles[int(idx_str)]
    save_user_session(
        callback.from_user.id,
        phone=chosen["phone"],
        park_id=chosen["park_id"],
        driver_id=chosen["id"],
    )
    await show_cabinet_for_profile(callback.message, callback.from_user.id, chosen)


async def lookup_and_route_phone(message: types.Message, phone: str):
    uid = message.from_user.id if message.from_user else None
    wait_msg = await send_ephemeral(message.chat.id, "⏳ Проверяю ваш номер в базе парка...", track=False)
    try:
        async with aiohttp.ClientSession() as session:
            profiles = await _find_all_profiles_by_phone(session, phone)
    except Exception as exc:
        try:
            await wait_msg.delete()
        except Exception:
            pass
        await send_ephemeral(
            message.chat.id,
            f"⚠️ Не удалось проверить профиль в парке: {exc}",
            reply_markup=start_menu(uid),
        )
        return

    try:
        await wait_msg.delete()
    except Exception:
        pass

    if not profiles:
        await send_ephemeral(
            message.chat.id,
            f"❌ Номер {phone} не найден в базе подключённых парков.\n\n"
            "Проверьте правильность номера или нажмите «🚀 Подключиться к парку», чтобы оставить заявку.",
            reply_markup=start_menu(uid),
        )
        return

    if len(profiles) == 1:
        chosen = profiles[0]
        save_user_session(message.from_user.id, phone=phone, park_id=chosen["park_id"], driver_id=chosen["id"])
        await show_cabinet_for_profile(message, message.from_user.id, chosen)
        return

    PENDING_PROFILES[message.from_user.id] = profiles
    lines = ["Выберите один из предложенных профилей:\n"]
    for idx, prof in enumerate(profiles[:10]):
        status_ru = "Работает" if prof["work_status"] == "working" else prof["work_status"]
        lines.append(
            f"🗒 {idx + 1}. {prof['full_name']}\n"
            f"{prof['phone']}\n"
            f"Статус - {status_ru};\n"
            f"Регион - {prof['city']};\n"
            f"Категория - {prof['category']};\n"
            f"Парк - {prof['park_name']}.\n"
        )
    await send_ephemeral(
        message.chat.id,
        "\n".join(lines),
        reply_markup=build_profiles_choice_kb(profiles),
    )


async def show_cabinet_for_profile(message: types.Message, tg_user_id: int, driver: dict[str, Any]):
    park = get_park_by_id(driver["park_id"])
    reg_dt = get_registered_dt(tg_user_id)
    try:
        async with aiohttp.ClientSession() as session:
            stats = await _get_driver_orders_stats(session, park, driver["id"], registered_dt=reg_dt)
    except Exception as exc:
        stats = {
            "ok": False,
            "error": str(exc),
            "completed_7d": 0,
            "completed_14d_from_reg": 0,
            "within_14d_window": True,
            "active_orders": [],
            "earnings_7d": 0.0,
        }

    if stats["ok"]:
        orders_block = (
            f"📦 Заказов за 7 дней: {stats['completed_7d']}\n"
            f"💰 Заработок за 7 дней: {stats['earnings_7d']:.2f} ₽\n"
        )
    else:
        orders_block = f"📦 Детализация заказов недоступна ({stats['error']})\n"

    text = (
        f"✅ Добро пожаловать, {driver['full_name']}!\n"
        f"🏢 Парк: {driver['park_name']}\n\n"
        f"💳 Баланс: {driver['balance']:.2f} ₽\n"
        f"{orders_block}\n"
        "Кабинет курьера открыт — выбирайте нужный раздел в меню ниже 👇"
    )
    await send_ephemeral(message.chat.id, text, reply_markup=courier_cabinet_menu())


async def _get_saved_profile_or_redirect(message: types.Message, state: FSMContext) -> dict[str, Any] | None:
    rec = get_user_record(message.from_user.id)
    phone = rec.get("phone")
    if not phone:
        await state.set_state(CourierAuthForm.phone)
        await send_ephemeral(
            message.chat.id,
            "Сначала укажите ваш номер телефона в парке:",
            reply_markup=phone_request_menu(),
        )
        return None

    async with aiohttp.ClientSession() as session:
        profiles = await _find_all_profiles_by_phone(session, phone)
    if not profiles:
        return None

    preferred_park_id = rec.get("park_id")
    if preferred_park_id:
        for p in profiles:
            if p["park_id"] == preferred_park_id:
                return p
    return profiles[0]


@dp.message(F.text == "📊 Моя статистика")
async def btn_stats(message: types.Message, state: FSMContext):
    driver = await _get_saved_profile_or_redirect(message, state)
    if not driver:
        return
    await show_cabinet_for_profile(message, message.from_user.id, driver)


@dp.message(F.text == "🤔 Я на линии?")
async def btn_line_status(message: types.Message, state: FSMContext):
    driver = await _get_saved_profile_or_redirect(message, state)
    if not driver:
        return

    park = get_park_by_id(driver["park_id"])
    reg_dt = get_registered_dt(message.from_user.id)
    wait_msg = await send_ephemeral(message.chat.id, "⏳ Проверяю статус на линии...", track=False)
    try:
        async with aiohttp.ClientSession() as session:
            stats = await _get_driver_orders_stats(session, park, driver["id"], registered_dt=reg_dt)
    except Exception as exc:
        await wait_msg.edit_text(f"⚠️ Ошибка проверки линии: {exc}")
        return

    line_map = {
        "online": "🟢 На линии (готов принимать заказы)",
        "busy": "🟡 На заказе",
        "offline": "🔴 Не на линии (офлайн)",
    }
    line_human = line_map.get(driver["line_status"], f"ℹ️ Статус: {driver['line_status']}")
    active_info = "\n".join(f"• {o}" for o in stats["active_orders"]) if stats["active_orders"] else "Нет активных заказов"

    text = (
        "🤔 Проверка статуса на линии\n\n"
        f"👤 Курьер: {driver['full_name']}\n"
        f"🏢 Парк: {driver['park_name']}\n"
        f"📌 Статус профиля: {driver['work_status']}\n"
        f"📡 Линия: {line_human}\n\n"
        f"📦 Текущие заказы:\n{active_info}"
    )
    await wait_msg.edit_text(text)


@dp.message(F.text == "🎁 Цель")
async def btn_goal(message: types.Message, state: FSMContext):
    driver = await _get_saved_profile_or_redirect(message, state)
    if not driver:
        return

    park = get_park_by_id(driver["park_id"])
    reg_dt = get_registered_dt(message.from_user.id)
    wait_msg = await send_ephemeral(message.chat.id, "⏳ Считаю прогресс по цели...", track=False)
    try:
        async with aiohttp.ClientSession() as session:
            stats = await _get_driver_orders_stats(session, park, driver["id"], registered_dt=reg_dt)
    except Exception as exc:
        await wait_msg.edit_text(f"⚠️ Ошибка расчёта цели: {exc}")
        return

    done = stats["completed_14d_from_reg"]
    left = max(0, GOAL_ORDERS - done)
    filled = min(GOAL_ORDERS, done)
    bar = "🟩" * filled + "⬜" * (GOAL_ORDERS - filled)

    reg_str = reg_dt.astimezone().strftime("%d.%m.%Y")
    if left == 0:
        status_line = f"🎉 Цель выполнена! Бонус +{GOAL_BONUS} ₽ разблокирован!"
    elif not stats["within_14d_window"]:
        status_line = f"Срок {GOAL_DAYS} дней с момента регистрации ({reg_str}) завершён."
    else:
        status_line = f"Осталось выполнить: {left} заказ(ов) за первые {GOAL_DAYS} дней (от {reg_str}) до бонуса +{GOAL_BONUS} ₽"

    text = (
        f"🎁 Цель: {GOAL_ORDERS} заказов за первые {GOAL_DAYS} дней (+{GOAL_BONUS} ₽)\n\n"
        f"Прогресс: {done} / {GOAL_ORDERS} заказов\n"
        f"{bar}\n\n"
        f"{status_line}"
    )
    await wait_msg.edit_text(text)


# ================= КНОПКА «НУЖНА ПОМОЩЬ» =================
@dp.message(F.text == "❓ Нужна помощь")
async def btn_help(message: types.Message, state: FSMContext):
    await state.clear()
    await state.set_state(HelpForm.waiting_problem)
    await send_ephemeral(message.chat.id, "Добрый день! Опишите Вашу проблему")


@dp.message(HelpForm.waiting_problem)
async def process_help_problem(message: types.Message, state: FSMContext):
    uid = message.from_user.id if message.from_user else None
    if message.text == "❌ Отмена":
        await state.clear()
        await send_ephemeral(message.chat.id, "Отменено.", reply_markup=start_menu(uid))
        return

    problem_text = (message.text or "").strip()
    if not problem_text:
        await send_ephemeral(message.chat.id, "Напишите текстом, что случилось — мы передадим обращение специалисту.")
        return

    await state.clear()
    await clear_tracked_message(message.chat.id)

    tg_id = message.from_user.id
    tg_username = message.from_user.username or ""
    full_name = message.from_user.full_name or "Пользователь"

    try:
        await send_help_to_amocrm(
            problem_text=problem_text,
            tg_id=tg_id,
            tg_username=tg_username,
            full_name=full_name,
        )
    except Exception as exc:
        logger.warning("Ошибка отправки обращения в amoCRM: %s", exc)

    await send_ephemeral(
        message.chat.id,
        "Получили ваше сообщение, скоро ответим.",
        reply_markup=start_menu(tg_id),
    )


# ================= ПОДКЛЮЧЕНИЕ, ПРОВЕРКА И РЕДАКТИРОВАНИЕ =================
async def _show_confirmation_screen(message: types.Message, state: FSMContext) -> None:
    data = await state.get_data()
    await state.set_state(RegForm.confirm)

    sts_line = ""
    if requires_two_sided_sts(data.get("transport", "")):
        has_front = bool(data.get("sts_front"))
        has_back = bool(data.get("sts_back"))
        sts_line = f"\nФото СТС: {'загружены с двух сторон' if (has_front and has_back) else 'не загружены'}\n"

    text = (
        "Проверьте, пожалуйста, всё ли корректно:\n\n"
        f"Имя: {data.get('name', '—')}\n"
        f"Город: {data.get('city', '—')}\n"
        f"Формат: {data.get('transport', '—')}\n"
        f"Телефон: {data.get('phone', '—')}"
        f"{sts_line}\n"
        "Всё верно?"
    )
    await send_ephemeral(message.chat.id, text, reply_markup=confirm_application_menu())


@dp.message(F.text == "🚀 Подключиться к парку")
async def start_reg(message: types.Message, state: FSMContext):
    await state.clear()
    if message.from_user:
        save_user_session(message.from_user.id)
    await state.set_state(RegForm.transport)
    await state.update_data(mode="courier", return_to_confirm=False)
    await send_ephemeral(
        message.chat.id,
        "Шаг 1 из 4. Как планируете доставлять заказы?",
        reply_markup=transport_choice_menu(),
    )


@dp.message(F.text == "❌ Отмена")
async def cancel_any(message: types.Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id if message.from_user else None
    await send_ephemeral(message.chat.id, "Отменено.", reply_markup=start_menu(uid))


@dp.message(RegForm.transport)
async def process_transport(message: types.Message, state: FSMContext):
    transport_val = (message.text or "").strip()
    if not transport_val:
        await send_ephemeral(message.chat.id, "Выберите формат доставки:", reply_markup=transport_choice_menu())
        return

    data = await state.get_data()
    return_to_confirm = bool(data.get("return_to_confirm"))
    await state.update_data(transport=transport_val)

    if return_to_confirm:
        if requires_two_sided_sts(transport_val) and not (data.get("sts_front") and data.get("sts_back")):
            await state.set_state(RegForm.sts_front)
            await send_ephemeral(
                message.chat.id,
                "Для выбранного формата нужно фото СТС с двух сторон. Пришлите лицевую сторону (1 из 2):",
                reply_markup=ReplyKeyboardRemove(),
            )
            return
        await _show_confirmation_screen(message, state)
        return

    await state.set_state(RegForm.city)
    await send_ephemeral(
        message.chat.id,
        "Шаг 2 из 4. В каком городе планируете работать?",
        reply_markup=ReplyKeyboardRemove(),
    )


@dp.message(RegForm.city)
async def process_city(message: types.Message, state: FSMContext):
    city_val = (message.text or "").strip()
    if len(city_val) < 2:
        await send_ephemeral(message.chat.id, "Напишите корректное название города:")
        return

    data = await state.get_data()
    await state.update_data(city=city_val)

    if data.get("return_to_confirm"):
        await _show_confirmation_screen(message, state)
        return

    await state.set_state(RegForm.name)
    await send_ephemeral(message.chat.id, "Шаг 3 из 4. Напишите ваши Фамилию и Имя:")


@dp.message(RegForm.name)
async def process_name(message: types.Message, state: FSMContext):
    name_val = (message.text or "").strip()
    if len(name_val) < 2:
        await send_ephemeral(message.chat.id, "Напишите ваши Фамилию и Имя:")
        return

    data = await state.get_data()
    await state.update_data(name=name_val)

    if data.get("return_to_confirm"):
        await _show_confirmation_screen(message, state)
        return

    await state.set_state(RegForm.phone)
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📱 Отправить мой номер телефона", request_contact=True)]],
        resize_keyboard=True,
    )
    await send_ephemeral(message.chat.id, "Шаг 4 из 4. Нажмите кнопку ниже или введите номер телефона:", reply_markup=kb)


async def _extract_image_from_message(message: types.Message, default_name: str) -> Optional[tuple[bytes, str]]:
    file_id: Optional[str] = None
    filename = default_name
    if message.photo:
        file_id = message.photo[-1].file_id
    elif message.document and (message.document.mime_type or "").startswith("image/"):
        file_id = message.document.file_id
        if message.document.file_name:
            filename = message.document.file_name
    if not file_id:
        return None
    tg_file = await bot.get_file(file_id)
    buf = await bot.download_file(tg_file.file_path)
    raw = buf.read() if buf else b""
    return (raw, filename) if raw else None


@dp.message(RegForm.phone)
async def process_phone(message: types.Message, state: FSMContext):
    raw_phone = message.contact.phone_number if message.contact else (message.text or "")
    digits = re.sub(r"\D", "", raw_phone)
    if len(digits) < 10:
        await send_ephemeral(message.chat.id, "Номер введён не полностью. Введите номер в формате +7 999 123-45-67:")
        return

    data = await state.get_data()
    norm_phone = normalize_phone(raw_phone)
    await state.update_data(phone=norm_phone)

    if data.get("return_to_confirm"):
        await _show_confirmation_screen(message, state)
        return

    if requires_two_sided_sts(data.get("transport", "")):
        await state.set_state(RegForm.sts_front)
        await send_ephemeral(
            message.chat.id,
            "Пришлите фото СТС — сначала лицевую сторону (1 из 2):",
            reply_markup=ReplyKeyboardRemove(),
        )
        return

    await _show_confirmation_screen(message, state)


@dp.message(RegForm.sts_front)
async def process_sts_front(message: types.Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await cancel_any(message, state)
        return
    img = await _extract_image_from_message(message, f"sts_front_{message.from_user.id}.jpg")
    if not img:
        await send_ephemeral(message.chat.id, "Пришлите фото лицевой стороны СТС (1 из 2):")
        return
    await state.update_data(sts_front=img)
    await state.set_state(RegForm.sts_back)
    await send_ephemeral(message.chat.id, "Отлично! Теперь пришлите фото обратной стороны СТС (2 из 2):")


@dp.message(RegForm.sts_back)
async def process_sts_back(message: types.Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await cancel_any(message, state)
        return
    img = await _extract_image_from_message(message, f"sts_back_{message.from_user.id}.jpg")
    if not img:
        await send_ephemeral(message.chat.id, "Пришлите фото обратной стороны СТС (2 из 2):")
        return
    await state.update_data(sts_back=img)
    await _show_confirmation_screen(message, state)


@dp.message(RegForm.confirm)
async def process_confirmation(message: types.Message, state: FSMContext):
    choice = (message.text or "").strip()
    if choice == "❌ Отмена":
        await cancel_any(message, state)
        return

    if choice == "Исправить":
        data = await state.get_data()
        await state.update_data(return_to_confirm=True)
        await state.set_state(RegForm.choosing_edit)
        await send_ephemeral(
            message.chat.id,
            "Выберите, что хотите изменить:",
            reply_markup=edit_field_choice_menu(data),
        )
        return

    if choice == "Да":
        data = await state.get_data()
        await _finalize_registration(
            message,
            state,
            sts_front=data.get("sts_front"),
            sts_back=data.get("sts_back"),
        )
        return

    await send_ephemeral(
        message.chat.id,
        "Нажмите «Да», если данные верны, или «Исправить», чтобы поменять данные:",
        reply_markup=confirm_application_menu(),
    )


@dp.message(RegForm.choosing_edit)
async def process_choosing_edit_field(message: types.Message, state: FSMContext):
    choice = (message.text or "").strip()
    if choice == "❌ Отмена":
        await cancel_any(message, state)
        return

    if choice == "Назад к проверке":
        await _show_confirmation_screen(message, state)
        return

    if choice == "Изменить формат":
        await state.set_state(RegForm.transport)
        await send_ephemeral(message.chat.id, "Выберите новый формат доставки:", reply_markup=transport_choice_menu())
        return

    if choice == "Изменить город":
        await state.set_state(RegForm.city)
        await send_ephemeral(message.chat.id, "Введите новый город:", reply_markup=ReplyKeyboardRemove())
        return

    if choice == "Изменить имя":
        await state.set_state(RegForm.name)
        await send_ephemeral(message.chat.id, "Введите новые Фамилию и Имя:", reply_markup=ReplyKeyboardRemove())
        return

    if choice == "Изменить телефон":
        await state.set_state(RegForm.phone)
        kb = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="📱 Отправить мой номер телефона", request_contact=True)]],
            resize_keyboard=True,
        )
        await send_ephemeral(message.chat.id, "Отправьте новый номер телефона:", reply_markup=kb)
        return

    if choice == "Изменить фото СТС":
        await state.set_state(RegForm.sts_front)
        await send_ephemeral(
            message.chat.id,
            "Пришлите новое фото лицевой стороны СТС (1 из 2):",
            reply_markup=ReplyKeyboardRemove(),
        )
        return

    data = await state.get_data()
    await send_ephemeral(
        message.chat.id,
        "Выберите пункт из меню ниже:",
        reply_markup=edit_field_choice_menu(data),
    )


async def _finalize_registration(
    message: types.Message,
    state: FSMContext,
    sts_front: Optional[tuple[bytes, str]],
    sts_back: Optional[tuple[bytes, str]],
):
    data = await state.get_data()
    await state.clear()
    await clear_tracked_message(message.chat.id)

    norm_phone = data.get("phone", "")
    save_user_session(message.from_user.id, phone=norm_phone, city=data.get("city", ""))

    username_raw = message.from_user.username or ""
    tg_login = format_tg_login(message.from_user.id, username_raw)

    lead_payload = {
        "type": "Подключение",
        "name": data["name"],
        "city": data["city"],
        "transport": data["transport"],
        "phone": norm_phone,
        "tg_username": tg_login,
        "tg_id": message.from_user.id,
    }
    save_application(lead_payload)

    try:
        await send_lead_to_amocrm(lead_payload, sts_front=sts_front, sts_back=sts_back)
    except Exception as exc:
        logger.warning("Ошибка отправки заявки в amoCRM: %s", exc)

    await send_ephemeral(
        message.chat.id,
        "Ваш профиль в процессе — в течение 10 минут создадим и свяжемся.",
        reply_markup=start_menu(message.from_user.id),
    )


# ================= ЗАПУСК =================
async def main():
    await bot.delete_webhook(drop_pending_updates=False)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
