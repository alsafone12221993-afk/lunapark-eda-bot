import asyncio
import json
import logging
import os
import re
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Optional

import aiohttp
from aiohttp import web

# ================= КОНФИГУРАЦИЯ (ЧЕРЕЗ ENV ИЛИ ЗНАЧЕНИЯ ПО УМОЛЧАНИЮ) =================
WAZZUP_API_KEY = os.getenv("WAZZUP_API_KEY", "60b72896d7c6490595a9f9f213a9b8ed")
WAZZUP_CHANNEL_ID = os.getenv("WAZZUP_CHANNEL_ID", "1213b290-ce0e-4ff8-abd2-31dcd5956aee")
PUBLIC_WEBHOOK_URL = os.getenv(
    "PUBLIC_WEBHOOK_URL",
    "https://bot-1795419700-8826-lunapark11111.bothost.tech/webhook/wazzup",
)
PORT = int(os.getenv("PORT", "8080"))

WAZZUP_API_BASE = "https://api.wazzup24.com/v3"

# --- НАСТРОЙКИ amoCRM ---
AMOCRM_BASE_URL = os.getenv("AMOCRM_BASE_URL", "https://lunapark7042.amocrm.ru")
AMOCRM_ACCESS_TOKEN = os.getenv(
    "AMOCRM_ACCESS_TOKEN",
    "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6ImQ3NDM4MThmM2IxMzMxYTE2ZDUwZDRhMDA5ZWI3ZmQ1OTI3YTQ5YTljMDE4OWMwZGY5NjY2YTM2OTU4YjdiYjA1ZjhkNmM2MjkyZDY3MjM2In0.eyJhdWQiOiI3NjNkNmJjNC1mMGVmLTQwY2EtYjg3Mi01MmMzMmY5MWM5MTQiLCJqdGkiOiJkNzQzODE4ZjNiMTMzMWExNmQ1MGQ0YTAwOWViN2ZkNTkyN2E0OWE5YzAxODljMGRmOTY2NmEzNjk1OGI3YmIwNWY4ZDZjNjI5MmQ2NzIzNiIsImlhdCI6MTc5NTMzMDQyNiwibmJmIjoxNzk1MzMwNDI2LCJleHAiOjE5NDc2Mjg4MDAsInN1YiI6IjEyNjQ1NDY2IiwiZ3JhbnRfdHlwZSI6IiIsImFjY291bnRfaWQiOjMxMjkwOTc0LCJiYXNlX2RvbWFpbiI6ImFtb2NybS5ydSIsInZlcnNpb24iOjIsInNjb3BlcyI6WyJwdXNoX25vdGlmaWNhdGlvbnMiLCJmaWxlcyIsImNybSIsImZpbGVzX2RlbGV0ZSIsIm5vdGlmaWNhdGlvbnMiXSwiaGFzaF91dWlkIjoiMDFjMWE3MWEtZWI5Yy00MjJjLTg4ODEtMGQ2Mjg3ZDA2ODA2IiwiYXBpX2RvbWFpbiI6ImFwaS1iLmFtb2NybS5ydSJ9.TyjWbPrNmzDY_yUNQwEFO-8kWSW0MNHyoF_Xd5Vir0TRTfhpLGcpUOAKKPRvLvpESIO1wJ-lUXMcmawGdiLFbKLTqQv3djdWUc2vypbZZVsHKXT2_ZHhaLyxYHNtWB-HJ1Z_F8TB404KqnjlQ2n6opAlu5HMdHpamquMn3AJUcZAqMTknQ8_mlaiMQRUvhD1Dvn6XRtJ9SETH3a78QUQbpj__qc-HOT_b91eLHgjUIV6uLVqRyMFioPjrpY0jpZnAk4CLRoClNZPqml5qbcSnFfJuLBD763O5JoprazMnIWIR_r80Nc7DpLLZk3Eb_yXKTiRqj67lskDtvoM6q19zQ",
)
AMOCRM_PIPELINE_ID = int(os.getenv("AMOCRM_PIPELINE_ID", "10053610"))
AMOCRM_STATUS_ID = int(os.getenv("AMOCRM_STATUS_ID", "79731826"))
AMOCRM_CLOSED_LOST_ID = int(os.getenv("AMOCRM_CLOSED_LOST_ID", "143"))
AMOCRM_TELEGRAM_FIELD_ID: int | None = None

USERS_FILE = "users_db.json"
APPS_FILE = "applications.json"
CLIENT_LEADS_FILE = "client_leads.json"
STATES_FILE = "user_states.json"
DELAYED_TASKS_FILE = "delayed_notifications.json"

PROFILE_READY_DELAY_SEC = 600  # 10 минут

_DETECTED_TG_FIELD_CACHE: dict[str, int | None] = {}
_PROCESSED_MSG_IDS: dict[str, float] = {}
_FILE_LOCKS: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

STS_TRANSPORT_KEYWORDS = ("авто", "мото", "груз")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("lunapark_wazzup_bot")

MSG_AFTER_SUBMIT = (
    "Заявка принята! Профиль создадим в течение 10 минут.\n\n"
    "Пока можно сразу скачать Яндекс Про:\n"
    "— iOS: https://apps.apple.com/ru/app/%D1%8F%D0%BD%D0%B4%D0%B5%D0%BA%D1%81-%D0%BF%D1%80%D0%BE-%D0%B2%D0%BE%D0%B4%D0%B8%D1%82%D0%B5%D0%BB%D0%B8-%D0%B8-%D0%BA%D1%83%D1%80%D1%8C%D0%B5%D1%80%D1%8B/id1496904594\n"
    "— Android: https://play.google.com/store/apps/details?id=ru.yandex.taximeter&hl=ru"
)

MSG_PROFILE_CREATED_10M = (
    "Профиль создан!\n\n"
    "Как начать:\n"
    "1. Откройте Яндекс Про и войдите по своему номеру телефона.\n"
    "2. Выберите сервис Еда и парк ЛунапаркЕда.\n"
    "3. Пройдите фотоконтроль и обучение, затем выходите на слот.\n\n"
    "Условия работы в парке ЛунапаркЕда:\n"
    "— Комиссия парка: 3%\n"
    "— Бонус от парка: +500 ₽ за первые 5 заказов в течение 14 дней\n"
    "— Бонус для новых курьеров Яндекс Еды: +13 000 ₽ за 100 заказов в первые 20 дней с момента регистрации\n\n"
    "Как выводить деньги:\n"
    "Вывод моментальный через приложение JumpTaxi (от 100 ₽):\n"
    "— На карты Т-Банка от 2 000 ₽ — без комиссии (0%)\n"
    "— На остальные карты — комиссия 1% (минимум 30 ₽)\n\n"
    "Подробная инструкция по настройке и выводу средств: https://t.me/LUNAPARK_study/52"
)


def normalize_choice_text(raw: str) -> str:
    cleaned = re.sub(r"\s+", " ", (raw or "").strip().lower())
    cleaned = re.sub(r"^[^\wа-яёa-z0-9]+", "", cleaned).strip()
    return cleaned


def parse_transport_choice(raw: str) -> str | None:
    norm = normalize_choice_text(raw)
    if not norm:
        return None
    mapping = {
        "пеший / вело": "🚶 Пеший / Вело",
        "пеший": "🚶 Пеший / Вело",
        "вело": "🚶 Пеший / Вело",
        "велосипед": "🚶 Пеший / Вело",
        "1": "🚶 Пеший / Вело",
        "мото": "🛵 Мото",
        "мотоцикл": "🛵 Мото",
        "скутер": "🛵 Мото",
        "2": "🛵 Мото",
        "авто": "🚗 Авто",
        "автомобиль": "🚗 Авто",
        "легковой": "🚗 Авто",
        "машина": "🚗 Авто",
        "3": "🚗 Авто",
        "грузовой": "🚚 Грузовой",
        "груз": "🚚 Грузовой",
        "грузовик": "🚚 Грузовой",
        "4": "🚚 Грузовой",
    }
    if norm in mapping:
        return mapping[norm]
    if "пеш" in norm or "вел" in norm:
        return "🚶 Пеший / Вело"
    if "мот" in norm or "скут" in norm:
        return "🛵 Мото"
    if "груз" in norm:
        return "🚚 Грузовой"
    if "авто" in norm or "маш" in norm or "легк" in norm:
        return "🚗 Авто"
    return None


def requires_two_sided_sts(transport_text: str) -> bool:
    low = (transport_text or "").lower()
    return any(k in low for k in STS_TRANSPORT_KEYWORDS)


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
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    tmp_path = f"{path}.{os.getpid()}.{uuid.uuid4().hex[:8]}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


def normalize_phone(raw_phone: str) -> str:
    digits = re.sub(r"\D", "", raw_phone or "")
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    elif len(digits) == 10:
        digits = "7" + digits
    return "+" + digits if digits else (raw_phone or "").strip()


def format_tg_login(chat_id: str, tg_username: str | None) -> str:
    if tg_username and tg_username != "нет юзернейма":
        clean = tg_username.lstrip("@").strip()
        if clean:
            return f"@{clean}"
    return f"id{chat_id}"


def format_tg_link(chat_id: str, tg_username: str | None) -> str:
    if tg_username and tg_username != "нет юзернейма":
        clean = tg_username.lstrip("@").strip()
        if clean:
            return f"https://t.me/{clean}"
    return f"tg://user?id={chat_id}"


def get_user_state(chat_id: str) -> dict[str, Any]:
    states = _read_json(STATES_FILE, {})
    raw = states.get(str(chat_id))
    if not isinstance(raw, dict):
        return {"step": None, "data": {}}
    data = raw.get("data")
    return {"step": raw.get("step"), "data": data if isinstance(data, dict) else {}}


def set_user_state(chat_id: str, step: str | None, data: dict[str, Any] | None = None) -> None:
    states = _read_json(STATES_FILE, {})
    cur = states.get(str(chat_id))
    if not isinstance(cur, dict):
        cur = {"step": None, "data": {}}
    cur["step"] = step
    if data is not None:
        cur["data"] = data
    elif not isinstance(cur.get("data"), dict):
        cur["data"] = {}
    states[str(chat_id)] = cur
    _write_json(STATES_FILE, states)


def update_user_state_data(chat_id: str, **kwargs: Any) -> dict[str, Any]:
    states = _read_json(STATES_FILE, {})
    cur = states.get(str(chat_id))
    if not isinstance(cur, dict):
        cur = {"step": None, "data": {}}
    cur_data = cur.get("data") if isinstance(cur.get("data"), dict) else {}
    cur_data.update(kwargs)
    cur["data"] = cur_data
    states[str(chat_id)] = cur
    _write_json(STATES_FILE, states)
    return cur_data


def clear_user_state(chat_id: str) -> None:
    set_user_state(chat_id, None, {})


def save_user_session(
    chat_id: str,
    phone: str = "",
    city: str = "",
    username: str = "",
    full_name: str = "",
) -> dict[str, Any]:
    db = _read_json(USERS_FILE, {})
    key = str(chat_id)
    entry = db.get(key)
    if not isinstance(entry, dict):
        entry = {}
    if not entry.get("registered_at"):
        entry["registered_at"] = datetime.now(timezone.utc).isoformat()
    if phone:
        entry["phone"] = normalize_phone(phone)
    if city:
        entry["city"] = city
    if username:
        entry["username"] = username.lstrip("@").strip()
    if full_name:
        entry["full_name"] = full_name.strip()
    db[key] = entry
    _write_json(USERS_FILE, db)
    return entry


def get_user_record(chat_id: str) -> dict[str, Any]:
    db = _read_json(USERS_FILE, {})
    rec = db.get(str(chat_id))
    return rec if isinstance(rec, dict) else {}


def save_application(lead_payload: dict[str, Any]) -> None:
    apps = _read_json(APPS_FILE, [])
    if not isinstance(apps, list):
        apps = []
    record = dict(lead_payload)
    record["created_at"] = datetime.now().strftime("%d.%m.%Y %H:%M")
    record["status"] = "Принята в работу"
    apps.append(record)
    _write_json(APPS_FILE, apps)


def load_client_leads() -> dict[str, int]:
    data = _read_json(CLIENT_LEADS_FILE, {})
    if isinstance(data, dict):
        return {str(k): int(v) for k, v in data.items() if str(v).isdigit()}
    return {}


def save_client_lead(chat_id: str, lead_id: int) -> None:
    data = load_client_leads()
    data[str(chat_id)] = int(lead_id)
    _write_json(CLIENT_LEADS_FILE, data)


# ================= ОТЛОЖЕННОЕ СООБЩЕНИЕ ЧЕРЕЗ 10 МИНУТ ПОСЛЕ ЗАЯВКИ =================
def schedule_delayed_profile_msg(
    chat_id: str,
    channel_id: str,
    delay_sec: int = PROFILE_READY_DELAY_SEC,
) -> None:
    tasks = _read_json(DELAYED_TASKS_FILE, [])
    if not isinstance(tasks, list):
        tasks = []
    tasks.append({
        "id": str(uuid.uuid4()),
        "chat_id": str(chat_id),
        "channel_id": str(channel_id),
        "send_at": time.time() + delay_sec,
        "text": MSG_PROFILE_CREATED_10M,
    })
    _write_json(DELAYED_TASKS_FILE, tasks)


async def delayed_notifications_worker(app: web.Application) -> None:
    logger.info("Запущен фоновый воркер автосообщений через 10 мин после заявки")
    while True:
        try:
            async with _FILE_LOCKS["delayed_tasks"]:
                tasks = _read_json(DELAYED_TASKS_FILE, [])
                if isinstance(tasks, list) and tasks:
                    now = time.time()
                    remaining = []
                    changed = False
                    for task in tasks:
                        if not isinstance(task, dict):
                            changed = True
                            continue
                        if now >= float(task.get("send_at", 0)):
                            cid = str(task.get("chat_id") or "")
                            ch_id = str(task.get("channel_id") or WAZZUP_CHANNEL_ID)
                            txt = str(task.get("text") or MSG_PROFILE_CREATED_10M)
                            ok = await wazzup_send_text(
                                chat_id=cid,
                                text=txt,
                                channel_id=ch_id,
                                clear_unanswered=False,
                            )
                            if ok:
                                changed = True
                            else:
                                task["send_at"] = now + 30
                                remaining.append(task)
                                changed = True
                        else:
                            remaining.append(task)
                    if changed:
                        _write_json(DELAYED_TASKS_FILE, remaining)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.exception("Ошибка в воркере отложенных уведомлений: %s", exc)
        await asyncio.sleep(5)


# ================= WAZZUP API =================
def _wazzup_headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {WAZZUP_API_KEY.strip()}",
        "Content-Type": "application/json",
    }


async def wazzup_send_text(
    chat_id: str,
    text: str,
    channel_id: str | None = None,
    clear_unanswered: bool = False,
) -> bool:
    ch = (channel_id or WAZZUP_CHANNEL_ID).strip()
    if not WAZZUP_API_KEY.strip() or not ch:
        logger.error("Не заполнены WAZZUP_API_KEY или WAZZUP_CHANNEL_ID")
        return False

    payload: dict[str, Any] = {
        "channelId": ch,
        "chatType": "telegram",
        "chatId": str(chat_id),
        "text": text,
        "crmMessageId": str(uuid.uuid4()),
        "clearUnanswered": clear_unanswered,
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{WAZZUP_API_BASE}/message",
                headers=_wazzup_headers(),
                json=payload,
                timeout=20,
            ) as resp:
                resp_text = await resp.text()
                if resp.status in (200, 201):
                    logger.info("Wazzup POST /v3/message (%s) -> chatId=%s", resp.status, chat_id)
                    return True
                logger.error("Ошибка Wazzup POST /v3/message (%s): %s", resp.status, resp_text[:300])
                return False
    except Exception as exc:
        logger.exception("Исключение при отправке в Wazzup: %s", exc)
        return False


async def register_wazzup_webhooks() -> None:
    if not PUBLIC_WEBHOOK_URL.strip() or not WAZZUP_API_KEY.strip():
        return
    payload = {
        "webhooksUri": PUBLIC_WEBHOOK_URL.strip(),
        "subscriptions": {
            "messagesAndStatuses": True,
            "contactsAndDealsCreation": True,
        },
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.patch(
                f"{WAZZUP_API_BASE}/webhooks",
                headers=_wazzup_headers(),
                json=payload,
                timeout=20,
            ) as resp:
                txt = await resp.text()
                logger.info("Регистрация вебхуков Wazzup (%s): %s", resp.status, txt[:300])
    except Exception as exc:
        logger.warning("Не удалось зарегистрировать вебхуки Wazzup: %s", exc)


async def download_file_from_url(url: str, default_name: str) -> Optional[tuple[bytes, str]]:
    if not url:
        return None
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=25) as resp:
                if resp.status != 200:
                    return None
                raw = await resp.read()
                if not raw:
                    return None
                fname = default_name
                cd = resp.headers.get("Content-Disposition", "")
                m = re.search(r'filename="?([^";]+)"?', cd)
                if m:
                    fname = m.group(1)
                return (raw, fname)
    except Exception as exc:
        logger.warning("Ошибка скачивания файла %s: %s", url, exc)
        return None


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
                if fname in (
                    "telegram",
                    "телеграм",
                    "тг",
                    "tg",
                    "аккаунт telegram",
                    "telegram аккаунт",
                    "логин telegram",
                ):
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
    chat_id: str,
    phone: str = "",
) -> int | None:
    stored = load_client_leads()
    saved_lead_id = stored.get(str(chat_id))
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

    queries = [q for q in [normalize_phone(phone) if phone else "", str(chat_id)] if q]
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
                                save_client_lead(chat_id, int(lid))
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

    chat_id = str(lead["tg_id"])
    async with _FILE_LOCKS[f"lead_{chat_id}"]:
        tg_username = lead.get("tg_username", "")
        tg_login = format_tg_login(chat_id, tg_username)
        tg_link = format_tg_link(chat_id, tg_username)
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
            f"Telegram ID: {chat_id}",
        ]

        try:
            async with aiohttp.ClientSession() as session:
                contact_tg_fid = await _detect_tg_custom_field_id(session, base_url, "contacts")
                lead_tg_fid = await _detect_tg_custom_field_id(session, base_url, "leads")

                existing_lead_id = await _find_existing_client_lead(session, base_url, chat_id, phone)
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
                    {"field_code": "PHONE", "values": [{"value": phone, "enum_code": "MOB"}]}
                ]
                if contact_tg_fid:
                    contact_fields.append({"field_id": contact_tg_fid, "values": [{"value": tg_login}]})

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
                                {"first_name": lead["name"], "custom_fields_values": contact_fields}
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
                    save_client_lead(chat_id, int(lead_id))
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
    chat_id: str,
    tg_username: str | None,
    full_name: str,
) -> tuple[bool, str]:
    base_url = AMOCRM_BASE_URL.strip().rstrip("/")
    token = AMOCRM_ACCESS_TOKEN.strip()
    if not base_url or not token:
        return False, "не заполнены AMOCRM_BASE_URL / AMOCRM_ACCESS_TOKEN"

    async with _FILE_LOCKS[f"lead_{chat_id}"]:
        tg_login = format_tg_login(chat_id, tg_username)
        tg_link = format_tg_link(chat_id, tg_username)
        safe_name = (full_name or "Пользователь").strip()
        user_rec = get_user_record(chat_id)
        phone = user_rec.get("phone", "")

        note_text = (
            "Парк: ЛунапаркЕда\n"
            "Обращение по запросу «Нужна помощь»\n"
            f"Имя: {safe_name}\n"
            f"Telegram: {tg_login}\n"
            f"Ссылка на чат: {tg_link}\n"
            f"Telegram ID: {chat_id}\n"
            f"Текст проблемы:\n{problem_text.strip()}"
        )

        try:
            async with aiohttp.ClientSession() as session:
                contact_tg_fid = await _detect_tg_custom_field_id(session, base_url, "contacts")
                lead_tg_fid = await _detect_tg_custom_field_id(session, base_url, "leads")

                existing_lead_id = await _find_existing_client_lead(session, base_url, chat_id, phone)
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
                    contact_fields.append({"field_code": "PHONE", "values": [{"value": phone, "enum_code": "MOB"}]})
                if contact_tg_fid:
                    contact_fields.append({"field_id": contact_tg_fid, "values": [{"value": tg_login}]})

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

                payload = [{**lead_obj, "_embedded": {"contacts": [contact_payload]}}]

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
                    save_client_lead(chat_id, int(lead_id))
                    await _add_amocrm_note(session, base_url, int(lead_id), note_text)

            return True, f"сделка #{lead_id or 'создана'}"
        except Exception as exc:
            logger.exception("Ошибка отправки обращения в amoCRM")
            return False, str(exc)


# ================= СЦЕНАРИЙ ЗАЯВКИ НА ПОДКЛЮЧЕНИЕ + 10 МИН =================
async def show_confirmation_screen(chat_id: str, channel_id: str, data: dict[str, Any]) -> None:
    set_user_state(chat_id, "reg_confirm", data)
    sts_line = ""
    if requires_two_sided_sts(data.get("transport", "")):
        has_front = bool(data.get("sts_front_url"))
        has_back = bool(data.get("sts_back_url"))
        sts_line = f"\nФото СТС: {'загружены с двух сторон' if (has_front and has_back) else 'не загружены'}\n"

    text = (
        "Проверьте данные заявки на подключение:\n\n"
        f"Имя: {data.get('name', '—')}\n"
        f"Город: {data.get('city', '—')}\n"
        f"Формат: {data.get('transport', '—')}\n"
        f"Телефон: {data.get('phone', '—')}"
        f"{sts_line}\n"
        "Напишите «Да», чтобы отправить заявку, или «Отмена»."
    )
    await wazzup_send_text(chat_id, text, channel_id=channel_id)


async def finalize_registration(
    chat_id: str,
    channel_id: str,
    data: dict[str, Any],
    tg_username: str,
    contact_name: str,
) -> None:
    clear_user_state(chat_id)
    norm_phone = normalize_phone(data.get("phone", ""))
    save_user_session(
        chat_id,
        phone=norm_phone,
        city=data.get("city", ""),
        username=tg_username,
        full_name=data.get("name") or contact_name,
    )

    tg_login = format_tg_login(chat_id, tg_username)
    lead_payload = {
        "type": "Подключение",
        "name": data.get("name") or contact_name,
        "city": data.get("city", "—"),
        "transport": data.get("transport", "—"),
        "phone": norm_phone,
        "tg_username": tg_login,
        "tg_id": chat_id,
    }
    save_application(lead_payload)

    sts_front = None
    sts_back = None
    if data.get("sts_front_url"):
        sts_front = await download_file_from_url(data["sts_front_url"], f"sts_front_{chat_id}.jpg")
    if data.get("sts_back_url"):
        sts_back = await download_file_from_url(data["sts_back_url"], f"sts_back_{chat_id}.jpg")

    try:
        await send_lead_to_amocrm(lead_payload, sts_front=sts_front, sts_back=sts_back)
    except Exception as exc:
        logger.warning("Ошибка отправки заявки в amoCRM: %s", exc)

    await wazzup_send_text(
        chat_id,
        MSG_AFTER_SUBMIT,
        channel_id=channel_id,
        clear_unanswered=False,
    )
    schedule_delayed_profile_msg(chat_id, channel_id, delay_sec=PROFILE_READY_DELAY_SEC)


def _extract_incoming_fields(msg: dict[str, Any]) -> tuple[str, str, str, str, str, str, str]:
    chat_id = str(msg.get("chatId") or msg.get("chat_id") or "").strip()
    channel_id = str(msg.get("channelId") or msg.get("channel_id") or WAZZUP_CHANNEL_ID).strip()

    raw_text = msg.get("text")
    if raw_text is None and isinstance(msg.get("message"), dict):
        raw_text = msg["message"].get("text")
    text = str(raw_text or "").strip()

    content_uri = str(msg.get("contentUri") or msg.get("fileUri") or msg.get("url") or "").strip()

    contact_info = msg.get("contact") if isinstance(msg.get("contact"), dict) else {}
    author_info = msg.get("author") if isinstance(msg.get("author"), dict) else {}

    tg_username = str(
        contact_info.get("username")
        or author_info.get("username")
        or msg.get("username")
        or ""
    ).strip()

    contact_name = str(
        contact_info.get("name")
        or author_info.get("name")
        or msg.get("senderName")
        or "Пользователь"
    ).strip()

    contact_phone = str(
        contact_info.get("phone")
        or author_info.get("phone")
        or msg.get("phone")
        or ""
    ).strip()

    return chat_id, channel_id, text, content_uri, tg_username, contact_name, contact_phone


async def handle_incoming_wazzup_message(msg: dict[str, Any]) -> None:
    """
    Wazzup отрабатывает ТОЛЬКО в двух случаях:
    1) клиент запросил помощь («нужна помощь» / «помощь» / «поддержка» / «оператор»);
    2) клиент оставляет / подтверждает заявку на подключение (после чего уходит автосообщение через 10 мин).
    Во всех остальных случаях Wazzup ничего не отправляет.
    """
    if not isinstance(msg, dict):
        return
    if msg.get("isEcho") is True:
        return

    chat_type = str(msg.get("chatType") or msg.get("channelType") or "").lower().strip()
    if chat_type and chat_type not in ("telegram", "tg"):
        return

    msg_id = str(msg.get("messageId") or msg.get("id") or "").strip()
    now_ts = time.monotonic()
    if msg_id:
        expired = [k for k, ts in _PROCESSED_MSG_IDS.items() if now_ts - ts > 300]
        for k in expired:
            _PROCESSED_MSG_IDS.pop(k, None)
        if msg_id in _PROCESSED_MSG_IDS:
            return
        _PROCESSED_MSG_IDS[msg_id] = now_ts

    chat_id, channel_id, text, content_uri, tg_username, contact_name, contact_phone = _extract_incoming_fields(msg)
    if not chat_id:
        return

    norm_cmd = normalize_choice_text(text)
    state = get_user_state(chat_id)
    step = state.get("step")
    data = state.get("data") or {}

    if norm_cmd in ("отмена", "отменить", "cancel") and step is not None:
        clear_user_state(chat_id)
        await wazzup_send_text(chat_id, "Действие отменено.", channel_id=channel_id)
        return

    # 1. СЛУЧАЙ «НУЖНА ПОМОЩЬ»
    if norm_cmd in ("нужна помощь", "помощь", "поддержка", "оператор"):
        logger.info("Wazzup [помощь]: старт для chatId=%s", chat_id)
        set_user_state(chat_id, "help_waiting_problem", {})
        await wazzup_send_text(
            chat_id,
            "Добрый день! Опишите Вашу проблему",
            channel_id=channel_id,
        )
        return

    if step == "help_waiting_problem":
        if not text and not content_uri:
            await wazzup_send_text(
                chat_id,
                "Напишите текстом, что случилось — мы передадим обращение специалисту.",
                channel_id=channel_id,
            )
            return
        clear_user_state(chat_id)
        problem_body = text or f"[Вложение: {content_uri}]"
        try:
            await send_help_to_amocrm(
                problem_text=problem_body,
                chat_id=chat_id,
                tg_username=tg_username,
                full_name=contact_name,
            )
        except Exception as exc:
            logger.warning("Ошибка отправки обращения в amoCRM: %s", exc)

        await wazzup_send_text(
            chat_id,
            "Получили ваше сообщение, скоро ответим.",
            channel_id=channel_id,
            clear_unanswered=False,
        )
        return

    # 2. СЛУЧАЙ «ЗАЯВКА НА ПОДКЛЮЧЕНИЕ» (с автосообщением через 10 минут)
    if step is None and norm_cmd in ("подключиться к парку", "подключиться", "заявка на подключение", "стать курьером"):
        logger.info("Wazzup [подключение]: старт заявки для chatId=%s", chat_id)
        set_user_state(chat_id, "reg_transport", {})
        await wazzup_send_text(
            chat_id,
            "Как планируете доставлять заказы?\n"
            "1. Пеший / Вело\n"
            "2. Мото\n"
            "3. Авто\n"
            "4. Грузовой",
            channel_id=channel_id,
        )
        return

    if step == "reg_transport":
        chosen_transport = parse_transport_choice(text)
        if not chosen_transport:
            await wazzup_send_text(
                chat_id,
                "Напишите цифру или формат: 1 — Пеший / Вело, 2 — Мото, 3 — Авто, 4 — Грузовой:",
                channel_id=channel_id,
            )
            return
        data = update_user_state_data(chat_id, transport=chosen_transport)
        set_user_state(chat_id, "reg_city", data)
        await wazzup_send_text(chat_id, "В каком городе планируете работать?", channel_id=channel_id)
        return

    if step == "reg_city":
        if len(text) < 2:
            await wazzup_send_text(chat_id, "Напишите название города:", channel_id=channel_id)
            return
        data = update_user_state_data(chat_id, city=text)
        set_user_state(chat_id, "reg_name", data)
        await wazzup_send_text(chat_id, "Напишите ваши Фамилию и Имя:", channel_id=channel_id)
        return

    if step == "reg_name":
        if len(text) < 2:
            await wazzup_send_text(chat_id, "Напишите ваши Фамилию и Имя:", channel_id=channel_id)
            return
        data = update_user_state_data(chat_id, name=text)
        set_user_state(chat_id, "reg_phone", data)
        await wazzup_send_text(chat_id, "Введите ваш номер телефона (например, +7 999 123-45-67):", channel_id=channel_id)
        return

    if step == "reg_phone":
        raw_phone = contact_phone or text
        digits = re.sub(r"\D", "", raw_phone)
        if len(digits) < 10:
            await wazzup_send_text(chat_id, "Введите номер телефона в формате +7 999 123-45-67:", channel_id=channel_id)
            return
        data = update_user_state_data(chat_id, phone=normalize_phone(raw_phone))
        if requires_two_sided_sts(data.get("transport", "")):
            set_user_state(chat_id, "reg_sts_front", data)
            await wazzup_send_text(chat_id, "Пришлите фото СТС — лицевую сторону (1 из 2):", channel_id=channel_id)
            return
        await show_confirmation_screen(chat_id, channel_id, data)
        return

    if step == "reg_sts_front":
        if not content_uri:
            await wazzup_send_text(chat_id, "Пришлите фото лицевой стороны СТС (1 из 2):", channel_id=channel_id)
            return
        data = update_user_state_data(chat_id, sts_front_url=content_uri)
        set_user_state(chat_id, "reg_sts_back", data)
        await wazzup_send_text(chat_id, "Пришлите фото обратной стороны СТС (2 из 2):", channel_id=channel_id)
        return

    if step == "reg_sts_back":
        if not content_uri:
            await wazzup_send_text(chat_id, "Пришлите фото обратной стороны СТС (2 из 2):", channel_id=channel_id)
            return
        data = update_user_state_data(chat_id, sts_back_url=content_uri)
        await show_confirmation_screen(chat_id, channel_id, data)
        return

    if step == "reg_confirm":
        if norm_cmd in ("да", "1", "верно", "подтверждаю", "ок", "yes"):
            await finalize_registration(chat_id, channel_id, data, tg_username, contact_name)
            return
        await wazzup_send_text(
            chat_id,
            "Напишите «Да», чтобы отправить заявку, или «Отмена».",
            channel_id=channel_id,
        )
        return

    # Во всех остальных случаях Wazzup не отрабатывает
    return


async def handle_health(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok", "service": "lunapark-wazzup-bot"})


async def handle_wazzup_webhook(request: web.Request) -> web.Response:
    try:
        payload = await request.json()
    except Exception:
        return web.Response(status=200, text="ok")

    if isinstance(payload, dict) and payload.get("test") is True:
        logger.info("Получен тестовый вебхук Wazzup (test: true)")
        return web.json_response({"status": "ok"})

    if isinstance(payload, dict):
        messages = payload.get("messages")
        if isinstance(messages, list):
            for msg in messages:
                if isinstance(msg, dict):
                    asyncio.create_task(handle_incoming_wazzup_message(msg))
        if isinstance(payload.get("chatId"), (str, int)) and not messages:
            asyncio.create_task(handle_incoming_wazzup_message(payload))

    return web.json_response({"status": "ok"})


async def on_startup(app: web.Application) -> None:
    app["delayed_worker"] = asyncio.create_task(delayed_notifications_worker(app))
    await register_wazzup_webhooks()


async def on_cleanup(app: web.Application) -> None:
    worker = app.get("delayed_worker")
    if worker:
        worker.cancel()
        try:
            await worker
        except asyncio.CancelledError:
            pass


def create_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/", handle_health)
    app.router.add_get("/health", handle_health)
    app.router.add_post("/webhook/wazzup", handle_wazzup_webhook)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


if __name__ == "__main__":
    web.run_app(create_app(), host="0.0.0.0", port=PORT)
