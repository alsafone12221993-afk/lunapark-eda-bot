import asyncio
import json
import logging
import os
import re
import time
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import aiohttp
from aiohttp import web

# ================= КОНФИГУРАЦИЯ =================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "9247740629:AAE96VCZow9OgTfpL6XHtLL0pJQFncawyO4").strip()
TELEGRAM_WEBHOOK_URL = os.getenv("TELEGRAM_WEBHOOK_URL", "").strip()
PORT = int(os.getenv("PORT", "8080"))

# Wazzup используется ТОЛЬКО для обращения «Помощь», чтобы менеджер мог ответить
WAZZUP_API_KEY = os.getenv("WAZZUP_API_KEY", "60b72896d7c6490595a9f9f213a9b8ed").strip()
WAZZUP_CHANNEL_ID = os.getenv("WAZZUP_CHANNEL_ID", "1213b290-ce0e-4ff8-abd2-31dcd5956aee").strip()
WAZZUP_API_BASE = "https://api.wazzup24.com/v3"

# Основной парк (Яндекс Fleet API)
PARKS_CONFIG: list[dict[str, str]] = [
    {
        "name": "ЛунапаркЕда",
        "park_id": os.getenv("YANDEX_PARK_ID", "1680a13d35084b0688ecab0f925c9d75"),
        "client_id": os.getenv("YANDEX_CLIENT_ID", "taxi/park/1680a13d35084b0688ecab0f925c9d75"),
        "api_key": os.getenv("YANDEX_API_KEY", "yapi10-E5IuB_Gpfog2E7XMXYxTl1SsS3HyVGbXduM9Jg_d1"),
    },
]

# amoCRM
AMOCRM_BASE_URL = os.getenv("AMOCRM_BASE_URL", "https://lunapark7042.amocrm.ru").strip()
AMOCRM_ACCESS_TOKEN = os.getenv(
    "AMOCRM_ACCESS_TOKEN",
    "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6ImQ3NDM4MThmM2IxMzMxYTE2ZDUwZDRhMDA5ZWI3ZmQ1OTI3YTQ5YTljMDE4OWMwZGY5NjY2YTM2OTU4YjdiYjA1ZjhkNmM2MjkyZDY3MjM2In0.eyJhdWQiOiI3NjNkNmJjNC1mMGVmLTQwY2EtYjg3Mi01MmMzMmY5MWM5MTQiLCJqdGkiOiJkNzQzODE4ZjNiMTMzMWExNmQ1MGQ0YTAwOWViN2ZkNTkyN2E0OWE5YzAxODljMGRmOTY2NmEzNjk1OGI3YmIwNWY4ZDZjNjI5MmQ2NzIzNiIsImlhdCI6MTc5NTMzMDQyNiwibmJmIjoxNzk1MzMwNDI2LCJleHAiOjE5NDc2Mjg4MDAsInN1YiI6IjEyNjQ1NDY2IiwiZ3JhbnRfdHlwZSI6IiIsImFjY291bnRfaWQiOjMxMjkwOTc0LCJiYXNlX2RvbWFpbiI6ImFtb2NybS5ydSIsInZlcnNpb24iOjIsInNjb3BlcyI6WyJwdXNoX25vdGlmaWNhdGlvbnMiLCJmaWxlcyIsImNybSIsImZpbGVzX2RlbGV0ZSIsIm5vdGlmaWNhdGlvbnMiXSwiaGFzaF91dWlkIjoiMDFjMWE3MWEtZWI5Yy00MjJjLTg4ODEtMGQ2Mjg3ZDA2ODA2IiwiYXBpX2RvbWFpbiI6ImFwaS1iLmFtb2NybS5ydSJ9.TyjWbPrNmzDY_yUNQwEFO-8kWSW0MNHyoF_Xd5Vir0TRTfhpLGcpUOAKKPRvLvpESIO1wJ-lUXMcmawGdiLFbKLTqQv3djdWUc2vypbZZVsHKXT2_ZHhaLyxYHNtWB-HJ1Z_F8TB404KqnjlQ2n6opAlu5HMdHpamquMn3AJUcZAqMTknQ8_mlaiMQRUvhD1Dvn6XRtJ9SETH3a78QUQbpj__qc-HOT_b91eLHgjUIV6uLVqRyMFioPjrpY0jpZnAk4CLRoClNZPqml5qbcSnFfJuLBD763O5JoprazMnIWIR_r80Nc7DpLLZk3Eb_yXKTiRqj67lskDtvoM6q19zQ",
).strip()
AMOCRM_PIPELINE_ID = int(os.getenv("AMOCRM_PIPELINE_ID", "10053610"))
AMOCRM_STATUS_ID = int(os.getenv("AMOCRM_STATUS_ID", "79731826"))
AMOCRM_CLOSED_LOST_ID = int(os.getenv("AMOCRM_CLOSED_LOST_ID", "143"))
AMOCRM_TELEGRAM_FIELD_ID: int | None = None

USERS_FILE = "users_db.json"
APPS_FILE = "applications.json"
DRIVERS_CACHE_FILE = "drivers_cache.json"
CLIENT_LEADS_FILE = "client_leads.json"
STATES_FILE = "user_states.json"
DELAYED_TASKS_FILE = "delayed_notifications.json"

FLEET_API_URL = "https://fleet-api.taxi.yandex.net"

GOAL_ORDERS = 5
GOAL_DAYS = 14
GOAL_BONUS = 500
PROFILE_READY_DELAY_SEC = 600

DRIVERS_CACHE: dict[str, dict[str, Any]] = {}
DRIVERS_TTL_SEC = 3600
PAGE_PAUSE_SEC = 0.5

DRIVER_STATS_CACHE: dict[str, dict[str, Any]] = {}
DRIVER_STATS_TTL_SEC = 180

PENDING_PROFILES: dict[str, list[dict[str, Any]]] = {}
_DETECTED_TG_FIELD_CACHE: dict[str, int | None] = {}
_FILE_LOCKS: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
_STATS_LOCKS: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

STS_TRANSPORT_KEYWORDS = ("авто", "мото", "груз")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("lunapark_bot")

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


# ================= КЛАВИАТУРЫ TELEGRAM (НА КАЖДОМ ЭТАПЕ ЕСТЬ «❓ Помощь») =================
def _reply_kb(rows: list[list[str]], one_time: bool = False) -> dict[str, Any]:
    return {
        "keyboard": [[{"text": btn} for btn in row] for row in rows],
        "resize_keyboard": True,
        "one_time_keyboard": one_time,
    }


def kb_start_menu() -> dict[str, Any]:
    return _reply_kb([
        ["🚀 Подключиться", "✅ Уже курьер"],
        ["❓ Помощь"],
    ])


def kb_transport_choice() -> dict[str, Any]:
    return _reply_kb([
        ["🚶 Пеший / Вело", "🛵 Мото"],
        ["🚗 Авто", "🚚 Грузовой"],
        ["❓ Помощь", "❌ Отмена"],
    ])


def kb_step_with_help() -> dict[str, Any]:
    return _reply_kb([
        ["❓ Помощь", "❌ Отмена"],
    ])


def kb_confirm_application() -> dict[str, Any]:
    return _reply_kb([
        ["Да", "Исправить"],
        ["❓ Помощь", "❌ Отмена"],
    ])


def kb_edit_field_choice(data: dict[str, Any]) -> dict[str, Any]:
    rows: list[list[str]] = [
        ["Изменить формат", "Изменить город"],
        ["Изменить имя", "Изменить телефон"],
    ]
    if requires_two_sided_sts(data.get("transport", "")):
        rows.append(["Изменить фото СТС"])
    rows.append(["Назад к проверке", "❓ Помощь"])
    rows.append(["❌ Отмена"])
    return _reply_kb(rows)


def kb_courier_cabinet() -> dict[str, Any]:
    return _reply_kb([
        ["📊 Моя статистика", "🤔 Я на линии?"],
        ["🎁 Цель", "❓ Помощь"],
        ["🔄 Сменить номер", "⬅️ В начало"],
    ])


def kb_profiles_choice(profiles: list[dict[str, Any]]) -> dict[str, Any]:
    rows: list[list[str]] = []
    for idx, prof in enumerate(profiles[:10]):
        rows.append([f"{idx + 1}. {prof['park_name']}"])
    rows.append(["Хочу ввести другой номер", "❓ Помощь"])
    rows.append(["❌ Отмена"])
    return _reply_kb(rows)


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


def format_tg_login(chat_id: int | str, tg_username: str | None) -> str:
    if tg_username and tg_username != "нет юзернейма":
        clean = tg_username.lstrip("@").strip()
        if clean:
            return f"@{clean}"
    return f"id{chat_id}"


def format_tg_link(chat_id: int | str, tg_username: str | None) -> str:
    if tg_username and tg_username != "нет юзернейма":
        clean = tg_username.lstrip("@").strip()
        if clean:
            return f"https://t.me/{clean}"
    return f"tg://user?id={chat_id}"


def requires_two_sided_sts(transport_text: str) -> bool:
    low = (transport_text or "").lower()
    return any(k in low for k in STS_TRANSPORT_KEYWORDS)


def get_user_state(chat_id: int | str) -> dict[str, Any]:
    states = _read_json(STATES_FILE, {})
    raw = states.get(str(chat_id))
    if not isinstance(raw, dict):
        return {"step": None, "data": {}}
    data = raw.get("data")
    return {"step": raw.get("step"), "data": data if isinstance(data, dict) else {}}


def set_user_state(chat_id: int | str, step: str | None, data: dict[str, Any] | None = None) -> None:
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


def update_user_state_data(chat_id: int | str, **kwargs: Any) -> dict[str, Any]:
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


def clear_user_state(chat_id: int | str) -> None:
    set_user_state(chat_id, None, {})


def save_user_session(
    chat_id: int | str,
    phone: str = "",
    park_id: str = "",
    driver_id: str = "",
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
    if park_id:
        entry["park_id"] = park_id
    if driver_id:
        entry["driver_id"] = driver_id
    if city:
        entry["city"] = city
    if username:
        entry["username"] = username.lstrip("@").strip()
    if full_name:
        entry["full_name"] = full_name.strip()
    db[key] = entry
    _write_json(USERS_FILE, db)
    return entry


def get_user_record(chat_id: int | str) -> dict[str, Any]:
    db = _read_json(USERS_FILE, {})
    rec = db.get(str(chat_id))
    return rec if isinstance(rec, dict) else {}


def get_registered_dt(chat_id: int | str) -> datetime:
    rec = get_user_record(chat_id)
    raw = rec.get("registered_at")
    if raw:
        try:
            dt = datetime.fromisoformat(str(raw))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            pass
    entry = save_user_session(chat_id)
    return datetime.fromisoformat(entry["registered_at"])


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


def save_client_lead(chat_id: int | str, lead_id: int) -> None:
    data = load_client_leads()
    data[str(chat_id)] = int(lead_id)
    _write_json(CLIENT_LEADS_FILE, data)


# ================= TELEGRAM BOT API =================
async def tg_api(session: aiohttp.ClientSession, method: str, payload: dict[str, Any]) -> dict[str, Any]:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
    async with session.post(url, json=payload, timeout=25) as resp:
        data = await resp.json()
        if not data.get("ok"):
            logger.warning("Telegram API %s вернул ok=False: %s", method, data)
        return data


async def tg_send_text(
    chat_id: int | str,
    text: str,
    reply_markup: dict[str, Any] | None = None,
) -> bool:
    if not TELEGRAM_BOT_TOKEN:
        logger.error("Не задан TELEGRAM_BOT_TOKEN")
        return False
    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "text": text,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        async with aiohttp.ClientSession() as session:
            res = await tg_api(session, "sendMessage", payload)
            return bool(res.get("ok"))
    except Exception as exc:
        logger.exception("Ошибка отправки сообщения в Telegram: %s", exc)
        return False


async def tg_download_file(file_id: str, default_name: str) -> Optional[tuple[bytes, str]]:
    if not TELEGRAM_BOT_TOKEN or not file_id:
        return None
    try:
        async with aiohttp.ClientSession() as session:
            info = await tg_api(session, "getFile", {"file_id": file_id})
            file_path = (info.get("result") or {}).get("file_path")
            if not file_path:
                return None
            dl_url = f"https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{file_path}"
            async with session.get(dl_url, timeout=25) as resp:
                if resp.status != 200:
                    return None
                raw = await resp.read()
                fname = os.path.basename(file_path) or default_name
                return (raw, fname)
    except Exception as exc:
        logger.warning("Ошибка скачивания файла Telegram %s: %s", file_id, exc)
        return None


# ================= ОТЛОЖЕННОЕ СООБЩЕНИЕ ЧЕРЕЗ 10 МИНУТ ПОСЛЕ ЗАЯВКИ =================
def schedule_delayed_profile_msg(chat_id: int | str, delay_sec: int = PROFILE_READY_DELAY_SEC) -> None:
    tasks = _read_json(DELAYED_TASKS_FILE, [])
    if not isinstance(tasks, list):
        tasks = []
    tasks.append({
        "id": str(uuid.uuid4()),
        "chat_id": str(chat_id),
        "send_at": time.time() + delay_sec,
        "text": MSG_PROFILE_CREATED_10M,
    })
    _write_json(DELAYED_TASKS_FILE, tasks)


async def delayed_notifications_worker() -> None:
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
                            txt = str(task.get("text") or MSG_PROFILE_CREATED_10M)
                            ok = await tg_send_text(cid, txt, reply_markup=kb_start_menu())
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


# ================= WAZZUP (ТОЛЬКО ДЛЯ «ПОМОЩИ», ЧТОБЫ МОЖНО БЫЛО ОТВЕТИТЬ) =================
async def send_help_via_wazzup(chat_id: int | str, text: str) -> bool:
    if not WAZZUP_API_KEY or not WAZZUP_CHANNEL_ID:
        return False
    payload = {
        "channelId": WAZZUP_CHANNEL_ID,
        "chatType": "telegram",
        "chatId": str(chat_id),
        "text": text,
        "crmMessageId": str(uuid.uuid4()),
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{WAZZUP_API_BASE}/message",
                headers={
                    "Authorization": f"Bearer {WAZZUP_API_KEY}",
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=20,
            ) as resp:
                resp_text = await resp.text()
                if resp.status in (200, 201):
                    logger.info("Wazzup [Помощь] отправлено (%s) -> chatId=%s", resp.status, chat_id)
                    return True
                logger.warning("Wazzup [Помощь] вернул (%s): %s", resp.status, resp_text[:300])
                return False
    except Exception as exc:
        logger.warning("Ошибка отправки в Wazzup по кнопке Помощь: %s", exc)
        return False


# ================= AMOCRM =================
def _amocrm_headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {AMOCRM_ACCESS_TOKEN}",
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
                headers={"Authorization": f"Bearer {AMOCRM_ACCESS_TOKEN}"},
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
    chat_id: int | str,
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
    if not AMOCRM_BASE_URL or not AMOCRM_ACCESS_TOKEN:
        return False, "не заполнены AMOCRM_BASE_URL / AMOCRM_ACCESS_TOKEN"

    base_url = AMOCRM_BASE_URL.rstrip("/")
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
    chat_id: int | str,
    tg_username: str | None,
    full_name: str,
) -> tuple[bool, str]:
    if not AMOCRM_BASE_URL or not AMOCRM_ACCESS_TOKEN:
        return False, "не заполнены AMOCRM_BASE_URL / AMOCRM_ACCESS_TOKEN"

    base_url = AMOCRM_BASE_URL.rstrip("/")
    async with _FILE_LOCKS[f"lead_{chat_id}"]:
        tg_login = format_tg_login(chat_id, tg_username)
        tg_link = format_tg_link(chat_id, tg_username)
        safe_name = (full_name or "Пользователь").strip()
        user_rec = get_user_record(chat_id)
        phone = user_rec.get("phone", "")

        note_text = (
            "Парк: ЛунапаркЕда\n"
            "Обращение по кнопке «Помощь»\n"
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


# ================= YANDEX FLEET API (КАБИНЕТ КУРЬЕРА) =================
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
    cleaned = re.sub(r"[^\d.-]", "", str(val).replace(" ", "").replace(",", "."))
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
    disk_entry = disk_all.get(pid, {}) if isinstance(disk_all, dict) else {}
    disk_ts = disk_entry.get("updated_at", 0.0) if isinstance(disk_entry, dict) else 0.0
    disk_profiles = disk_entry.get("profiles", []) if isinstance(disk_entry, dict) else []

    if not force_refresh and disk_profiles and (time.time() - float(disk_ts or 0.0)) < DRIVERS_TTL_SEC:
        DRIVERS_CACHE[pid] = {"ts": now, "data": disk_profiles}
        return disk_profiles

    try:
        drivers = await _fetch_all_drivers_for_park(
            session, park, partial_fallback=disk_profiles or (mem or {}).get("data")
        )
    except RuntimeError as exc:
        if "429" in str(exc) and (disk_profiles or (mem or {}).get("data")):
            return (mem or {}).get("data") or disk_profiles
        raise

    if drivers:
        DRIVERS_CACHE[pid] = {"ts": now, "data": drivers}
        if not isinstance(disk_all, dict):
            disk_all = {}
        disk_all[pid] = {"updated_at": time.time(), "profiles": drivers}
        _write_json(DRIVERS_CACHE_FILE, disk_all)
    return drivers


def _extract_matches_in_park(
    park: dict[str, str],
    drivers: list[dict[str, Any]],
    target_phone: str,
) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for item in drivers:
        if not isinstance(item, dict):
            continue
        profile = item.get("driver_profile") or {}
        phones = profile.get("phones") or []
        if any(normalize_phone(str(p)) == target_phone for p in phones):
            accounts = item.get("accounts") or []
            balance_raw = accounts[0].get("balance", "0") if accounts and isinstance(accounts[0], dict) else "0"
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


async def _find_all_profiles_by_phone(
    session: aiohttp.ClientSession,
    target_phone: str,
) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    for park in PARKS_CONFIG:
        drivers = await get_cached_drivers(session, park, force_refresh=False)
        found.extend(_extract_matches_in_park(park, drivers, target_phone))
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
            if not isinstance(order, dict):
                continue
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


# ================= ЭКРАНЫ И СЦЕНАРИИ =================
async def send_start_menu(chat_id: int | str, first_name: str = "друг") -> None:
    clear_user_state(chat_id)
    text = (
        f"Привет, {first_name}!\n\n"
        "Официальный бот парка ЛунапаркЕда. Выберите нужный раздел:"
    )
    await tg_send_text(chat_id, text, reply_markup=kb_start_menu())


async def show_confirmation_screen(chat_id: int | str, data: dict[str, Any]) -> None:
    set_user_state(chat_id, "reg_confirm", data)
    sts_line = ""
    if requires_two_sided_sts(data.get("transport", "")):
        has_front = bool(data.get("sts_front_file_id"))
        has_back = bool(data.get("sts_back_file_id"))
        sts_line = f"\nФото СТС: {'загружены с двух сторон' if (has_front and has_back) else 'не загружены'}\n"

    text = (
        "Проверьте, пожалуйста, всё ли корректно:\n\n"
        f"Имя: {data.get('name', '—')}\n"
        f"Город: {data.get('city', '—')}\n"
        f"Формат: {data.get('transport', '—')}\n"
        f"Телефон: {data.get('phone', '—')}"
        f"{sts_line}\n"
        "Всё верно? Нажмите «Да» или «Исправить»."
    )
    await tg_send_text(chat_id, text, reply_markup=kb_confirm_application())


async def show_cabinet_for_profile(chat_id: int | str, driver: dict[str, Any]) -> None:
    park = get_park_by_id(driver["park_id"])
    reg_dt = get_registered_dt(chat_id)
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
        "Выберите действие на клавиатуре:"
    )
    await tg_send_text(chat_id, text, reply_markup=kb_courier_cabinet())


async def get_saved_profile_or_redirect(chat_id: int | str) -> dict[str, Any] | None:
    rec = get_user_record(chat_id)
    phone = rec.get("phone")
    if not phone:
        set_user_state(chat_id, "courier_auth_phone", {})
        await tg_send_text(
            chat_id,
            "Сначала укажите ваш номер телефона в парке:",
            reply_markup=kb_step_with_help(),
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


async def lookup_and_route_phone(chat_id: int | str, phone: str) -> None:
    await tg_send_text(chat_id, "⏳ Проверяю ваш номер в базе парка...", reply_markup=kb_step_with_help())
    try:
        async with aiohttp.ClientSession() as session:
            profiles = await _find_all_profiles_by_phone(session, phone)
    except Exception as exc:
        await tg_send_text(
            chat_id,
            f"⚠️ Не удалось проверить профиль в парке: {exc}",
            reply_markup=kb_start_menu(),
        )
        return

    if not profiles:
        await tg_send_text(
            chat_id,
            f"❌ Номер {phone} не найден в базе подключённых парков.\n\n"
            "Проверьте правильность номера или нажмите «🚀 Подключиться».",
            reply_markup=kb_start_menu(),
        )
        return

    if len(profiles) == 1:
        chosen = profiles[0]
        save_user_session(chat_id, phone=phone, park_id=chosen["park_id"], driver_id=chosen["id"])
        await show_cabinet_for_profile(chat_id, chosen)
        return

    PENDING_PROFILES[str(chat_id)] = profiles
    set_user_state(chat_id, "courier_pick_profile", {})
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
    await tg_send_text(chat_id, "\n".join(lines), reply_markup=kb_profiles_choice(profiles))


async def finalize_registration(
    chat_id: int | str,
    data: dict[str, Any],
    tg_username: str,
    contact_name: str,
) -> None:
    clear_user_state(chat_id)
    norm_phone = data.get("phone", "")
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
        "tg_id": str(chat_id),
    }
    save_application(lead_payload)

    sts_front = None
    sts_back = None
    if data.get("sts_front_file_id"):
        sts_front = await tg_download_file(data["sts_front_file_id"], f"sts_front_{chat_id}.jpg")
    if data.get("sts_back_file_id"):
        sts_back = await tg_download_file(data["sts_back_file_id"], f"sts_back_{chat_id}.jpg")

    try:
        await send_lead_to_amocrm(lead_payload, sts_front=sts_front, sts_back=sts_back)
    except Exception as exc:
        logger.warning("Ошибка отправки заявки в amoCRM: %s", exc)

    await tg_send_text(chat_id, MSG_AFTER_SUBMIT, reply_markup=kb_start_menu())
    schedule_delayed_profile_msg(chat_id, delay_sec=PROFILE_READY_DELAY_SEC)


def _extract_photo_file_id(message: dict[str, Any]) -> str:
    photos = message.get("photo")
    if isinstance(photos, list) and photos:
        last = photos[-1]
        if isinstance(last, dict):
            return str(last.get("file_id") or "")
    doc = message.get("document")
    if isinstance(doc, dict):
        return str(doc.get("file_id") or "")
    return ""


async def handle_telegram_update(update: dict[str, Any]) -> None:
    if not isinstance(update, dict):
        return

    message = update.get("message") or update.get("edited_message")
    if not isinstance(message, dict):
        return

    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    if not chat_id:
        return

    from_user = message.get("from") or {}
    tg_username = str(from_user.get("username") or "").strip()
    first_name = str(from_user.get("first_name") or chat.get("first_name") or "друг").strip()
    last_name = str(from_user.get("last_name") or "").strip()
    contact_name = " ".join(x for x in (first_name, last_name) if x).strip() or "Пользователь"

    raw_text = str(message.get("text") or message.get("caption") or "").strip()
    contact_obj = message.get("contact") if isinstance(message.get("contact"), dict) else {}
    contact_phone = str(contact_obj.get("phone_number") or "").strip()
    file_id = _extract_photo_file_id(message)

    save_user_session(chat_id, phone=contact_phone, username=tg_username, full_name=contact_name)
    norm_cmd = normalize_choice_text(raw_text)

    if raw_text == "/start" or norm_cmd in ("в начало", "главное меню", "старт", "start", "меню"):
        await send_start_menu(chat_id, first_name=first_name)
        return

    # Кнопка «❓ Помощь» доступна на ЛЮБОМ этапе
    if norm_cmd in ("помощь", "нужна помощь", "поддержка", "оператор"):
        state = get_user_state(chat_id)
        prev_step = state.get("step")
        prev_data = state.get("data") or {}
        set_user_state(chat_id, "help_waiting_problem", {"prev_step": prev_step, "prev_data": prev_data})
        await tg_send_text(
            chat_id,
            "Добрый день! Опишите Вашу проблему — мы передадим её в amoCRM и сможем ответить вам в чате.",
            reply_markup=kb_step_with_help(),
        )
        return

    state = get_user_state(chat_id)
    step = state.get("step")
    data = state.get("data") or {}

    if norm_cmd in ("отмена", "отменить", "cancel"):
        if step == "help_waiting_problem" and data.get("prev_step"):
            set_user_state(chat_id, data["prev_step"], data.get("prev_data") or {})
            await tg_send_text(chat_id, "Обращение в поддержку отменено. Продолжаем.", reply_markup=kb_step_with_help())
            return
        await send_start_menu(chat_id, first_name=first_name)
        return

    if step == "help_waiting_problem":
        if not raw_text and not file_id:
            await tg_send_text(
                chat_id,
                "Напишите текстом или отправьте скриншот, что случилось:",
                reply_markup=kb_step_with_help(),
            )
            return
        problem_body = raw_text or "[Прикреплено фото / файл]"
        set_user_state(chat_id, data.get("prev_step"), data.get("prev_data") or {})

        await send_help_to_amocrm(
            problem_text=problem_body,
            chat_id=chat_id,
            tg_username=tg_username,
            full_name=contact_name,
        )
        # Отправляем через Wazzup только при обращении за помощью, чтобы менеджер мог ответить
        await send_help_via_wazzup(
            chat_id=chat_id,
            text=f"Обращение от {contact_name} ({format_tg_login(chat_id, tg_username)}):\n{problem_body}",
        )
        await tg_send_text(
            chat_id,
            "Получили ваше сообщение, скоро ответим.",
            reply_markup=kb_start_menu(),
        )
        return

    if norm_cmd in ("подключиться", "подключиться к парку", "регистрация", "стать курьером"):
        set_user_state(chat_id, "reg_transport", {"return_to_confirm": False})
        await tg_send_text(
            chat_id,
            "Шаг 1 из 4. Как планируете доставлять заказы?",
            reply_markup=kb_transport_choice(),
        )
        return

    if norm_cmd in ("уже курьер", "уже курьер парка", "кабинет", "мой кабинет"):
        set_user_state(chat_id, "courier_auth_phone", {})
        await tg_send_text(
            chat_id,
            "Введите номер телефона, привязанный к аккаунту в парке:",
            reply_markup=kb_step_with_help(),
        )
        return

    if norm_cmd in ("сменить номер", "изменить номер аккаунта"):
        set_user_state(chat_id, "courier_auth_phone", {})
        await tg_send_text(
            chat_id,
            "Отправьте номер телефона для входа в кабинет курьера:",
            reply_markup=kb_step_with_help(),
        )
        return

    if norm_cmd in ("моя статистика", "статистика"):
        driver = await get_saved_profile_or_redirect(chat_id)
        if driver:
            await show_cabinet_for_profile(chat_id, driver)
        return

    if norm_cmd in ("я на линии?", "я на линии", "статус линии", "линия"):
        driver = await get_saved_profile_or_redirect(chat_id)
        if not driver:
            return
        park = get_park_by_id(driver["park_id"])
        reg_dt = get_registered_dt(chat_id)
        try:
            async with aiohttp.ClientSession() as session:
                stats = await _get_driver_orders_stats(session, park, driver["id"], registered_dt=reg_dt)
        except Exception as exc:
            await tg_send_text(chat_id, f"⚠️ Ошибка проверки линии: {exc}", reply_markup=kb_courier_cabinet())
            return

        line_map = {
            "online": "🟢 На линии (готов принимать заказы)",
            "busy": "🟡 На заказе",
            "offline": "🔴 Не на линии (офлайн)",
        }
        line_human = line_map.get(driver["line_status"], f"ℹ️ Статус: {driver['line_status']}")
        active_info = "\n".join(f"• {o}" for o in stats["active_orders"]) if stats["active_orders"] else "Нет активных заказов"
        resp_text = (
            "🤔 Проверка статуса на линии\n\n"
            f"👤 Курьер: {driver['full_name']}\n"
            f"🏢 Парк: {driver['park_name']}\n"
            f"📌 Статус профиля: {driver['work_status']}\n"
            f"📡 Линия: {line_human}\n\n"
            f"📦 Текущие заказы:\n{active_info}"
        )
        await tg_send_text(chat_id, resp_text, reply_markup=kb_courier_cabinet())
        return

    if norm_cmd in ("цель", "бонус", "моя цель"):
        driver = await get_saved_profile_or_redirect(chat_id)
        if not driver:
            return
        park = get_park_by_id(driver["park_id"])
        reg_dt = get_registered_dt(chat_id)
        try:
            async with aiohttp.ClientSession() as session:
                stats = await _get_driver_orders_stats(session, park, driver["id"], registered_dt=reg_dt)
        except Exception as exc:
            await tg_send_text(chat_id, f"⚠️ Ошибка расчёта цели: {exc}", reply_markup=kb_courier_cabinet())
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

        resp_text = (
            f"🎁 Цель: {GOAL_ORDERS} заказов за первые {GOAL_DAYS} дней (+{GOAL_BONUS} ₽)\n\n"
            f"Прогресс: {done} / {GOAL_ORDERS} заказов\n"
            f"{bar}\n\n"
            f"{status_line}"
        )
        await tg_send_text(chat_id, resp_text, reply_markup=kb_courier_cabinet())
        return

    if step == "courier_auth_phone":
        raw_phone = contact_phone or raw_text
        if len(re.sub(r"\D", "", raw_phone)) < 10:
            await tg_send_text(
                chat_id,
                "Номер введён не полностью. Введите номер в формате +7 999 123-45-67:",
                reply_markup=kb_step_with_help(),
            )
            return
        clear_user_state(chat_id)
        await lookup_and_route_phone(chat_id, normalize_phone(raw_phone))
        return

    if step == "courier_pick_profile":
        if norm_cmd == "хочу ввести другой номер":
            set_user_state(chat_id, "courier_auth_phone", {})
            await tg_send_text(
                chat_id,
                "Введите номер телефона, привязанный к аккаунту в парке:",
                reply_markup=kb_step_with_help(),
            )
            return
        profiles = PENDING_PROFILES.get(str(chat_id), [])
        m = re.match(r"^(\d+)", raw_text.strip())
        if not m or int(m.group(1)) < 1 or int(m.group(1)) > len(profiles):
            await tg_send_text(
                chat_id,
                "Выберите профиль на клавиатуре или напишите его номер:",
                reply_markup=kb_profiles_choice(profiles) if profiles else kb_start_menu(),
            )
            return
        chosen = profiles[int(m.group(1)) - 1]
        clear_user_state(chat_id)
        save_user_session(chat_id, phone=chosen["phone"], park_id=chosen["park_id"], driver_id=chosen["id"])
        await show_cabinet_for_profile(chat_id, chosen)
        return

    if step == "reg_transport":
        chosen_transport = parse_transport_choice(raw_text)
        if not chosen_transport:
            await tg_send_text(
                chat_id,
                "Выберите формат доставки на клавиатуре:",
                reply_markup=kb_transport_choice(),
            )
            return
        data = update_user_state_data(chat_id, transport=chosen_transport)
        if data.get("return_to_confirm"):
            if requires_two_sided_sts(chosen_transport) and not (data.get("sts_front_file_id") and data.get("sts_back_file_id")):
                set_user_state(chat_id, "reg_sts_front", data)
                await tg_send_text(
                    chat_id,
                    "Для выбранного формата нужно фото СТС с двух сторон. Пришлите лицевую сторону (1 из 2):",
                    reply_markup=kb_step_with_help(),
                )
                return
            await show_confirmation_screen(chat_id, data)
            return
        set_user_state(chat_id, "reg_city", data)
        await tg_send_text(
            chat_id,
            "Шаг 2 из 4. В каком городе планируете работать?",
            reply_markup=kb_step_with_help(),
        )
        return

    if step == "reg_city":
        if len(raw_text) < 2:
            await tg_send_text(chat_id, "Напишите корректное название города:", reply_markup=kb_step_with_help())
            return
        data = update_user_state_data(chat_id, city=raw_text)
        if data.get("return_to_confirm"):
            await show_confirmation_screen(chat_id, data)
            return
        set_user_state(chat_id, "reg_name", data)
        await tg_send_text(
            chat_id,
            "Шаг 3 из 4. Напишите ваши Фамилию и Имя:",
            reply_markup=kb_step_with_help(),
        )
        return

    if step == "reg_name":
        if len(raw_text) < 2:
            await tg_send_text(chat_id, "Напишите ваши Фамилию и Имя:", reply_markup=kb_step_with_help())
            return
        data = update_user_state_data(chat_id, name=raw_text)
        if data.get("return_to_confirm"):
            await show_confirmation_screen(chat_id, data)
            return
        set_user_state(chat_id, "reg_phone", data)
        await tg_send_text(
            chat_id,
            "Шаг 4 из 4. Введите ваш номер телефона (например, +7 999 123-45-67):",
            reply_markup=kb_step_with_help(),
        )
        return

    if step == "reg_phone":
        raw_phone = contact_phone or raw_text
        if len(re.sub(r"\D", "", raw_phone)) < 10:
            await tg_send_text(
                chat_id,
                "Номер введён не полностью. Введите номер в формате +7 999 123-45-67:",
                reply_markup=kb_step_with_help(),
            )
            return
        data = update_user_state_data(chat_id, phone=normalize_phone(raw_phone))
        if data.get("return_to_confirm"):
            await show_confirmation_screen(chat_id, data)
            return
        if requires_two_sided_sts(data.get("transport", "")):
            set_user_state(chat_id, "reg_sts_front", data)
            await tg_send_text(
                chat_id,
                "Пришлите фото СТС — сначала лицевую сторону (1 из 2):",
                reply_markup=kb_step_with_help(),
            )
            return
        await show_confirmation_screen(chat_id, data)
        return

    if step == "reg_sts_front":
        if not file_id:
            await tg_send_text(
                chat_id,
                "Пришлите фото лицевой стороны СТС (1 из 2):",
                reply_markup=kb_step_with_help(),
            )
            return
        data = update_user_state_data(chat_id, sts_front_file_id=file_id)
        set_user_state(chat_id, "reg_sts_back", data)
        await tg_send_text(
            chat_id,
            "Отлично! Теперь пришлите фото обратной стороны СТС (2 из 2):",
            reply_markup=kb_step_with_help(),
        )
        return

    if step == "reg_sts_back":
        if not file_id:
            await tg_send_text(
                chat_id,
                "Пришлите фото обратной стороны СТС (2 из 2):",
                reply_markup=kb_step_with_help(),
            )
            return
        data = update_user_state_data(chat_id, sts_back_file_id=file_id)
        await show_confirmation_screen(chat_id, data)
        return

    if step == "reg_confirm":
        if norm_cmd in ("исправить", "изменить", "редактировать"):
            data = update_user_state_data(chat_id, return_to_confirm=True)
            set_user_state(chat_id, "reg_choosing_edit", data)
            await tg_send_text(
                chat_id,
                "Что хотите изменить?",
                reply_markup=kb_edit_field_choice(data),
            )
            return
        if norm_cmd in ("да", "верно", "подтверждаю", "ок", "yes"):
            await finalize_registration(chat_id, data, tg_username, contact_name)
            return
        await tg_send_text(
            chat_id,
            "Нажмите «Да», если данные верны, или «Исправить»:",
            reply_markup=kb_confirm_application(),
        )
        return

    if step == "reg_choosing_edit":
        if norm_cmd in ("назад к проверке", "назад"):
            await show_confirmation_screen(chat_id, data)
            return
        if norm_cmd in ("изменить формат", "формат", "транспорт"):
            set_user_state(chat_id, "reg_transport", data)
            await tg_send_text(chat_id, "Выберите новый формат доставки:", reply_markup=kb_transport_choice())
            return
        if norm_cmd in ("изменить город", "город"):
            set_user_state(chat_id, "reg_city", data)
            await tg_send_text(chat_id, "Введите новый город:", reply_markup=kb_step_with_help())
            return
        if norm_cmd in ("изменить имя", "имя"):
            set_user_state(chat_id, "reg_name", data)
            await tg_send_text(chat_id, "Введите новые Фамилию и Имя:", reply_markup=kb_step_with_help())
            return
        if norm_cmd in ("изменить телефон", "телефон"):
            set_user_state(chat_id, "reg_phone", data)
            await tg_send_text(chat_id, "Отправьте новый номер телефона:", reply_markup=kb_step_with_help())
            return
        if norm_cmd in ("изменить фото стс", "фото стс", "стс"):
            set_user_state(chat_id, "reg_sts_front", data)
            await tg_send_text(
                chat_id,
                "Пришлите новое фото лицевой стороны СТС (1 из 2):",
                reply_markup=kb_step_with_help(),
            )
            return
        await tg_send_text(chat_id, "Выберите поле для изменения:", reply_markup=kb_edit_field_choice(data))
        return

    await send_start_menu(chat_id, first_name=first_name)


async def polling_loop() -> None:
    logger.info("Запуск Telegram polling")
    offset: int | None = None
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                params: dict[str, Any] = {"timeout": 30}
                if offset is not None:
                    params["offset"] = offset
                async with session.get(
                    f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
                    params=params,
                    timeout=40,
                ) as resp:
                    data = await resp.json()
                    if data.get("ok"):
                        for upd in data.get("result", []):
                            upd_id = upd.get("update_id")
                            if isinstance(upd_id, int):
                                offset = upd_id + 1
                            asyncio.create_task(handle_telegram_update(upd))
                    else:
                        await asyncio.sleep(3)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.warning("Ошибка polling: %s", exc)
            await asyncio.sleep(3)


async def handle_health(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok"})


async def handle_tg_webhook(request: web.Request) -> web.Response:
    try:
        upd = await request.json()
        if isinstance(upd, dict):
            asyncio.create_task(handle_telegram_update(upd))
    except Exception:
        pass
    return web.json_response({"ok": True})


async def on_startup(app: web.Application) -> None:
    app["delayed_worker"] = asyncio.create_task(delayed_notifications_worker())
    if not TELEGRAM_BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN пуст! Укажите токен бота от @BotFather.")
        return

    async with aiohttp.ClientSession() as session:
        if TELEGRAM_WEBHOOK_URL:
            await tg_api(session, "setWebhook", {"url": TELEGRAM_WEBHOOK_URL})
            logger.info("Установлен Telegram webhook: %s", TELEGRAM_WEBHOOK_URL)
        else:
            await tg_api(session, "deleteWebhook", {"drop_pending_updates": False})
            app["polling_task"] = asyncio.create_task(polling_loop())


async def on_cleanup(app: web.Application) -> None:
    for task_key in ("polling_task", "delayed_worker"):
        task = app.get(task_key)
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass


def create_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/", handle_health)
    app.router.add_get("/health", handle_health)
    app.router.add_post("/webhook/telegram", handle_tg_webhook)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


if __name__ == "__main__":
    web.run_app(create_app(), host="0.0.0.0", port=PORT)
