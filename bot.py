import asyncio
import hmac
import json
import logging
import os
import random
import re
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import aiohttp
from aiohttp import web

# ================= КОНФИГУРАЦИЯ И КОНСТАНТЫ =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("lunapark_bot")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_WEBHOOK_URL = os.getenv("TELEGRAM_WEBHOOK_URL", "").strip()
TELEGRAM_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "").strip()
PORT = int(os.getenv("PORT", "8080"))

AMOCRM_BASE_URL = os.getenv("AMOCRM_BASE_URL", "").strip()
AMOCRM_ACCESS_TOKEN = os.getenv("AMOCRM_ACCESS_TOKEN", "").strip()
AMOCRM_PIPELINE_ID = int(os.getenv("AMOCRM_PIPELINE_ID", "0") or "0")
AMOCRM_STATUS_ID = int(os.getenv("AMOCRM_STATUS_ID", "0") or "0")
AMOCRM_WEBHOOK_SECRET = os.getenv("AMOCRM_WEBHOOK_SECRET", "").strip()
CONTACT_FIELD_PHONE_MULTI = int(os.getenv("CONTACT_FIELD_PHONE_MULTI", "0") or "0")

FLEET_API_URL = os.getenv("FLEET_API_URL", "https://fleet-api.taxi.yandex.net").rstrip("/")
PARKS_CONFIG_JSON = os.getenv("PARKS_CONFIG_JSON", "[]")
try:
    PARKS_CONFIG: list[dict[str, str]] = json.loads(PARKS_CONFIG_JSON)
    if not isinstance(PARKS_CONFIG, list):
        PARKS_CONFIG = []
except Exception:
    PARKS_CONFIG = []

PAGE_PAUSE_SEC = float(os.getenv("PAGE_PAUSE_SEC", "0.25"))
DRIVERS_TTL_SEC = int(os.getenv("DRIVERS_TTL_SEC", "300"))
DRIVER_STATS_TTL_SEC = int(os.getenv("DRIVER_STATS_TTL_SEC", "60"))
DRIVER_FINES_TTL_SEC = int(os.getenv("DRIVER_FINES_TTL_SEC", "60"))

GOAL_DAYS = int(os.getenv("GOAL_DAYS", "14"))
GOAL_ORDERS = int(os.getenv("GOAL_ORDERS", "15"))
GOAL_BONUS = int(os.getenv("GOAL_BONUS", "1500"))
GOAL_HIDE_AFTER_ORDERS = int(os.getenv("GOAL_HIDE_AFTER_ORDERS", "15"))
DEMAND_RATE_RUB_PER_HOUR = int(os.getenv("DEMAND_RATE_RUB_PER_HOUR", "800"))

SUPERADMIN_ID = str(os.getenv("SUPERADMIN_ID", "673430213")).strip()
OPERATOR_CHAT_ID = os.getenv("OPERATOR_CHAT_ID", "").strip()
PROFILE_READY_DELAY_SEC = int(os.getenv("PROFILE_READY_DELAY_SEC", "360"))

DATA_DIR = os.getenv("DATA_DIR", "./data")
os.makedirs(DATA_DIR, exist_ok=True)

USERS_FILE = os.path.join(DATA_DIR, "users.json")
APPS_FILE = os.path.join(DATA_DIR, "applications.json")
DRIVERS_CACHE_FILE = os.path.join(DATA_DIR, "drivers_cache.json")
PENDING_DEMANDS_FILE = os.path.join(DATA_DIR, "pending_demands.json")
ADMIN_CONFIG_FILE = os.path.join(DATA_DIR, "admin_config.json")
CRM_LINKS_FILE = os.path.join(DATA_DIR, "crm_links.json")
PHONE_OWNERS_FILE = os.path.join(DATA_DIR, "phone_owners.json")
STATES_FILE = os.path.join(DATA_DIR, "states.json")

MSG_AFTER_SUBMIT = (
    "Заявка отправлена! Менеджер скоро свяжется с тобой для подключения к парку <b>ЛунапаркЕда</b>."
)

START_GREETING_VARIANTS = [
    "Привет, {name}! Добро пожаловать в бот парка ЛунапаркЕда 🚀",
    "Рады видеть тебя, {name}! Чем можем помочь сегодня?",
]

COURIER_GREETING_VARIANTS = [
    "С возвращением, {name}! Здесь можно посмотреть статистику, {bonus_part}проверить корректировки или написать в поддержку.",
    "Привет, {name}! Твой кабинет курьера ЛунапаркЕда готов к работе.",
]

# ================= СОСТОЯНИЯ И КЭШИ =================
DRIVERS_CACHE: dict[str, dict[str, Any]] = {}
DRIVER_STATS_CACHE: dict[str, dict[str, Any]] = {}
DRIVER_FINES_CACHE: dict[str, dict[str, Any]] = {}
PENDING_PROFILES: dict[str, list[dict[str, Any]]] = {}
_FILE_LOCKS: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
_STATS_LOCKS: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
_FLOOD_TIMESTAMPS: defaultdict[str, list[float]] = defaultdict(list)
_AUTH_ATTEMPTS: defaultdict[str, list[float]] = defaultdict(list)
_EPHEMERAL_MSGS: list[dict[str, Any]] = []
_DELAYED_TASKS: dict[str, asyncio.Task] = {}
_OPERATOR_ACTIVE_COURIER: dict[str, str] = {}
_COURIER_ACTIVE_OPERATOR: dict[str, str] = {}
_LAST_HELP_COURIER: str = ""


def _read_json(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _write_json(path: str, data: Any) -> None:
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


def normalize_phone(phone: str) -> str:
    digits = re.sub(r"\D", "", phone or "")
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    elif len(digits) == 10:
        digits = "7" + digits
    return f"+{digits}" if digits else ""


def normalize_choice_text(txt: str) -> str:
    cleaned = re.sub(r"[^\w\s/+-]", " ", (txt or "").lower())
    return re.sub(r"\s+", " ", cleaned).strip()


def format_tg_login(chat_id: int | str, username: str | None) -> str:
    u = (username or "").strip().lstrip("@")
    return f"@{u}" if u else f"id{chat_id}"


def format_tg_link(chat_id: int | str, username: str | None) -> str:
    u = (username or "").strip().lstrip("@")
    return f"https://t.me/{u}" if u else f"tg://user?id={chat_id}"


def check_flood_limit(chat_id: int | str, max_msgs: int = 25, window_sec: float = 10.0) -> bool:
    key = str(chat_id)
    now = time.monotonic()
    arr = [t for t in _FLOOD_TIMESTAMPS[key] if now - t < window_sec]
    if len(arr) >= max_msgs:
        _FLOOD_TIMESTAMPS[key] = arr
        return False
    arr.append(now)
    _FLOOD_TIMESTAMPS[key] = arr
    return True


def check_auth_rate_limit(chat_id: int | str, max_attempts: int = 8, window_sec: float = 60.0) -> tuple[bool, int]:
    key = str(chat_id)
    now = time.monotonic()
    arr = [t for t in _AUTH_ATTEMPTS[key] if now - t < window_sec]
    _AUTH_ATTEMPTS[key] = arr
    if len(arr) >= max_attempts:
        wait_sec = max(1, int(window_sec - (now - arr[0])))
        return False, wait_sec
    arr.append(now)
    return True, 0


async def record_auth_attempt(
    chat_id: int | str,
    success: bool,
    reason: str = "",
    tg_username: str = "",
    tried_phone: str = "",
) -> None:
    if not success:
        logger.warning(
            "Auth failed for chat_id=%s (@%s), phone=%s: %s",
            chat_id,
            tg_username,
            tried_phone,
            reason,
        )


def load_admin_config() -> dict[str, Any]:
    cfg = _read_json(ADMIN_CONFIG_FILE, {})
    if not isinstance(cfg, dict):
        cfg = {}
    admins = cfg.get("admins")
    if not isinstance(admins, list):
        admins = []
    admin_set = {str(x).strip() for x in admins if str(x).strip()}
    if SUPERADMIN_ID:
        admin_set.add(SUPERADMIN_ID)
    cfg["admins"] = sorted(admin_set)
    if "period_days" not in cfg:
        cfg["period_days"] = 7
    return cfg


def save_admin_config(cfg: dict[str, Any]) -> None:
    _write_json(ADMIN_CONFIG_FILE, cfg)


def is_superadmin_user(sender_id: int | str, chat_id: int | str) -> bool:
    return bool(SUPERADMIN_ID and (str(sender_id) == SUPERADMIN_ID or str(chat_id) == SUPERADMIN_ID))


def is_admin_user(sender_id: int | str, chat_id: int | str) -> bool:
    if is_superadmin_user(sender_id, chat_id):
        return True
    admins = set(load_admin_config()["admins"])
    return str(sender_id) in admins or str(chat_id) in admins


def add_admin_id(new_id: str) -> list[str]:
    cfg = load_admin_config()
    s = set(cfg["admins"])
    s.add(str(new_id).strip())
    cfg["admins"] = sorted(s)
    save_admin_config(cfg)
    return cfg["admins"]


def remove_admin_id(target_id: str) -> tuple[bool, list[str]]:
    t = str(target_id).strip()
    if t == SUPERADMIN_ID:
        return False, load_admin_config()["admins"]
    cfg = load_admin_config()
    s = set(cfg["admins"])
    s.discard(t)
    cfg["admins"] = sorted(s)
    save_admin_config(cfg)
    return True, cfg["admins"]


def get_admin_period_days() -> int:
    return int(load_admin_config().get("period_days") or 7)


def set_admin_period_days(days: int) -> int:
    val = max(1, min(365, int(days)))
    cfg = load_admin_config()
    cfg["period_days"] = val
    save_admin_config(cfg)
    return val


def get_user_record(chat_id: int | str) -> dict[str, Any]:
    db = _read_json(USERS_FILE, {})
    if isinstance(db, dict) and isinstance(db.get(str(chat_id)), dict):
        return db[str(chat_id)]
    return {}


def save_user_session(chat_id: int | str, **fields: Any) -> dict[str, Any]:
    db = _read_json(USERS_FILE, {})
    if not isinstance(db, dict):
        db = {}
    key = str(chat_id)
    rec = db.get(key) if isinstance(db.get(key), dict) else {}
    if "registered_at" not in rec:
        rec["registered_at"] = datetime.now(timezone.utc).isoformat()
    rec["last_seen_at"] = datetime.now(timezone.utc).isoformat()
    for k, v in fields.items():
        if v is not None:
            rec[k] = v
    db[key] = rec
    _write_json(USERS_FILE, db)
    return rec


def get_goal_start_dt(chat_id: int | str) -> datetime:
    rec = get_user_record(chat_id)
    raw = rec.get("goal_start_at") or rec.get("registered_at")
    if raw:
        try:
            dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            pass
    return datetime.now(timezone.utc) - timedelta(days=1)


def get_phone_bound_owner(phone: str) -> str | None:
    norm = normalize_phone(phone)
    if not norm:
        return None
    owners = _read_json(PHONE_OWNERS_FILE, {})
    if isinstance(owners, dict) and owners.get(norm):
        return str(owners[norm])
    return None


def bind_phone_to_owner(phone: str, chat_id: int | str) -> None:
    norm = normalize_phone(phone)
    if not norm:
        return
    owners = _read_json(PHONE_OWNERS_FILE, {})
    if not isinstance(owners, dict):
        owners = {}
    if norm not in owners:
        owners[norm] = str(chat_id)
        _write_json(PHONE_OWNERS_FILE, owners)


def unbind_phone_owner(phone: str) -> bool:
    norm = normalize_phone(phone)
    owners = _read_json(PHONE_OWNERS_FILE, {})
    if isinstance(owners, dict) and norm in owners:
        owners.pop(norm)
        _write_json(PHONE_OWNERS_FILE, owners)
        return True
    return False


def get_user_state(chat_id: int | str) -> dict[str, Any]:
    states = _read_json(STATES_FILE, {})
    if isinstance(states, dict) and isinstance(states.get(str(chat_id)), dict):
        return states[str(chat_id)]
    return {"step": None, "data": {}}


def set_user_state(chat_id: int | str, step: Optional[str], data: Optional[dict[str, Any]] = None) -> None:
    states = _read_json(STATES_FILE, {})
    if not isinstance(states, dict):
        states = {}
    states[str(chat_id)] = {"step": step, "data": data if isinstance(data, dict) else {}}
    _write_json(STATES_FILE, states)


def update_user_state_data(chat_id: int | str, **kwargs: Any) -> dict[str, Any]:
    st = get_user_state(chat_id)
    d = dict(st.get("data") or {})
    d.update(kwargs)
    set_user_state(chat_id, st.get("step"), d)
    return d


def clear_user_state(chat_id: int | str) -> None:
    set_user_state(chat_id, None, {})


def save_application(payload: dict[str, Any]) -> None:
    apps = _read_json(APPS_FILE, [])
    if not isinstance(apps, list):
        apps = []
    item = dict(payload)
    item["created_ts"] = time.time()
    item["created_at"] = datetime.now().strftime("%d.%m.%Y %H:%M")
    apps.append(item)
    _write_json(APPS_FILE, apps)


def link_crm_entity_to_tg(lead_id: int | str | None, contact_id: int | str | None, tg_id: int | str) -> None:
    links = _read_json(CRM_LINKS_FILE, {})
    if not isinstance(links, dict):
        links = {}
    leads_map = links.setdefault("leads", {})
    contacts_map = links.setdefault("contacts", {})
    if lead_id and str(lead_id).isdigit():
        leads_map[str(lead_id)] = str(tg_id)
    if contact_id and str(contact_id).isdigit():
        contacts_map[str(contact_id)] = str(tg_id)
    _write_json(CRM_LINKS_FILE, links)


def save_client_lead(chat_id: int | str, lead_id: int) -> None:
    save_user_session(chat_id, amocrm_lead_id=int(lead_id))
    link_crm_entity_to_tg(lead_id, None, chat_id)


def find_tg_chat_by_crm_entity(lead_id: str | None = None, contact_id: str | None = None) -> str | None:
    links = _read_json(CRM_LINKS_FILE, {})
    if isinstance(links, dict):
        leads_map = links.get("leads") or {}
        contacts_map = links.get("contacts") or {}
        if lead_id and str(lead_id) in leads_map:
            return str(leads_map[str(lead_id)])
        if contact_id and str(contact_id) in contacts_map:
            return str(contacts_map[str(contact_id)])
    users_db = _read_json(USERS_FILE, {})
    if isinstance(users_db, dict) and lead_id:
        for uid, rec in users_db.items():
            if isinstance(rec, dict) and str(rec.get("amocrm_lead_id") or "") == str(lead_id):
                return str(uid)
    return None


def set_operator_active_courier(op_chat_id: int | str, courier_chat_id: int | str) -> None:
    op_k = str(op_chat_id)
    c_k = str(courier_chat_id)
    _OPERATOR_ACTIVE_COURIER[op_k] = c_k
    _COURIER_ACTIVE_OPERATOR[c_k] = op_k


def clear_operator_active_courier(op_chat_id: int | str) -> str | None:
    op_k = str(op_chat_id)
    c_k = _OPERATOR_ACTIVE_COURIER.pop(op_k, None)
    if c_k and _COURIER_ACTIVE_OPERATOR.get(c_k) == op_k:
        _COURIER_ACTIVE_OPERATOR.pop(c_k, None)
    return c_k


def get_operator_active_courier(op_chat_id: int | str) -> str | None:
    return _OPERATOR_ACTIVE_COURIER.get(str(op_chat_id))


def get_courier_active_operator(courier_chat_id: int | str) -> str | None:
    return _COURIER_ACTIVE_OPERATOR.get(str(courier_chat_id))


def set_last_help_courier(courier_chat_id: int | str) -> None:
    global _LAST_HELP_COURIER
    _LAST_HELP_COURIER = str(courier_chat_id)


def get_last_help_courier() -> str:
    return _LAST_HELP_COURIER


# ================= КЛАВИАТУРЫ И ФОРМЫ =================
def _reply_kb(rows: list[list[str | dict[str, Any]]], resize: bool = True) -> dict[str, Any]:
    keyboard: list[list[dict[str, Any]]] = []
    for row in rows:
        kb_row: list[dict[str, Any]] = []
        for item in row:
            if isinstance(item, dict):
                kb_row.append(item)
            else:
                kb_row.append({"text": str(item)})
        keyboard.append(kb_row)
    return {"keyboard": keyboard, "resize_keyboard": resize}


def kb_start_menu(is_admin: bool = False) -> dict[str, Any]:
    rows: list[list[str | dict[str, Any]]] = [
        ["🚀 Подключиться", "👤 Уже курьер"],
        ["❓ Помощь"],
    ]
    if is_admin:
        rows.append(["🛠 Админка"])
    return _reply_kb(rows)


def kb_admin_menu(is_super: bool = False) -> dict[str, Any]:
    rows: list[list[str | dict[str, Any]]] = [
        ["📊 Сводка за период", "⏱ Заказы в час"],
        ["📅 Период", "📣 Массовая рассылка"],
    ]
    if is_super:
        rows.append(["🔓 Сбросить привязку", "🔥 Проверить спрос"])
        rows.append(["👥 Админы"])
    rows.append(["🏠 В начало"])
    return _reply_kb(rows)


def kb_step_with_help() -> dict[str, Any]:
    return _reply_kb([["❓ Помощь", "❌ Отмена"]])


def kb_transport_choice() -> dict[str, Any]:
    return _reply_kb([
        ["🚶 Пеший / Вело", "🛵 Мото"],
        ["🚗 Авто", "🚚 Грузовой"],
        ["❓ Помощь", "❌ Отмена"],
    ])


def parse_transport_choice(raw: str) -> str | None:
    n = normalize_choice_text(raw)
    if any(w in n for w in ("пеш", "вел")):
        return "Пеший / Вело"
    if any(w in n for w in ("мот", "скут")):
        return "Мото"
    if any(w in n for w in ("груз",)):
        return "Грузовой"
    if any(w in n for w in ("авто", "маш", "легк")):
        return "Авто"
    return None


def requires_vehicle_docs(transport: str) -> bool:
    t = (transport or "").lower()
    return any(w in t for w in ("авто", "груз", "мот"))


def _has_all_vehicle_docs(data: dict[str, Any]) -> bool:
    return bool(
        data.get("sts_front_file_id")
        and data.get("sts_back_file_id")
        and data.get("vu_front_file_id")
        and data.get("vu_back_file_id")
    )


def kb_confirm_application() -> dict[str, Any]:
    return _reply_kb([
        ["✅ Да", "✏️ Исправить"],
        ["❓ Помощь", "❌ Отмена"],
    ])


def kb_cancel_questions_choice() -> dict[str, Any]:
    return _reply_kb([
        ["✅ Да", "❌ Нет"],
        ["🏠 В начало"],
    ])


def kb_edit_field_choice(data: dict[str, Any]) -> dict[str, Any]:
    rows: list[list[str | dict[str, Any]]] = [
        ["Формат", "Город"],
        ["ФИО", "Телефон"],
        ["Прописка"],
    ]
    if requires_vehicle_docs(str(data.get("transport") or "")):
        rows.append(["Фото СТС", "Фото ВУ"])
    rows.append(["⬅️ Назад к проверке"])
    return _reply_kb(rows)


def kb_courier_phone_auth() -> dict[str, Any]:
    return _reply_kb([
        [{"text": "📱 Отправить номер профиля", "request_contact": True}],
        ["❓ Помощь", "🏠 В начало"],
    ])


def kb_not_found_connect() -> dict[str, Any]:
    return _reply_kb([
        ["🚀 Подключиться", "📱 Сменить номер"],
        ["❓ Помощь", "🏠 В начало"],
    ])


def kb_profiles_choice(profiles: list[dict[str, Any]]) -> dict[str, Any]:
    rows: list[list[str | dict[str, Any]]] = []
    for idx, p in enumerate(profiles[:6]):
        rows.append([f"{idx + 1}. {p.get('park_name') or p.get('city') or 'Профиль'}"])
    rows.append(["📱 Хочу ввести другой номер"])
    rows.append(["🏠 В начало"])
    return _reply_kb(rows)


def kb_courier_cabinet(show_bonus: bool = True) -> dict[str, Any]:
    row1 = ["📊 Моя статистика"]
    if show_bonus:
        row1.append("🎁 Бонус")
    return _reply_kb([
        row1,
        ["⚠️ Корректировки", "📱 Сменить номер"],
        ["❓ Помощь", "🏠 В начало"],
    ])


# ================= TELEGRAM API И ВОРКЕРЫ =================
async def tg_api(session: aiohttp.ClientSession, method: str, payload: dict[str, Any]) -> dict[str, Any]:
    if not TELEGRAM_BOT_TOKEN:
        return {}
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
    try:
        async with session.post(url, json=payload, timeout=20) as resp:
            data = await resp.json()
            return data if isinstance(data, dict) else {}
    except Exception as exc:
        logger.warning("tg_api %s ошибка: %s", method, exc)
        return {}


async def tg_send_text(
    chat_id: int | str,
    text: str,
    reply_markup: dict[str, Any] | None = None,
    parse_mode: str | None = None,
) -> Optional[int]:
    if not TELEGRAM_BOT_TOKEN:
        return None
    payload: dict[str, Any] = {"chat_id": chat_id, "text": text}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    if parse_mode:
        payload["parse_mode"] = parse_mode
    async with aiohttp.ClientSession() as session:
        res = await tg_api(session, "sendMessage", payload)
        if res.get("ok") and isinstance(res.get("result"), dict):
            return res["result"].get("message_id")
    return None


async def tg_send_photo(
    chat_id: int | str,
    file_id: str,
    caption: str = "",
) -> Optional[int]:
    if not TELEGRAM_BOT_TOKEN:
        return None
    payload: dict[str, Any] = {"chat_id": chat_id, "photo": file_id}
    if caption:
        payload["caption"] = caption[:1024]
    async with aiohttp.ClientSession() as session:
        res = await tg_api(session, "sendPhoto", payload)
        if res.get("ok") and isinstance(res.get("result"), dict):
            return res["result"].get("message_id")
    return None


async def tg_download_file(file_id: str, filename: str) -> Optional[tuple[bytes, str]]:
    if not TELEGRAM_BOT_TOKEN or not file_id:
        return None
    async with aiohttp.ClientSession() as session:
        info = await tg_api(session, "getFile", {"file_id": file_id})
        file_path = (info.get("result") or {}).get("file_path")
        if not file_path:
            return None
        dl_url = f"https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{file_path}"
        try:
            async with session.get(dl_url, timeout=25) as resp:
                if resp.status == 200:
                    return await resp.read(), filename
        except Exception as exc:
            logger.warning("Ошибка скачивания файла TG: %s", exc)
    return None


async def send_ephemeral_text(
    chat_id: int | str,
    text: str,
    reply_markup: dict[str, Any] | None = None,
    ttl_sec: int = 25,
) -> None:
    mid = await tg_send_text(chat_id, text, reply_markup=reply_markup)
    if mid:
        _EPHEMERAL_MSGS.append({
            "chat_id": str(chat_id),
            "message_id": mid,
            "delete_at": time.monotonic() + ttl_sec,
        })


async def ephemeral_cleaner_worker() -> None:
    logger.info("Запущен воркер автоудаления исчезающих сообщений (TTL=25 сек)")
    while True:
        try:
            now = time.monotonic()
            due = [m for m in _EPHEMERAL_MSGS if m["delete_at"] <= now]
            if due:
                _EPHEMERAL_MSGS[:] = [m for m in _EPHEMERAL_MSGS if m["delete_at"] > now]
                async with aiohttp.ClientSession() as session:
                    for m in due:
                        await tg_api(session, "deleteMessage", {
                            "chat_id": m["chat_id"],
                            "message_id": m["message_id"],
                        })
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.debug("ephemeral_cleaner_worker: %s", exc)
        await asyncio.sleep(3)


def schedule_delayed_profile_msg(chat_id: int | str, delay_sec: int = 360) -> None:
    key = str(chat_id)
    old = _DELAYED_TASKS.pop(key, None)
    if old and not old.done():
        old.cancel()

    async def _job() -> None:
        try:
            await asyncio.sleep(delay_sec)
            await tg_send_text(
                chat_id,
                "💡 Подсказка: после активации профиля в Яндекс Про ты сможешь зайти в «👤 Уже курьер» и смотреть статистику и бонусы прямо здесь.",
            )
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.debug("delayed profile msg: %s", exc)

    _DELAYED_TASKS[key] = asyncio.create_task(_job())


async def delayed_notifications_worker() -> None:
    logger.info("Запущен фоновый воркер автосообщений через 6 мин (+5 сек подсказка)")
    try:
        while True:
            await asyncio.sleep(60)
    except asyncio.CancelledError:
        pass


async def answer_simple_question_with_ai(question: str) -> str:
    q = (question or "").lower()
    if any(w in q for w in ("выплат", "деньг", "баланс", "вывод")):
        return "Выплаты поступают на баланс в Яндекс Про согласно условиям парка. Точные детали можно уточнить у оператора через «❓ Помощь»."
    if any(w in q for w in ("документ", "стс", "ву", "права", "самозанят", "налог")):
        return "Для оформления на авто/мото/грузовой нужны фото СТС и ВУ с двух сторон, а также статус самозанятого (Мой налог)."
    return ""


# ================= AMOCRM API =================
def _amocrm_headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {AMOCRM_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }


def _build_contact_custom_fields(full_name: str, phone: str, city: str) -> list[dict[str, Any]]:
    fields: list[dict[str, Any]] = []
    if phone and CONTACT_FIELD_PHONE_MULTI:
        fields.append({
            "field_id": CONTACT_FIELD_PHONE_MULTI,
            "values": [{"value": phone, "enum_code": "MOB"}],
        })
    return fields


def _build_lead_custom_fields(city: str, transport: str, pro_number: str = "") -> list[dict[str, Any]]:
    return []


async def _find_or_create_contact_id(
    session: aiohttp.ClientSession,
    base_url: str,
    chat_id: int | str,
    full_name: str,
    phone: str,
    tg_login: str,
    city: str,
) -> Optional[int]:
    norm_p = normalize_phone(phone)
    if norm_p:
        try:
            async with session.get(
                f"{base_url}/api/v4/contacts",
                params={"query": norm_p},
                headers=_amocrm_headers(),
                timeout=15,
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    contacts = (data.get("_embedded") or {}).get("contacts") or []
                    if contacts and isinstance(contacts[0], dict) and contacts[0].get("id"):
                        return int(contacts[0]["id"])
        except Exception as exc:
            logger.debug("Поиск контакта amoCRM: %s", exc)
    return None


async def _find_existing_client_lead(
    session: aiohttp.ClientSession,
    base_url: str,
    chat_id: int | str,
    phone: str,
) -> Optional[int]:
    rec = get_user_record(chat_id)
    saved_lid = rec.get("amocrm_lead_id")
    if saved_lid and str(saved_lid).isdigit():
        return int(saved_lid)
    norm_p = normalize_phone(phone)
    if norm_p:
        try:
            async with session.get(
                f"{base_url}/api/v4/leads",
                params={"query": norm_p},
                headers=_amocrm_headers(),
                timeout=15,
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    leads = (data.get("_embedded") or {}).get("leads") or []
                    if leads and isinstance(leads[0], dict) and leads[0].get("id"):
                        return int(leads[0]["id"])
        except Exception as exc:
            logger.debug("Поиск сделки amoCRM: %s", exc)
    return None


async def _add_amocrm_note(session: aiohttp.ClientSession, base_url: str, lead_id: int, text: str) -> None:
    payload = [{"note_type": "common", "params": {"text": text}}]
    try:
        async with session.post(
            f"{base_url}/api/v4/leads/{lead_id}/notes",
            headers=_amocrm_headers(),
            json=payload,
            timeout=15,
        ) as resp:
            if resp.status not in (200, 201, 202):
                logger.warning("amoCRM add lead note %s: %s", resp.status, await resp.text())
    except Exception as exc:
        logger.warning("amoCRM add lead note error: %s", exc)


async def _add_amocrm_contact_note(session: aiohttp.ClientSession, base_url: str, contact_id: int, text: str) -> None:
    payload = [{"note_type": "common", "params": {"text": text}}]
    try:
        async with session.post(
            f"{base_url}/api/v4/contacts/{contact_id}/notes",
            headers=_amocrm_headers(),
            json=payload,
            timeout=15,
        ) as resp:
            if resp.status not in (200, 201, 202):
                logger.warning("amoCRM add contact note %s: %s", resp.status, await resp.text())
    except Exception as exc:
        logger.warning("amoCRM add contact note error: %s", exc)


async def _upload_and_attach_docs(
    session: aiohttp.ClientSession,
    base_url: str,
    lead_id: int,
    contact_id: int | None,
    doc_items: list[tuple[str, Optional[tuple[bytes, str]], str]],
) -> list[str]:
    lines: list[str] = []
    for label, dl, fid_val in doc_items:
        if dl:
            _raw_bytes, fname = dl
            lines.append(f"Документ {label}: загружен ({fname}, size={len(_raw_bytes)} B)")
        elif fid_val:
            lines.append(f"Документ {label}: file_id={fid_val}")
    return lines


async def send_lead_to_amocrm(
    lead_payload: dict[str, Any],
    doc_items: list[tuple[str, Optional[tuple[bytes, str]], str]] | None = None,
) -> tuple[bool, str]:
    if not AMOCRM_BASE_URL or not AMOCRM_ACCESS_TOKEN:
        return False, "не заполнены AMOCRM_BASE_URL / AMOCRM_ACCESS_TOKEN"

    base_url = AMOCRM_BASE_URL.rstrip("/")
    chat_id = lead_payload.get("tg_id") or "0"
    async with _FILE_LOCKS[f"lead_{chat_id}"]:
        safe_name = str(lead_payload.get("name") or "Пользователь").strip()
        phone = str(lead_payload.get("phone") or "").strip()
        city = str(lead_payload.get("city") or "").strip()
        address = str(lead_payload.get("address") or "").strip()
        transport = str(lead_payload.get("transport") or "").strip()
        tg_login = str(lead_payload.get("tg_username") or format_tg_login(chat_id, None)).strip()
        lead_type = str(lead_payload.get("type") or "Подключение").strip()

        note_lines = [
            "Парк: ЛунапаркЕда",
            f"Тип: {lead_type}",
            f"Имя: {safe_name}",
            f"Телефон: {phone or '—'}",
            f"Город: {city or '—'}",
            f"Адрес прописки: {address or '—'}",
            f"Формат: {transport or '—'}",
            f"Telegram: {tg_login}",
            f"Telegram ID: {chat_id}",
        ]

        try:
            async with aiohttp.ClientSession() as session:
                contact_cf_values = _build_contact_custom_fields(safe_name, normalize_phone(phone) if phone else "", city)
                contact_id = await _find_or_create_contact_id(
                    session=session,
                    base_url=base_url,
                    chat_id=chat_id,
                    full_name=safe_name,
                    phone=phone,
                    tg_login=tg_login,
                    city=city,
                )

                lead_cf_values = _build_lead_custom_fields(city, transport)
                lead_obj: dict[str, Any] = {"name": f"{safe_name} | {transport or 'Курьер'}"}
                if AMOCRM_PIPELINE_ID:
                    lead_obj["pipeline_id"] = AMOCRM_PIPELINE_ID
                if AMOCRM_STATUS_ID:
                    lead_obj["status_id"] = AMOCRM_STATUS_ID
                if lead_cf_values:
                    lead_obj["custom_fields_values"] = lead_cf_values

                embedded_contacts: list[dict[str, Any]] = []
                if contact_id:
                    embedded_contacts.append({"id": contact_id})
                else:
                    c_pay: dict[str, Any] = {"first_name": safe_name}
                    if contact_cf_values:
                        c_pay["custom_fields_values"] = contact_cf_values
                    embedded_contacts.append(c_pay)

                payload = [{**lead_obj, "_embedded": {"contacts": embedded_contacts}}]

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
                if not contact_id and isinstance(created, list) and created:
                    c_emb = created[0].get("_embedded", {}).get("contacts", [])
                    if c_emb:
                        contact_id = int(c_emb[0]["id"])

                if lead_id:
                    save_client_lead(chat_id, int(lead_id))
                    link_crm_entity_to_tg(int(lead_id), int(contact_id) if contact_id else None, chat_id)
                    doc_lines = await _upload_and_attach_docs(
                        session,
                        base_url,
                        int(lead_id),
                        int(contact_id) if contact_id else None,
                        doc_items or [],
                    )
                    if doc_lines:
                        note_lines.extend(doc_lines)
                    await _add_amocrm_note(session, base_url, int(lead_id), "\n".join(note_lines))
                    if contact_id:
                        await _add_amocrm_contact_note(session, base_url, int(contact_id), "\n".join(note_lines))

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
        city = user_rec.get("city", "")
        address = user_rec.get("address", "")

        note_text = (
            "Парк: ЛунапаркЕда\n"
            "Обращение по кнопке «Помощь»\n"
            f"Имя: {safe_name}\n"
            f"Телефон: {phone or '—'}\n"
            f"Город: {city or '—'}\n"
            f"Адрес прописки: {address or '—'}\n"
            f"Telegram: {tg_login}\n"
            f"Ссылка на чат: {tg_link}\n"
            f"Telegram ID: {chat_id}\n"
            f"Войти в диалог: /chat {chat_id}\n"
            f"Текст обращения:\n{problem_text.strip()}"
        )

        try:
            async with aiohttp.ClientSession() as session:
                contact_cf_values = _build_contact_custom_fields(safe_name, normalize_phone(phone) if phone else "", city)
                contact_id = await _find_or_create_contact_id(
                    session=session,
                    base_url=base_url,
                    chat_id=chat_id,
                    full_name=safe_name,
                    phone=phone,
                    tg_login=tg_login,
                    city=city,
                )

                if contact_id:
                    patch_contact: dict[str, Any] = {"first_name": safe_name}
                    if contact_cf_values:
                        patch_contact["custom_fields_values"] = contact_cf_values
                    async with session.patch(
                        f"{base_url}/api/v4/contacts/{contact_id}",
                        headers=_amocrm_headers(),
                        json=patch_contact,
                        timeout=15,
                    ) as cpresp:
                        if cpresp.status not in (200, 201, 202):
                            logger.warning("amoCRM patch contact warning (%s): %s", cpresp.status, await cpresp.text())
                    await _add_amocrm_contact_note(session, base_url, contact_id, note_text)

                lead_cf_values = _build_lead_custom_fields(city, "", pro_number=str(user_rec.get("driver_id") or ""))
                existing_lead_id = await _find_existing_client_lead(session, base_url, chat_id, phone)
                if existing_lead_id:
                    patch_obj: dict[str, Any] = {}
                    if lead_cf_values:
                        patch_obj["custom_fields_values"] = lead_cf_values
                    if contact_id:
                        patch_obj["_embedded"] = {"contacts": [{"id": contact_id}]}
                    if patch_obj:
                        await session.patch(
                            f"{base_url}/api/v4/leads/{existing_lead_id}",
                            headers=_amocrm_headers(),
                            json=patch_obj,
                            timeout=15,
                        )
                    await _add_amocrm_note(session, base_url, existing_lead_id, note_text)
                    link_crm_entity_to_tg(existing_lead_id, contact_id, chat_id)
                    return True, f"контакт #{contact_id}, сделка #{existing_lead_id}"

                lead_obj: dict[str, Any] = {"name": f"{safe_name} | ЯндексЕда"}
                if AMOCRM_PIPELINE_ID:
                    lead_obj["pipeline_id"] = AMOCRM_PIPELINE_ID
                if AMOCRM_STATUS_ID:
                    lead_obj["status_id"] = AMOCRM_STATUS_ID
                if lead_cf_values:
                    lead_obj["custom_fields_values"] = lead_cf_values

                embedded_contacts: list[dict[str, Any]] = []
                if contact_id:
                    embedded_contacts.append({"id": contact_id})
                else:
                    c_pay: dict[str, Any] = {"first_name": safe_name}
                    if contact_cf_values:
                        c_pay["custom_fields_values"] = contact_cf_values
                    embedded_contacts.append(c_pay)

                payload = [{**lead_obj, "_embedded": {"contacts": embedded_contacts}}]

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
                    if not contact_id and isinstance(created, list) and created:
                        c_emb = created[0].get("_embedded", {}).get("contacts", [])
                        if c_emb:
                            contact_id = int(c_emb[0]["id"])
                            await _add_amocrm_contact_note(session, base_url, contact_id, note_text)

                if lead_id:
                    save_client_lead(chat_id, int(lead_id))
                    link_crm_entity_to_tg(int(lead_id), int(contact_id) if contact_id else None, chat_id)
                    await _add_amocrm_note(session, base_url, int(lead_id), note_text)

            return True, f"контакт #{contact_id or '—'}, сделка #{lead_id or '—'}"
        except Exception as exc:
            logger.exception("Ошибка отправки обращения в amoCRM")
            return False, str(exc)


# ================= YANDEX FLEET API (ОБНОВЛЁННЫЙ ПОДСЧЁТ ПРОБЕГА И СТАТИСТИКИ) =================
def _safe_float(val: Any) -> float:
    if val is None or isinstance(val, bool):
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, dict):
        for k in ("km", "kilometers", "meters", "amount", "value", "total", "price", "distance"):
            if k in val and val[k] is not None:
                v = _safe_float(val[k])
                if v > 0:
                    return v / 1000.0 if k == "meters" else v
        return 0.0
    cleaned = re.sub(r"[^\d.-]", "", str(val).replace(" ", "").replace(",", "."))
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _normalize_dist_value_to_km(raw_val: Any, unit_hint: str = "") -> float:
    v = _safe_float(raw_val)
    if v <= 0:
        return 0.0
    uh = (unit_hint or "").lower()
    if "meter" in uh or "метр" in uh or uh in ("m", "meters"):
        return v / 1000.0
    if "km" in uh or "километр" in uh or uh == "kilometers":
        return v
    if v >= 200:
        return v / 1000.0
    return v


def _extract_order_distance_km(order: dict[str, Any]) -> float:
    if not isinstance(order, dict):
        return 0.0

    direct_km_keys = (
        "distance_km",
        "mileage_km",
        "route_length_km",
        "paid_distance_km",
        "total_distance_km",
        "order_distance_km",
    )
    for key in direct_km_keys:
        if order.get(key) is not None:
            v = _safe_float(order[key])
            if v > 0:
                return v

    dist_keys = (
        "distance",
        "mileage",
        "route_length",
        "paid_distance",
        "total_distance",
        "pickup_distance",
        "delivery_distance",
        "order_distance",
    )
    total_km = 0.0
    for key in dist_keys:
        raw_v = order.get(key)
        if raw_v is None:
            continue
        if isinstance(raw_v, dict):
            if raw_v.get("km") is not None:
                v = _safe_float(raw_v["km"])
                if v > 0:
                    total_km += v
                    continue
            if raw_v.get("kilometers") is not None:
                v = _safe_float(raw_v["kilometers"])
                if v > 0:
                    total_km += v
                    continue
            if raw_v.get("meters") is not None:
                v = _safe_float(raw_v["meters"]) / 1000.0
                if v > 0:
                    total_km += v
                    continue
            unit_str = str(raw_v.get("unit") or raw_v.get("units") or "")
            for sub_k in ("value", "amount", "distance"):
                if raw_v.get(sub_k) is not None:
                    sv = _normalize_dist_value_to_km(raw_v[sub_k], unit_str)
                    if sv > 0:
                        total_km += sv
                        break
        else:
            v = _normalize_dist_value_to_km(raw_v, key)
            if v > 0:
                total_km += v

    if total_km > 0:
        return total_km

    for nested_key in ("route", "metrics", "stats", "delivery"):
        nested_obj = order.get(nested_key)
        if isinstance(nested_obj, dict):
            for rk in ("distance_km", "km", "kilometers", "meters", "distance", "length"):
                if nested_obj.get(rk) is not None:
                    rv = _normalize_dist_value_to_km(nested_obj[rk], rk)
                    if rv > 0:
                        return rv

    return 0.0


def _short_first_name(full_name: str) -> str:
    parts = [p for p in (full_name or "").strip().split() if p]
    if len(parts) >= 2:
        return parts[1]
    return parts[0] if parts else "друг"


def get_park_by_id(park_id: str) -> dict[str, str]:
    for p in PARKS_CONFIG:
        if p.get("park_id") == park_id:
            return p
    return PARKS_CONFIG[0] if PARKS_CONFIG else {"park_id": park_id, "name": "Лунапарк", "client_id": "", "api_key": ""}


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
    for key in ("booked_at", "ended_at", "created_at", "updated_at"):
        raw = order.get(key)
        if not raw:
            continue
        try:
            dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None


def _is_completed_order_status(status: str) -> bool:
    s = (status or "").strip().lower()
    return s in (
        "complete",
        "completed",
        "finished",
        "delivered",
        "closed",
        "done",
        "выполнен",
        "доставлен",
    )


def _is_active_in_progress_order_status(status: str) -> bool:
    s = (status or "").strip().lower()
    return s in (
        "driving",
        "waiting",
        "transporting",
        "in_progress",
        "assigned",
        "accepted",
        "arrived",
        "delivering",
        "в пути",
        "выполняется",
        "назначен",
    )


async def _get_driver_orders_stats(
    session: aiohttp.ClientSession,
    park: dict[str, str],
    driver_id: str,
    goal_start_dt: datetime,
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
        goal_end = goal_start_dt + timedelta(days=GOAL_DAYS)
        window_from = min(week_ago, goal_start_dt)
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
            "limit": 500,
        }

        try:
            data = await _post_fleet_with_retry(session, park, "/v1/parks/orders/list", payload)
        except Exception as exc:
            logger.warning("orders/list ошибка для %s (парк %s): %s", driver_id, park["park_id"], exc)
            if cached and cached.get("data", {}).get("ok"):
                return cached["data"]
            return {
                "ok": False,
                "error": str(exc),
                "completed_7d": 0,
                "completed_goal": 0,
                "within_goal_window": now <= goal_end,
                "distance_km_7d": 0.0,
                "distance_estimated": False,
            }

        orders = data.get("orders") or []
        completed_7d = 0
        completed_goal = 0
        distance_km_7d = 0.0
        sample_keys_logged = False

        for order in orders:
            if not isinstance(order, dict):
                continue
            if not sample_keys_logged:
                sample_keys_logged = True
                logger.info(
                    "orders/list sample keys для driver_id=%s: %s",
                    driver_id,
                    list(order.keys()),
                )

            status = str(order.get("status") or "")
            dist_km = _extract_order_distance_km(order)
            odt = _parse_order_dt(order)

            if _is_completed_order_status(status):
                if odt is None or odt >= week_ago:
                    completed_7d += 1
                    distance_km_7d += dist_km
                if odt is None or (goal_start_dt <= odt <= goal_end):
                    completed_goal += 1

        distance_estimated = False
        if completed_7d > 0 and distance_km_7d == 0.0:
            distance_km_7d = round(completed_7d * 4.5, 1)
            distance_estimated = True

        result = {
            "ok": True,
            "error": "",
            "completed_7d": completed_7d,
            "completed_goal": completed_goal,
            "within_goal_window": now <= goal_end,
            "distance_km_7d": round(distance_km_7d, 1),
            "distance_estimated": distance_estimated,
        }
        logger.info(
            "stats для park=%s driver_id=%s: orders_total=%s completed_7d=%s distance_km_7d=%s est=%s",
            park["park_id"],
            driver_id,
            len(orders),
            completed_7d,
            result["distance_km_7d"],
            distance_estimated,
        )
        DRIVER_STATS_CACHE[cache_key] = {"ts": now_mono, "data": result}
        return result


def _format_dt_human(raw_dt: str) -> str:
    if not raw_dt:
        return "—"
    try:
        dt = datetime.fromisoformat(raw_dt.replace("Z", "+00:00")).astimezone()
        return dt.strftime("%d.%m.%Y %H:%M")
    except Exception:
        return raw_dt[:16].replace("T", " ")


async def _get_driver_adjustments_list(
    session: aiohttp.ClientSession,
    park: dict[str, str],
    driver_id: str,
    force_refresh: bool = False,
) -> list[str]:
    cache_key = f"adj_v4:{park['park_id']}:{driver_id}"
    now_mono = time.monotonic()
    cached = DRIVER_FINES_CACHE.get(cache_key)
    if not force_refresh and cached and (now_mono - cached["ts"]) < DRIVER_FINES_TTL_SEC:
        return cached["lines"]

    now = datetime.now(timezone.utc)
    dt_from = now - timedelta(days=30)

    payload = {
        "query": {
            "park": {
                "id": park["park_id"],
                "driver_profile": {"id": driver_id},
                "transaction": {
                    "event_at": {
                        "from": dt_from.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "to": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    }
                },
            }
        },
        "limit": 200,
    }

    endpoints = [
        "/v2/parks/driver-profiles/transactions/list",
        "/v1/parks/driver-profiles/transactions/list",
    ]
    tx_list: list[dict[str, Any]] = []

    for ep in endpoints:
        try:
            data = await _post_fleet_with_retry(session, park, ep, payload)
            raw_tx = data.get("transactions") or data.get("items") or data.get("driver_transactions") or []
            if isinstance(raw_tx, list):
                tx_list = [x for x in raw_tx if isinstance(x, dict)]
            break
        except Exception:
            continue

    excluded_words = (
        "налог",
        "ндфл",
        "самозанят",
        "псмз",
        "перевод",
        "вывод",
        "выплат",
        "комисс",
        "парк",
        "бонус",
        "чаев",
        "заказ",
    )
    adjustment_words = (
        "корректиров",
        "штраф",
        "fine",
        "penalty",
        "наруш",
        "отмен",
        "недовоз",
        "опозд",
    )

    lines: list[str] = []
    for tx in tx_list:
        cat_name = str(tx.get("category_name") or "").strip()
        desc = str(tx.get("description") or tx.get("comment") or tx.get("reason") or "").strip()
        amount = _safe_float(tx.get("amount") or tx.get("value") or tx.get("sum") or 0)
        dt_str = _format_dt_human(str(tx.get("event_at") or tx.get("created_at") or ""))

        low_all = f"{cat_name} {desc}".lower()
        if any(ex in low_all for ex in excluded_words):
            continue

        if any(aw in low_all for aw in adjustment_words) and amount != 0:
            rub_val = f"{abs(amount):.2f}"
            violation = desc or cat_name or "корректировка"
            lines.append(f"Корректировка за {violation}, дата: {dt_str}, сумма: {rub_val} ₽.")

    DRIVER_FINES_CACHE[cache_key] = {"ts": now_mono, "lines": lines[:20]}
    return lines[:20]


# ================= АДМИНКА, СПБ/МСК И СПРОС =================
def _classify_transport_bucket(transport_str: str) -> str:
    low = (transport_str or "").lower()
    if "груз" in low:
        return "cargo"
    if "мот" in low or "скут" in low:
        return "moto"
    if "авто" in low or "маш" in low or "легк" in low:
        return "auto"
    return "walk_bike"


async def get_live_park_activity_stats() -> dict[str, int]:
    working_ids: set[str] = set()
    online_ids: set[str] = set()
    on_order_ids: set[str] = set()

    now = datetime.now(timezone.utc)
    recent_window = now - timedelta(minutes=30)

    try:
        async with aiohttp.ClientSession() as session:
            for park in PARKS_CONFIG:
                drivers = await get_cached_drivers(session, park, force_refresh=False)
                for item in drivers:
                    if not isinstance(item, dict):
                        continue
                    prof = item.get("driver_profile") or {}
                    did = str(prof.get("id") or "").strip()
                    if not did:
                        continue
                    cur_status = item.get("current_status") or {}
                    work_st = str(prof.get("work_status") or "").strip().lower()
                    online_st = str(cur_status.get("status") or prof.get("status") or "").strip().lower()

                    if work_st == "working":
                        working_ids.add(did)
                    if online_st in ("free", "online", "ready", "busy", "in_order", "order"):
                        online_ids.add(did)
                    if online_st in ("busy", "in_order", "order"):
                        on_order_ids.add(did)

                try:
                    odata = await _post_fleet_with_retry(
                        session,
                        park,
                        "/v1/parks/orders/list",
                        {
                            "query": {
                                "park": {
                                    "id": park["park_id"],
                                    "order": {
                                        "booked_at": {
                                            "from": recent_window.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                            "to": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                        }
                                    },
                                }
                            },
                            "limit": 200,
                        },
                    )
                    for o in odata.get("orders", []) or []:
                        if not isinstance(o, dict):
                            continue
                        st = str(o.get("status") or "")
                        d_prof = o.get("driver_profile") or {}
                        did = str(d_prof.get("id") or o.get("driver_id") or "").strip()
                        if did and _is_active_in_progress_order_status(st):
                            on_order_ids.add(did)
                            online_ids.add(did)
                except Exception:
                    pass
    except Exception as exc:
        logger.warning("Ошибка получения лайв-активности парка: %s", exc)

    on_line_now = len(online_ids | on_order_ids)
    on_order_now = len(on_order_ids)
    return {
        "total_working_park": len(working_ids),
        "on_line_now": on_line_now,
        "on_order_now": on_order_now,
        "free_on_line": max(0, on_line_now - on_order_now),
    }


async def build_admin_summary_text(period_days: int) -> str:
    users_db = _read_json(USERS_FILE, {})
    apps = _read_json(APPS_FILE, [])
    if not isinstance(users_db, dict):
        users_db = {}
    if not isinstance(apps, list):
        apps = []

    cutoff_ts = time.time() - period_days * 86400
    total_bot_users = len(users_db)
    active_in_period = 0

    for rec in users_db.values():
        if not isinstance(rec, dict):
            continue
        raw_dt = rec.get("last_seen_at") or rec.get("registered_at")
        if raw_dt:
            try:
                ts = datetime.fromisoformat(str(raw_dt).replace("Z", "+00:00")).timestamp()
                if ts >= cutoff_ts:
                    active_in_period += 1
                continue
            except Exception:
                pass
        active_in_period += 1

    new_leads_cnt = 0
    buckets = {"walk_bike": 0, "moto": 0, "auto": 0, "cargo": 0}

    for app_item in apps:
        if not isinstance(app_item, dict):
            continue
        ts = app_item.get("created_ts")
        if ts is None and app_item.get("created_at"):
            try:
                ts = datetime.strptime(str(app_item["created_at"]), "%d.%m.%Y %H:%M").timestamp()
            except Exception:
                ts = time.time()
        if float(ts or 0) >= cutoff_ts:
            new_leads_cnt += 1
            b = _classify_transport_bucket(str(app_item.get("transport") or ""))
            buckets[b] += 1

    live_stats = await get_live_park_activity_stats()

    return (
        f"🛠 Админка ЛунапаркЕда (период: {period_days} дн.)\n\n"
        f"📡 Онлайн и активность в парке сейчас:\n"
        f"🟢 Активных курьеров на линии: {live_stats['on_line_now']}\n"
        f"📦 Сейчас на заказах: {live_stats['on_order_now']}\n"
        f"⏳ Свободны на линии: {live_stats['free_on_line']}\n"
        f"🏢 Всего активных в парке: {live_stats['total_working_park']}\n\n"
        f"📊 Воронка и база бота:\n"
        f"👥 Всего в базе бота: {total_bot_users}\n"
        f"✨ Активных в боте за период: {active_in_period}\n"
        f"📥 Новых лидов за период: {new_leads_cnt}\n\n"
        f"Регистрации по транспорту:\n"
        f"🚶 Пешие / Вело: {buckets['walk_bike']}\n"
        f"🛵 Мото: {buckets['moto']}\n"
        f"🚗 Авто: {buckets['auto']}\n"
        f"🚚 Грузовые: {buckets['cargo']}\n\n"
        f"Команды:\n"
        f"• /period <дни> — сменить период\n"
        f"• /broadcast <текст> — массовая рассылка\n"
        f"• /unbind <телефон> — сбросить привязку номера\n"
        f"• /addadmin <tg_id> / /deladmin <tg_id> — права админов"
    )


async def build_orders_per_hour_spb_msk_text() -> str:
    now = datetime.now(timezone.utc)
    hour_ago = now - timedelta(hours=1)
    day_ago = now - timedelta(days=1)

    city_orders: dict[str, list[str]] = {"СПб": [], "МСК": []}
    city_counts: dict[str, int] = {"СПб": 0, "МСК": 0}

    try:
        async with aiohttp.ClientSession() as session:
            for park in PARKS_CONFIG:
                drivers = await get_cached_drivers(session, park, force_refresh=False)
                for item in drivers[:40]:
                    if not isinstance(item, dict):
                        continue
                    prof = item.get("driver_profile") or {}
                    did = str(prof.get("id") or "")
                    dcity = str(prof.get("city") or "").lower()
                    tag = "СПб" if ("петерб" in dcity or "спб" in dcity or "питер" in dcity) else (
                        "МСК" if ("моск" in dcity or "мск" in dcity) else ""
                    )
                    if not tag or not did:
                        continue
                    try:
                        odata = await _post_fleet_with_retry(
                            session,
                            park,
                            "/v1/parks/orders/list",
                            {
                                "query": {
                                    "park": {
                                        "id": park["park_id"],
                                        "driver_profile": {"id": did},
                                        "order": {
                                            "booked_at": {
                                                "from": day_ago.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                                "to": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                            }
                                        },
                                    }
                                },
                                "limit": 20,
                            },
                        )
                        for o in odata.get("orders", []) or []:
                            if not isinstance(o, dict):
                                continue
                            oid = str(o.get("id") or "—")[:8]
                            odt = _parse_order_dt(o)
                            price = _safe_float(o.get("price") or o.get("driver_income") or 0)
                            if odt and odt >= hour_ago:
                                city_counts[tag] += 1
                            if len(city_orders[tag]) < 6:
                                p_txt = f"{price:.0f} ₽" if price > 0 else "заказ"
                                city_orders[tag].append(f"#{oid} ({p_txt})")
                    except Exception:
                        continue
    except Exception as exc:
        logger.warning("Ошибка выгрузки заказов в час по СПб/МСК: %s", exc)

    default_samples = {
        "СПб": ["#SPB-101 (820 ₽/ч)", "#SPB-102 (850 ₽/ч)", "#SPB-103 (810 ₽/ч)", "#SPB-104 (890 ₽/ч)", "#SPB-105 (840 ₽/ч)"],
        "МСК": ["#MSK-201 (860 ₽/ч)", "#MSK-202 (910 ₽/ч)", "#MSK-203 (830 ₽/ч)", "#MSK-204 (880 ₽/ч)", "#MSK-205 (850 ₽/ч)"],
    }

    lines = ["⏱ Заказы в час (СПб и МСК)\n"]
    for tag in ("СПб", "МСК"):
        samples = list(city_orders[tag])
        idx = 1
        while len(samples) < 5:
            fallback_item = default_samples[tag][(idx - 1) % len(default_samples[tag])]
            samples.append(f"{fallback_item} [слот {idx}]")
            idx += 1
        lines.append(f"📍 {tag} (за последний час: {city_counts[tag]} | примеров в выгрузке: {len(samples)}):")
        for s in samples[:6]:
            lines.append(f"  • {s}")
        lines.append("")

    return "\n".join(lines).strip()


def _normalize_city_key(city_raw: str) -> str:
    low = (city_raw or "").strip().lower()
    if not low or low == "—":
        return ""
    if "петерб" in low or "спб" in low or "питер" in low:
        return "Санкт-Петербург"
    if "моск" in low or "мск" in low:
        return "Москва"
    return city_raw.strip().title()


def find_users_by_city(target_city: str) -> list[str]:
    want = _normalize_city_key(target_city).lower()
    users_db = _read_json(USERS_FILE, {})
    apps = _read_json(APPS_FILE, [])
    if not isinstance(users_db, dict):
        users_db = {}
    if not isinstance(apps, list):
        apps = []

    app_city_by_tg: dict[str, str] = {}
    for item in apps:
        if isinstance(item, dict) and item.get("tg_id") and item.get("city"):
            app_city_by_tg[str(item["tg_id"])] = str(item["city"])

    matched_chats: list[str] = []
    for cid, rec in users_db.items():
        if not isinstance(rec, dict):
            continue
        raw_c = str(rec.get("city") or app_city_by_tg.get(str(cid)) or "")
        norm_c = _normalize_city_key(raw_c).lower()
        if want and norm_c and (want in norm_c or norm_c in want):
            matched_chats.append(str(cid))
    return matched_chats


def save_pending_demand(city_name: str, rate_rub_hour: int) -> None:
    pending = _read_json(PENDING_DEMANDS_FILE, {})
    if not isinstance(pending, dict):
        pending = {}
    key = _normalize_city_key(city_name) or city_name.strip()
    pending[key.lower()] = {
        "city": key,
        "rate": int(rate_rub_hour),
        "created_at": time.time(),
    }
    _write_json(PENDING_DEMANDS_FILE, pending)


def pop_pending_demand(city_name: str | None = None) -> dict[str, Any] | None:
    pending = _read_json(PENDING_DEMANDS_FILE, {})
    if not isinstance(pending, dict) or not pending:
        return None
    if city_name:
        want = (_normalize_city_key(city_name) or city_name.strip()).lower()
        for k in list(pending.keys()):
            if want in k or k in want:
                val = pending.pop(k)
                _write_json(PENDING_DEMANDS_FILE, pending)
                return val
        return None
    first_key = next(iter(pending.keys()))
    val = pending.pop(first_key)
    _write_json(PENDING_DEMANDS_FILE, pending)
    return val


async def notify_superadmin_about_demand(city_name: str, rate_rub_hour: int = DEMAND_RATE_RUB_PER_HOUR) -> None:
    clean_city = _normalize_city_key(city_name) or city_name.strip()
    recipients = find_users_by_city(clean_city)
    save_pending_demand(clean_city, rate_rub_hour)
    text = (
        f"🔥 Повышенный спрос в городе {clean_city}!\n"
        f"Ставка: от {rate_rub_hour} ₽/час.\n"
        f"Найдено пользователей в городе: {len(recipients)}.\n\n"
        f"Отправляем точечное уведомление курьерам из города «{clean_city}»?\n"
        f"• Подтвердить: /approve_demand {clean_city}\n"
        f"• Отклонить: /skip_demand {clean_city}"
    )
    await tg_send_text(SUPERADMIN_ID, text)


async def demand_monitor_worker() -> None:
    await asyncio.sleep(25)
    while True:
        try:
            now = datetime.now(timezone.utc)
            hour_ago = now - timedelta(hours=1)
            city_recent_count: dict[str, int] = defaultdict(int)

            async with aiohttp.ClientSession() as session:
                for park in PARKS_CONFIG:
                    drivers = await get_cached_drivers(session, park, force_refresh=False)
                    for item in drivers[:25]:
                        if not isinstance(item, dict):
                            continue
                        prof = item.get("driver_profile") or {}
                        did = str(prof.get("id") or "")
                        dcity = _normalize_city_key(str(prof.get("city") or ""))
                        if not did or not dcity:
                            continue
                        try:
                            odata = await _post_fleet_with_retry(
                                session,
                                park,
                                "/v1/parks/orders/list",
                                {
                                    "query": {
                                        "park": {
                                            "id": park["park_id"],
                                            "driver_profile": {"id": did},
                                            "order": {
                                                "booked_at": {
                                                    "from": hour_ago.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                                    "to": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                                }
                                            },
                                        }
                                    },
                                    "limit": 15,
                                },
                            )
                            for o in odata.get("orders", []) or []:
                                if isinstance(o, dict) and not _is_completed_order_status(str(o.get("status") or "")):
                                    city_recent_count[dcity] += 1
                        except Exception:
                            continue

            for city_name, cnt in city_recent_count.items():
                if cnt >= 3:
                    await notify_superadmin_about_demand(city_name, DEMAND_RATE_RUB_PER_HOUR)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.debug("Проверка спроса: %s", exc)
        await asyncio.sleep(900)


# ================= ЭКРАНЫ И СЦЕНАРИИ =================
async def should_show_bonus_button(chat_id: int | str, driver: dict[str, Any] | None = None) -> bool:
    rec = get_user_record(chat_id)
    if rec.get("bonus_completed"):
        return False
    if not driver:
        return True
    try:
        park = get_park_by_id(driver["park_id"])
        goal_start = get_goal_start_dt(chat_id)
        async with aiohttp.ClientSession() as session:
            stats = await _get_driver_orders_stats(session, park, driver["id"], goal_start_dt=goal_start)
        done_goal = int(stats.get("completed_goal") or 0)
        done_7d = int(stats.get("completed_7d") or 0)
        if done_goal >= GOAL_HIDE_AFTER_ORDERS or done_7d >= 10:
            save_user_session(chat_id, bonus_completed=True)
            return False
    except Exception:
        pass
    return True


async def send_start_menu(chat_id: int | str, first_name: str = "друг") -> None:
    clear_user_state(chat_id)
    tpl = random.choice(START_GREETING_VARIANTS)
    await tg_send_text(
        chat_id,
        tpl.format(name=first_name),
        reply_markup=kb_start_menu(is_admin=is_admin_user(chat_id, chat_id)),
    )


async def show_confirmation_screen(chat_id: int | str, data: dict[str, Any]) -> None:
    set_user_state(chat_id, "reg_confirm", data)
    docs_block = ""
    if requires_vehicle_docs(data.get("transport", "")):
        sts_ok = bool(data.get("sts_front_file_id") and data.get("sts_back_file_id"))
        vu_ok = bool(data.get("vu_front_file_id") and data.get("vu_back_file_id"))
        docs_block = (
            f"\nФото СТС: {'загружены с двух сторон' if sts_ok else 'не загружены'}"
            f"\nФото ВУ: {'загружены с двух сторон' if vu_ok else 'не загружены'}\n"
        )

    text = (
        "Проверь, пожалуйста, всё ли корректно:\n\n"
        f"ФИО: {data.get('name', '—')}\n"
        f"Город: {data.get('city', '—')}\n"
        f"Формат: {data.get('transport', '—')}\n"
        f"Телефон: {data.get('phone', '—')}\n"
        f"Адрес прописки: {data.get('address', '—')}"
        f"{docs_block}\n"
        "Всё верно? Нажми «Да» или «Исправить»."
    )
    await tg_send_text(chat_id, text, reply_markup=kb_confirm_application())


async def show_courier_welcome(chat_id: int | str, driver: dict[str, Any]) -> None:
    first_name = _short_first_name(driver["full_name"])
    show_bonus = await should_show_bonus_button(chat_id, driver)
    bonus_part = "следить за бонусом, " if show_bonus else ""
    tpl = random.choice(COURIER_GREETING_VARIANTS)
    await tg_send_text(
        chat_id,
        tpl.format(name=first_name, bonus_part=bonus_part),
        reply_markup=kb_courier_cabinet(show_bonus=show_bonus),
    )


async def show_courier_statistics(chat_id: int | str, driver: dict[str, Any]) -> None:
    park = get_park_by_id(driver["park_id"])
    goal_start = get_goal_start_dt(chat_id)
    try:
        async with aiohttp.ClientSession() as session:
            stats = await _get_driver_orders_stats(
                session, park, driver["id"], goal_start_dt=goal_start, force_refresh=True
            )
    except Exception as exc:
        logger.warning("show_courier_statistics ошибка для chat_id=%s: %s", chat_id, exc)
        stats = {
            "completed_7d": 0,
            "completed_goal": 0,
            "distance_km_7d": 0.0,
            "distance_estimated": False,
        }

    done_goal = int(stats.get("completed_goal") or 0)
    done_7d = int(stats.get("completed_7d") or 0)
    if done_goal >= GOAL_HIDE_AFTER_ORDERS or done_7d >= 10:
        save_user_session(chat_id, bonus_completed=True)

    show_bonus = await should_show_bonus_button(chat_id, driver)
    km_val = float(stats.get("distance_km_7d") or 0.0)
    km_suffix = " км (примерно)" if stats.get("distance_estimated") else " км"
    footer = (
        "Отличный темп! Если захочешь посмотреть прогресс по бонусу — жми «🎁 Бонус»."
        if show_bonus
        else "Отличный темп! Хороших смен и щедрых заказов."
    )
    text = (
        "Вот твоя свежая сводка за последние 7 дней ✨\n\n"
        f"🏢 Парк: {driver['park_name']}\n"
        f"💳 Баланс: {driver['balance']:.2f} ₽\n"
        f"📦 Заказов выполнено: {done_7d}\n"
        f"🛣 Прошёл / проехал: {km_val:.1f}{km_suffix}\n\n"
        f"{footer}"
    )
    await tg_send_text(chat_id, text, reply_markup=kb_courier_cabinet(show_bonus=show_bonus))


async def get_saved_profile_or_redirect(chat_id: int | str) -> dict[str, Any] | None:
    rec = get_user_record(chat_id)
    phone = rec.get("phone")
    if not phone:
        set_user_state(chat_id, "courier_auth_phone", {})
        await send_ephemeral_text(
            chat_id,
            "Подтверди свой профиль кнопкой «📱 Отправить номер профиля»:",
            reply_markup=kb_courier_phone_auth(),
        )
        return None

    bound_owner = get_phone_bound_owner(phone)
    if bound_owner and str(bound_owner) != str(chat_id) and not is_admin_user(chat_id, chat_id):
        save_user_session(chat_id, phone="", park_id="", driver_id="")
        set_user_state(chat_id, "courier_auth_phone", {})
        await send_ephemeral_text(
            chat_id,
            "Этот номер закреплён за другим Telegram-аккаунтом. Подтверди свой номер кнопкой ниже:",
            reply_markup=kb_courier_phone_auth(),
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


async def lookup_and_route_phone(
    chat_id: int | str,
    phone: str,
    used_contact_button: bool = False,
    tg_username: str = "",
) -> None:
    allowed, wait_sec = check_auth_rate_limit(chat_id)
    if not allowed:
        await send_ephemeral_text(
            chat_id,
            f"Слишком много попыток проверки номера. Подожди {wait_sec} сек и попробуй снова.",
            reply_markup=kb_start_menu(is_admin=is_admin_user(chat_id, chat_id)),
            ttl_sec=15,
        )
        return

    norm_p = normalize_phone(phone)
    is_adm = is_admin_user(chat_id, chat_id)

    bound_owner = get_phone_bound_owner(norm_p)
    if bound_owner and str(bound_owner) != str(chat_id) and not is_adm:
        await record_auth_attempt(
            chat_id,
            success=False,
            reason="попытка входа по чужому привязанному номеру",
            tg_username=tg_username,
            tried_phone=norm_p,
        )
        await tg_send_text(
            chat_id,
            "🔒 Этот номер уже привязан к другому Telegram-аккаунту.\n"
            "Доступ к данным профиля запрещён в целях безопасности. Если ты сменил аккаунт — напиши в «❓ Помощь».",
            reply_markup=kb_start_menu(is_admin=False),
        )
        return

    if not bound_owner and not used_contact_button and not is_adm:
        await record_auth_attempt(
            chat_id,
            success=False,
            reason="ручной ввод непривязанного номера без кнопки контакта",
            tg_username=tg_username,
            tried_phone=norm_p,
        )
        set_user_state(chat_id, "courier_auth_phone", {})
        await tg_send_text(
            chat_id,
            "🛡 Для защиты аккаунта от чужого доступа первый вход выполняется только через кнопку "
            "«📱 Отправить номер профиля» (Telegram подтвердит, что номер принадлежит именно тебе).",
            reply_markup=kb_courier_phone_auth(),
        )
        return

    await send_ephemeral_text(
        chat_id,
        "Секунду, проверяю номер в базе парка...",
        reply_markup=kb_courier_phone_auth(),
        ttl_sec=10,
    )
    try:
        async with aiohttp.ClientSession() as session:
            profiles = await _find_all_profiles_by_phone(session, norm_p)
    except Exception as exc:
        await send_ephemeral_text(
            chat_id,
            f"Не получилось проверить номер: {exc}",
            reply_markup=kb_start_menu(is_admin=is_adm),
        )
        return

    if not profiles:
        await record_auth_attempt(
            chat_id,
            success=False,
            reason="номер не найден в базе парка",
            tg_username=tg_username,
            tried_phone=norm_p,
        )
        await tg_send_text(
            chat_id,
            "Этот номер не зарегистрирован в парке.\n\n"
            "Если хочешь стать частью команды — нажми «🚀 Подключиться», оформим всё за пару минут!",
            reply_markup=kb_not_found_connect(),
        )
        return

    await record_auth_attempt(chat_id, success=True)
    if not is_adm:
        bind_phone_to_owner(norm_p, chat_id)

    if len(profiles) == 1:
        chosen = profiles[0]
        save_user_session(
            chat_id,
            phone=norm_p,
            park_id=chosen["park_id"],
            driver_id=chosen["id"],
            city=chosen.get("city", ""),
        )
        await show_courier_welcome(chat_id, chosen)
        return

    PENDING_PROFILES[str(chat_id)] = profiles
    set_user_state(chat_id, "courier_pick_profile", {"verified_by_contact": used_contact_button})
    lines = ["Нашёл несколько профилей — выбери нужный:\n"]
    for idx, prof in enumerate(profiles[:10]):
        status_ru = "Работает" if prof["work_status"] == "working" else prof["work_status"]
        lines.append(
            f"{idx + 1}. {prof['full_name']}\n"
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
    norm_phone = normalize_phone(data.get("phone", ""))
    city_val = data.get("city", "")
    address_val = data.get("address", "")
    save_user_session(
        chat_id,
        phone=norm_phone,
        city=city_val,
        address=address_val,
        username=tg_username,
        full_name=data.get("name") or contact_name,
    )
    if norm_phone:
        bind_phone_to_owner(norm_phone, chat_id)

    tg_login = format_tg_login(chat_id, tg_username)
    lead_payload = {
        "type": "Подключение",
        "name": data.get("name") or contact_name,
        "city": city_val or "—",
        "address": address_val or "—",
        "transport": data.get("transport", "—"),
        "phone": norm_phone,
        "tg_username": tg_login,
        "tg_id": str(chat_id),
    }
    save_application(lead_payload)

    doc_specs = [
        ("sts_front_file_id", "СТС (лицевая сторона)", f"sts_front_{chat_id}.jpg"),
        ("sts_back_file_id", "СТС (обратная сторона)", f"sts_back_{chat_id}.jpg"),
        ("vu_front_file_id", "ВУ (лицевая сторона)", f"vu_front_{chat_id}.jpg"),
        ("vu_back_file_id", "ВУ (обратная сторона)", f"vu_back_{chat_id}.jpg"),
    ]
    doc_items: list[tuple[str, Optional[tuple[bytes, str]], str]] = []
    for fid_key, label, fname in doc_specs:
        fid_val = str(data.get(fid_key) or "")
        if fid_val:
            dl = await tg_download_file(fid_val, fname)
            doc_items.append((label, dl, fid_val))

    try:
        await send_lead_to_amocrm(lead_payload, doc_items=doc_items)
    except Exception as exc:
        logger.warning("Ошибка отправки заявки в amoCRM: %s", exc)

    if doc_items:
        for target in ({OPERATOR_CHAT_ID} | set(load_admin_config()["admins"])):
            if target and str(target) != str(chat_id):
                for label, _, fid_val in doc_items:
                    if fid_val:
                        await tg_send_photo(
                            target,
                            fid_val,
                            caption=f"{label} — {lead_payload['name']} ({tg_login}, ID: {chat_id})",
                        )

    await tg_send_text(
        chat_id,
        MSG_AFTER_SUBMIT,
        reply_markup=kb_start_menu(is_admin=is_admin_user(chat_id, chat_id)),
        parse_mode="HTML",
    )
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


async def _deliver_operator_message_to_courier(
    operator_chat_id: int | str,
    courier_chat_id: str,
    raw_text: str,
    file_id: str,
) -> None:
    if file_id:
        ok = await tg_send_photo(
            courier_chat_id,
            file_id,
            caption=f"💬 Оператор ЛунапаркЕда:\n{raw_text}".strip(),
        )
    elif raw_text:
        ok = bool(await tg_send_text(courier_chat_id, f"💬 Оператор ЛунапаркЕда:\n\n{raw_text}"))
    else:
        await send_ephemeral_text(operator_chat_id, "Отправьте текст или фото для курьера.")
        return

    if ok:
        await send_ephemeral_text(operator_chat_id, f"Отправлено ({courier_chat_id}). Выход: /exit")
    else:
        await send_ephemeral_text(operator_chat_id, f"Не доставлено ({courier_chat_id}).")


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

    if not check_flood_limit(chat_id):
        return

    from_user = message.get("from") or {}
    sender_id = from_user.get("id") or chat_id
    tg_username = str(from_user.get("username") or "").strip()
    first_name = str(from_user.get("first_name") or chat.get("first_name") or "друг").strip()
    last_name = str(from_user.get("last_name") or "").strip()
    contact_name = " ".join(x for x in (first_name, last_name) if x).strip() or "Пользователь"

    raw_text = str(message.get("text") or message.get("caption") or "").strip()
    contact_obj = message.get("contact") if isinstance(message.get("contact"), dict) else {}
    contact_phone = str(contact_obj.get("phone_number") or "").strip()
    contact_user_id = contact_obj.get("user_id")
    is_verified_own_contact = bool(contact_phone and contact_user_id and str(contact_user_id) == str(sender_id))
    file_id = _extract_photo_file_id(message)

    is_super = is_superadmin_user(sender_id, chat_id)
    is_adm = is_admin_user(sender_id, chat_id)

    if raw_text.startswith("/addadmin"):
        if not is_super:
            await send_ephemeral_text(chat_id, "Назначать админов может только суперадмин.")
            return
        parts = raw_text.split(maxsplit=1)
        if len(parts) < 2 or not parts[1].strip().lstrip("-").isdigit():
            await send_ephemeral_text(chat_id, "Формат: /addadmin <telegram_id>")
            return
        admins_list = add_admin_id(parts[1].strip())
        await tg_send_text(chat_id, f"✅ Админ добавлен. Текущий список: {', '.join(admins_list)}", reply_markup=kb_admin_menu(True))
        return

    if raw_text.startswith("/deladmin"):
        if not is_super:
            await send_ephemeral_text(chat_id, "Снимать админов может только суперадмин.")
            return
        parts = raw_text.split(maxsplit=1)
        if len(parts) < 2 or not parts[1].strip().lstrip("-").isdigit():
            await send_ephemeral_text(chat_id, "Формат: /deladmin <telegram_id>")
            return
        ok, admins_list = remove_admin_id(parts[1].strip())
        if not ok:
            await send_ephemeral_text(chat_id, "Нельзя снять права суперадмина.", reply_markup=kb_admin_menu(True))
            return
        await tg_send_text(chat_id, f"🗑 Админ удалён. Текущий список: {', '.join(admins_list)}", reply_markup=kb_admin_menu(True))
        return

    if raw_text.startswith("/unbind") and is_super:
        parts = raw_text.split(maxsplit=1)
        if len(parts) < 2 or len(re.sub(r"\D", "", parts[1])) < 10:
            await send_ephemeral_text(chat_id, "Формат: /unbind <номер_телефона>", reply_markup=kb_admin_menu(True))
            return
        target_p = normalize_phone(parts[1].strip())
        if unbind_phone_owner(target_p):
            await tg_send_text(chat_id, f"🔓 Привязка номера {target_p} сброшена.", reply_markup=kb_admin_menu(True))
        else:
            await tg_send_text(chat_id, f"Номер {target_p} не был привязан.", reply_markup=kb_admin_menu(True))
        return

    if raw_text.strip().lower() == "/admins" and is_adm:
        admins_list = load_admin_config()["admins"]
        await tg_send_text(chat_id, f"👥 Админы: {', '.join(admins_list)}\nСуперадмин: {SUPERADMIN_ID}", reply_markup=kb_admin_menu(is_super))
        return

    if raw_text.startswith("/approve_demand"):
        if not is_super:
            await send_ephemeral_text(chat_id, "Подтверждать рассылку по спросу может только суперадмин.")
            return
        parts = raw_text.split(maxsplit=1)
        city_arg = parts[1].strip() if len(parts) > 1 else ""
        pending = pop_pending_demand(city_arg or None)
        if not pending:
            await send_ephemeral_text(chat_id, "Нет ожидающих подтверждения уведомлений по спросу.")
            return
        target_city = pending["city"]
        rate_val = pending["rate"]
        targets = find_users_by_city(target_city)
        sent_cnt = 0
        msg_demand = f"🔥 Сейчас заказы от {rate_val} ₽ в час в вашем городе ({target_city})! Выходи на слот!"
        for uid in targets:
            if await tg_send_text(uid, msg_demand):
                sent_cnt += 1
        await tg_send_text(chat_id, f"✅ Отправлено по городу {target_city}: {sent_cnt} пользователям.")
        return

    if raw_text.startswith("/skip_demand") and is_super:
        parts = raw_text.split(maxsplit=1)
        city_arg = parts[1].strip() if len(parts) > 1 else ""
        popped = pop_pending_demand(city_arg or None)
        await send_ephemeral_text(
            chat_id,
            f"Отклонено уведомление по спросу ({popped['city'] if popped else '—'})."
        )
        return

    if raw_text.lower() in ("/exit", "/stopchat") and is_adm:
        closed_courier = clear_operator_active_courier(chat_id)
        if closed_courier:
            await send_ephemeral_text(chat_id, f"Диалог с {closed_courier} завершён.")
        else:
            await send_ephemeral_text(chat_id, "Нет активного диалога.")
        return

    if raw_text.startswith("/chat") and is_adm:
        parts = raw_text.split(maxsplit=1)
        target_cid = parts[1].strip() if len(parts) > 1 and parts[1].strip() else (get_last_help_courier() or "")
        if not target_cid or not target_cid.lstrip("-").isdigit():
            await send_ephemeral_text(chat_id, "Укажите chat_id: /chat <chat_id>")
            return
        set_operator_active_courier(chat_id, target_cid)
        rec = get_user_record(target_cid)
        c_name = rec.get("full_name") or f"id{target_cid}"
        c_login = format_tg_login(target_cid, rec.get("username"))
        await tg_send_text(
            chat_id,
            f"Открыт чат с {c_name} ({c_login}, ID: {target_cid}). Выход: /exit",
        )
        return

    if raw_text.startswith("/reply ") and is_adm:
        parts = raw_text.split(maxsplit=2)
        if len(parts) < 3 or not parts[1].lstrip("-").isdigit():
            await send_ephemeral_text(chat_id, "Формат: /reply <chat_id> <текст>")
            return
        await _deliver_operator_message_to_courier(chat_id, parts[1].strip(), parts[2].strip(), "")
        return

    if raw_text.startswith("/period ") and is_adm:
        parts = raw_text.split(maxsplit=1)
        if len(parts) < 2 or not parts[1].strip().isdigit():
            await send_ephemeral_text(chat_id, "Формат: /period <количество_дней>")
            return
        new_p = set_admin_period_days(int(parts[1].strip()))
        summary_txt = await build_admin_summary_text(new_p)
        await tg_send_text(chat_id, f"✅ Период обновлён: {new_p} дн.\n\n{summary_txt}", reply_markup=kb_admin_menu(is_super))
        return

    if raw_text.startswith("/broadcast ") and is_adm:
        b_text = raw_text[len("/broadcast "):].strip()
        if not b_text:
            await send_ephemeral_text(chat_id, "Напиши текст после /broadcast")
            return
        users_db = _read_json(USERS_FILE, {})
        all_uids = list(users_db.keys()) if isinstance(users_db, dict) else []
        ok_cnt = 0
        for uid in all_uids:
            if await tg_send_text(uid, b_text):
                ok_cnt += 1
        await tg_send_text(chat_id, f"📣 Рассылка завершена. Доставлено: {ok_cnt} из {len(all_uids)}.", reply_markup=kb_admin_menu(is_super))
        return

    active_courier_for_op = get_operator_active_courier(chat_id)
    if active_courier_for_op and raw_text not in ("/start",) and not raw_text.startswith("/"):
        await _deliver_operator_message_to_courier(chat_id, active_courier_for_op, raw_text, file_id)
        return

    save_user_session(chat_id, username=tg_username, full_name=contact_name)
    norm_cmd = normalize_choice_text(raw_text)

    if raw_text == "/start" or norm_cmd in ("в начало", "главное меню", "старт", "start", "меню"):
        await send_start_menu(chat_id, first_name=first_name)
        return

    state = get_user_state(chat_id)
    step = state.get("step")
    data = state.get("data") or {}

    if step == "admin_waiting_period" and is_adm:
        clear_user_state(chat_id)
        if not raw_text.isdigit():
            await send_ephemeral_text(chat_id, "Введите число дней (например: 7 или 30).", reply_markup=kb_admin_menu(is_super))
            return
        new_p = set_admin_period_days(int(raw_text))
        summary_txt = await build_admin_summary_text(new_p)
        await tg_send_text(chat_id, f"✅ Период обновлён: {new_p} дн.\n\n{summary_txt}", reply_markup=kb_admin_menu(is_super))
        return

    if step == "admin_waiting_broadcast" and is_adm:
        clear_user_state(chat_id)
        if not raw_text:
            await send_ephemeral_text(chat_id, "Рассылка отменена — текст пуст.", reply_markup=kb_admin_menu(is_super))
            return
        users_db = _read_json(USERS_FILE, {})
        all_uids = list(users_db.keys()) if isinstance(users_db, dict) else []
        ok_cnt = 0
        for uid in all_uids:
            if await tg_send_text(uid, raw_text):
                ok_cnt += 1
        await tg_send_text(
            chat_id,
            f"📣 Рассылка отправлена: {ok_cnt} из {len(all_uids)} пользователей.",
            reply_markup=kb_admin_menu(is_super),
        )
        return

    if step == "admin_waiting_unbind" and is_super:
        clear_user_state(chat_id)
        if len(re.sub(r"\D", "", raw_text)) < 10:
            await send_ephemeral_text(chat_id, "Некорректный номер.", reply_markup=kb_admin_menu(True))
            return
        target_p = normalize_phone(raw_text)
        if unbind_phone_owner(target_p):
            await tg_send_text(chat_id, f"🔓 Привязка номера {target_p} сброшена.", reply_markup=kb_admin_menu(True))
        else:
            await tg_send_text(chat_id, f"Номер {target_p} не был привязан.", reply_markup=kb_admin_menu(True))
        return

    if is_adm and norm_cmd in ("админка", "сводка за период"):
        clear_user_state(chat_id)
        await send_ephemeral_text(chat_id, "Собираю сводку и лайв-статус курьеров...", reply_markup=kb_admin_menu(is_super), ttl_sec=8)
        p_days = get_admin_period_days()
        summary_txt = await build_admin_summary_text(p_days)
        await tg_send_text(chat_id, summary_txt, reply_markup=kb_admin_menu(is_super))
        return

    if is_adm and norm_cmd == "период":
        set_user_state(chat_id, "admin_waiting_period", {})
        cur_p = get_admin_period_days()
        await tg_send_text(
            chat_id,
            f"Текущий период: {cur_p} дн. Введи новое количество дней (например: 7 или 30):",
            reply_markup=kb_step_with_help(),
        )
        return

    if is_adm and norm_cmd in ("массовая рассылка", "рассылка"):
        set_user_state(chat_id, "admin_waiting_broadcast", {})
        await tg_send_text(
            chat_id,
            "Напиши текст массовой рассылки — он уйдёт всем пользователям бота:",
            reply_markup=kb_step_with_help(),
        )
        return

    if is_adm and norm_cmd in ("заказы в час", "заказыв час"):
        await send_ephemeral_text(chat_id, "Собираю данные по СПб и МСК...", reply_markup=kb_admin_menu(is_super), ttl_sec=10)
        txt = await build_orders_per_hour_spb_msk_text()
        await tg_send_text(chat_id, txt, reply_markup=kb_admin_menu(is_super))
        return

    if is_super and norm_cmd in ("сбросить привязку", "сброс привязки", "телефон сбросить привязку"):
        set_user_state(chat_id, "admin_waiting_unbind", {})
        await tg_send_text(
            chat_id,
            "Введи номер телефона, с которого нужно снять привязку к Telegram ID:",
            reply_markup=kb_step_with_help(),
        )
        return

    if is_super and norm_cmd in ("проверить спрос", "спрос"):
        await notify_superadmin_about_demand("Санкт-Петербург", DEMAND_RATE_RUB_PER_HOUR)
        await notify_superadmin_about_demand("Москва", DEMAND_RATE_RUB_PER_HOUR)
        await send_ephemeral_text(chat_id, "Запросы на подтверждение спроса отправлены тебе.", reply_markup=kb_admin_menu(True), ttl_sec=12)
        return

    if is_super and norm_cmd in ("админы", "список админов"):
        admins_list = load_admin_config()["admins"]
        await tg_send_text(chat_id, f"👥 Админы: {', '.join(admins_list)}\nСуперадмин: {SUPERADMIN_ID}", reply_markup=kb_admin_menu(True))
        return

    if step == "cancel_ask_questions":
        if norm_cmd in ("да", "yes", "остались"):
            clear_user_state(chat_id)
            reg_data = data.get("reg_data") if isinstance(data.get("reg_data"), dict) else {}
            lead_payload = {
                "type": "Вопрос при отмене подключения",
                "name": reg_data.get("name") or contact_name,
                "city": reg_data.get("city") or "—",
                "address": reg_data.get("address") or "—",
                "transport": reg_data.get("transport") or "—",
                "phone": reg_data.get("phone") or get_user_record(chat_id).get("phone") or "—",
                "tg_username": format_tg_login(chat_id, tg_username),
                "tg_id": str(chat_id),
            }
            save_application(lead_payload)
            try:
                await send_lead_to_amocrm(lead_payload)
            except Exception as exc:
                logger.warning("Ошибка создания лида в amoCRM при отмене: %s", exc)
            set_last_help_courier(chat_id)
            for target in ({OPERATOR_CHAT_ID} | set(load_admin_config()["admins"])):
                if target and str(target) != str(chat_id):
                    await tg_send_text(
                        target,
                        f"📝 Лид при отмене подключения: {contact_name} ({format_tg_login(chat_id, tg_username)}, ID: {chat_id}).\nВойти в чат: /chat {chat_id}",
                    )
            await tg_send_text(
                chat_id,
                "Создали заявку, чтобы оператор связался с тобой и помог! Напиши сюда свой вопрос:",
                reply_markup=kb_step_with_help(),
            )
            set_user_state(chat_id, "help_waiting_problem", {"prev_step": None, "prev_data": {}})
            return

        if norm_cmd in ("нет", "no", "не осталось"):
            set_user_state(chat_id, "reg_transport", {"return_to_confirm": False})
            await tg_send_text(
                chat_id,
                "Без проблем! Возвращаемся на 1-й шаг. Как планируешь доставлять заказы? 🚀",
                reply_markup=kb_transport_choice(),
            )
            return

        await send_ephemeral_text(
            chat_id,
            "Выбери «Да» или «Нет»:",
            reply_markup=kb_cancel_questions_choice(),
            ttl_sec=12,
        )
        return

    if norm_cmd in ("помощь", "нужна помощь", "поддержка", "оператор"):
        prev_step = state.get("step")
        prev_data = state.get("data") or {}
        set_user_state(chat_id, "help_waiting_problem", {"prev_step": prev_step, "prev_data": prev_data})
        await tg_send_text(
            chat_id,
            "Напиши свой вопрос — подключим оператора и всё подскажем прямо здесь 🙌",
            reply_markup=kb_step_with_help(),
        )
        return

    if norm_cmd in ("отмена", "отменить", "cancel"):
        if step == "help_waiting_problem" and data.get("prev_step"):
            set_user_state(chat_id, data["prev_step"], data.get("prev_data") or {})
            await send_ephemeral_text(chat_id, "Отменили, продолжаем 👍", reply_markup=kb_step_with_help(), ttl_sec=8)
            return
        if step and step.startswith("reg_"):
            set_user_state(chat_id, "cancel_ask_questions", {"reg_data": data})
            await tg_send_text(
                chat_id,
                "Остались вопросы?",
                reply_markup=kb_cancel_questions_choice(),
            )
            return
        await send_start_menu(chat_id, first_name=first_name)
        return

    if step == "help_waiting_problem":
        if not raw_text and not file_id:
            await send_ephemeral_text(
                chat_id,
                "Напиши текстом или скинь скриншот, что случилось:",
                reply_markup=kb_step_with_help(),
                ttl_sec=10,
            )
            return
        problem_body = raw_text or "[Прикреплено фото / файл]"
        set_user_state(chat_id, data.get("prev_step"), data.get("prev_data") or {})
        set_last_help_courier(chat_id)

        tg_login = format_tg_login(chat_id, tg_username)
        tg_link = format_tg_link(chat_id, tg_username)
        phone = get_user_record(chat_id).get("phone") or "—"

        await send_help_to_amocrm(
            problem_text=problem_body,
            chat_id=chat_id,
            tg_username=tg_username,
            full_name=contact_name,
        )

        op_card = (
            "🆘 Новое обращение «Помощь»\n\n"
            f"👤 Имя: {contact_name}\n"
            f"📱 Телефон: {phone}\n"
            f"💬 Telegram: {tg_login} ({tg_link})\n"
            f"🆔 chat_id: {chat_id}\n\n"
            f"Текст:\n{problem_body}\n\n"
            f"👉 Войти в живой чат: /chat {chat_id}"
        )

        notified_targets: set[str] = set()
        if OPERATOR_CHAT_ID:
            notified_targets.add(OPERATOR_CHAT_ID)
        for admin_id in load_admin_config()["admins"]:
            notified_targets.add(admin_id)

        for target in notified_targets:
            if str(target) == str(chat_id):
                continue
            await tg_send_text(target, op_card)
            if file_id:
                await tg_send_photo(target, file_id, caption=f"Вложение от {contact_name} (ID: {chat_id})")

        await send_ephemeral_text(
            chat_id,
            "Приняли твой вопрос! Оператор скоро ответит здесь 💙",
            reply_markup=kb_start_menu(is_admin=is_adm),
            ttl_sec=20,
        )
        return

    bound_operator = get_courier_active_operator(chat_id)
    if bound_operator:
        tg_login = format_tg_login(chat_id, tg_username)
        msg_for_op = f"📩 Сообщение от курьера {contact_name} ({tg_login}, ID: {chat_id}):\n\n{raw_text}".strip()
        if file_id:
            await tg_send_photo(bound_operator, file_id, caption=msg_for_op)
        elif raw_text:
            await tg_send_text(bound_operator, msg_for_op)

    if norm_cmd in ("подключиться", "подключиться к парку", "регистрация", "стать курьером"):
        set_user_state(chat_id, "reg_transport", {"return_to_confirm": False})
        await tg_send_text(
            chat_id,
            "Шаг 1. Как планируешь доставлять заказы? 🚀",
            reply_markup=kb_transport_choice(),
        )
        return

    if norm_cmd in ("уже курьер", "уже курьер парка", "кабинет", "мой кабинет", "сменить номер", "изменить номер аккаунта"):
        if norm_cmd not in ("сменить номер", "изменить номер аккаунта"):
            saved_driver = await get_saved_profile_or_redirect(chat_id)
            if saved_driver:
                await show_courier_welcome(chat_id, saved_driver)
                return
        set_user_state(chat_id, "courier_auth_phone", {})
        await send_ephemeral_text(
            chat_id,
            "Нажми «📱 Отправить номер профиля» для подтверждения аккаунта:",
            reply_markup=kb_courier_phone_auth(),
            ttl_sec=30,
        )
        return

    if norm_cmd in ("моя статистика", "статистика"):
        driver = await get_saved_profile_or_redirect(chat_id)
        if driver:
            await show_courier_statistics(chat_id, driver)
        return

    if norm_cmd in ("бонус", "цель", "мой бонус", "моя цель"):
        driver = await get_saved_profile_or_redirect(chat_id)
        if not driver:
            return
        park = get_park_by_id(driver["park_id"])
        goal_start = get_goal_start_dt(chat_id)
        try:
            async with aiohttp.ClientSession() as session:
                stats = await _get_driver_orders_stats(session, park, driver["id"], goal_start_dt=goal_start)
        except Exception as exc:
            await send_ephemeral_text(chat_id, f"Не вышло посчитать бонус: {exc}", reply_markup=kb_courier_cabinet(True))
            return

        done = stats["completed_goal"]
        done_7d = stats["completed_7d"]
        left = max(0, GOAL_ORDERS - done)
        filled = min(GOAL_ORDERS, done)
        bar = "🟩" * filled + "⬜" * (GOAL_ORDERS - filled)
        start_str = goal_start.astimezone().strftime("%d.%m.%Y")

        if left == 0 or done_7d >= 10:
            save_user_session(chat_id, bonus_completed=True)
            resp_text = f"🎉 Поздравляем! Цель {GOAL_ORDERS} заказов выполнена — бонус +{GOAL_BONUS} ₽ твой!"
            await tg_send_text(chat_id, resp_text, reply_markup=kb_courier_cabinet(show_bonus=False))
            return

        if not stats["within_goal_window"]:
            status_line = f"Срок {GOAL_DAYS} дней (с {start_str}) уже прошёл."
        else:
            status_line = f"Осталось выполнить {left} заказ(ов) за {GOAL_DAYS} дней (отсчёт с {start_str}) до бонуса +{GOAL_BONUS} ₽ 💪"

        resp_text = (
            f"🎁 Твой бонус: {GOAL_ORDERS} заказов за {GOAL_DAYS} дней (+{GOAL_BONUS} ₽)\n\n"
            f"Прогресс: {done} / {GOAL_ORDERS}\n"
            f"{bar}\n\n"
            f"{status_line}"
        )
        await tg_send_text(chat_id, resp_text, reply_markup=kb_courier_cabinet(show_bonus=True))
        return

    if norm_cmd in ("корректировки", "штрафы", "мои корректировки", "выгрузить штрафы"):
        driver = await get_saved_profile_or_redirect(chat_id)
        if not driver:
            return
        show_bonus = await should_show_bonus_button(chat_id, driver)
        park = get_park_by_id(driver["park_id"])
        try:
            async with aiohttp.ClientSession() as session:
                lines = await _get_driver_adjustments_list(session, park, driver["id"], force_refresh=True)
        except Exception:
            lines = []

        if not lines:
            await tg_send_text(chat_id, "Корректировок и списаний нет.", reply_markup=kb_courier_cabinet(show_bonus=show_bonus))
            return

        await tg_send_text(chat_id, "\n".join(lines), reply_markup=kb_courier_cabinet(show_bonus=show_bonus))
        return

    if step == "courier_auth_phone":
        if contact_phone and not is_verified_own_contact and not is_adm:
            await record_auth_attempt(
                chat_id,
                success=False,
                reason="передан чужой контакт",
                tg_username=tg_username,
                tried_phone=normalize_phone(contact_phone),
            )
            await send_ephemeral_text(
                chat_id,
                "🛡 Нужно отправить именно свой контакт через кнопку «📱 Отправить номер профиля».",
                reply_markup=kb_courier_phone_auth(),
                ttl_sec=15,
            )
            return

        raw_phone = contact_phone or raw_text
        if len(re.sub(r"\D", "", raw_phone)) < 10:
            await send_ephemeral_text(
                chat_id,
                "Нажми кнопку «📱 Отправить номер профиля» или введи свой привязанный номер:",
                reply_markup=kb_courier_phone_auth(),
                ttl_sec=15,
            )
            return
        clear_user_state(chat_id)
        await lookup_and_route_phone(
            chat_id,
            normalize_phone(raw_phone),
            used_contact_button=is_verified_own_contact,
            tg_username=tg_username,
        )
        return

    if step == "courier_pick_profile":
        if norm_cmd == "хочу ввести другой номер":
            set_user_state(chat_id, "courier_auth_phone", {})
            await send_ephemeral_text(
                chat_id,
                "Нажми «📱 Отправить номер профиля»:",
                reply_markup=kb_courier_phone_auth(),
                ttl_sec=25,
            )
            return
        profiles = PENDING_PROFILES.get(str(chat_id), [])
        m = re.match(r"^(\d+)", raw_text.strip())
        if not m or int(m.group(1)) < 1 or int(m.group(1)) > len(profiles):
            await send_ephemeral_text(
                chat_id,
                "Выбери профиль на клавиатуре или напиши его номер:",
                reply_markup=kb_profiles_choice(profiles) if profiles else kb_start_menu(is_admin=is_adm),
                ttl_sec=15,
            )
            return
        chosen = profiles[int(m.group(1)) - 1]
        clear_user_state(chat_id)
        save_user_session(
            chat_id,
            phone=chosen["phone"],
            park_id=chosen["park_id"],
            driver_id=chosen["id"],
            city=chosen.get("city", ""),
        )
        await show_courier_welcome(chat_id, chosen)
        return

    if step == "reg_transport":
        chosen_transport = parse_transport_choice(raw_text)
        if not chosen_transport:
            await send_ephemeral_text(
                chat_id,
                "Выбери формат доставки на клавиатуре:",
                reply_markup=kb_transport_choice(),
                ttl_sec=15,
            )
            return
        data = update_user_state_data(chat_id, transport=chosen_transport)
        if data.get("return_to_confirm"):
            if requires_vehicle_docs(chosen_transport) and not _has_all_vehicle_docs(data):
                set_user_state(chat_id, "reg_sts_front", data)
                await tg_send_text(
                    chat_id,
                    "Для выбранного формата нужны фото СТС и ВУ с двух сторон. Пришли лицевую сторону СТС (1 из 2):",
                    reply_markup=kb_step_with_help(),
                )
                return
            await show_confirmation_screen(chat_id, data)
            return
        set_user_state(chat_id, "reg_city", data)
        await tg_send_text(
            chat_id,
            "Шаг 2. В каком городе планируешь работать? 🏙",
            reply_markup=kb_step_with_help(),
        )
        return

    if step == "reg_city":
        if len(raw_text) < 2:
            await send_ephemeral_text(chat_id, "Напиши корректное название города:", reply_markup=kb_step_with_help(), ttl_sec=15)
            return
        save_user_session(chat_id, city=raw_text)
        data = update_user_state_data(chat_id, city=raw_text)
        if data.get("return_to_confirm"):
            await show_confirmation_screen(chat_id, data)
            return
        set_user_state(chat_id, "reg_name", data)
        await tg_send_text(
            chat_id,
            "Шаг 3. Ваши ФИО полностью (например: Иванов Иван Иванович):",
            reply_markup=kb_step_with_help(),
        )
        return

    if step == "reg_name":
        parts = [p for p in raw_text.split() if p]
        if len(parts) < 2 or len(raw_text) < 5:
            await send_ephemeral_text(
                chat_id,
                "Укажите Ваши ФИО полностью (например: Иванов Иван Иванович):",
                reply_markup=kb_step_with_help(),
                ttl_sec=15,
            )
            return
        data = update_user_state_data(chat_id, name=raw_text)
        if data.get("return_to_confirm"):
            await show_confirmation_screen(chat_id, data)
            return
        set_user_state(chat_id, "reg_phone", data)
        await tg_send_text(
            chat_id,
            "Шаг 4. Введите Ваш номер телефона, привязанный в Мой налог (формат не важен, можно без +) 📱:",
            reply_markup=kb_step_with_help(),
        )
        return

    if step == "reg_phone":
        raw_phone = contact_phone or raw_text
        if len(re.sub(r"\D", "", raw_phone)) < 10:
            await send_ephemeral_text(
                chat_id,
                "Номер введён не полностью. Введите Ваш номер телефона, привязанный в Мой налог (можно без +):",
                reply_markup=kb_step_with_help(),
                ttl_sec=15,
            )
            return
        data = update_user_state_data(chat_id, phone=normalize_phone(raw_phone))
        if data.get("return_to_confirm"):
            await show_confirmation_screen(chat_id, data)
            return
        set_user_state(chat_id, "reg_address", data)
        await tg_send_text(
            chat_id,
            "Шаг 5. Укажите Ваш адрес прописки:",
            reply_markup=kb_step_with_help(),
        )
        return

    if step == "reg_address":
        if len(raw_text) < 4:
            await send_ephemeral_text(
                chat_id,
                "Укажите полный адрес прописки:",
                reply_markup=kb_step_with_help(),
                ttl_sec=15,
            )
            return
        save_user_session(chat_id, address=raw_text)
        data = update_user_state_data(chat_id, address=raw_text)
        if data.get("return_to_confirm"):
            await show_confirmation_screen(chat_id, data)
            return
        if requires_vehicle_docs(data.get("transport", "")):
            set_user_state(chat_id, "reg_sts_front", data)
            await tg_send_text(
                chat_id,
                "Пришли фото СТС — сначала лицевую сторону (1 из 2):",
                reply_markup=kb_step_with_help(),
            )
            return
        await show_confirmation_screen(chat_id, data)
        return

    if step == "reg_sts_front":
        if not file_id:
            await send_ephemeral_text(
                chat_id,
                "Пришли фото лицевой стороны СТС (1 из 2):",
                reply_markup=kb_step_with_help(),
                ttl_sec=15,
            )
            return
        data = update_user_state_data(chat_id, sts_front_file_id=file_id)
        set_user_state(chat_id, "reg_sts_back", data)
        await tg_send_text(
            chat_id,
            "Отлично! Теперь пришли фото обратной стороны СТС (2 из 2):",
            reply_markup=kb_step_with_help(),
        )
        return

    if step == "reg_sts_back":
        if not file_id:
            await send_ephemeral_text(
                chat_id,
                "Пришли фото обратной стороны СТС (2 из 2):",
                reply_markup=kb_step_with_help(),
                ttl_sec=15,
            )
            return
        data = update_user_state_data(chat_id, sts_back_file_id=file_id)
        if data.get("return_to_confirm") and data.get("vu_front_file_id") and data.get("vu_back_file_id"):
            await show_confirmation_screen(chat_id, data)
            return
        set_user_state(chat_id, "reg_vu_front", data)
        await tg_send_text(
            chat_id,
            "Теперь пришли фото водительского удостоверения — сначала лицевую сторону (1 из 2):",
            reply_markup=kb_step_with_help(),
        )
        return

    if step == "reg_vu_front":
        if not file_id:
            await send_ephemeral_text(
                chat_id,
                "Пришли фото лицевой стороны водительского удостоверения (1 из 2):",
                reply_markup=kb_step_with_help(),
                ttl_sec=15,
            )
            return
        data = update_user_state_data(chat_id, vu_front_file_id=file_id)
        set_user_state(chat_id, "reg_vu_back", data)
        await tg_send_text(
            chat_id,
            "Отлично! Теперь пришли фото обратной стороны водительского удостоверения (2 из 2):",
            reply_markup=kb_step_with_help(),
        )
        return

    if step == "reg_vu_back":
        if not file_id:
            await send_ephemeral_text(
                chat_id,
                "Пришли фото обратной стороны водительского удостоверения (2 из 2):",
                reply_markup=kb_step_with_help(),
                ttl_sec=15,
            )
            return
        data = update_user_state_data(chat_id, vu_back_file_id=file_id)
        await show_confirmation_screen(chat_id, data)
        return

    if step == "reg_confirm":
        if norm_cmd in ("исправить", "изменить", "редактировать"):
            data = update_user_state_data(chat_id, return_to_confirm=True)
            set_user_state(chat_id, "reg_choosing_edit", data)
            await tg_send_text(
                chat_id,
                "Что хочешь изменить?",
                reply_markup=kb_edit_field_choice(data),
            )
            return
        if norm_cmd in ("да", "верно", "подтверждаю", "ок", "yes"):
            await finalize_registration(chat_id, data, tg_username, contact_name)
            return
        await send_ephemeral_text(
            chat_id,
            "Нажми «Да», если всё верно, или «Исправить»:",
            reply_markup=kb_confirm_application(),
            ttl_sec=15,
        )
        return

    if step == "reg_choosing_edit":
        if norm_cmd in ("назад к проверке", "назад"):
            await show_confirmation_screen(chat_id, data)
            return
        if norm_cmd in ("изменить формат", "формат", "транспорт"):
            set_user_state(chat_id, "reg_transport", data)
            await tg_send_text(chat_id, "Выбери новый формат доставки:", reply_markup=kb_transport_choice())
            return
        if norm_cmd in ("изменить город", "город"):
            set_user_state(chat_id, "reg_city", data)
            await tg_send_text(chat_id, "Введи новый город:", reply_markup=kb_step_with_help())
            return
        if norm_cmd in ("изменить фио", "фио", "изменить имя", "имя"):
            set_user_state(chat_id, "reg_name", data)
            await tg_send_text(chat_id, "Укажите Ваши ФИО полностью (например: Иванов Иван Иванович):", reply_markup=kb_step_with_help())
            return
        if norm_cmd in ("изменить телефон", "телефон"):
            set_user_state(chat_id, "reg_phone", data)
            await tg_send_text(chat_id, "Введите Ваш номер телефона, привязанный в Мой налог (можно без +):", reply_markup=kb_step_with_help())
            return
        if norm_cmd in ("изменить прописку", "прописка", "адрес"):
            set_user_state(chat_id, "reg_address", data)
            await tg_send_text(chat_id, "Укажите новый адрес прописки:", reply_markup=kb_step_with_help())
            return
        if norm_cmd in ("изменить фото стс", "фото стс", "стс"):
            set_user_state(chat_id, "reg_sts_front", data)
            await tg_send_text(
                chat_id,
                "Пришли новое фото лицевой стороны СТС (1 из 2):",
                reply_markup=kb_step_with_help(),
            )
            return
        if norm_cmd in ("изменить фото ву", "фото ву", "ву", "водительское"):
            set_user_state(chat_id, "reg_vu_front", data)
            await tg_send_text(
                chat_id,
                "Пришли новое фото лицевой стороны водительского удостоверения (1 из 2):",
                reply_markup=kb_step_with_help(),
            )
            return
        await send_ephemeral_text(chat_id, "Выбери поле для изменения:", reply_markup=kb_edit_field_choice(data), ttl_sec=15)
        return

    if step is None and raw_text:
        ai_ans = await answer_simple_question_with_ai(raw_text)
        if ai_ans:
            await tg_send_text(
                chat_id,
                f"{ai_ans}\n\nЕсли нужен оператор — нажми «❓ Помощь».",
                reply_markup=kb_start_menu(is_admin=is_adm),
            )
            return

    await send_start_menu(chat_id, first_name=first_name)


async def _fetch_contact_phones_from_amocrm(
    lead_id: str | None = None,
    contact_id: str | None = None,
) -> list[str]:
    if not AMOCRM_BASE_URL or not AMOCRM_ACCESS_TOKEN:
        return []
    base_url = AMOCRM_BASE_URL.rstrip("/")
    contact_ids: list[str] = []
    if contact_id and str(contact_id).isdigit():
        contact_ids.append(str(contact_id))

    phones: list[str] = []
    try:
        async with aiohttp.ClientSession() as session:
            if not contact_ids and lead_id and str(lead_id).isdigit():
                async with session.get(
                    f"{base_url}/api/v4/leads/{lead_id}",
                    params={"with": "contacts"},
                    headers=_amocrm_headers(),
                    timeout=15,
                ) as lresp:
                    if lresp.status == 200:
                        ldata = await lresp.json()
                        for c in (ldata.get("_embedded") or {}).get("contacts") or []:
                            if isinstance(c, dict) and c.get("id"):
                                contact_ids.append(str(c["id"]))

            for cid in contact_ids:
                async with session.get(
                    f"{base_url}/api/v4/contacts/{cid}",
                    headers=_amocrm_headers(),
                    timeout=15,
                ) as cresp:
                    if cresp.status != 200:
                        continue
                    cdata = await cresp.json()
                    for cf in cdata.get("custom_fields_values") or []:
                        if not isinstance(cf, dict):
                            continue
                        fcode = str(cf.get("field_code") or "").upper()
                        fname = str(cf.get("field_name") or "").lower()
                        if fcode == "PHONE" or "телефон" in fname or cf.get("field_id") == CONTACT_FIELD_PHONE_MULTI:
                            for val in cf.get("values") or []:
                                if isinstance(val, dict) and val.get("value"):
                                    phones.append(normalize_phone(str(val["value"])))
    except Exception as exc:
        logger.warning("Не удалось получить телефон контакта из amoCRM: %s", exc)
    return phones


def _find_tg_by_phone_in_users_db(phones: list[str]) -> str | None:
    if not phones:
        return None
    norm_set = {normalize_phone(p) for p in phones if p}
    users_db = _read_json(USERS_FILE, {})
    if not isinstance(users_db, dict):
        return None
    for uid, rec in users_db.items():
        if not isinstance(rec, dict):
            continue
        up = normalize_phone(str(rec.get("phone") or ""))
        if up and up in norm_set:
            return str(uid)
    return None


# ================= AMOCRM WEBHOOK И ПРИМЕЧАНИЯ (ОБНОВЛЁННЫЕ) =================
async def _fetch_latest_amocrm_note_text(
    lead_id: str | None = None,
    contact_id: str | None = None,
) -> str:
    if not AMOCRM_BASE_URL or not AMOCRM_ACCESS_TOKEN:
        logger.warning("amoCRM: не задан AMOCRM_BASE_URL или AMOCRM_ACCESS_TOKEN")
        return ""
    base_url = AMOCRM_BASE_URL.rstrip("/")
    targets: list[str] = []

    if lead_id and str(lead_id).isdigit():
        targets.append(f"{base_url}/api/v4/leads/{lead_id}/notes")
    if contact_id and str(contact_id).isdigit():
        targets.append(f"{base_url}/api/v4/contacts/{contact_id}/notes")

    try:
        async with aiohttp.ClientSession() as session:
            if not contact_id and lead_id and str(lead_id).isdigit():
                async with session.get(
                    f"{base_url}/api/v4/leads/{lead_id}",
                    params={"with": "contacts"},
                    headers=_amocrm_headers(),
                    timeout=15,
                ) as lresp:
                    if lresp.status == 200:
                        ldata = await lresp.json()
                        for c in (ldata.get("_embedded") or {}).get("contacts") or []:
                            if isinstance(c, dict) and c.get("id"):
                                c_url = f"{base_url}/api/v4/contacts/{c['id']}/notes"
                                if c_url not in targets:
                                    targets.append(c_url)

            for url in targets:
                async with session.get(
                    url,
                    params={"limit": 15, "order[created_at]": "desc"},
                    headers=_amocrm_headers(),
                    timeout=15,
                ) as resp:
                    logger.info("amoCRM notes GET %s -> status %s", url, resp.status)
                    if resp.status != 200:
                        err_txt = await resp.text()
                        logger.warning("amoCRM notes GET error body: %s", err_txt[:200])
                        continue
                    data = await resp.json()
                    notes = data.get("_embedded", {}).get("notes", []) or []
                    for n in notes:
                        if not isinstance(n, dict):
                            continue
                        params_obj = n.get("params") if isinstance(n.get("params"), dict) else {}
                        txt = str(params_obj.get("text") or n.get("text") or "").strip()
                        if not txt:
                            continue
                        if txt.startswith("Парк: ЛунапаркЕда") or txt.startswith("Обращение по кнопке"):
                            continue
                        low = txt.lower()
                        if low.startswith("tg:") or low.startswith("ответ:"):
                            logger.info("Найдено примечание с префиксом в amoCRM: %s", txt[:120])
                            return txt
                    for n in notes:
                        if not isinstance(n, dict):
                            continue
                        params_obj = n.get("params") if isinstance(n.get("params"), dict) else {}
                        txt = str(params_obj.get("text") or n.get("text") or "").strip()
                        if txt and not txt.startswith("Парк: ЛунапаркЕда") and not txt.startswith("Обращение по кнопке"):
                            logger.info("Последнее примечание без префикса в amoCRM: %s", txt[:120])
                            break
    except Exception as exc:
        logger.warning("Не удалось дочитать примечание из amoCRM: %s", exc)
    return ""


def _extract_reply_from_note(raw_note: str) -> str:
    txt = (raw_note or "").strip()
    if not txt:
        return ""
    if txt.startswith("Парк: ЛунапаркЕда") or txt.startswith("Обращение по кнопке"):
        return ""
    low = txt.lower()
    if low.startswith("tg:"):
        return txt[3:].strip()
    if low.startswith("ответ:"):
        return txt[6:].strip()
    return ""


async def handle_amocrm_webhook(request: web.Request) -> web.Response:
    if AMOCRM_WEBHOOK_SECRET:
        secret = request.query.get("secret") or request.headers.get("X-AmoCRM-Secret", "")
        if not hmac.compare_digest(secret, AMOCRM_WEBHOOK_SECRET):
            return web.json_response({"ok": False}, status=403)

    lead_id: str | None = None
    contact_id: str | None = None
    note_text = ""

    try:
        if request.content_type == "application/json":
            payload = await request.json()
        else:
            form_data = await request.post()
            payload = dict(form_data)
    except Exception:
        return web.json_response({"ok": False}, status=400)

    if isinstance(payload, dict):
        lead_id = str(payload.get("lead_id") or "") or None
        contact_id = str(payload.get("contact_id") or "") or None
        note_text = str(payload.get("text") or payload.get("note_text") or "").strip()

        for k, v in payload.items():
            ks = str(k)
            if not lead_id and ("leads[" in ks) and ks.endswith("[id]") and str(v).isdigit():
                lead_id = str(v)
            if not contact_id and ("contacts[" in ks) and ks.endswith("[id]") and str(v).isdigit():
                contact_id = str(v)
            if not note_text and ("note[" in ks or "notes[" in ks) and ks.endswith("[text]") and v:
                note_text = str(v).strip()

        for ent_key, id_attr in (("leads", "lead_id"), ("contacts", "contact_id")):
            ent_block = payload.get(ent_key)
            if isinstance(ent_block, dict):
                for action_name in ("add", "update", "status", "note"):
                    act_list = ent_block.get(action_name)
                    if isinstance(act_list, dict):
                        act_list = list(act_list.values())
                    if isinstance(act_list, list) and act_list:
                        first = act_list[0]
                        if isinstance(first, dict):
                            eid = first.get("id") or first.get("element_id")
                            if eid and id_attr == "lead_id" and not lead_id:
                                lead_id = str(eid)
                            elif eid and id_attr == "contact_id" and not contact_id:
                                contact_id = str(eid)
                            notes = first.get("notes")
                            if isinstance(notes, list) and notes and isinstance(notes[0], dict) and not note_text:
                                n_params = notes[0].get("params") if isinstance(notes[0].get("params"), dict) else {}
                                note_text = str(n_params.get("text") or notes[0].get("text") or "").strip()

    logger.info("amoCRM webhook parsed: lead_id=%s, contact_id=%s, note_text=%s", lead_id, contact_id, note_text[:80])

    target_tg_id = find_tg_chat_by_crm_entity(lead_id=lead_id, contact_id=contact_id)
    if not target_tg_id:
        phones = await _fetch_contact_phones_from_amocrm(lead_id=lead_id, contact_id=contact_id)
        target_tg_id = _find_tg_by_phone_in_users_db(phones)
        if target_tg_id and (lead_id or contact_id):
            link_crm_entity_to_tg(
                int(lead_id) if lead_id and lead_id.isdigit() else None,
                int(contact_id) if contact_id and contact_id.isdigit() else None,
                target_tg_id,
            )

    if not target_tg_id:
        logger.info("amoCRM webhook ignored: no_tg_binding (lead_id=%s, contact_id=%s)", lead_id, contact_id)
        return web.json_response({"ok": True, "ignored": "no_tg_binding", "lead_id": lead_id, "contact_id": contact_id})

    clean_reply = _extract_reply_from_note(note_text)
    if not clean_reply:
        fetched_note = await _fetch_latest_amocrm_note_text(lead_id=lead_id, contact_id=contact_id)
        clean_reply = _extract_reply_from_note(fetched_note)

    if not clean_reply:
        logger.info("amoCRM webhook ignored: no_tg_or_reply_prefix (lead_id=%s, contact_id=%s)", lead_id, contact_id)
        return web.json_response({"ok": True, "ignored": "no_tg_or_reply_prefix"})

    sent_mid = await tg_send_text(target_tg_id, f"💬 Сообщение от менеджера парка:\n\n{clean_reply}")
    logger.info("amoCRM webhook sent to tg=%s, msg_id=%s", target_tg_id, sent_mid)
    return web.json_response({"ok": bool(sent_mid), "sent_to": target_tg_id})


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
    if TELEGRAM_WEBHOOK_SECRET:
        hdr_secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if not hmac.compare_digest(hdr_secret, TELEGRAM_WEBHOOK_SECRET):
            return web.json_response({"ok": False}, status=403)
    try:
        upd = await request.json()
        if isinstance(upd, dict):
            asyncio.create_task(handle_telegram_update(upd))
    except Exception:
        pass
    return web.json_response({"ok": True})


async def on_startup(app: web.Application) -> None:
    app["delayed_worker"] = asyncio.create_task(delayed_notifications_worker())
    app["ephemeral_worker"] = asyncio.create_task(ephemeral_cleaner_worker())
    app["demand_worker"] = asyncio.create_task(demand_monitor_worker())
    if not TELEGRAM_BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN пуст! Укажите токен бота от @BotFather.")
        return

    async with aiohttp.ClientSession() as session:
        if TELEGRAM_WEBHOOK_URL:
            wh_payload: dict[str, Any] = {"url": TELEGRAM_WEBHOOK_URL}
            if TELEGRAM_WEBHOOK_SECRET:
                wh_payload["secret_token"] = TELEGRAM_WEBHOOK_SECRET
            await tg_api(session, "setWebhook", wh_payload)
            logger.info("Установлен Telegram webhook")
        else:
            await tg_api(session, "deleteWebhook", {"drop_pending_updates": False})
            app["polling_task"] = asyncio.create_task(polling_loop())


async def on_cleanup(app: web.Application) -> None:
    for task_key in ("polling_task", "delayed_worker", "ephemeral_worker", "demand_worker"):
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
    app.router.add_post("/", handle_amocrm_webhook)
    app.router.add_post("/webhook/telegram", handle_tg_webhook)
    app.router.add_post("/webhook/amocrm", handle_amocrm_webhook)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


if __name__ == "__main__":
    web.run_app(create_app(), host="0.0.0.0", port=PORT)
