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

# ================= КОНФИГУРАЦИЯ (ЧЕРЕЗ ENV ИЛИ ЗНАЧЕНИЯ ПО УМОЛЧАНИЮ) =================
WAZZUP_API_KEY = os.getenv("WAZZUP_API_KEY", "60b72896d7c6490595a9f9f213a9b8ed")
WAZZUP_CHANNEL_ID = os.getenv("WAZZUP_CHANNEL_ID", "1213b290-ce0e-4ff8-abd2-31dcd5956aee")
PUBLIC_WEBHOOK_URL = os.getenv(
    "PUBLIC_WEBHOOK_URL",
    "https://bot-1795419700-8826-lunapark11111.bothost.tech/webhook/wazzup",
)
PORT = int(os.getenv("PORT", "8080"))

WAZZUP_API_BASE = "https://api.wazzup24.com/v3"

# Основной парк (Карасев Павел Максимович)
PARKS_CONFIG: list[dict[str, str]] = [
    {
        "name": "ЛунапаркЕда",
        "park_id": os.getenv("YANDEX_PARK_ID", "1680a13d35084b0688ecab0f925c9d75"),
        "client_id": os.getenv("YANDEX_CLIENT_ID", "taxi/park/1680a13d35084b0688ecab0f925c9d75"),
        "api_key": os.getenv("YANDEX_API_KEY", "yapi10-E5IuB_Gpfog2E7XMXYxTl1SsS3HyVGbXduM9Jg_d1"),
    },
]

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
_PROCESSED_MSG_IDS: dict[str, float] = {}
_FILE_LOCKS: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
_STATS_LOCKS: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

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


def _build_keyboard_payload(
    rows: list[list[str]],
    reply_markup: str = "reply",
    one_time_keyboard: bool = False,
) -> dict[str, Any]:
    buttons_list: list[dict[str, str]] = []
    for r_idx, row in enumerate(rows):
        for c_idx, label in enumerate(row):
            clean_label = str(label).strip()[:64]
            payload_val = f"btn_{r_idx}_{c_idx}"
            buttons_list.append({
                "text": clean_label,
                "payload": payload_val,
            })
    return {
        "reply_markup": reply_markup,
        "one_time_keyboard": one_time_keyboard,
        "buttons": buttons_list[:12],
    }


def kb_start_menu() -> dict[str, Any]:
    return _build_keyboard_payload(
        [
            ["🚀 Подключиться к парку", "✅ Уже курьер парка"],
            ["❓ Нужна помощь"],
        ],
        reply_markup="reply",
    )


def kb_courier_cabinet() -> dict[str, Any]:
    return _build_keyboard_payload(
        [
            ["📊 Моя статистика", "🤔 Я на линии?"],
            ["🎁 Цель", "❓ Нужна помощь"],
            ["🔄 Сменить номер", "⬅️ В начало"],
        ],
        reply_markup="reply",
    )


def kb_cancel_only() -> dict[str, Any]:
    return _build_keyboard_payload([["❌ Отмена"]], reply_markup="reply")


def kb_transport_choice() -> dict[str, Any]:
    return _build_keyboard_payload(
        [
            ["🚶 Пеший / Вело", "🛵 Мото"],
            ["🚗 Авто", "🚚 Грузовой"],
            ["❌ Отмена"],
        ],
        reply_markup="reply",
    )


def kb_confirm_application() -> dict[str, Any]:
    return _build_keyboard_payload(
        [["Да", "Исправить"], ["❌ Отмена"]],
        reply_markup="reply",
    )


def kb_edit_field_choice(data: dict[str, Any]) -> dict[str, Any]:
    rows: list[list[str]] = [
        ["Изменить формат", "Изменить город"],
        ["Изменить имя", "Изменить телефон"],
    ]
    if requires_two_sided_sts(data.get("transport", "")):
        rows.append(["Изменить фото СТС"])
    rows.append(["Назад к проверке", "❌ Отмена"])
    return _build_keyboard_payload(rows, reply_markup="reply")


def kb_profiles_choice(profiles: list[dict[str, Any]]) -> dict[str, Any]:
    rows: list[list[str]] = []
    for idx, prof in enumerate(profiles[:10]):
        rows.append([f"{idx + 1}. {prof['park_name']}"])
    rows.append(["Хочу ввести другой номер", "❌ Отмена"])
    return _build_keyboard_payload(rows, reply_markup="reply")


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


def requires_two_sided_sts(transport_text: str) -> bool:
    low = (transport_text or "").lower()
    return any(k in low for k in STS_TRANSPORT_KEYWORDS)


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


def get_user_record(chat_id: str) -> dict[str, Any]:
    db = _read_json(USERS_FILE, {})
    rec = db.get(str(chat_id))
    return rec if isinstance(rec, dict) else {}


def get_registered_dt(chat_id: str) -> datetime:
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


def save_client_lead(chat_id: str, lead_id: int) -> None:
    data = load_client_leads()
    data[str(chat_id)] = int(lead_id)
    _write_json(CLIENT_LEADS_FILE, data)


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
    logger.info("Запущен фоновый воркер отложенных уведомлений (10 мин)")
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
                                keyboard=kb_start_menu(),
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


def _wazzup_headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {WAZZUP_API_KEY.strip()}",
        "Content-Type": "application/json",
    }


async def _post_wazzup_message(
    session: aiohttp.ClientSession,
    ch: str,
    chat_id: str,
    text: str,
    keyboard: dict[str, Any] | None,
    clear_unanswered: bool,
) -> tuple[bool, int, str]:
    payload: dict[str, Any] = {
        "channelId": ch,
        "chatType": "telegram",
        "chatId": str(chat_id),
        "text": text,
        "crmMessageId": str(uuid.uuid4()),
        "clearUnanswered": clear_unanswered,
    }
    if keyboard:
        payload["keyboard"] = keyboard

    async with session.post(
        f"{WAZZUP_API_BASE}/message",
        headers=_wazzup_headers(),
        json=payload,
        timeout=20,
    ) as resp:
        resp_text = await resp.text()
        return resp.status in (200, 201), resp.status, resp_text


async def wazzup_send_text(
    chat_id: str,
    text: str,
    keyboard: dict[str, Any] | None = None,
    channel_id: str | None = None,
    clear_unanswered: bool = False,
) -> bool:
    ch = (channel_id or WAZZUP_CHANNEL_ID).strip()
    if not WAZZUP_API_KEY.strip() or not ch:
        logger.error("Не заполнены WAZZUP_API_KEY или WAZZUP_CHANNEL_ID")
        return False

    try:
        async with aiohttp.ClientSession() as session:
            ok, status, resp_text = await _post_wazzup_message(
                session=session,
                ch=ch,
                chat_id=str(chat_id),
                text=text,
                keyboard=keyboard,
                clear_unanswered=clear_unanswered,
            )
            if ok:
                logger.info("Wazzup POST /v3/message (%s) -> chatId=%s", status, chat_id)
                return True

            logger.warning("Wazzup POST /v3/message с keyboard вернул (%s): %s", status, resp_text[:300])
            if keyboard:
                ok_no_kb, status_no_kb, resp_no_kb = await _post_wazzup_message(
                    session=session,
                    ch=ch,
                    chat_id=str(chat_id),
                    text=text,
                    keyboard=None,
                    clear_unanswered=clear_unanswered,
                )
                if ok_no_kb:
                    logger.info("Wazzup POST /v3/message без keyboard прошёл (%s) -> chatId=%s", status_no_kb, chat_id)
                    return True
                logger.error("Ошибка Wazzup POST /v3/message без keyboard (%s): %s", status_no_kb, resp_no_kb[:300])
                return False

            logger.error("Ошибка Wazzup POST /v3/message (%s): %s", status, resp_text[:300])
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
            "Обращение по кнопке «Нужна помощь»\n"
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


async def send_start_menu(chat_id: str, channel_id: str, greeting_name: str | None = None) -> None:
    clear_user_state(chat_id)
    if greeting_name:
        text = (
            f"Привет, {greeting_name}!\n\n"
            "Официальный бот парка ЛунапаркЕда. Выберите нужный раздел:\n"
            "1. Подключиться к парку\n"
            "2. Уже курьер парка\n"
            "3. Нужна помощь"
        )
    else:
        text = (
            "Главное меню парка ЛунапаркЕда:\n"
            "1. Подключиться к парку\n"
            "2. Уже курьер парка\n"
            "3. Нужна помощь"
        )
    await wazzup_send_text(chat_id, text, keyboard=kb_start_menu(), channel_id=channel_id)


async def show_confirmation_screen(chat_id: str, channel_id: str, data: dict[str, Any]) -> None:
    set_user_state(chat_id, "reg_confirm", data)
    sts_line = ""
    if requires_two_sided_sts(data.get("transport", "")):
        has_front = bool(data.get("sts_front_url"))
        has_back = bool(data.get("sts_back_url"))
        sts_line = f"\nФото СТС: {'загружены с двух сторон' if (has_front and has_back) else 'не загружены'}\n"

    text = (
        "Проверьте, пожалуйста, всё ли корректно:\n\n"
        f"Имя: {data.get('name', '—')}\n"
        f"Город: {data.get('city', '—')}\n"
        f"Формат: {data.get('transport', '—')}\n"
        f"Телефон: {data.get('phone', '—')}"
        f"{sts_line}\n"
        "Всё верно? Напишите или нажмите «Да» / «Исправить»."
    )
    await wazzup_send_text(chat_id, text, keyboard=kb_confirm_application(), channel_id=channel_id)


async def show_cabinet_for_profile(chat_id: str, channel_id: str, driver: dict[str, Any]) -> None:
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
        "Кабинет курьера открыт: выберите раздел или напишите «В начало»."
    )
    await wazzup_send_text(chat_id, text, keyboard=kb_courier_cabinet(), channel_id=channel_id)


async def get_saved_profile_or_redirect(chat_id: str, channel_id: str) -> dict[str, Any] | None:
    rec = get_user_record(chat_id)
    phone = rec.get("phone")
    if not phone:
        set_user_state(chat_id, "courier_auth_phone", {})
        await wazzup_send_text(
            chat_id,
            "Сначала укажите ваш номер телефона в парке:",
            keyboard=kb_cancel_only(),
            channel_id=channel_id,
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


async def lookup_and_route_phone(chat_id: str, channel_id: str, phone: str) -> None:
    await wazzup_send_text(chat_id, "⏳ Проверяю ваш номер в базе парка...", channel_id=channel_id)
    try:
        async with aiohttp.ClientSession() as session:
            profiles = await _find_all_profiles_by_phone(session, phone)
    except Exception as exc:
        await wazzup_send_text(
            chat_id,
            f"⚠️ Не удалось проверить профиль в парке: {exc}",
            keyboard=kb_start_menu(),
            channel_id=channel_id,
        )
        return

    if not profiles:
        await wazzup_send_text(
            chat_id,
            f"❌ Номер {phone} не найден в базе подключённых парков.\n\n"
            "Проверьте правильность номера или выберите «Подключиться к парку», чтобы оставить заявку.",
            keyboard=kb_start_menu(),
            channel_id=channel_id,
        )
        return

    if len(profiles) == 1:
        chosen = profiles[0]
        save_user_session(chat_id, phone=phone, park_id=chosen["park_id"], driver_id=chosen["id"])
        await show_cabinet_for_profile(chat_id, channel_id, chosen)
        return

    PENDING_PROFILES[chat_id] = profiles
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
    await wazzup_send_text(
        chat_id,
        "\n".join(lines),
        keyboard=kb_profiles_choice(profiles),
        channel_id=channel_id,
    )


async def finalize_registration(
    chat_id: str,
    channel_id: str,
    data: dict[str, Any],
    tg_username: str,
) -> None:
    clear_user_state(chat_id)
    norm_phone = data.get("phone", "")
    save_user_session(
        chat_id,
        phone=norm_phone,
        city=data.get("city", ""),
        username=tg_username,
        full_name=data.get("name", ""),
    )

    tg_login = format_tg_login(chat_id, tg_username)
    lead_payload = {
        "type": "Подключение",
        "name": data.get("name", ""),
        "city": data.get("city", ""),
        "transport": data.get("transport", ""),
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
        keyboard=kb_start_menu(),
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

    logger.info("Входящее сообщение Wazzup: chatId=%s chatType=%s text=%r", chat_id, chat_type or "unknown", text)
    save_user_session(chat_id, phone=contact_phone, username=tg_username, full_name=contact_name)

    norm_cmd = normalize_choice_text(text)

    if text == "/start" or norm_cmd in ("в начало", "главное меню", "старт", "start", "меню"):
        await send_start_menu(chat_id, channel_id, greeting_name=contact_name if text == "/start" else None)
        return

    if norm_cmd in ("отмена", "отменить", "cancel"):
        await send_start_menu(chat_id, channel_id)
        return

    state = get_user_state(chat_id)
    step = state.get("step")
    data = state.get("data") or {}

    if step is None and norm_cmd in ("1", "подключиться к парку", "подключиться", "регистрация", "стать курьером"):
        set_user_state(chat_id, "reg_transport", {"mode": "courier", "return_to_confirm": False})
        await wazzup_send_text(
            chat_id,
            "Шаг 1 из 4. Как планируете доставлять заказы?\n\n"
            "Выберите вариант или напишите цифру:\n"
            "1. Пеший / Вело\n"
            "2. Мото\n"
            "3. Авто\n"
            "4. Грузовой",
            keyboard=kb_transport_choice(),
            channel_id=channel_id,
        )
        return

    if step is None and norm_cmd in ("2", "уже курьер парка", "уже курьер", "кабинет", "мой кабинет"):
        set_user_state(chat_id, "courier_auth_phone", {})
        await wazzup_send_text(
            chat_id,
            "Введите номер телефона, привязанный к аккаунту в парке:",
            keyboard=kb_cancel_only(),
            channel_id=channel_id,
        )
        return

    if step is None and norm_cmd in ("3", "нужна помощь", "помощь", "поддержка", "оператор"):
        set_user_state(chat_id, "help_waiting_problem", {})
        await wazzup_send_text(
            chat_id,
            "Добрый день! Опишите Вашу проблему",
            keyboard=kb_cancel_only(),
            channel_id=channel_id,
        )
        return

    if norm_cmd in ("сменить номер", "изменить номер аккаунта"):
        set_user_state(chat_id, "courier_auth_phone", {})
        await wazzup_send_text(
            chat_id,
            "Отправьте номер телефона для входа в кабинет курьера:",
            keyboard=kb_cancel_only(),
            channel_id=channel_id,
        )
        return

    if norm_cmd in ("моя статистика", "статистика"):
        driver = await get_saved_profile_or_redirect(chat_id, channel_id)
        if driver:
            await show_cabinet_for_profile(chat_id, channel_id, driver)
        return

    if norm_cmd in ("я на линии?", "я на линии", "статус линии", "линия"):
        driver = await get_saved_profile_or_redirect(chat_id, channel_id)
        if not driver:
            return
        park = get_park_by_id(driver["park_id"])
        reg_dt = get_registered_dt(chat_id)
        try:
            async with aiohttp.ClientSession() as session:
                stats = await _get_driver_orders_stats(session, park, driver["id"], registered_dt=reg_dt)
        except Exception as exc:
            await wazzup_send_text(chat_id, f"⚠️ Ошибка проверки линии: {exc}", channel_id=channel_id)
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
        await wazzup_send_text(chat_id, resp_text, keyboard=kb_courier_cabinet(), channel_id=channel_id)
        return

    if norm_cmd in ("цель", "бонус", "моя цель"):
        driver = await get_saved_profile_or_redirect(chat_id, channel_id)
        if not driver:
            return
        park = get_park_by_id(driver["park_id"])
        reg_dt = get_registered_dt(chat_id)
        try:
            async with aiohttp.ClientSession() as session:
                stats = await _get_driver_orders_stats(session, park, driver["id"], registered_dt=reg_dt)
        except Exception as exc:
            await wazzup_send_text(chat_id, f"⚠️ Ошибка расчёта цели: {exc}", channel_id=channel_id)
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
        await wazzup_send_text(chat_id, resp_text, keyboard=kb_courier_cabinet(), channel_id=channel_id)
        return

    if step == "courier_auth_phone":
        raw_phone = contact_phone or text
        digits = re.sub(r"\D", "", raw_phone)
        if len(digits) < 10:
            await wazzup_send_text(
                chat_id,
                "Номер введён не полностью. Введите номер в формате +7 999 123-45-67:",
                keyboard=kb_cancel_only(),
                channel_id=channel_id,
            )
            return
        clear_user_state(chat_id)
        await lookup_and_route_phone(chat_id, channel_id, normalize_phone(raw_phone))
        return

    if step == "courier_pick_profile":
        if norm_cmd == "хочу ввести другой номер":
            set_user_state(chat_id, "courier_auth_phone", {})
            await wazzup_send_text(
                chat_id,
                "Введите номер телефона, привязанный к аккаунту в парке:",
                keyboard=kb_cancel_only(),
                channel_id=channel_id,
            )
            return
        profiles = PENDING_PROFILES.get(chat_id, [])
        m = re.match(r"^(\d+)", text.strip())
        if not m or int(m.group(1)) < 1 or int(m.group(1)) > len(profiles):
            await wazzup_send_text(
                chat_id,
                "Выберите профиль из списка или напишите его номер:",
                keyboard=kb_profiles_choice(profiles) if profiles else kb_start_menu(),
                channel_id=channel_id,
            )
            return
        chosen = profiles[int(m.group(1)) - 1]
        clear_user_state(chat_id)
        save_user_session(chat_id, phone=chosen["phone"], park_id=chosen["park_id"], driver_id=chosen["id"])
        await show_cabinet_for_profile(chat_id, channel_id, chosen)
        return

    if step == "help_waiting_problem":
        if not text and not content_uri:
            await wazzup_send_text(
                chat_id,
                "Напишите текстом, что случилось — мы передадим обращение специалисту.",
                keyboard=kb_cancel_only(),
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
            keyboard=kb_start_menu(),
            channel_id=channel_id,
            clear_unanswered=False,
        )
        return

    if step == "reg_transport":
        chosen_transport = parse_transport_choice(text)
        if not chosen_transport:
            await wazzup_send_text(
                chat_id,
                "Выберите формат доставки (нажмите кнопку или напишите цифру):\n"
                "1. Пеший / Вело\n"
                "2. Мото\n"
                "3. Авто\n"
                "4. Грузовой",
                keyboard=kb_transport_choice(),
                channel_id=channel_id,
            )
            return
        data = update_user_state_data(chat_id, transport=chosen_transport)
        if data.get("return_to_confirm"):
            if requires_two_sided_sts(chosen_transport) and not (data.get("sts_front_url") and data.get("sts_back_url")):
                set_user_state(chat_id, "reg_sts_front", data)
                await wazzup_send_text(
                    chat_id,
                    "Для выбранного формата нужно фото СТС с двух сторон. Пришлите лицевую сторону (1 из 2):",
                    keyboard=kb_cancel_only(),
                    channel_id=channel_id,
                )
                return
            await show_confirmation_screen(chat_id, channel_id, data)
            return
        set_user_state(chat_id, "reg_city", data)
        await wazzup_send_text(
            chat_id,
            "Шаг 2 из 4. В каком городе планируете работать?",
            keyboard=kb_cancel_only(),
            channel_id=channel_id,
        )
        return

    if step == "reg_city":
        if len(text) < 2:
            await wazzup_send_text(chat_id, "Напишите корректное название города:", keyboard=kb_cancel_only(), channel_id=channel_id)
            return
        data = update_user_state_data(chat_id, city=text)
        if data.get("return_to_confirm"):
            await show_confirmation_screen(chat_id, channel_id, data)
            return
        set_user_state(chat_id, "reg_name", data)
        await wazzup_send_text(
            chat_id,
            "Шаг 3 из 4. Напишите ваши Фамилию и Имя:",
            keyboard=kb_cancel_only(),
            channel_id=channel_id,
        )
        return

    if step == "reg_name":
        if len(text) < 2:
            await wazzup_send_text(chat_id, "Напишите ваши Фамилию и Имя:", keyboard=kb_cancel_only(), channel_id=channel_id)
            return
        data = update_user_state_data(chat_id, name=text)
        if data.get("return_to_confirm"):
            await show_confirmation_screen(chat_id, channel_id, data)
            return
        set_user_state(chat_id, "reg_phone", data)
        await wazzup_send_text(
            chat_id,
            "Шаг 4 из 4. Введите ваш номер телефона (например, +7 999 123-45-67):",
            keyboard=kb_cancel_only(),
            channel_id=channel_id,
        )
        return

    if step == "reg_phone":
        raw_phone = contact_phone or text
        digits = re.sub(r"\D", "", raw_phone)
        if len(digits) < 10:
            await wazzup_send_text(
                chat_id,
                "Номер введён не полностью. Введите номер в формате +7 999 123-45-67:",
                keyboard=kb_cancel_only(),
                channel_id=channel_id,
            )
            return
        norm_phone = normalize_phone(raw_phone)
        data = update_user_state_data(chat_id, phone=norm_phone)
        if data.get("return_to_confirm"):
            await show_confirmation_screen(chat_id, channel_id, data)
            return
        if requires_two_sided_sts(data.get("transport", "")):
            set_user_state(chat_id, "reg_sts_front", data)
            await wazzup_send_text(
                chat_id,
                "Пришлите фото СТС — сначала лицевую сторону (1 из 2):",
                keyboard=kb_cancel_only(),
                channel_id=channel_id,
            )
            return
        await show_confirmation_screen(chat_id, channel_id, data)
        return

    if step == "reg_sts_front":
        if not content_uri:
            await wazzup_send_text(
                chat_id,
                "Пришлите фото лицевой стороны СТС (1 из 2):",
                keyboard=kb_cancel_only(),
                channel_id=channel_id,
            )
            return
        data = update_user_state_data(chat_id, sts_front_url=content_uri)
        set_user_state(chat_id, "reg_sts_back", data)
        await wazzup_send_text(
            chat_id,
            "Отлично! Теперь пришлите фото обратной стороны СТС (2 из 2):",
            keyboard=kb_cancel_only(),
            channel_id=channel_id,
        )
        return

    if step == "reg_sts_back":
        if not content_uri:
            await wazzup_send_text(
                chat_id,
                "Пришлите фото обратной стороны СТС (2 из 2):",
                keyboard=kb_cancel_only(),
                channel_id=channel_id,
            )
            return
        data = update_user_state_data(chat_id, sts_back_url=content_uri)
        await show_confirmation_screen(chat_id, channel_id, data)
        return

    if step == "reg_confirm":
        if norm_cmd in ("2", "исправить", "изменить", "редактировать"):
            data = update_user_state_data(chat_id, return_to_confirm=True)
            set_user_state(chat_id, "reg_choosing_edit", data)
            await wazzup_send_text(
                chat_id,
                "Что хотите изменить? Выберите вариант или напишите: формат, город, имя, телефон или назад.",
                keyboard=kb_edit_field_choice(data),
                channel_id=channel_id,
            )
            return
        if norm_cmd in ("1", "да", "верно", "подтверждаю", "ок", "yes"):
            await finalize_registration(chat_id, channel_id, data, tg_username)
            return
        await wazzup_send_text(
            chat_id,
            "Нажмите «Да» или напишите «Да», если данные верны, либо «Исправить», чтобы поменять данные:",
            keyboard=kb_confirm_application(),
            channel_id=channel_id,
        )
        return

    if step == "reg_choosing_edit":
        if norm_cmd in ("назад к проверке", "назад"):
            await show_confirmation_screen(chat_id, channel_id, data)
            return
        if norm_cmd in ("изменить формат", "формат", "транспорт"):
            set_user_state(chat_id, "reg_transport", data)
            await wazzup_send_text(
                chat_id,
                "Выберите новый формат доставки:\n1. Пеший / Вело\n2. Мото\n3. Авто\n4. Грузовой",
                keyboard=kb_transport_choice(),
                channel_id=channel_id,
            )
            return
        if norm_cmd in ("изменить город", "город"):
            set_user_state(chat_id, "reg_city", data)
            await wazzup_send_text(chat_id, "Введите новый город:", keyboard=kb_cancel_only(), channel_id=channel_id)
            return
        if norm_cmd in ("изменить имя", "имя"):
            set_user_state(chat_id, "reg_name", data)
            await wazzup_send_text(chat_id, "Введите новые Фамилию и Имя:", keyboard=kb_cancel_only(), channel_id=channel_id)
            return
        if norm_cmd in ("изменить телефон", "телефон"):
            set_user_state(chat_id, "reg_phone", data)
            await wazzup_send_text(chat_id, "Отправьте новый номер телефона:", keyboard=kb_cancel_only(), channel_id=channel_id)
            return
        if norm_cmd in ("изменить фото стс", "фото стс", "стс"):
            set_user_state(chat_id, "reg_sts_front", data)
            await wazzup_send_text(
                chat_id,
                "Пришлите новое фото лицевой стороны СТС (1 из 2):",
                keyboard=kb_cancel_only(),
                channel_id=channel_id,
            )
            return
        await wazzup_send_text(
            chat_id,
            "Выберите поле для изменения или напишите: формат, город, имя, телефон или назад.",
            keyboard=kb_edit_field_choice(data),
            channel_id=channel_id,
        )
        return

    # Если пользователь написал произвольный текст вне сценария (например, «тест») — показываем меню
    await send_start_menu(chat_id, channel_id)


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
