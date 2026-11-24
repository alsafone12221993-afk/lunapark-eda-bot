import asyncio
import hashlib
import hmac
import json
import logging
import os
import random
import re
import time
import urllib.parse
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import aiohttp
from aiohttp import web

# ================= КОНФИГУРАЦИЯ =================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_WEBHOOK_URL = os.getenv("TELEGRAM_WEBHOOK_URL", "").strip()
TELEGRAM_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "").strip()
# Wazzup is the ONLY Telegram transport when enabled. Do not run Telegram getUpdates in parallel.
WAZZUP_API_KEY = os.getenv("WAZZUP_API_KEY", "").strip()
WAZZUP_CHANNEL_ID = os.getenv("WAZZUP_CHANNEL_ID", "").strip()
WAZZUP_WEBHOOK_SECRET = os.getenv("WAZZUP_WEBHOOK_SECRET", "").strip()
# Exact value configured in Wazzup webhook subscription as authorization_header.
# If not set, the existing WAZZUP_WEBHOOK_SECRET is used for Authorization too.
WAZZUP_WEBHOOK_AUTHORIZATION = os.getenv(
    "WAZZUP_WEBHOOK_AUTHORIZATION", WAZZUP_WEBHOOK_SECRET
).strip()
WAZZUP_ENABLED = os.getenv("WAZZUP_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
WAZZUP_API_URL = "https://api.wazzup24.com/v3/message"
# Публичный адрес бота (например https://my-bot.bothost.ru). При старте бот сам
# пропишет в Wazzup адрес вебхука вида <адрес>/webhook/wazzup?secret=<секрет>.
WAZZUP_PUBLIC_URL = os.getenv("WAZZUP_PUBLIC_URL", "").strip().rstrip("/")
WAZZUP_WEBHOOKS_API_URL = "https://api.wazzup24.com/v3/webhooks"
AMOCRM_WEBHOOK_SECRET = os.getenv("AMOCRM_WEBHOOK_SECRET", "").strip()
PORT = int(os.getenv("PORT", "8080"))

AI_API_KEY = os.getenv("AI_API_KEY", "").strip()
AI_BASE_URL = os.getenv("AI_BASE_URL", "https://api.openai.com/v1/chat/completions").strip()
AI_MODEL = os.getenv("AI_MODEL", "gpt-4o-mini").strip()

EPHEMERAL_TTL_SEC = int(os.getenv("EPHEMERAL_TTL_SEC", "25"))

SUPERADMIN_ID = os.getenv("SUPERADMIN_ID", "673430213").strip()

_raw_admins = os.getenv("OPERATOR_ADMIN_IDS", SUPERADMIN_ID).strip()
INITIAL_ADMIN_IDS: set[str] = {
    x.strip() for x in _raw_admins.split(",") if x.strip()
} | {SUPERADMIN_ID}

OPERATOR_CHAT_ID = os.getenv("OPERATOR_CHAT_ID", SUPERADMIN_ID).strip()
SUPPORT_TG_USERNAME = os.getenv("SUPPORT_TG_USERNAME", "LUNAPARK_DR").strip().lstrip("@")
SUPPORT_TG_URL = f"https://t.me/{SUPPORT_TG_USERNAME}"

DEMAND_RATE_RUB_PER_HOUR = int(os.getenv("DEMAND_RATE_RUB_PER_HOUR", "800"))

AUTH_MAX_ATTEMPTS = 3
AUTH_WINDOW_SEC = 600
AUTH_LOCK_LEVELS_SEC = [900, 3600, 86400]
FLOOD_MAX_MSG_PER_MIN = 25

PARKS_CONFIG: list[dict[str, str]] = [
    {
        "name": "ЛунапаркЕда",
        "park_id": os.getenv("YANDEX_PARK_ID", "1680a13d35084b0688ecab0f925c9d75"),
        "client_id": os.getenv("YANDEX_CLIENT_ID", "taxi/park/1680a13d35084b0688ecab0f925c9d75"),
        "api_key": os.getenv("YANDEX_API_KEY", ""),
    },
]

AMOCRM_BASE_URL = os.getenv("AMOCRM_BASE_URL", "https://lunapark7042.amocrm.ru").strip()
AMOCRM_ACCESS_TOKEN = os.getenv("AMOCRM_ACCESS_TOKEN", "").strip()
AMOCRM_PIPELINE_ID = int(os.getenv("AMOCRM_PIPELINE_ID", "10053610"))
AMOCRM_STATUS_ID = int(os.getenv("AMOCRM_STATUS_ID", "79731826"))
AMOCRM_CLOSED_LOST_ID = int(os.getenv("AMOCRM_CLOSED_LOST_ID", "143"))

LEAD_FIELD_DRIVER_TYPE = 2815163
LEAD_FIELD_PRO_NUMBER = 2818307
LEAD_FIELD_CITY_LIST = 2840190

CONTACT_FIELD_PHONE_MULTI = 2205563
CONTACT_ENUM_PHONE_MOB = 4831969
CONTACT_FIELD_CITY = 2520625
CONTACT_FIELD_FIO = 2815893
CONTACT_FIELD_PHONE_TEXT = 2854331

DRIVER_TYPE_ENUM_MAP: dict[str, int] = {
    "пеший доставка": 5241175,
    "пеший / вело": 5241175,
    "пеший": 5241175,
    "вело": 5241175,
    "мото курьер": 5241177,
    "мото": 5241177,
    "грузовые тарифы": 5241179,
    "грузовой": 5241179,
    "автокурьер": 5241173,
    "авто": 5241173,
    "такси": 5302385,
}

CITY_LIST_ENUM_MAP: dict[str, int] = {
    "санкт-петербург": 5301604,
    "москва": 5301538,
    "адлер": 5301690,
    "алматы": 5301436,
    "альметьевск": 5301428,
    "анапа": 5301430,
    "архангельск": 5301432,
    "астана": 5301438,
    "астрахань": 5301434,
    "балашиха": 5301440,
    "барнаул": 5301442,
    "белгород": 5301444,
    "бишкек": 5301448,
    "брянск": 5301446,
    "великий новгород": 5301452,
    "волгоград": 5301462,
    "видное": 5301454,
    "владивосток": 5301456,
    "владикавказ": 5301458,
    "волжский": 5301464,
    "вологда": 5301466,
    "воронеж": 5301468,
    "геленджик": 5301470,
    "липецк": 5301528,
    "лобня": 5301530,
    "люберцы": 5301532,
    "майкоп": 5301536,
    "магнитогорск": 5301534,
    "мурино": 5301540,
    "мурманск": 5301542,
    "мытищи": 5301544,
    "нальчик": 5301550,
    "наро-фоминск": 5301552,
    "нижневартовск": 5301554,
    "нижнекамск": 5301556,
    "нижний новгород": 5301558,
    "сургут": 5301628,
    "сыктывкар": 5301630,
    "таганрог": 5301632,
    "тамбов": 5301634,
    "тверь": 5301636,
    "троицк": 5301642,
    "томск": 5301640,
    "тольятти": 5301638,
    "тула": 5301644,
    "тюмень": 5301646,
    "уфа": 5301652,
    "ташкент": 5301648,
    "ульяновск": 5301650,
    "хабаровск": 5301654,
    "владимир": 5301460,
    "ханты-мансийск": 5301656,
    "новокузнецк": 5301560,
    "химки": 5301658,
    "новороссийск": 5301562,
    "чебоксары": 5301660,
    "новосибирск": 5301564,
    "челябинск": 5301662,
    "новый уренгой": 5301566,
    "череповец": 5301664,
    "ногинск": 5301568,
    "чехов": 5301666,
    "дзержинск": 5301472,
    "обнинск": 5301570,
    "шушары": 5301668,
    "дмитров": 5301474,
    "одинцово": 5301572,
    "шымкент": 5301670,
    "долгопрудный": 5301476,
    "омск": 5301574,
    "щербинка": 5301672,
    "домодедово": 5301478,
    "оренбург": 5301576,
    "щёлково": 5301674,
    "щелково": 5301674,
    "егорьевск": 5301480,
    "орёл": 5301578,
    "орел": 5301578,
    "электросталь": 5301676,
    "екатеринбург": 5301482,
    "пенза": 5301580,
    "ярославль": 5301678,
    "железнодорожный": 5301484,
    "пермь": 5301582,
    "жуковский": 5301486,
    "петрозаводск": 5301584,
    "зеленоград": 5301488,
    "подольск": 5301586,
    "иваново": 5301490,
    "псков": 5301588,
    "ивантеевка": 5301492,
    "пушкин": 5301590,
    "ижевск": 5301494,
    "пушкино": 5301592,
    "иркутск": 5301496,
    "пятигорск": 5301594,
    "йошкар-ола": 5301498,
    "раменское": 5301596,
    "казань": 5301500,
    "реутов": 5301598,
    "калининград": 5301502,
    "ростов-на-дону": 5301600,
    "калуга": 5301504,
    "самара": 5301602,
    "кемерово": 5301506,
    "киров": 5301508,
    "коломна": 5301510,
    "колпино": 5301512,
    "королёв": 5301514,
    "королев": 5301514,
    "кострома": 5301516,
    "красногорск": 5301518,
    "краснодар": 5301520,
    "красноярск": 5301522,
    "кудрово": 5301524,
    "курск": 5301526,
    "рязань": 5301688,
    "саранск": 5301606,
    "саратов": 5301608,
    "сергиев посад": 5301610,
    "серпухов": 5301612,
    "смоленск": 5301614,
    "солнечногорск": 5301616,
    "сочи": 5301618,
    "ставрополь": 5301620,
    "старый оскол": 5301622,
    "старыйоскол": 5301622,
    "стерлитамак": 5301624,
    "ступино": 5301626,
    "курган": 5301808,
    "южно-сахалинск": 5301912,
    "новочеркасск": 5302054,
    "элиста": 5302080,
    "армавир": 5302198,
    "набережные челны": 5302278,
    "апрелевка": 5302328,
    "вольск": 5302352,
    "учалы": 5302619,
    "балаково": 5302709,
    "махачкала": 5303031,
    "невинномысск": 5303039,
    "якутск": 5303229,
    "кубинка": 5303889,
    "андреевка": 5303891,
    "кашира": 5303893,
    "клин": 5303897,
    "яхрома": 5303947,
    "белоозёрский": 5303949,
    "белоозерский": 5303949,
    "кстово": 5303951,
    "шахты": 5303969,
    "муром": 5304081,
    "всеволожск": 5304171,
    "гатчина": 5304173,
    "благовещенск": 5304329,
    "орехово-зуево": 5304331,
    "звенигород": 5304453,
    "истра": 5304466,
    "кореновск": 5304486,
    "бронницы": 5304648,
    "рыбинск": 5305093,
    "октябрьский": 5305107,
    "выборг": 5305235,
    "можайск": 5305335,
    "черноголовка": 5305635,
    "сарапул": 5306653,
    "дзержинский": 5306771,
    "минск": 5306957,
    "нефтеюганск": 5307267,
    "луганск": 5307645,
    "каспийск": 5307659,
    "черкесск": 5307897,
    "хасавюрт": 5307971,
    "новомосковск": 5308027,
    "донецк": 5308047,
    "северодвинск": 5308211,
    "бийск": 5308559,
    "хотьково": 5308681,
    "воскресенск": 5308945,
    "нижний тагил": 5308989,
    "тобольск": 5309047,
    "александров": 5309253,
    "руза": 5309273,
    "ивангород": 5309283,
    "улан-удэ": 5310407,
    "правдинск": 5310519,
    "гуково": 5311649,
    "азов": 5316891,
    "глазов": 5318291,
    "энгельс": 5318595,
    "малоярославец": 5318773,
    "ессентуки": 5319367,
    "абакан": 5322681,
    "буйнакск": 5322785,
    "орск": 5324121,
    "дедовск": 5324379,
    "старая купавна": 5324381,
    "кропоткин": 5324431,
    "красноармейск": 5324545,
    "симферополь": 5324765,
    "горно-алтайск": 5324959,
    "рубцовск": 5325207,
    "ильинский": 5325843,
    "ялта": 5325895,
    "чита": 5326267,
    "первоуральск": 5326287,
    "свердловский": 5326331,
    "ноябрьск": 5326555,
    "краснокамск": 5326557,
    "бор": 5327683,
    "нефтекамск": 5327703,
    "кольчугино": 5330467,
    "ухта": 5330725,
    "камень-на-оби": 5334522,
    "комсомольск-на-амуре": 5335883,
    "минеральные воды": 5336513,
    "тихвин": 5338173,
    "сызрань": 5339053,
}

USERS_FILE = "users_db.json"
APPS_FILE = "applications.json"
DRIVERS_CACHE_FILE = "drivers_cache.json"
CLIENT_LEADS_FILE = "client_leads.json"
CRM_ENTITY_MAP_FILE = "crm_entity_map.json"
STATES_FILE = "user_states.json"
DELAYED_TASKS_FILE = "delayed_notifications.json"
LIVE_CHATS_FILE = "live_chats.json"
EPHEMERAL_MSGS_FILE = "ephemeral_messages.json"
ADMIN_CONFIG_FILE = "admin_config.json"
PENDING_DEMANDS_FILE = "pending_demands.json"
AUTH_GUARD_FILE = "auth_guard.json"
PHONE_OWNERS_FILE = "phone_owners.json"

FLEET_API_URL = "https://fleet-api.taxi.yandex.net"

GOAL_ORDERS = 5
GOAL_HIDE_AFTER_ORDERS = 5
GOAL_DAYS = 14
GOAL_BONUS = 500
PROFILE_READY_DELAY_SEC = 360
AFTER_10M_REMINDER_DELAY_SEC = 5

DRIVERS_CACHE: dict[str, dict[str, Any]] = {}
DRIVERS_TTL_SEC = 3600
PAGE_PAUSE_SEC = 0.5

DRIVER_STATS_CACHE: dict[str, dict[str, Any]] = {}
DRIVER_STATS_TTL_SEC = 180

DRIVER_FINES_CACHE: dict[str, dict[str, Any]] = {}
DRIVER_FINES_TTL_SEC = 180

PENDING_PROFILES: dict[str, list[dict[str, Any]]] = {}
_FILE_LOCKS: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
_STATS_LOCKS: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
_FLOOD_TIMESTAMPS: dict[str, list[float]] = defaultdict(list)

VEHICLE_DOCS_KEYWORDS = ("авто", "мото", "груз")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("lunapark_bot")

IOS_URL = "https://apps.apple.com/ru/app/%D1%8F%D0%BD%D0%B4%D0%B5%D0%BA%D1%81-%D0%BF%D1%80%D0%BE-%D0%B2%D0%BE%D0%B4%D0%B8%D1%82%D0%B5%D0%BB%D0%B8-%D0%B8-%D0%BA%D1%83%D1%80%D1%8C%D0%B5%D1%80%D1%8B/id1496904594"
ANDROID_URL = "https://play.google.com/store/apps/details?id=ru.yandex.taximeter&hl=ru"
RUSTORE_URL = "https://www.rustore.ru/catalog/app/ru.yandex.taximeter"

MSG_AFTER_SUBMIT = (
    "✅ Заявка принята! Профиль создадим в течение 6 минут.\n\n"
    "Пока можно скачать Яндекс Про:\n"
    f"🍎 <a href=\"{IOS_URL}\">iOS (App Store)</a>\n"
    f"🤖 <a href=\"{ANDROID_URL}\">Android (Google Play)</a>\n"
    f"🛒 <a href=\"{RUSTORE_URL}\">RuStore</a>"
)

MSG_PROFILE_CREATED_10M = (
    "🎉 Профиль создан! Добро пожаловать в команду ЛунапаркЕда!\n\n"
    "Вот простые шаги, чтобы быстро стартовать:\n"
    "📱 Заходи в Яндекс Про по своему номеру телефона\n"
    "🍔 Выбирай сервис Еда и парк ЛунапаркЕда\n"
    "📸 Проходи фотоконтроль, получи термокороб, пройди обучение и выбери удобный слот для работы!\n"
    "🛵 Обязательно нажми на \"Профиль\" далее \"Тип передвижения\" и выбери как будешь доставлять посылки.\n"
    "🩺 Не забудь оформить медицинскую книжку, но первое время можно работать без нее.\n\n"
    "Если возникнут вопросы — мы всегда рядом и подскажем!"
)

MSG_AFTER_10M_CABINET_HINT = (
    "💡 Чтобы смотреть статистику, бонус и корректировки в боте, нажми «✅ Уже курьер»."
)

START_GREETING_VARIANTS = [
    "Привет, {name}! 👋\n\nЭто бот парка ЛунапаркЕда. Выбирай, с чего начнём:",
    "Салют, {name}! 🚀\n\nНа связи ЛунапаркЕда. Подскажи, что нужно прямо сейчас:",
    "Привет, {name}! 😊\n\nДобро пожаловать в ЛунапаркЕда. Выбирай нужный раздел ниже:",
    "Добрый день, {name}! 👋\n\nЭто бот поддержки и подключения ЛунапаркЕда. Чем помочь сегодня?",
]

COURIER_GREETING_VARIANTS = [
    "Привет, {name}! Рады видеть тебя в ЛунапаркЕда.\n\nЗдесь можно посмотреть свежую статистику по заказам и пробегу, {bonus_part}проверить корректировки или написать оператору. Хороших смен и ровных маршрутов!",
    "Салют, {name}! На связи ЛунапаркЕда.\n\nЗаглядывай в статистику по сменам и километражу, {bonus_part}смотри корректировки или пиши в «Помощь», если что-то понадобится. Удачных доставок!",
    "Привет, {name}! Отличного дня в ЛунапаркЕда.\n\nВ кабинете под рукой свежая статистика, {bonus_part}корректировки и быстрая связь с нами. Пусть заказов будет много и без лишних пробок!",
]


# ================= КЛАВИАТУРЫ TELEGRAM =================
def _reply_kb(
    rows: list[list[str | dict[str, Any]]],
    one_time: bool = False,
) -> dict[str, Any]:
    keyboard_rows: list[list[dict[str, Any]]] = []
    for row in rows:
        built_row: list[dict[str, Any]] = []
        for btn in row:
            if isinstance(btn, dict):
                built_row.append(btn)
            else:
                built_row.append({"text": str(btn)})
        keyboard_rows.append(built_row)
    return {
        "keyboard": keyboard_rows,
        "resize_keyboard": True,
        "one_time_keyboard": one_time,
    }


def kb_support_inline() -> dict[str, Any]:
    return {
        "inline_keyboard": [
            [{"text": f"💬 Написать оператору @{SUPPORT_TG_USERNAME}", "url": SUPPORT_TG_URL}],
        ]
    }


def kb_start_menu(is_admin: bool = False) -> dict[str, Any]:
    rows: list[list[str | dict[str, Any]]] = [
        ["🚀 Подключиться", "✅ Уже курьер"],
        ["❓ Помощь"],
    ]
    if is_admin:
        rows.append(["🛠 Админка", "⏱ Заказы в час"])
    return _reply_kb(rows)


def kb_admin_menu(is_superadmin: bool = False) -> dict[str, Any]:
    rows: list[list[str | dict[str, Any]]] = [
        ["📊 Сводка за период", "⏱ Заказы в час"],
        ["🗓 Период", "📣 Массовая рассылка"],
        ["🔥 Проверить спрос"],
    ]
    if is_superadmin:
        rows.append(["👥 Админы", "🔓 Сбросить привязку"])
    rows.append(["⬅️ В начало"])
    return _reply_kb(rows)


def kb_cancel_questions_choice() -> dict[str, Any]:
    return _reply_kb([
        ["Да", "Нет"],
        ["⬅️ В начало"],
    ])


def kb_not_found_connect() -> dict[str, Any]:
    return _reply_kb([
        ["🚀 Подключиться"],
        ["❓ Помощь", "⬅️ В начало"],
    ])


def kb_transport_choice() -> dict[str, Any]:
    return _reply_kb([
        ["🚶 Пеший / Вело", "🛵 Мото"],
        ["🚗 Авто", "🚚 Грузовой"],
        ["❓ Помощь", "❌ Отмена"],
        ["⬅️ В начало"],
    ])


def kb_step_with_help() -> dict[str, Any]:
    return _reply_kb([
        ["❓ Помощь", "❌ Отмена"],
        ["⬅️ В начало"],
    ])


def kb_courier_phone_auth() -> dict[str, Any]:
    return _reply_kb([
        [{"text": "📱 Отправить номер профиля", "request_contact": True}],
        ["❓ Помощь", "❌ Отмена"],
        ["⬅️ В начало"],
    ])


def kb_confirm_application() -> dict[str, Any]:
    return _reply_kb([
        ["Да", "Исправить"],
        ["❓ Помощь", "❌ Отмена"],
        ["⬅️ В начало"],
    ])


def kb_edit_field_choice(data: dict[str, Any]) -> dict[str, Any]:
    rows: list[list[str | dict[str, Any]]] = [
        ["Изменить формат", "Изменить город"],
        ["Изменить ФИО", "Изменить телефон"],
        ["Изменить прописку"],
    ]
    if requires_vehicle_docs(data.get("transport", "")):
        rows.append(["Изменить фото СТС", "Изменить фото ВУ"])
    rows.append(["Назад к проверке", "❓ Помощь"])
    rows.append(["❌ Отмена", "⬅️ В начало"])
    return _reply_kb(rows)


def kb_courier_cabinet(show_bonus: bool = True) -> dict[str, Any]:
    if show_bonus:
        rows: list[list[str | dict[str, Any]]] = [
            ["📊 Моя статистика", "🎁 Бонус"],
            ["⚖️ Корректировки", "❓ Помощь"],
            ["🔄 Сменить номер", "⬅️ В начало"],
        ]
    else:
        rows = [
            ["📊 Моя статистика", "⚖️ Корректировки"],
            ["❓ Помощь", "🔄 Сменить номер"],
            ["⬅️ В начало"],
        ]
    return _reply_kb(rows)


def kb_profiles_choice(profiles: list[dict[str, Any]]) -> dict[str, Any]:
    rows: list[list[str | dict[str, Any]]] = []
    for idx, prof in enumerate(profiles[:10]):
        rows.append([f"{idx + 1}. {prof['park_name']}"])
    rows.append(["Хочу ввести другой номер", "❓ Помощь"])
    rows.append(["❌ Отмена", "⬅️ В начало"])
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


def requires_vehicle_docs(transport_text: str) -> bool:
    low = (transport_text or "").lower()
    return any(k in low for k in VEHICLE_DOCS_KEYWORDS)


def _has_all_vehicle_docs(data: dict[str, Any]) -> bool:
    return bool(
        data.get("sts_front_file_id")
        and data.get("sts_back_file_id")
        and data.get("vu_front_file_id")
        and data.get("vu_back_file_id")
    )


# ================= ПРОСТОЙ ИИ / АВТООТВЕТЧИК НА ЧАСТЫЕ ВОПРОСЫ =================
def _quick_faq_answer(text_low: str) -> str | None:
    if any(w in text_low for w in ("бонус", "500", "5 заказ", "цель")):
        return (
            "🎁 За выполнение 5 заказов за 14 дней на старте начисляется бонус +500 ₽.\n"
            "Точный прогресс можно глянуть в кабинете по кнопке «✅ Уже курьер» -> «🎁 Бонус»."
        )
    if any(w in text_low for w in ("штраф", "корректиров", "списан", "удержан")):
        return (
            "⚖️ Корректировки и списания можно проверить в кабинете: нажми «✅ Уже курьер» -> «⚖️ Корректировки».\n"
            f"Если нужно разобраться по конкретной сумме — жми «❓ Помощь» или пиши оператору @{SUPPORT_TG_USERNAME}."
        )
    if any(w in text_low for w in ("медкниж", "медицинск", "короб", "термокороб", "слот")):
        return (
            "📦 Начать работу можно без оформленной медкнижки, оформить её нужно будет позже.\n"
            "Термокороб и слоты доступны после входа в Яндекс Про и прохождения фотоконтроля."
        )
    if any(w in text_low for w in ("выплат", "деньг", "баланс", "самозанят", "мой налог")):
        return (
            "💳 Баланс и статистика доступны в разделе «✅ Уже курьер» -> «📊 Моя статистика».\n"
            f"По вопросам выплат и самозанятости пиши оператору @{SUPPORT_TG_USERNAME} или жми «❓ Помощь»."
        )
    return None


async def answer_simple_question_with_ai(user_text: str) -> str | None:
    low = (user_text or "").strip().lower()
    if not low or len(low) < 3:
        return None
    faq = _quick_faq_answer(low)
    if faq:
        return faq
    if not AI_API_KEY:
        return None
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                AI_BASE_URL,
                headers={
                    "Authorization": f"Bearer {AI_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": AI_MODEL,
                    "temperature": 0.5,
                    "max_tokens": 180,
                    "messages": [
                        {
                            "role": "system",
                            "content": (
                                "Ты помощник курьерского парка ЛунапаркЕда (Яндекс Еда / Яндекс Про). "
                                "Отвечай кратко, по-дружески и по делу на русском (до 3-4 предложений). "
                                "Не выдумывай точных цифр выплат или внутренних статусов — если нужен точный разбор, "
                                f"предложи написать оператору @{SUPPORT_TG_USERNAME} или нажать кнопку «Помощь»."
                            ),
                        },
                        {"role": "user", "content": user_text[:400]},
                    ],
                },
                timeout=10,
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    choices = data.get("choices") or []
                    if choices:
                        content = str(((choices[0].get("message") or {}).get("content") or "")).strip()
                        if content:
                            return content
    except Exception:
        pass
    return None


# ================= МНОГОУРОВНЕВАЯ ЗАЩИТА ОТ ВЗЛОМА И ПЕРЕБОРА =================
def check_flood_limit(chat_id: int | str) -> bool:
    key = str(chat_id)
    now = time.monotonic()
    arr = [t for t in _FLOOD_TIMESTAMPS[key] if now - t <= 60.0]
    if len(arr) >= FLOOD_MAX_MSG_PER_MIN:
        _FLOOD_TIMESTAMPS[key] = arr
        return False
    arr.append(now)
    _FLOOD_TIMESTAMPS[key] = arr
    return True


def check_auth_rate_limit(chat_id: int | str) -> tuple[bool, int]:
    guard = _read_json(AUTH_GUARD_FILE, {})
    if not isinstance(guard, dict):
        guard = {}
    key = str(chat_id)
    now = time.time()
    rec = guard.get(key) if isinstance(guard.get(key), dict) else {}
    locked_until = float(rec.get("locked_until") or 0)
    if locked_until > now:
        return False, int(locked_until - now) + 1
    attempts = [float(t) for t in (rec.get("attempts") or []) if now - float(t) <= AUTH_WINDOW_SEC]
    rec["attempts"] = attempts
    rec["locked_until"] = 0
    guard[key] = rec
    _write_json(AUTH_GUARD_FILE, guard)
    return True, 0


async def record_auth_attempt(
    chat_id: int | str,
    success: bool,
    reason: str = "",
    tg_username: str = "",
    tried_phone: str = "",
) -> None:
    guard = _read_json(AUTH_GUARD_FILE, {})
    if not isinstance(guard, dict):
        guard = {}
    key = str(chat_id)
    now = time.time()
    if success:
        guard.pop(key, None)
        _write_json(AUTH_GUARD_FILE, guard)
        return

    rec = guard.get(key) if isinstance(guard.get(key), dict) else {}
    attempts = [float(t) for t in (rec.get("attempts") or []) if now - float(t) <= AUTH_WINDOW_SEC]
    attempts.append(now)
    rec["attempts"] = attempts
    lock_level = int(rec.get("lock_level") or 0)

    if len(attempts) >= AUTH_MAX_ATTEMPTS:
        lock_sec = AUTH_LOCK_LEVELS_SEC[min(lock_level, len(AUTH_LOCK_LEVELS_SEC) - 1)]
        rec["locked_until"] = now + lock_sec
        rec["attempts"] = []
        rec["lock_level"] = lock_level + 1
        guard[key] = rec
        _write_json(AUTH_GUARD_FILE, guard)

        tg_login = format_tg_login(chat_id, tg_username)
        masked_phone = (tried_phone[:5] + "***" + tried_phone[-2:]) if len(tried_phone) >= 7 else tried_phone
        alert = (
            "🚨 Сработала защита от взлома / перебора номеров!\n\n"
            f"🆔 Telegram ID: {chat_id} ({tg_login})\n"
            f"📌 Причина: {reason or 'превышен лимит попыток входа'}\n"
            f"📱 Последний номер: {masked_phone or '—'}\n"
            f"🔒 Блокировка на {lock_sec // 60} мин (уровень {lock_level + 1})."
        )
        if str(chat_id) != SUPERADMIN_ID:
            await tg_send_text(SUPERADMIN_ID, alert)
        return

    guard[key] = rec
    _write_json(AUTH_GUARD_FILE, guard)


def _phone_hash(norm_phone: str) -> str:
    salt = hashlib.sha256(SUPERADMIN_ID.encode("utf-8")).hexdigest()[:16]
    return hashlib.sha256(f"{salt}:{norm_phone}".encode("utf-8")).hexdigest()


def get_phone_bound_owner(norm_phone: str) -> str | None:
    owners = _read_json(PHONE_OWNERS_FILE, {})
    if not isinstance(owners, dict):
        return None
    val = owners.get(_phone_hash(norm_phone))
    if isinstance(val, dict):
        owner_id = val.get("owner_tg_id")
        return str(owner_id) if owner_id else None
    return str(val) if val else None


def bind_phone_to_owner(norm_phone: str, chat_id: int | str) -> None:
    owners = _read_json(PHONE_OWNERS_FILE, {})
    if not isinstance(owners, dict):
        owners = {}
    owners[_phone_hash(norm_phone)] = {
        "owner_tg_id": str(chat_id),
        "bound_at": datetime.now(timezone.utc).isoformat(),
    }
    _write_json(PHONE_OWNERS_FILE, owners)


def unbind_phone_owner(norm_phone: str) -> bool:
    owners = _read_json(PHONE_OWNERS_FILE, {})
    if not isinstance(owners, dict):
        return False
    h = _phone_hash(norm_phone)
    if h in owners:
        owners.pop(h, None)
        _write_json(PHONE_OWNERS_FILE, owners)
        return True
    return False


# ================= ПРАВА СУПЕРАДМИНА И АДМИНОВ =================
def load_admin_config() -> dict[str, Any]:
    raw = _read_json(ADMIN_CONFIG_FILE, {})
    if not isinstance(raw, dict):
        raw = {}
    admins = raw.get("admins")
    if not isinstance(admins, list):
        admins = sorted(INITIAL_ADMIN_IDS)
    admin_set = {str(x).strip() for x in admins if str(x).strip()} | {SUPERADMIN_ID}
    period_days = raw.get("period_days", 7)
    try:
        period_days = max(1, min(365, int(period_days)))
    except Exception:
        period_days = 7
    return {"admins": sorted(admin_set), "period_days": period_days}


def save_admin_config(cfg: dict[str, Any]) -> None:
    admin_set = {str(x).strip() for x in (cfg.get("admins") or []) if str(x).strip()} | {SUPERADMIN_ID}
    period_days = cfg.get("period_days", 7)
    try:
        period_days = max(1, min(365, int(period_days)))
    except Exception:
        period_days = 7
    _write_json(ADMIN_CONFIG_FILE, {"admins": sorted(admin_set), "period_days": period_days})


def is_superadmin_user(user_id: int | str, chat_id: int | str) -> bool:
    return str(user_id) == SUPERADMIN_ID or str(chat_id) == SUPERADMIN_ID


def is_admin_user(user_id: int | str, chat_id: int | str) -> bool:
    cfg = load_admin_config()
    admin_set = set(cfg["admins"])
    return str(user_id) in admin_set or str(chat_id) in admin_set


def get_admin_period_days() -> int:
    return int(load_admin_config().get("period_days", 7))


def set_admin_period_days(days: int) -> int:
    cfg = load_admin_config()
    cfg["period_days"] = max(1, min(365, int(days)))
    save_admin_config(cfg)
    return cfg["period_days"]


def add_admin_id(new_id: str) -> list[str]:
    cfg = load_admin_config()
    s = set(cfg["admins"]) | {str(new_id).strip(), SUPERADMIN_ID}
    cfg["admins"] = sorted(x for x in s if x)
    save_admin_config(cfg)
    return cfg["admins"]


def remove_admin_id(target_id: str) -> tuple[bool, list[str]]:
    clean = str(target_id).strip()
    if clean == SUPERADMIN_ID:
        return False, load_admin_config()["admins"]
    cfg = load_admin_config()
    s = set(cfg["admins"])
    s.discard(clean)
    s.add(SUPERADMIN_ID)
    cfg["admins"] = sorted(s)
    save_admin_config(cfg)
    return True, cfg["admins"]


# ================= ЖИВОЙ ДИАЛОГ ОПЕРАТОР <-> КУРЬЕР =================
def load_live_chats() -> dict[str, Any]:
    raw = _read_json(LIVE_CHATS_FILE, {})
    return raw if isinstance(raw, dict) else {}


def save_live_chats(data: dict[str, Any]) -> None:
    _write_json(LIVE_CHATS_FILE, data)


def set_operator_active_courier(operator_chat_id: int | str, courier_chat_id: int | str) -> None:
    data = load_live_chats()
    active_by_op = data.get("active_by_op") if isinstance(data.get("active_by_op"), dict) else {}
    courier_to_op = data.get("courier_to_op") if isinstance(data.get("courier_to_op"), dict) else {}

    op_key = str(operator_chat_id)
    prev_courier = active_by_op.get(op_key)
    if prev_courier and courier_to_op.get(str(prev_courier)) == op_key:
        courier_to_op.pop(str(prev_courier), None)

    c_key = str(courier_chat_id)
    active_by_op[op_key] = c_key
    courier_to_op[c_key] = op_key
    data["active_by_op"] = active_by_op
    data["courier_to_op"] = courier_to_op
    save_live_chats(data)


def clear_operator_active_courier(operator_chat_id: int | str) -> str | None:
    data = load_live_chats()
    active_by_op = data.get("active_by_op") if isinstance(data.get("active_by_op"), dict) else {}
    courier_to_op = data.get("courier_to_op") if isinstance(data.get("courier_to_op"), dict) else {}

    op_key = str(operator_chat_id)
    prev_courier = active_by_op.pop(op_key, None)
    if prev_courier and courier_to_op.get(str(prev_courier)) == op_key:
        courier_to_op.pop(str(prev_courier), None)

    data["active_by_op"] = active_by_op
    data["courier_to_op"] = courier_to_op
    save_live_chats(data)
    return str(prev_courier) if prev_courier else None


def get_operator_active_courier(operator_chat_id: int | str) -> str | None:
    data = load_live_chats()
    active_by_op = data.get("active_by_op") if isinstance(data.get("active_by_op"), dict) else {}
    val = active_by_op.get(str(operator_chat_id))
    return str(val) if val else None


def get_courier_active_operator(courier_chat_id: int | str) -> str | None:
    data = load_live_chats()
    courier_to_op = data.get("courier_to_op") if isinstance(data.get("courier_to_op"), dict) else {}
    val = courier_to_op.get(str(courier_chat_id))
    return str(val) if val else None


def get_last_help_courier() -> str | None:
    data = load_live_chats()
    val = data.get("last_help_courier")
    return str(val) if val else None


def set_last_help_courier(courier_chat_id: int | str) -> None:
    data = load_live_chats()
    data["last_help_courier"] = str(courier_chat_id)
    save_live_chats(data)


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
    address: str = "",
    username: str = "",
    full_name: str = "",
    bonus_completed: bool | None = None,
) -> dict[str, Any]:
    db = _read_json(USERS_FILE, {})
    key = str(chat_id)
    entry = db.get(key)
    if not isinstance(entry, dict):
        entry = {}
    now_iso = datetime.now(timezone.utc).isoformat()
    if not entry.get("registered_at"):
        entry["registered_at"] = now_iso
    entry["last_seen_at"] = now_iso
    if phone:
        entry["phone"] = normalize_phone(phone)
    if park_id:
        entry["park_id"] = park_id
    if driver_id:
        entry["driver_id"] = driver_id
    if city:
        entry["city"] = city
    if address:
        entry["address"] = address
    if username:
        entry["username"] = username.lstrip("@").strip()
    if full_name:
        entry["full_name"] = full_name.strip()
    if bonus_completed is not None:
        entry["bonus_completed"] = bool(bonus_completed)
    db[key] = entry
    _write_json(USERS_FILE, db)
    return entry


def get_user_record(chat_id: int | str) -> dict[str, Any]:
    db = _read_json(USERS_FILE, {})
    rec = db.get(str(chat_id))
    return rec if isinstance(rec, dict) else {}


def get_goal_start_dt(chat_id: int | str) -> datetime:
    now = datetime.now(timezone.utc)
    default_start = now - timedelta(days=GOAL_DAYS)
    rec = get_user_record(chat_id)
    raw = rec.get("registered_at")
    if raw:
        try:
            dt = datetime.fromisoformat(str(raw))
            dt = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            return min(dt, default_start)
        except Exception:
            pass
    return default_start


def save_application(lead_payload: dict[str, Any]) -> None:
    apps = _read_json(APPS_FILE, [])
    if not isinstance(apps, list):
        apps = []
    record = dict(lead_payload)
    record["created_at"] = datetime.now().strftime("%d.%m.%Y %H:%M")
    record["created_ts"] = time.time()
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


def link_crm_entity_to_tg(lead_id: int | None, contact_id: int | None, tg_chat_id: int | str) -> None:
    mapping = _read_json(CRM_ENTITY_MAP_FILE, {})
    if not isinstance(mapping, dict):
        mapping = {}
    leads_map = mapping.get("leads") if isinstance(mapping.get("leads"), dict) else {}
    contacts_map = mapping.get("contacts") if isinstance(mapping.get("contacts"), dict) else {}
    if lead_id:
        leads_map[str(lead_id)] = str(tg_chat_id)
    if contact_id:
        contacts_map[str(contact_id)] = str(tg_chat_id)
    mapping["leads"] = leads_map
    mapping["contacts"] = contacts_map
    _write_json(CRM_ENTITY_MAP_FILE, mapping)


def find_tg_chat_by_crm_entity(lead_id: int | str | None = None, contact_id: int | str | None = None) -> str | None:
    mapping = _read_json(CRM_ENTITY_MAP_FILE, {})
    if not isinstance(mapping, dict):
        return None
    leads_map = mapping.get("leads") if isinstance(mapping.get("leads"), dict) else {}
    contacts_map = mapping.get("contacts") if isinstance(mapping.get("contacts"), dict) else {}
    if lead_id and str(lead_id) in leads_map:
        return str(leads_map[str(lead_id)])
    if contact_id and str(contact_id) in contacts_map:
        return str(contacts_map[str(contact_id)])
    return None


# ================= TELEGRAM BOT API + ИСЧЕЗАЮЩИЕ СООБЩЕНИЯ =================
async def tg_api(session: aiohttp.ClientSession, method: str, payload: dict[str, Any]) -> dict[str, Any]:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
    async with session.post(url, json=payload, timeout=25) as resp:
        data = await resp.json()
        if not data.get("ok"):
            logger.warning("Telegram API %s вернул ok=False: %s", method, data.get("description") or "error")
        return data


async def wazzup_send_message(
    chat_id: int | str,
    text: str = "",
    content_uri: str | None = None,
    reply_markup: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not WAZZUP_API_KEY or not WAZZUP_CHANNEL_ID:
        logger.error("Wazzup включён, но не заданы WAZZUP_API_KEY или WAZZUP_CHANNEL_ID")
        return {"ok": False, "error": "missing_wazzup_config"}
    payload: dict[str, Any] = {
        "channelId": WAZZUP_CHANNEL_ID,
        "chatType": "telegram",
        "chatId": str(chat_id),
    }
    if text:
        payload["text"] = text
    if content_uri:
        payload["contentUri"] = content_uri
    # Wazzup supports Telegram keyboard buttons via buttonsObject.
    if reply_markup:
        keyboard = reply_markup.get("inline_keyboard")
        if isinstance(keyboard, list):
            buttons = []
            for row in keyboard:
                for btn in row if isinstance(row, list) else []:
                    if not isinstance(btn, dict):
                        continue
                    item = {"text": str(btn.get("text") or "Кнопка")}
                    if btn.get("callback_data") is not None:
                        item["callbackData"] = str(btn["callback_data"])[:64]
                    elif btn.get("url"):
                        item["url"] = str(btn["url"])
                    else:
                        continue
                    buttons.append(item)
            if buttons:
                payload["buttonsObject"] = {"buttons": buttons, "replyMarkup": "inline"}
        elif isinstance(reply_markup.get("keyboard"), list):
            buttons = []
            for row in reply_markup["keyboard"]:
                for btn in row if isinstance(row, list) else []:
                    label = btn.get("text") if isinstance(btn, dict) else btn
                    if label:
                        buttons.append({"text": str(label)})
            if buttons:
                payload["buttonsObject"] = {"buttons": buttons, "replyMarkup": "keyboard"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                WAZZUP_API_URL,
                json=payload,
                headers={"Authorization": f"Bearer {WAZZUP_API_KEY}", "Content-Type": "application/json"},
                timeout=30,
            ) as resp:
                raw = await resp.text()
                try:
                    data = json.loads(raw) if raw else {}
                except Exception:
                    data = {"raw": raw[:500]}
                if resp.status >= 300:
                    logger.error("Wazzup send failed: HTTP %s: %s", resp.status, raw[:500])
                    return {"ok": False, "status": resp.status, "data": data}
                return {"ok": True, "data": data}
    except Exception as exc:
        logger.exception("Ошибка отправки через Wazzup: %s", exc)
        return {"ok": False, "error": str(exc)}


async def tg_send_text(
    chat_id: int | str,
    text: str,
    reply_markup: dict[str, Any] | None = None,
    parse_mode: str | None = None,
) -> int | None:
    # Bot menus and automated replies must go directly through Telegram Bot API.
    # Wazzup is the operator inbox; routing every bot reply through Wazzup
    # breaks Telegram reply-keyboards and fills the operator chat with bot menus.
    if not TELEGRAM_BOT_TOKEN:
        logger.error("Не задан TELEGRAM_BOT_TOKEN")
        return None
    payload: dict[str, Any] = {"chat_id": chat_id, "text": text}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    if parse_mode:
        payload["parse_mode"] = parse_mode
    try:
        async with aiohttp.ClientSession() as session:
            res = await tg_api(session, "sendMessage", payload)
            if not res.get("ok"):
                return None
            mid = (res.get("result") or {}).get("message_id")
            return int(mid) if isinstance(mid, int) else None
    except Exception as exc:
        logger.exception("Ошибка отправки сообщения в Telegram: %s", exc)
        return None


async def tg_delete_message(chat_id: int | str, message_id: int) -> bool:
    if not TELEGRAM_BOT_TOKEN or not message_id:
        return False
    try:
        async with aiohttp.ClientSession() as session:
            res = await tg_api(session, "deleteMessage", {"chat_id": chat_id, "message_id": int(message_id)})
            return bool(res.get("ok"))
    except Exception:
        return False


def schedule_ephemeral_delete(
    chat_id: int | str,
    message_id: int | None,
    ttl_sec: int = EPHEMERAL_TTL_SEC,
) -> None:
    if not message_id:
        return
    items = _read_json(EPHEMERAL_MSGS_FILE, [])
    if not isinstance(items, list):
        items = []
    items.append({
        "chat_id": str(chat_id),
        "message_id": int(message_id),
        "delete_at": time.time() + max(3, int(ttl_sec)),
    })
    _write_json(EPHEMERAL_MSGS_FILE, items)


async def send_ephemeral_text(
    chat_id: int | str,
    text: str,
    reply_markup: dict[str, Any] | None = None,
    ttl_sec: int = EPHEMERAL_TTL_SEC,
) -> int | None:
    mid = await tg_send_text(chat_id, text, reply_markup=reply_markup)
    if mid:
        async with _FILE_LOCKS["ephemeral_msgs"]:
            schedule_ephemeral_delete(chat_id, mid, ttl_sec=ttl_sec)
    return mid


async def ephemeral_cleaner_worker() -> None:
    logger.info("Запущен воркер автоудаления исчезающих сообщений (TTL=%s сек)", EPHEMERAL_TTL_SEC)
    while True:
        try:
            async with _FILE_LOCKS["ephemeral_msgs"]:
                items = _read_json(EPHEMERAL_MSGS_FILE, [])
                if isinstance(items, list) and items:
                    now = time.time()
                    remaining: list[dict[str, Any]] = []
                    changed = False
                    for item in items:
                        if not isinstance(item, dict):
                            changed = True
                            continue
                        if now >= float(item.get("delete_at", 0)):
                            cid = item.get("chat_id")
                            mid = item.get("message_id")
                            if cid and mid:
                                await tg_delete_message(cid, int(mid))
                            changed = True
                        else:
                            remaining.append(item)
                    if changed:
                        _write_json(EPHEMERAL_MSGS_FILE, remaining)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.exception("Ошибка в воркере исчезающих сообщений: %s", exc)
        await asyncio.sleep(3)


async def tg_send_photo(
    chat_id: int | str,
    file_id: str,
    caption: str = "",
) -> bool:
    if not file_id:
        return False
    # Photos sent by the bot remain Telegram messages; operator replies are handled by Wazzup.
    if not TELEGRAM_BOT_TOKEN:
        return False
    payload: dict[str, Any] = {"chat_id": chat_id, "photo": file_id}
    if caption:
        payload["caption"] = caption
    try:
        async with aiohttp.ClientSession() as session:
            res = await tg_api(session, "sendPhoto", payload)
            return bool(res.get("ok"))
    except Exception as exc:
        logger.warning("Ошибка отправки фото в Telegram: %s", exc)
        return False


async def tg_download_file(file_id: str, default_name: str) -> Optional[tuple[bytes, str]]:
    if not file_id:
        return None
    try:
        async with aiohttp.ClientSession() as session:
            if str(file_id).startswith(("https://", "http://")):
                async with session.get(str(file_id), timeout=30, allow_redirects=True) as resp:
                    if resp.status != 200:
                        return None
                    raw = await resp.read()
                    ctype = resp.headers.get("Content-Type", "")
                    ext = os.path.splitext(default_name)[1] or (".png" if "png" in ctype else ".jpg")
                    return raw, os.path.splitext(default_name)[0] + ext
            if not TELEGRAM_BOT_TOKEN:
                return None
            info = await tg_api(session, "getFile", {"file_id": file_id})
            file_path = (info.get("result") or {}).get("file_path")
            if not file_path:
                return None
            dl_url = f"https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{file_path}"
            async with session.get(dl_url, timeout=25) as resp:
                if resp.status != 200:
                    return None
                raw = await resp.read()
                ext = os.path.splitext(file_path)[1] or os.path.splitext(default_name)[1] or ".jpg"
                stem = os.path.splitext(default_name)[0]
                fname = f"{stem}{ext}"
                return (raw, fname)
    except Exception as exc:
        logger.warning("Ошибка скачивания файла Telegram %s: %s", file_id, exc)
        return None


# ================= ОТЛОЖЕННОЕ СООБЩЕНИЕ ЧЕРЕЗ 6 МИНУТ + ДОП. СООБЩЕНИЕ ЧЕРЕЗ 5 СЕК =================
def schedule_delayed_profile_msg(chat_id: int | str, delay_sec: int = PROFILE_READY_DELAY_SEC) -> None:
    tasks = _read_json(DELAYED_TASKS_FILE, [])
    if not isinstance(tasks, list):
        tasks = []
    now = time.time()
    tasks.append({
        "id": str(uuid.uuid4()),
        "kind": "profile_ready",
        "chat_id": str(chat_id),
        "send_at": now + delay_sec,
        "text": MSG_PROFILE_CREATED_10M,
    })
    tasks.append({
        "id": str(uuid.uuid4()),
        "kind": "cabinet_hint",
        "chat_id": str(chat_id),
        "send_at": now + delay_sec + AFTER_10M_REMINDER_DELAY_SEC,
        "text": MSG_AFTER_10M_CABINET_HINT,
    })
    _write_json(DELAYED_TASKS_FILE, tasks)


async def delayed_notifications_worker() -> None:
    logger.info("Запущен фоновый воркер автосообщений через 6 мин (+5 сек подсказка)")
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
                            kind = str(task.get("kind") or "profile_ready")
                            txt = str(task.get("text") or MSG_PROFILE_CREATED_10M)
                            is_adm = is_admin_user(cid, cid)
                            mid = await tg_send_text(cid, txt, reply_markup=kb_start_menu(is_admin=is_adm))
                            if mid:
                                if kind == "cabinet_hint":
                                    schedule_ephemeral_delete(cid, mid, ttl_sec=45)
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
        await asyncio.sleep(2)


# ================= AMOCRM =================
def _amocrm_headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {AMOCRM_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }


def _guess_image_mime(fname: str) -> str:
    low = (fname or "").lower()
    if low.endswith(".png"):
        return "image/png"
    if low.endswith(".webp"):
        return "image/webp"
    return "image/jpeg"


def _resolve_city_enum_id(city_raw: str) -> int | None:
    low = (city_raw or "").strip().lower()
    if not low or low == "—":
        return None
    if low in CITY_LIST_ENUM_MAP:
        return CITY_LIST_ENUM_MAP[low]
    if low in ("спб", "питер", "санкт петербург", "с-петербург"):
        return CITY_LIST_ENUM_MAP["санкт-петербург"]
    if low in ("мск",):
        return CITY_LIST_ENUM_MAP["москва"]
    clean = re.sub(r"[^а-яёa-z0-9\s-]", "", low).strip()
    if clean in CITY_LIST_ENUM_MAP:
        return CITY_LIST_ENUM_MAP[clean]
    for k, eid in CITY_LIST_ENUM_MAP.items():
        if len(clean) >= 4 and (clean in k or k in clean):
            return eid
    return None


def _resolve_driver_type_enum_id(transport_raw: str) -> int | None:
    low = (transport_raw or "").strip().lower()
    if not low:
        return None
    if low in DRIVER_TYPE_ENUM_MAP:
        return DRIVER_TYPE_ENUM_MAP[low]
    clean = normalize_choice_text(transport_raw)
    if clean in DRIVER_TYPE_ENUM_MAP:
        return DRIVER_TYPE_ENUM_MAP[clean]
    if "груз" in clean:
        return 5241179
    if "мот" in clean or "скут" in clean:
        return 5241177
    if "авто" in clean or "маш" in clean or "легк" in clean:
        return 5241173
    if "пеш" in clean or "вел" in clean:
        return 5241175
    return None


def _build_lead_custom_fields(
    city_val: str,
    transport_val: str,
    pro_number: str = "",
) -> list[dict[str, Any]]:
    cf: list[dict[str, Any]] = []
    dt_enum = _resolve_driver_type_enum_id(transport_val)
    if dt_enum:
        cf.append({"field_id": LEAD_FIELD_DRIVER_TYPE, "values": [{"enum_id": dt_enum}]})

    city_enum = _resolve_city_enum_id(city_val)
    if city_enum:
        cf.append({"field_id": LEAD_FIELD_CITY_LIST, "values": [{"enum_id": city_enum}]})

    digits_pro = re.sub(r"\D", "", pro_number or "")
    if digits_pro:
        cf.append({"field_id": LEAD_FIELD_PRO_NUMBER, "values": [{"value": int(digits_pro)}]})
    return cf


def _build_contact_custom_fields(
    full_name: str,
    phone: str,
    city_val: str,
) -> list[dict[str, Any]]:
    cf: list[dict[str, Any]] = []
    if phone:
        cf.append({
            "field_id": CONTACT_FIELD_PHONE_MULTI,
            "values": [{"value": phone, "enum_id": CONTACT_ENUM_PHONE_MOB}],
        })
        cf.append({
            "field_id": CONTACT_FIELD_PHONE_TEXT,
            "values": [{"value": phone}],
        })
    if full_name:
        cf.append({
            "field_id": CONTACT_FIELD_FIO,
            "values": [{"value": full_name.strip()}],
        })
    if city_val and city_val != "—":
        cf.append({
            "field_id": CONTACT_FIELD_CITY,
            "values": [{"value": city_val.strip()}],
        })
    return cf


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


async def _add_amocrm_contact_note(
    session: aiohttp.ClientSession,
    base_url: str,
    contact_id: int,
    text: str,
) -> None:
    notes_url = f"{base_url}/api/v4/contacts/{contact_id}/notes"
    notes_payload = [{"note_type": "common", "params": {"text": text}}]
    async with session.post(notes_url, headers=_amocrm_headers(), json=notes_payload, timeout=15) as nresp:
        if nresp.status not in (200, 201, 202):
            logger.warning("amoCRM contact note warning (%s): %s", nresp.status, await nresp.text())


async def _find_or_create_contact_id(
    session: aiohttp.ClientSession,
    base_url: str,
    chat_id: int | str,
    full_name: str,
    phone: str = "",
    tg_login: str = "",
    city: str = "",
) -> int | None:
    norm_p = normalize_phone(phone) if phone else ""
    queries = [q for q in [norm_p, tg_login, str(chat_id)] if q]

    for query in queries:
        async with session.get(
            f"{base_url}/api/v4/contacts",
            params={"query": query},
            headers=_amocrm_headers(),
            timeout=15,
        ) as resp:
            if resp.status != 200:
                continue
            data = await resp.json()
            contacts = data.get("_embedded", {}).get("contacts", []) or []
            if contacts:
                return int(contacts[0]["id"])

    contact_cf_values = _build_contact_custom_fields(full_name, norm_p, city)
    payload = [
        {
            "first_name": (full_name or "Пользователь").strip(),
            "custom_fields_values": contact_cf_values,
        }
    ]
    async with session.post(
        f"{base_url}/api/v4/contacts",
        headers=_amocrm_headers(),
        json=payload,
        timeout=15,
    ) as cresp:
        if cresp.status not in (200, 201, 202):
            logger.warning("amoCRM create contact warning (%s): %s", cresp.status, await cresp.text())
            return None
        created = await cresp.json()
        c_list = created.get("_embedded", {}).get("contacts", []) or []
        if c_list:
            return int(c_list[0]["id"])
    return None


async def _get_drive_base_url(session: aiohttp.ClientSession, base_url: str) -> str | None:
    try:
        async with session.get(
            f"{base_url}/api/v4/account",
            params={"with": "drive_url"},
            headers=_amocrm_headers(),
            timeout=15,
        ) as resp:
            if resp.status == 200:
                acc = await resp.json()
                for key in ("drive_url", "files_url", "drive_host"):
                    val = acc.get(key)
                    if isinstance(val, str) and val.strip().startswith("http"):
                        return val.strip().rstrip("/")
                emb = acc.get("_embedded") or {}
                for key in ("drive_url", "drive"):
                    val = emb.get(key)
                    if isinstance(val, str) and val.strip().startswith("http"):
                        return val.strip().rstrip("/")
    except Exception:
        pass
    return None


async def _upload_and_attach_docs(
    session: aiohttp.ClientSession,
    base_url: str,
    lead_id: int | None,
    contact_id: int | None,
    doc_items: list[tuple[str, Optional[tuple[bytes, str]], str]],
) -> list[str]:
    non_empty = [(label, doc_tuple, fid) for label, doc_tuple, fid in doc_items if doc_tuple or fid]
    if not non_empty:
        return []

    drive_url = await _get_drive_base_url(session, base_url)
    uploaded_lines: list[str] = []

    for label, doc_tuple, tg_file_id in non_empty:
        fname = doc_tuple[1] if doc_tuple else f"{label}.jpg"
        raw_bytes = doc_tuple[0] if doc_tuple else None

        if not drive_url or not raw_bytes:
            uploaded_lines.append(f"{label}: {fname} — фото получено в Telegram (отправлено оператору)")
            continue

        try:
            async with session.post(
                f"{drive_url}/v1.0/sessions",
                headers=_amocrm_headers(),
                json={"file_name": fname, "file_size": len(raw_bytes)},
                timeout=20,
            ) as s_resp:
                if s_resp.status not in (200, 201, 202):
                    uploaded_lines.append(f"{label}: {fname} — фото получено в Telegram")
                    continue
                s_data = await s_resp.json()
                upload_url = s_data.get("upload_url") or s_data.get("next_url")
                if not upload_url:
                    uploaded_lines.append(f"{label}: {fname} — фото получено в Telegram")
                    continue
                if not str(upload_url).startswith("http"):
                    upload_url = f"{drive_url}/{str(upload_url).lstrip('/')}"

                form = aiohttp.FormData()
                form.add_field("file", raw_bytes, filename=fname, content_type=_guess_image_mime(fname))
                async with session.post(
                    upload_url,
                    data=form,
                    headers={"Authorization": f"Bearer {AMOCRM_ACCESS_TOKEN}"},
                    timeout=30,
                ) as u_resp:
                    if u_resp.status not in (200, 201, 202):
                        uploaded_lines.append(f"{label}: {fname} — фото получено в Telegram")
                        continue
                    u_data = await u_resp.json()
                    file_uuid = u_data.get("uuid") or u_data.get("file_uuid")
                    if file_uuid:
                        for ent, eid in [("leads", lead_id), ("contacts", contact_id)]:
                            if eid:
                                await session.put(
                                    f"{base_url}/api/v4/{ent}/{eid}/files",
                                    headers=_amocrm_headers(),
                                    json=[{"file_uuid": file_uuid}],
                                    timeout=20,
                                )
                        uploaded_lines.append(f"{label}: {fname} (прикреплён)")
                    else:
                        uploaded_lines.append(f"{label}: {fname} (загружен)")
        except Exception:
            uploaded_lines.append(f"{label}: {fname} — фото получено в Telegram")

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


async def _patch_lead_fields_safe(
    session: aiohttp.ClientSession,
    base_url: str,
    lead_id: int,
    lead_title: str,
    lead_cf_values: list[dict[str, Any]],
) -> list[str]:
    warnings: list[str] = []
    patch_obj: dict[str, Any] = {"name": lead_title}
    if lead_cf_values:
        patch_obj["custom_fields_values"] = lead_cf_values

    async with session.patch(
        f"{base_url}/api/v4/leads/{lead_id}",
        headers=_amocrm_headers(),
        json=patch_obj,
        timeout=20,
    ) as presp:
        if presp.status in (200, 201, 202):
            return warnings
        logger.warning("amoCRM patch warning (%s): %s", presp.status, (await presp.text())[:250])

    for item in lead_cf_values:
        async with session.patch(
            f"{base_url}/api/v4/leads/{lead_id}",
            headers=_amocrm_headers(),
            json={"custom_fields_values": [item]},
            timeout=15,
        ) as single_resp:
            if single_resp.status not in (200, 201, 202):
                warnings.append(f"поле id={item.get('field_id')} не записано")
    return warnings


async def send_lead_to_amocrm(
    lead: dict[str, Any],
    doc_items: list[tuple[str, Optional[tuple[bytes, str]], str]] | None = None,
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
        address = str(lead.get("address") or "").strip()
        city_val = str(lead.get("city") or "").strip()
        transport_val = str(lead.get("transport") or "").strip()
        pro_number = str(lead.get("pro_number") or get_user_record(chat_id).get("driver_id") or "")
        lead_title = f"{lead['name']} | ЯндексЕда"

        note_lines = [
            "Парк: ЛунапаркЕда",
            f"Тип заявки: {lead['type']}",
            f"ФИО: {lead['name']}",
            f"Город: {city_val or '—'}",
            f"Адрес прописки: {address or '—'}",
            f"Формат/транспорт: {transport_val or '—'}",
            f"Телефон: {phone}",
            f"Telegram: {tg_login}",
            f"Ссылка на чат: {tg_link}",
            f"Telegram ID: {chat_id}",
            f"Живой чат с курьером: /chat {chat_id}",
        ]

        try:
            async with aiohttp.ClientSession() as session:
                lead_cf_values = _build_lead_custom_fields(city_val, transport_val, pro_number=pro_number)
                contact_cf_values = _build_contact_custom_fields(lead["name"], phone, city_val)

                existing_lead_id = await _find_existing_client_lead(session, base_url, chat_id, phone)
                if existing_lead_id:
                    cf_warns = await _patch_lead_fields_safe(session, base_url, existing_lead_id, lead_title, lead_cf_values)
                    if cf_warns:
                        note_lines.append("Примечание по полям: " + ", ".join(cf_warns))
                    doc_lines = await _upload_and_attach_docs(
                        session, base_url, existing_lead_id, None, doc_items or []
                    )
                    if doc_lines:
                        note_lines.extend(doc_lines)
                    await _add_amocrm_note(session, base_url, existing_lead_id, "\n".join(note_lines))
                    link_crm_entity_to_tg(existing_lead_id, None, chat_id)
                    return True, f"обновлена сделка #{existing_lead_id}"

                lead_obj: dict[str, Any] = {"name": lead_title}
                if AMOCRM_PIPELINE_ID:
                    lead_obj["pipeline_id"] = AMOCRM_PIPELINE_ID
                if AMOCRM_STATUS_ID:
                    lead_obj["status_id"] = AMOCRM_STATUS_ID
                if lead_cf_values:
                    lead_obj["custom_fields_values"] = lead_cf_values

                payload = [
                    {
                        **lead_obj,
                        "_embedded": {
                            "contacts": [
                                {"first_name": lead["name"], "custom_fields_values": contact_cf_values}
                            ]
                        },
                    }
                ]

                url = f"{base_url}/api/v4/leads/complex"
                async with session.post(url, headers=_amocrm_headers(), json=payload, timeout=20) as resp:
                    resp_text = await resp.text()
                    if resp.status not in (200, 201, 202):
                        fallback_payload = [
                            {
                                "name": lead_title,
                                **({"pipeline_id": AMOCRM_PIPELINE_ID} if AMOCRM_PIPELINE_ID else {}),
                                **({"status_id": AMOCRM_STATUS_ID} if AMOCRM_STATUS_ID else {}),
                                "_embedded": {
                                    "contacts": [
                                        {
                                            "first_name": lead["name"],
                                            "custom_fields_values": (
                                                [
                                                    {
                                                        "field_id": CONTACT_FIELD_PHONE_MULTI,
                                                        "values": [{"value": phone, "enum_id": CONTACT_ENUM_PHONE_MOB}],
                                                    }
                                                ]
                                                if phone
                                                else []
                                            ),
                                        }
                                    ]
                                },
                            }
                        ]
                        async with session.post(url, headers=_amocrm_headers(), json=fallback_payload, timeout=20) as fb_resp:
                            fb_text = await fb_resp.text()
                            if fb_resp.status not in (200, 201, 202):
                                return False, f"HTTP {fb_resp.status}: {fb_text[:200]}"
                            created = json.loads(fb_text)
                            note_lines.append(f"Внимание по полям amoCRM: {resp_text[:180]}")
                    else:
                        created = json.loads(resp_text)

                    lead_id = created[0].get("id") if isinstance(created, list) and created else None
                    contact_id = None
                    if isinstance(created, list) and created:
                        c_emb = created[0].get("_embedded", {}).get("contacts", [])
                        if c_emb:
                            contact_id = c_emb[0].get("id")

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


# ================= YANDEX FLEET API =================
def _safe_float(val: Any) -> float:
    if val is None:
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, dict):
        for k in ("km", "kilometers", "meters", "amount", "value", "total", "price"):
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
    if "meter" in uh or "метр" in uh or uh == "m":
        return v / 1000.0
    if "km" in uh or "километр" in uh:
        return v
    if v >= 200:
        return v / 1000.0
    return v


def _extract_order_distance_km(order: dict[str, Any]) -> float:
    direct_km_keys = (
        "distance_km",
        "mileage_km",
        "route_length_km",
        "paid_distance_km",
        "total_distance_km",
        "order_distance_km",
    )
    for key in direct_km_keys:
        if key in order and order[key] is not None:
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
        if key not in order or order[key] is None:
            continue
        raw_v = order[key]
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
                if sub_k in raw_v and raw_v[sub_k] is not None:
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

    route_obj = order.get("route")
    if isinstance(route_obj, dict):
        for rk in ("distance_km", "km", "distance", "length", "meters"):
            if rk in route_obj and route_obj[rk] is not None:
                rv = _normalize_dist_value_to_km(route_obj[rk], rk)
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
            logger.warning("orders/list ошибка для %s: %s", driver_id, exc)
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

        completed_7d = 0
        completed_goal = 0
        distance_km_7d = 0.0

        for order in data.get("orders", []):
            if not isinstance(order, dict):
                continue
            status = str(order.get("status") or "")
            dist_km = _extract_order_distance_km(order)
            odt = _parse_order_dt(order)

            if _is_completed_order_status(status):
                if odt is None or odt >= week_ago:
                    completed_7d += 1
                    distance_km_7d += dist_km
                if odt is None or (goal_start_dt <= odt <= goal_end):
                    completed_goal += 1

        result = {
            "ok": True,
            "error": "",
            "completed_7d": completed_7d,
            "completed_goal": completed_goal,
            "within_goal_window": now <= goal_end,
            "distance_km_7d": round(distance_km_7d, 1),
            "distance_estimated": False,
        }
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
    except Exception:
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
        f"📦 Заказов выполнено: {stats['completed_7d']}\n"
        f"🛣 Прошёл / проехал: {stats['distance_km_7d']:.1f}{km_suffix}\n\n"
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
            f"Доступ к данным профиля запрещён в целях безопасности. Если ты сменил аккаунт — напиши оператору @{SUPPORT_TG_USERNAME}.",
            reply_markup=kb_support_inline(),
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
    attachment = message.get("attachment") or message.get("attachments")
    if isinstance(attachment, list) and attachment:
        attachment = attachment[0]
    if isinstance(attachment, dict):
        return str(attachment.get("contentUri") or attachment.get("url") or attachment.get("download_url") or "")
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
                f"Создали заявку! Также ты можешь сразу написать нашему оператору в Telegram: @{SUPPORT_TG_USERNAME} 🙌",
                reply_markup=kb_support_inline(),
            )
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

    # ================= КНОПКА «ПОМОЩЬ» -> СРАЗУ ОТКРЫВАЕТ ЧАТ @LUNAPARK_DR =================
    if norm_cmd in ("помощь", "нужна помощь", "поддержка", "оператор"):
        await tg_send_text(
            chat_id,
            f"Нажми на кнопку ниже, чтобы сразу открыть чат с оператором @{SUPPORT_TG_USERNAME} 🙌",
            reply_markup=kb_support_inline(),
        )
        return

    if norm_cmd in ("отмена", "отменить", "cancel"):
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
                f"{ai_ans}\n\nЕсли нужен оператор — пиши @{SUPPORT_TG_USERNAME} или нажми кнопку ниже:",
                reply_markup=kb_support_inline(),
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


async def _fetch_latest_amocrm_note_text(
    lead_id: str | None = None,
    contact_id: str | None = None,
) -> str:
    if not AMOCRM_BASE_URL or not AMOCRM_ACCESS_TOKEN:
        return ""
    base_url = AMOCRM_BASE_URL.rstrip("/")
    targets: list[str] = []
    if lead_id and str(lead_id).isdigit():
        targets.append(f"{base_url}/api/v4/leads/{lead_id}/notes")
    if contact_id and str(contact_id).isdigit():
        targets.append(f"{base_url}/api/v4/contacts/{contact_id}/notes")

    try:
        async with aiohttp.ClientSession() as session:
            for url in targets:
                async with session.get(
                    url,
                    params={"limit": 10, "order[created_at]": "desc"},
                    headers=_amocrm_headers(),
                    timeout=15,
                ) as resp:
                    logger.info("amoCRM notes GET %s -> status %s", url, resp.status)
                    if resp.status != 200:
                        continue
                    data = await resp.json()
                    notes = data.get("_embedded", {}).get("notes", []) or []
                    for n in notes:
                        if not isinstance(n, dict):
                            continue
                        txt = str((n.get("params") or {}).get("text") or n.get("text") or "").strip()
                        if not txt:
                            continue
                        if txt.startswith("Парк: ЛунапаркЕда") or txt.startswith("Обращение по кнопке"):
                            continue
                        logger.info("Найдено примечание в amoCRM: %s", txt[:120])
                        return txt
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
                                note_text = str((notes[0].get("params") or {}).get("text") or notes[0].get("text") or "").strip()

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


_WAZZUP_SEEN_MESSAGES: dict[str, float] = {}
_WAZZUP_SEEN_EVENTS: dict[str, float] = {}


def _wazzup_to_telegram_update(payload: dict[str, Any]) -> dict[str, Any] | None:
    """Convert one Wazzup message.add data item into the bot's update shape."""
    msg = payload.get("message") if isinstance(payload.get("message"), dict) else payload

    if "messageId" in msg or ("chatId" in msg and "chatType" in msg):
        if msg.get("isEcho") or msg.get("isDeleted"):
            return None
        if str(msg.get("status") or "inbound").lower() != "inbound":
            return None
        chat_type = str(msg.get("chatType") or "").lower()
        if chat_type and chat_type != "telegram":
            return None
        v3_chat_id = str(msg.get("chatId") or "").strip()
        if not v3_chat_id:
            logger.warning("Wazzup message: пустой chatId; keys=%s", list(msg.keys()))
            return None
        if not v3_chat_id.lstrip("-").isdigit():
            logger.warning(
                "Wazzup message: chatId=%r не числовой — ответ через Telegram Bot API может не уйти",
                v3_chat_id,
            )

        contact = msg.get("contact") if isinstance(msg.get("contact"), dict) else {}
        v3_first_name = str(contact.get("name") or "друг")
        v3_username = str(contact.get("username") or "")
        v3_message_id = msg.get("messageId") or str(time.time_ns())

        v3_message: dict[str, Any] = {
            "message_id": v3_message_id,
            "date": int(time.time()),
            "chat": {"id": v3_chat_id, "type": "private", "first_name": v3_first_name, "last_name": ""},
            "from": {
                "id": v3_chat_id, "is_bot": False,
                "first_name": v3_first_name, "last_name": "", "username": v3_username,
            },
            "text": str(msg.get("text") or ""),
        }

        content_uri = msg.get("contentUri")
        if content_uri:
            file_obj = {"file_id": str(content_uri), "file_unique_id": str(v3_message_id)}
            if str(msg.get("type") or "").lower() == "image":
                v3_message["photo"] = [file_obj]
            else:
                file_obj["file_name"] = "attachment"
                v3_message["document"] = file_obj

        return {"update_id": int(time.time() * 1000), "message": v3_message}

    direction = str(msg.get("direction") or payload.get("direction") or "").lower()
    if direction not in ("in", "incoming", "inbound"):
        return None

    recipient = msg.get("recipient") if isinstance(msg.get("recipient"), dict) else {}
    sender = msg.get("sender") if isinstance(msg.get("sender"), dict) else {}

    chat_id = (
        msg.get("chat_id") or msg.get("chatId")
        or recipient.get("chat_id") or recipient.get("chatId")
        or sender.get("chat_id") or sender.get("chatId")
    )
    if not chat_id:
        logger.warning(
            "Wazzup message.add: Telegram chat_id отсутствует; keys=%s recipient_keys=%s sender_keys=%s",
            list(msg.keys()), list(recipient.keys()), list(sender.keys())
        )
        return None

    chat_id = str(chat_id)
    username = sender.get("username") or recipient.get("username") or ""
    first_name = sender.get("name") or sender.get("first_name") or recipient.get("name") or "друг"
    last_name = sender.get("last_name") or recipient.get("last_name") or ""
    text_value = msg.get("text") or ""
    raw_message_id = msg.get("message_id") or msg.get("id") or str(time.time_ns())

    message: dict[str, Any] = {
        "message_id": raw_message_id,
        "date": int((msg.get("timestamp") or time.time()) / (1000 if (msg.get("timestamp") or 0) > 10**12 else 1)),
        "chat": {"id": chat_id, "type": "private", "first_name": str(first_name), "last_name": str(last_name)},
        "from": {"id": chat_id, "is_bot": False, "first_name": str(first_name), "last_name": str(last_name), "username": str(username)},
        "text": str(text_value),
    }

    attachment = msg.get("attachment") or msg.get("attachments")
    if isinstance(attachment, list) and attachment:
        attachment = attachment[0]
    if isinstance(attachment, dict):
        url = attachment.get("url") or attachment.get("contentUri") or attachment.get("download_url")
        kind = str(attachment.get("mimetype") or attachment.get("mime_type") or attachment.get("content_type") or attachment.get("type") or "").lower()
        if url:
            file_obj = {"file_id": str(url), "file_unique_id": str(raw_message_id)}
            if "image" in kind or str(url).lower().split("?")[0].endswith((".jpg", ".jpeg", ".png", ".webp")):
                message["photo"] = [file_obj]
            else:
                file_obj["file_name"] = attachment.get("name") or attachment.get("file_name") or "attachment"
                message["document"] = file_obj

    return {"update_id": int(time.time() * 1000), "message": message}


def _wazzup_get_channel_id(obj: dict[str, Any]) -> str:
    """Получает channelId из разных вариантов структуры webhook."""
    if not isinstance(obj, dict):
        return ""

    for key in ("channelId", "channel_id"):
        value = obj.get(key)
        if isinstance(value, dict):
            value = value.get("id") or value.get("channelId")
        if value:
            return str(value).strip()

    channel = obj.get("channel")
    if isinstance(channel, dict):
        value = channel.get("id") or channel.get("channelId")
        if value:
            return str(value).strip()

    message = obj.get("message")
    if isinstance(message, dict):
        return _wazzup_get_channel_id(message)

    meta = obj.get("meta")
    if isinstance(meta, dict):
        return _wazzup_get_channel_id(meta)

    return ""


async def handle_wazzup_webhook(request: web.Request) -> web.Response:
    expected = WAZZUP_WEBHOOK_AUTHORIZATION or WAZZUP_WEBHOOK_SECRET
    if expected:
        authorization = request.headers.get("Authorization", "").strip()
        legacy_supplied = (
            request.query.get("secret")
            or request.headers.get("X-Wazzup-Secret", "")
        ).strip()

        valid_authorization = (
            hmac.compare_digest(authorization, expected)
            or hmac.compare_digest(authorization, f"Bearer {expected}")
        )
        valid_legacy = bool(legacy_supplied) and hmac.compare_digest(
            legacy_supplied, expected
        )

        if not (valid_authorization or valid_legacy):
            logger.warning(
                "Wazzup webhook: отклонён запрос без корректной авторизации "
                "(Authorization present=%s, legacy present=%s)",
                bool(authorization),
                bool(legacy_supplied),
            )
            return web.json_response({"ok": False}, status=403)

    try:
        payload = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400)
    if not isinstance(payload, dict):
        return web.json_response({"ok": True, "ignored": "not_object"})

    v3_messages = payload.get("messages")
    if isinstance(v3_messages, list):
        items = [item for item in v3_messages if isinstance(item, dict)]
    else:
        if payload.get("test"):
            return web.json_response({"ok": True, "test": True})
        event = str(payload.get("event") or payload.get("type") or "").lower()
        if event not in ("message.add", "message_add", "message"):
            logger.debug("Wazzup webhook: событие пропущено, keys=%s", list(payload.keys()))
            return web.json_response({"ok": True, "ignored": event or "unknown_event"})

        raw_items = payload.get("data")
        if isinstance(raw_items, list):
            items = [item for item in raw_items if isinstance(item, dict)]
        elif isinstance(payload.get("message"), dict):
            items = [payload["message"]]
        else:
            items = [payload]

    now = time.time()
    for cache in (_WAZZUP_SEEN_MESSAGES, _WAZZUP_SEEN_EVENTS):
        for key, stamp in list(cache.items()):
            if now - stamp > 86400:
                cache.pop(key, None)

    meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
    event_key = str(meta.get("idempotency_key") or "")
    if event_key and event_key in _WAZZUP_SEEN_EVENTS:
        return web.json_response({"ok": True, "duplicate": True, "processed": 0})
    if event_key:
        _WAZZUP_SEEN_EVENTS[event_key] = now

    accepted = 0
    duplicates = 0
    ignored = 0
    for item in items:
        message_id = str(item.get("messageId") or item.get("message_id") or item.get("id") or "")
        if message_id and message_id in _WAZZUP_SEEN_MESSAGES:
            duplicates += 1
            continue

        update = _wazzup_to_telegram_update(item)
        if not update:
            ignored += 1
            continue

        if message_id:
            _WAZZUP_SEEN_MESSAGES[message_id] = now
        asyncio.create_task(handle_telegram_update(update))
        accepted += 1

    logger.info(
        "Wazzup webhook: items=%s accepted=%s duplicates=%s ignored=%s",
        len(items), accepted, duplicates, ignored
    )
    return web.json_response({"ok": True, "accepted": accepted, "duplicates": duplicates, "ignored": ignored})


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


async def wazzup_register_webhook() -> None:
    """Прописывает в Wazzup адрес вебхука с секретом в query string."""
    await asyncio.sleep(5)

    if not WAZZUP_PUBLIC_URL:
        logger.warning("WAZZUP_PUBLIC_URL не задан: адрес вебхука в Wazzup не обновляется")
        return
    secret = WAZZUP_WEBHOOK_AUTHORIZATION or WAZZUP_WEBHOOK_SECRET
    if not secret:
        logger.warning("Не задан WAZZUP_WEBHOOK_SECRET/WAZZUP_WEBHOOK_AUTHORIZATION: вебхук не обновляется")
        return

    base = WAZZUP_PUBLIC_URL
    if not base.endswith("/webhook/wazzup"):
        base += "/webhook/wazzup"
    webhooks_uri = f"{base}?secret={urllib.parse.quote(secret, safe='')}"
    if len(webhooks_uri) > 200:
        logger.error("Адрес вебхука длиннее 200 символов, Wazzup его не примет")
        return

    headers = {
        "Authorization": f"Bearer {WAZZUP_API_KEY}",
        "Content-Type": "application/json",
    }
    body = {
        "webhooksUri": webhooks_uri,
        "subscriptions": {"messagesAndStatuses": True},
    }
    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.patch(WAZZUP_WEBHOOKS_API_URL, json=body, headers=headers) as resp:
                text = await resp.text()
                if resp.status == 200:
                    logger.info("Wazzup: адрес вебхука обновлён (%s%s)", base, "?secret=***")
                else:
                    logger.error("Wazzup: не удалось обновить вебхук, status=%s body=%s", resp.status, text[:300])
    except Exception:
        logger.exception("Wazzup: ошибка при обновлении адреса вебхука")


async def on_startup(app: web.Application) -> None:
    app["delayed_worker"] = asyncio.create_task(delayed_notifications_worker())
    app["ephemeral_worker"] = asyncio.create_task(ephemeral_cleaner_worker())
    app["demand_worker"] = asyncio.create_task(demand_monitor_worker())
    if WAZZUP_ENABLED:
        if not WAZZUP_API_KEY or not WAZZUP_CHANNEL_ID:
            raise RuntimeError("WAZZUP_ENABLED=1, но не заданы WAZZUP_API_KEY и/или WAZZUP_CHANNEL_ID")
        logger.warning("Режим WAZZUP включён: Telegram polling и Telegram webhook НЕ запускаются")
        app["wazzup_webhook_task"] = asyncio.create_task(wazzup_register_webhook())
        return
    if not TELEGRAM_BOT_TOKEN:
        logger.error("Не задан TELEGRAM_BOT_TOKEN. Для старого режима укажите его в переменных окружения.")
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
    for task_key in ("polling_task", "delayed_worker", "ephemeral_worker", "demand_worker", "wazzup_webhook_task"):
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
    app.router.add_post("/webhook/wazzup", handle_wazzup_webhook)
    app.router.add_post("/webhook/amocrm", handle_amocrm_webhook)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


if __name__ == "__main__":
    web.run_app(create_app(), host="0.0.0.0", port=PORT)
