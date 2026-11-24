import asyncio
import hashlib
import hmac
import json
import logging
import os
import random
import re
import time
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
WAZZUP_ENABLED = os.getenv("WAZZUP_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
WAZZUP_API_URL = "https://api.wazzup24.com/v3/message"
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
