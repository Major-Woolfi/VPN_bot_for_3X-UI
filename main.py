import asyncio
import html
import json
import logging
import os
import re
import secrets
import string
import sys
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import aiofiles
import paramiko
import aiohttp
import aiosqlite
from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from aiogram.utils.callback_answer import CallbackAnswerMiddleware
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))


# --- Логирование ---
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            os.path.join(LOG_DIR, f"{datetime.now().strftime('%Y-%m-%d')}.log"),
            encoding="utf-8",
        ),
    ],
)
logger = logging.getLogger(__name__)


# --- Конфиг ---
def str_to_bool(val: str) -> bool:
    return str(val).strip().lower() in ("1", "true", "yes", "y", "on")


def is_valid_bot_token_format(token: str) -> bool:
    return bool(re.fullmatch(r"\d+:[A-Za-z0-9_-]+", str(token or "").strip()))


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default))
    try:
        return int(str(raw).strip())
    except Exception:
        return default


def env_float(name: str, default: float) -> float:
    raw = os.getenv(name, str(default))
    try:
        return float(str(raw).strip())
    except Exception:
        return default


def env_int_list(name: str) -> List[int]:
    values: List[int] = []
    for raw_item in os.getenv(name, "").split(","):
        item = raw_item.strip()
        if not item:
            continue
        try:
            values.append(int(item))
        except Exception:
            logger.warning(f"Некорректное значение в {name}: {item}")
    return values


class Config:
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
    ADMIN_USER_IDS: List[int] = env_int_list("ADMIN_USER_IDS")
    PAYMENT_CARD_NUMBER: str = os.getenv("PAYMENT_CARD_NUMBER", "")
    PANEL_BASE: str = os.getenv("PANEL_BASE", "").rstrip("/")
    SUB_PANEL_BASE: str = os.getenv("SUB_PANEL_BASE", "")
    PANEL_LOGIN: str = os.getenv("PANEL_LOGIN", "")
    PANEL_PASSWORD: str = os.getenv("PANEL_PASSWORD", "")
    PANEL_TOKEN: str = os.getenv("PANEL_TOKEN", "")
    VERIFY_SSL: bool = str_to_bool(os.getenv("VERIFY_SSL", "true"))
    DATA_DIR: str = os.getenv("DATA_DIR", "/data")
    DATA_FILE: str = os.getenv("DATA_FILE", os.path.join(DATA_DIR, "users.db"))
    DATA_AWAIT: str = os.getenv(
        "DATA_AWAIT", os.path.join(DATA_DIR, "await_payments.json")
    )
    SITE_URL: str = os.getenv("SITE_URL", "")
    SUPPORT_URL: str = os.getenv("SUPPORT_URL", "")
    QNA_URL: str = os.getenv("QNA_URL", "")
    PRIVACY_POLICY_URL: str = os.getenv("PRIVACY_POLICY_URL", "")
    PUBLIC_OFFER_URL: str = os.getenv("PUBLIC_OFFER_URL", "")
    TIKTOK_URL: str = os.getenv("TIKTOK_URL", "")
    YOUTUBE_URL: str = os.getenv("YOUTUBE_URL", "")
    TELEGRAM_URL: str = os.getenv("TELEGRAM_URL", "")
    REF_BONUS_DAYS: int = env_int("REF_BONUS_DAYS", 7)
    SSH_HOST: str = os.getenv("SSH_HOST", "")
    SSH_USER: str = os.getenv("SSH_USER", "")
    SSH_KEY_PATH: str = os.getenv("SSH_KEY_PATH", "")
    SSH_PASSWORD: str = os.getenv("SSH_PASSWORD", "")
    TARIFFS_PATH: str = os.getenv(
        "TARIFFS_PATH", os.path.join(BASE_DIR, "data", "tarifs.json")
    )
    PAYMENT_PROCESSING_TIMEOUT_SEC: int = env_int(
        "PAYMENT_PROCESSING_TIMEOUT_SEC", 15 * 60
    )
    TRUST_SCORE_MIN: int = env_int("TRUST_SCORE_MIN", 0)
    TRUST_SCORE_MAX: int = env_int("TRUST_SCORE_MAX", 100)
    TRUST_SCORE_EARN_PERCENT: int = env_int("TRUST_SCORE_EARN_PERCENT", 5)
    TRUST_SCORE_MAX_DISCOUNT_PERCENT: int = env_int(
        "TRUST_SCORE_MAX_DISCOUNT_PERCENT", 50
    )
    TRUST_SCORE_PENALTY_TRAFFIC_EXHAUSTED: int = env_int(
        "TRUST_SCORE_PENALTY_TRAFFIC_EXHAUSTED", 5
    )
    TRUST_SCORE_PENALTY_PAYMENT_REJECTED: int = env_int(
        "TRUST_SCORE_PENALTY_PAYMENT_REJECTED", 10
    )
    CUSTOM_TARIFF_ENABLED: bool = str_to_bool(
        os.getenv("CUSTOM_TARIFF_ENABLED", "false")
    )
    CUSTOM_TARIFF_MIN_IP: int = env_int("CUSTOM_TARIFF_MIN_IP", 1)
    CUSTOM_TARIFF_MAX_IP: int = env_int("CUSTOM_TARIFF_MAX_IP", 30)
    CUSTOM_TARIFF_MIN_GB: int = env_int("CUSTOM_TARIFF_MIN_GB", 1)
    CUSTOM_TARIFF_MAX_GB: int = env_int("CUSTOM_TARIFF_MAX_GB", 36500)
    CUSTOM_TARIFF_MIN_DAYS: int = env_int("CUSTOM_TARIFF_MIN_DAYS", 1)
    CUSTOM_TARIFF_MAX_DAYS: int = env_int("CUSTOM_TARIFF_MAX_DAYS", 365)
    CUSTOM_TARIFF_BASE_PRICE: float = env_float("CUSTOM_TARIFF_BASE_PRICE", 10.0)
    CUSTOM_TARIFF_GB_COEF: float = env_float("CUSTOM_TARIFF_GB_COEF", 0.2)
    CUSTOM_TARIFF_IP_DAY_COEF: float = env_float("CUSTOM_TARIFF_IP_DAY_COEF", 1.0)
    CUSTOM_TARIFF_LOCATION_DAY_PRICE: float = env_float(
        "CUSTOM_TARIFF_LOCATION_DAY_PRICE", 5.0
    )


try:
    os.makedirs(Config.DATA_DIR, exist_ok=True)
except Exception as e:
    logger.warning(f"Не удалось создать папку DATA_DIR={Config.DATA_DIR}: {e}")

ADMIN_USER_ID_SET = set(Config.ADMIN_USER_IDS)


# --- Тарифы ---
TARIFFS_PATH = Config.TARIFFS_PATH


REGIONAL_INDICATOR_A = 0x1F1E6


def flag_to_country_code(value: Any) -> str:
    text = str(value or "").strip()
    regional = [
        ch
        for ch in text
        if REGIONAL_INDICATOR_A <= ord(ch) <= REGIONAL_INDICATOR_A + 25
    ]
    if len(regional) < 2:
        return ""
    return "".join(
        chr(ord(ch) - REGIONAL_INDICATOR_A + ord("A")) for ch in regional[:2]
    )


def country_code_to_flag(value: Any) -> str:
    code = str(value or "").strip().upper()
    if len(code) != 2 or not code.isalpha() or not code.isascii():
        return ""
    return "".join(chr(REGIONAL_INDICATOR_A + ord(ch) - ord("A")) for ch in code)


def normalize_server_code(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""

    flag_code = flag_to_country_code(text)
    if flag_code:
        return flag_code

    return text.upper() if text.isascii() else text


def normalize_servers(value: Any) -> List[str]:
    if value is None:
        return []

    raw_values: List[Any]
    if isinstance(value, str):
        raw_values = [part.strip() for part in value.replace(";", ",").split(",")]
    elif isinstance(value, list):
        raw_values = value
    else:
        raw_values = [value]

    result: List[str] = []
    seen = set()
    for raw in raw_values:
        code = normalize_server_code(raw)
        if not code or code in seen:
            continue
        seen.add(code)
        result.append(code)
    return result


def parse_float_value(value: Any, default: float = 0.0) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return default


class TariffCatalog:
    """Хранит и предоставляет тарифы из JSON-файла."""

    def __init__(self, path: str):
        self.path = path
        self._active: List[Dict[str, Any]] = []
        self._by_id: Dict[str, Dict[str, Any]] = {}
        self._by_name: Dict[str, Dict[str, Any]] = {}
        self._locations: List[Dict[str, Any]] = []
        self._locations_by_code: Dict[str, Dict[str, Any]] = {}

    @staticmethod
    def is_trial(plan: Optional[Dict[str, Any]]) -> bool:
        if not plan:
            return False
        return plan.get("id") == "trial" or plan.get("price_rub", 0) == 0

    def load(self) -> None:
        if not os.path.exists(self.path):
            raise FileNotFoundError(f"Файл тарифов не найден: {self.path}")

        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            logger.error(f"Ошибка декодирования JSON в файле тарифов {self.path}: {e}")
            raise ValueError("Некорректный формат файла тарифов (JSON ошибка).") from e
        except Exception as e:
            logger.error(
                f"Непредвиденная ошибка при чтении файла тарифов {self.path}: {e}"
            )
            raise

        raw_locations = data.get("locations") or []
        if not isinstance(raw_locations, list):
            raise ValueError("tarifs.json должен содержать 'locations' в виде массива.")

        locations: List[Dict[str, Any]] = []
        locations_by_code: Dict[str, Dict[str, Any]] = {}
        for i, raw_location in enumerate(raw_locations):
            if not isinstance(raw_location, dict):
                logger.warning(
                    f"Пропуск некорректного элемента в locations на позиции {i}"
                )
                continue

            try:
                code = normalize_server_code(
                    raw_location.get("code")
                    or raw_location.get("id")
                    or raw_location.get("flag")
                    or raw_location.get("name")
                )
                if not code:
                    logger.warning(
                        f"Локация на позиции {i} не имеет кода, пропускается."
                    )
                    continue

                flag = str(
                    raw_location.get("flag") or country_code_to_flag(code)
                ).strip()
                name = str(
                    raw_location.get("name") or raw_location.get("title") or code
                )
                label = str(raw_location.get("label") or "").strip()
                if not label:
                    label = f"{flag} {code}".strip() if flag else code

                price_per_day = parse_float_value(
                    raw_location.get(
                        "price_per_day_rub", Config.CUSTOM_TARIFF_LOCATION_DAY_PRICE
                    ),
                    Config.CUSTOM_TARIFF_LOCATION_DAY_PRICE,
                )
                aliases = raw_location.get("aliases") or []
                if not isinstance(aliases, list):
                    aliases = [aliases]
                match = raw_location.get("match") or []
                if not isinstance(match, list):
                    match = [match]
                match_tokens = normalize_servers([code, flag] + aliases + match)

                location = {
                    "code": code,
                    "flag": flag,
                    "name": name,
                    "label": label,
                    "price_per_day_rub": max(0.0, price_per_day),
                    "match_tokens": match_tokens,
                }
                locations_by_code[code] = location
                locations.append(location)

            except Exception as e:
                logger.error(f"Ошибка при обработке локации на позиции {i}: {e}")
                continue

        plans = data.get("plans") or []
        if not isinstance(plans, list):
            raise ValueError("tarifs.json должен содержать 'plans' в виде массива.")

        normalized: List[Dict[str, Any]] = []
        for raw_plan in plans:
            if not isinstance(raw_plan, dict):
                logger.warning("Пропуск некорректного элемента в plans.")
                continue
            try:
                plan = dict(raw_plan)
                legacy_description = plan.pop("description", None)
                plan["servers"] = normalize_servers(
                    plan.get("servers") if "servers" in plan else legacy_description
                )
                plan.setdefault("active", True)
                normalized.append(plan)
            except Exception as e:
                logger.error(f"Ошибка при обработке плана: {e}")

        self._locations = locations
        self._locations_by_code = locations_by_code
        self._active = [p for p in normalized if p.get("active", True)]
        self._active.sort(key=lambda p: (p.get("sort", 9999), p.get("price_rub", 0)))
        self._by_id = {p.get("id"): p for p in normalized if p.get("id")}
        self._by_name = {p.get("name"): p for p in normalized if p.get("name")}

    def get_all_active(self) -> List[Dict[str, Any]]:
        return list(self._active)

    def get_by_id(self, plan_id: str) -> Optional[Dict[str, Any]]:
        return self._by_id.get(plan_id)

    def get_by_name(self, plan_name: str) -> Optional[Dict[str, Any]]:
        return self._by_name.get(plan_name)

    def get_minimal_by_price(self) -> Optional[Dict[str, Any]]:
        if not self._active:
            return None

        eligible = [p for p in self._active if not self.is_trial(p)]
        if not eligible:
            return None

        return min(
            eligible,
            key=lambda p: (
                p.get("price_rub", 0),
                p.get("traffic_gb", 0),
                p.get("ip_limit", 0),
            ),
        )

    def get_locations(self) -> List[Dict[str, Any]]:
        return [dict(location) for location in self._locations]

    def get_location_by_code(self, code: str) -> Optional[Dict[str, Any]]:
        location = self._locations_by_code.get(normalize_server_code(code))
        return dict(location) if location else None


tariff_catalog = TariffCatalog(TARIFFS_PATH)


def load_tariffs() -> None:
    tariff_catalog.load()


def get_all_active() -> List[Dict[str, Any]]:
    return tariff_catalog.get_all_active()


def get_by_id(plan_id: str) -> Optional[Dict[str, Any]]:
    return tariff_catalog.get_by_id(plan_id)


def get_by_name(plan_name: str) -> Optional[Dict[str, Any]]:
    return tariff_catalog.get_by_name(plan_name)


def is_trial_plan(plan: Optional[Dict[str, Any]]) -> bool:
    return tariff_catalog.is_trial(plan)


def get_minimal_by_price() -> Optional[Dict[str, Any]]:
    return tariff_catalog.get_minimal_by_price()


def get_custom_locations() -> List[Dict[str, Any]]:
    return tariff_catalog.get_locations()


def get_location_by_code(code: str) -> Optional[Dict[str, Any]]:
    return tariff_catalog.get_location_by_code(code)


def get_plan_servers(plan: Optional[Dict[str, Any]]) -> List[str]:
    if not plan:
        return []
    return normalize_servers(plan.get("servers"))


def get_location_price_per_day(code: str) -> float:
    location = get_location_by_code(code)
    if not location:
        return Config.CUSTOM_TARIFF_LOCATION_DAY_PRICE
    return max(0.0, parse_float_value(location.get("price_per_day_rub"), 0.0))


def format_server_label(code: Any) -> str:
    normalized = normalize_server_code(code)
    if not normalized:
        return ""

    location = get_location_by_code(normalized)
    if location:
        return str(location.get("label") or normalized)

    flag = country_code_to_flag(normalized)
    return f"{flag} {normalized}".strip() if flag else normalized


def format_servers(servers: Any) -> str:
    normalized = normalize_servers(servers)
    if not normalized:
        return "Все доступные"
    labels = [format_server_label(server) for server in normalized]
    return ", ".join(label for label in labels if label)


def get_server_match_tokens(server: Any) -> List[str]:
    normalized = normalize_server_code(server)
    if not normalized:
        return []

    tokens: List[str] = []

    def add_token(raw_token: Any) -> None:
        token = str(raw_token or "").strip()
        if token and token not in tokens:
            tokens.append(token)

    add_token(server)
    add_token(normalized)
    add_token(country_code_to_flag(normalized))
    location = get_location_by_code(normalized)
    if location:
        add_token(location.get("flag"))
        for token in location.get("match_tokens") or []:
            add_token(token)
    return tokens


def token_matches_inbound_label(label: str, token: Any) -> bool:
    value = str(token or "").strip()
    if not value:
        return False

    if flag_to_country_code(value):
        return value in label

    # Regex for alphanumeric matching, respecting word boundaries
    if value.isascii() and value.isalnum():
        # Pattern to match the token as a whole word (case-insensitive on label)
        pattern = rf"(?<![A-Z]){re.escape(value.upper())}(?![A-Z])"
        return re.search(pattern, label.upper()) is not None

    # Simple substring match for non-alphanumeric or other types
    if value.isascii():
        return value.upper() in label.upper()

    return value in label


def get_purchasable_catalog_plan(plan_id: str) -> Tuple[Optional[Dict[str, Any]], str]:
    plan = get_by_id(plan_id)
    if not plan or not plan.get("active", True):
        return None, "❌ Тариф не найден или недоступен"
    if is_trial_plan(plan):
        return None, "⚠️ Пробный тариф оформляется отдельно."
    return plan, ""


def format_traffic(traffic_gb: Any) -> str:
    try:
        value = float(traffic_gb)
    except Exception:
        return str(traffic_gb)

    if value >= 1024 and value % 1024 == 0:
        return f"{int(value / 1024)} ТБ"
    if value.is_integer():
        return f"{int(value)} ГБ"
    return f"{value} ГБ"


def format_duration(days: int) -> str:
    return f"{days} дней"


BYTES_IN_GB = 1073741824
PAYMENT_PROCESSING_TIMEOUT_SEC = Config.PAYMENT_PROCESSING_TIMEOUT_SEC

# --- Система очков доверия ---
TRUST_SCORE_MIN = Config.TRUST_SCORE_MIN
TRUST_SCORE_MAX = Config.TRUST_SCORE_MAX
TRUST_SCORE_EARN_PERCENT = Config.TRUST_SCORE_EARN_PERCENT
TRUST_SCORE_MAX_DISCOUNT_PERCENT = Config.TRUST_SCORE_MAX_DISCOUNT_PERCENT
TRUST_SCORE_PENALTY_TRAFFIC_EXHAUSTED = Config.TRUST_SCORE_PENALTY_TRAFFIC_EXHAUSTED
TRUST_SCORE_PENALTY_PAYMENT_REJECTED = Config.TRUST_SCORE_PENALTY_PAYMENT_REJECTED


def calculate_discount_percent(trust_score: int) -> int:
    """Вычислить процент скидки по количеству очков доверия.
    Диапазон очков 0-100 → скидка 0-50%."""
    if trust_score <= 0:
        return 0
    return min(
        TRUST_SCORE_MAX_DISCOUNT_PERCENT,
        (trust_score * TRUST_SCORE_MAX_DISCOUNT_PERCENT) // TRUST_SCORE_MAX,
    )


def calculate_discount_amount(price: float, discount_percent: int) -> float:
    """Вычислить размер скидки в рублях."""
    return (price * discount_percent) / 100.0


def apply_trust_discount(price: float, trust_score: int) -> Tuple[float, int]:
    """Применить скидку по очкам доверия. Возвращает (финальную цену, процент скидки)."""
    if trust_score <= 0:
        return price, 0
    discount_percent = calculate_discount_percent(trust_score)
    discount_amount = calculate_discount_amount(price, discount_percent)
    final_price = price - discount_amount
    return final_price, discount_percent


async def apply_trust_score_delta(
    user_id: int, requested_delta: int
) -> Tuple[bool, int, int, int]:
    """Изменяет trust score и возвращает:
    (успех, было, стало, фактическое_изменение)."""
    before = await db.get_trust_score(user_id)
    if requested_delta == 0:
        return True, before, before, 0

    updated = await db.add_trust_score(user_id, requested_delta)
    if not updated:
        return False, before, before, 0

    after = await db.get_trust_score(user_id)
    return True, before, after, after - before


def build_trust_change_line(actual_delta: int, before: int, after: int) -> str:
    if actual_delta > 0:
        return f"Очки доверия: <b>+{actual_delta}</b> (было {before} → стало {after})."
    if actual_delta < 0:
        return f"Очки доверия: <b>{actual_delta}</b> (было {before} → стало {after})."
    return f"Очки доверия: <b>без изменений</b> (текущий баланс: {after})."


def to_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return default


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


CUSTOM_TARIFF_UNAVAILABLE_TEXT = "⛔ Данный раздел сейчас недоступен."


def custom_tariff_enabled() -> bool:
    return bool(Config.CUSTOM_TARIFF_ENABLED)


def format_number(value: float) -> str:
    if float(value).is_integer():
        return str(int(value))
    return f"{value:.2f}".rstrip("0").rstrip(".")


def custom_ip_bounds() -> Tuple[int, int]:
    min_ip = min(Config.CUSTOM_TARIFF_MIN_IP, Config.CUSTOM_TARIFF_MAX_IP)
    max_ip = max(Config.CUSTOM_TARIFF_MIN_IP, Config.CUSTOM_TARIFF_MAX_IP)
    return max(1, min_ip), max(1, max_ip)


def custom_gb_bounds() -> Tuple[int, int]:
    min_gb = min(Config.CUSTOM_TARIFF_MIN_GB, Config.CUSTOM_TARIFF_MAX_GB)
    max_gb = max(Config.CUSTOM_TARIFF_MIN_GB, Config.CUSTOM_TARIFF_MAX_GB)
    return max(1, min_gb), max(1, max_gb)


def custom_days_bounds() -> Tuple[int, int]:
    min_days = min(Config.CUSTOM_TARIFF_MIN_DAYS, Config.CUSTOM_TARIFF_MAX_DAYS)
    max_days = max(Config.CUSTOM_TARIFF_MIN_DAYS, Config.CUSTOM_TARIFF_MAX_DAYS)
    return max(1, min_days), max(1, max_days)


def is_valid_custom_limits(traffic_gb: int, ip_limit: int, duration_days: int) -> bool:
    min_gb, max_gb = custom_gb_bounds()
    min_ip, max_ip = custom_ip_bounds()
    min_days, max_days = custom_days_bounds()
    return (
        min_gb <= traffic_gb <= max_gb
        and min_ip <= ip_limit <= max_ip
        and min_days <= duration_days <= max_days
    )


def is_valid_custom_servers(servers: Any) -> bool:
    selected = normalize_servers(servers)
    if not selected:
        return False

    available = {location.get("code") for location in get_custom_locations()}
    return all(server in available for server in selected)


def calculate_custom_tariff_total(
    traffic_gb: int,
    ip_limit: int,
    duration_days: int,
    servers: Optional[List[str]] = None,
) -> float:
    location_daily_total = sum(
        get_location_price_per_day(server) for server in normalize_servers(servers)
    )
    return (
        Config.CUSTOM_TARIFF_BASE_PRICE
        + (traffic_gb * Config.CUSTOM_TARIFF_GB_COEF)
        + (ip_limit * duration_days * Config.CUSTOM_TARIFF_IP_DAY_COEF)
        + (location_daily_total * duration_days)
    )


def build_custom_plan_name(
    traffic_gb: int,
    ip_limit: int,
    duration_days: int,
    servers: Optional[List[str]] = None,
) -> str:
    servers_text = format_servers(servers)
    return (
        f"Кастомный ({traffic_gb} ГБ, {ip_limit} IP, "
        f"{duration_days} дн., {servers_text})"
    )


def build_custom_plan(
    traffic_gb: int,
    ip_limit: int,
    duration_days: int,
    *,
    servers: Optional[List[str]] = None,
    plan_name: Optional[str] = None,
) -> Dict[str, Any]:
    selected_servers = normalize_servers(servers)
    base_price = int(
        round(
            calculate_custom_tariff_total(
                traffic_gb, ip_limit, duration_days, selected_servers
            )
        )
    )
    return {
        "id": "custom",
        "name": plan_name
        or build_custom_plan_name(
            traffic_gb, ip_limit, duration_days, selected_servers
        ),
        "price_rub": base_price,
        "ip_limit": ip_limit,
        "traffic_gb": traffic_gb,
        "duration_days": duration_days,
        "servers": selected_servers,
        "active": True,
    }


def build_custom_tariff_info_block() -> str:
    min_gb, max_gb = custom_gb_bounds()
    min_ip, max_ip = custom_ip_bounds()
    min_days, max_days = custom_days_bounds()
    base = format_number(Config.CUSTOM_TARIFF_BASE_PRICE)
    gb_coef = format_number(Config.CUSTOM_TARIFF_GB_COEF)
    ip_day_coef = format_number(Config.CUSTOM_TARIFF_IP_DAY_COEF)
    locations = get_custom_locations()
    if locations:
        location_lines = "\n".join(
            f"• {location.get('label')}: +{format_number(parse_float_value(location.get('price_per_day_rub'), 0.0))} ₽/день"
            for location in locations
        )
    else:
        location_lines = "• Локации не настроены"
    return (
        "<b>Кастомный тариф</b>\n"
        f"Формула: <code>total = {base} + GB × {gb_coef} + IP × D × {ip_day_coef} + ΣLOC × D</code>\n"
        "Параметры формулы:\n"
        f"• <b>GB</b> — объём трафика ({min_gb}-{max_gb})\n"
        f"• <b>IP</b> — лимит устройств ({min_ip}-{max_ip})\n"
        f"• <b>D</b> — срок подписки в днях ({min_days}-{max_days})\n"
        f"• <b>LOC</b> — выбранные локации:\n{location_lines}"
    )


def normalize_sub_id(raw_value: Any) -> str:
    value = str(raw_value or "").strip()
    if not value:
        return ""

    value = value.split("?", 1)[0].split("#", 1)[0].rstrip("/")
    if "/" in value:
        value = value.rsplit("/", 1)[-1]
    return value.strip()


def build_subscription_url(sub_id: Any) -> str:
    clean_sub_id = normalize_sub_id(sub_id)
    if not clean_sub_id:
        return ""

    base = str(Config.SUB_PANEL_BASE or "").strip()
    if not base:
        return clean_sub_id

    if base.endswith("/"):
        return f"{base}{clean_sub_id}"
    return f"{base}/{clean_sub_id}"


def build_base_email(user_id: int) -> str:
    return f"user_{user_id}@vpn.com"


def parse_stored_servers(value: Any) -> List[str]:
    if isinstance(value, list):
        return normalize_servers(value)

    text = str(value or "").strip()
    if not text:
        return []

    try:
        parsed = json.loads(text)
        return normalize_servers(parsed)
    except Exception:
        return normalize_servers(text)


def get_user_plan_servers(user: Optional[Dict[str, Any]]) -> List[str]:
    if not user:
        return []

    stored = parse_stored_servers(user.get("plan_servers"))
    if stored:
        return stored

    plan_text = str(user.get("plan_text") or "").strip()
    plan = get_by_name(plan_text)
    if not plan and plan_text:
        base_plan_text = plan_text.split(" (", 1)[0].strip()
        if base_plan_text:
            plan = get_by_name(base_plan_text)
    return get_plan_servers(plan)


# --- SQLite ---
class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn: Optional[aiosqlite.Connection] = None
        self.lock = asyncio.Lock()

    async def connect(self) -> None:
        self.conn = await aiosqlite.connect(self.db_path)
        self.conn.row_factory = aiosqlite.Row
        await self.init_db()

    async def close(self) -> None:
        if self.conn:
            await self.conn.close()

    async def init_db(self) -> None:
        if not self.conn:
            return
        async with self.lock:
            await self.conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    join_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    banned BOOLEAN DEFAULT FALSE,
                    ban_reason TEXT DEFAULT '',
                    ref_code TEXT,
                    ref_by INTEGER,
                    ref_rewarded INTEGER DEFAULT 0,
                    bonus_days_pending INTEGER DEFAULT 0,
                    trial_used INTEGER DEFAULT 0,
                    has_subscription INTEGER DEFAULT 0,
                    plan_text TEXT DEFAULT '',
                    plan_servers TEXT DEFAULT '',
                    ip_limit INTEGER DEFAULT 0,
                    traffic_gb INTEGER DEFAULT 0,
                    vpn_url TEXT DEFAULT '',
                    trust_score INTEGER DEFAULT 0
                )
                """)
            async with self.conn.execute("PRAGMA table_info(users)") as cursor:
                rows = await cursor.fetchall()
            columns = {row[1] for row in rows}
            if "plan_servers" not in columns:
                await self.conn.execute(
                    "ALTER TABLE users ADD COLUMN plan_servers TEXT DEFAULT ''"
                )
            await self.conn.commit()

    async def add_user(self, user_id: int) -> bool:
        if not self.conn:
            return False
        async with self.lock:
            try:
                await self.conn.execute(
                    "INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,)
                )
                await self.conn.commit()
                return True
            except Exception as e:
                logger.error(f"Ошибка добавления пользователя {user_id}: {e}")
                return False

    async def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        if not self.conn:
            return None
        async with self.lock:
            async with self.conn.execute(
                "SELECT * FROM users WHERE user_id = ?", (user_id,)
            ) as cursor:
                row = await cursor.fetchone()
        return dict(row) if row else None

    async def get_user_by_ref_code(self, ref_code: str) -> Optional[Dict[str, Any]]:
        if not self.conn:
            return None
        async with self.lock:
            async with self.conn.execute(
                "SELECT * FROM users WHERE ref_code = ?", (ref_code,)
            ) as cursor:
                row = await cursor.fetchone()
        return dict(row) if row else None

    async def update_user(self, user_id: int, **kwargs) -> bool:
        if not self.conn or not kwargs:
            return False
        set_clause = ", ".join([f"{key} = ?" for key in kwargs.keys()])
        values = list(kwargs.values())
        values.append(user_id)
        async with self.lock:
            try:
                await self.conn.execute(
                    f"UPDATE users SET {set_clause} WHERE user_id = ?", values
                )
                await self.conn.commit()
                return True
            except Exception as e:
                logger.error(f"Ошибка обновления пользователя {user_id}: {e}")
                return False

    async def get_total_users(self) -> int:
        if not self.conn:
            return 0
        async with self.lock:
            async with self.conn.execute("SELECT COUNT(*) FROM users") as cursor:
                row = await cursor.fetchone()
        return int(row[0]) if row else 0

    async def get_banned_users_count(self) -> int:
        if not self.conn:
            return 0
        async with self.lock:
            async with self.conn.execute(
                "SELECT COUNT(*) FROM users WHERE banned = TRUE"
            ) as cursor:
                row = await cursor.fetchone()
        return int(row[0]) if row else 0

    async def get_banned_user_ids(self) -> List[int]:
        if not self.conn:
            return []
        async with self.lock:
            async with self.conn.execute(
                "SELECT user_id FROM users WHERE banned = TRUE"
            ) as cursor:
                rows = await cursor.fetchall()
        return [int(row[0]) for row in rows]

    async def get_subscribed_user_ids(self) -> List[int]:
        if not self.conn:
            return []
        async with self.lock:
            async with self.conn.execute(
                "SELECT user_id FROM users WHERE vpn_url != '' AND vpn_url IS NOT NULL"
            ) as cursor:
                rows = await cursor.fetchall()
        return [int(row[0]) for row in rows]

    async def get_all_non_banned_user_ids(self) -> List[int]:
        if not self.conn:
            return []
        async with self.lock:
            async with self.conn.execute(
                "SELECT user_id FROM users WHERE banned = 0"
            ) as cursor:
                rows = await cursor.fetchall()
        return [int(row[0]) for row in rows]

    async def ban_user(self, user_id: int, reason: str = "") -> bool:
        # Добавить пользователя, если его нет в БД
        await self.add_user(user_id)
        # Обнулить очки доверия при блокировке
        await self.reset_trust_score(user_id)
        return await self.update_user(user_id, banned=True, ban_reason=reason)

    async def unban_user(self, user_id: int) -> bool:
        return await self.update_user(user_id, banned=False, ban_reason="")

    async def set_subscription(
        self,
        user_id: int,
        plan_text: str,
        ip_limit: int,
        vpn_url: str,
        traffic_gb: int,
        plan_servers: Optional[List[str]] = None,
    ) -> bool:
        return await self.update_user(
            user_id=user_id,
            plan_text=plan_text,
            plan_servers=json.dumps(
                normalize_servers(plan_servers), ensure_ascii=False
            ),
            ip_limit=ip_limit,
            vpn_url=vpn_url,
            traffic_gb=traffic_gb,
        )

    async def remove_subscription(self, user_id: int) -> bool:
        return await self.update_user(
            user_id=user_id,
            plan_text="",
            plan_servers="",
            ip_limit=0,
            vpn_url="",
            traffic_gb=0,
        )

    async def set_ref_by(self, user_id: int, ref_by: int) -> bool:
        if not self.conn:
            return False
        async with self.lock:
            cursor = await self.conn.execute(
                """
                UPDATE users
                SET ref_by = ?
                WHERE user_id = ? AND (ref_by IS NULL OR ref_by = 0)
                """,
                (ref_by, user_id),
            )
            await self.conn.commit()
            return cursor.rowcount > 0

    async def mark_ref_rewarded(self, user_id: int) -> bool:
        return await self.update_user(user_id, ref_rewarded=1)

    async def count_referrals(self, ref_by: int) -> int:
        if not self.conn:
            return 0
        async with self.lock:
            async with self.conn.execute(
                "SELECT COUNT(*) FROM users WHERE ref_by = ?", (ref_by,)
            ) as cursor:
                row = await cursor.fetchone()
        return int(row[0]) if row else 0

    async def count_referrals_paid(self, ref_by: int) -> int:
        if not self.conn:
            return 0
        async with self.lock:
            async with self.conn.execute(
                "SELECT COUNT(*) FROM users WHERE ref_by = ? AND ref_rewarded = 1",
                (ref_by,),
            ) as cursor:
                row = await cursor.fetchone()
        return int(row[0]) if row else 0

    async def get_bonus_days_pending(self, user_id: int) -> int:
        if not self.conn:
            return 0
        async with self.lock:
            async with self.conn.execute(
                "SELECT bonus_days_pending FROM users WHERE user_id = ?", (user_id,)
            ) as cursor:
                row = await cursor.fetchone()
        return int(row[0]) if row and row[0] is not None else 0

    async def clear_bonus_days_pending(self, user_id: int) -> bool:
        return await self.update_user(user_id, bonus_days_pending=0)

    async def add_bonus_days_pending(self, user_id: int, days: int) -> bool:
        if not self.conn:
            return False
        async with self.lock:
            await self.conn.execute(
                """
                UPDATE users
                SET bonus_days_pending = COALESCE(bonus_days_pending, 0) + ?
                WHERE user_id = ?
                """,
                (days, user_id),
            )
            await self.conn.commit()
        return True

    async def mark_trial_used(self, user_id: int) -> bool:
        return await self.update_user(user_id, trial_used=1)

    async def set_has_subscription(self, user_id: int) -> bool:
        return await self.update_user(user_id, has_subscription=1)

    async def get_trust_score(self, user_id: int) -> int:
        user = await self.get_user(user_id)
        if not user:
            return 0
        return int(user.get("trust_score", 0))

    async def add_trust_score(self, user_id: int, points: int) -> bool:
        """Добавить очки доверия (ограничены диапазоном 0-100)."""
        if not self.conn:
            return False
        await self.add_user(user_id)
        current = await self.get_trust_score(user_id)
        new_score = max(TRUST_SCORE_MIN, min(TRUST_SCORE_MAX, current + points))
        return await self.update_user(user_id, trust_score=new_score)

    async def reset_trust_score(self, user_id: int) -> bool:
        """Обнулить очки доверия (при блокировке)."""
        if not self.conn:
            return False
        await self.add_user(user_id)
        return await self.update_user(user_id, trust_score=0)

    async def ensure_ref_code(self, user_id: int) -> Optional[str]:
        user = await self.get_user(user_id)
        if not user:
            await self.add_user(user_id)
            user = await self.get_user(user_id)

        if not user:
            return None

        if user.get("ref_code"):
            return user.get("ref_code")

        for _ in range(20):
            code = generate_ref_code()
            existing = await self.get_user_by_ref_code(code)
            if existing:
                continue
            updated = await self.update_user(user_id, ref_code=code)
            if updated:
                return code

        return None


# --- .json ---
class JSONStorage:
    def __init__(self, path: str):
        self.path = path
        self._lock = asyncio.Lock()

    async def _ensure_file(self) -> None:
        if os.path.exists(self.path):
            return
        try:
            parent = os.path.dirname(self.path)
            if parent:
                os.makedirs(parent, exist_ok=True)
        except Exception as e:
            logger.warning(f"Не удалось создать папку для {self.path}: {e}")
        async with aiofiles.open(self.path, "w", encoding="utf-8") as f:
            await f.write("[]")

    async def _read_all_unlocked(self) -> List[Dict[str, Any]]:
        async with aiofiles.open(self.path, "r", encoding="utf-8") as f:
            content = await f.read()
        if not content:
            return []
        try:
            data = json.loads(content)
            return data if isinstance(data, list) else []
        except Exception:
            return []

    async def _write_all_unlocked(self, data: List[Dict[str, Any]]) -> None:
        async with aiofiles.open(self.path, "w", encoding="utf-8") as f:
            await f.write(json.dumps(data, ensure_ascii=False, indent=2))

    async def read_all(self) -> List[Dict[str, Any]]:
        await self._ensure_file()
        async with self._lock:
            return await self._read_all_unlocked()

    async def write_all(self, data: List[Dict[str, Any]]) -> None:
        await self._ensure_file()
        async with self._lock:
            await self._write_all_unlocked(data)

    async def add(self, item: Dict[str, Any]) -> None:
        await self._ensure_file()
        async with self._lock:
            data = await self._read_all_unlocked()
            data.append(item)
            await self._write_all_unlocked(data)

    async def add_pending_for_user(self, user_id: int, item: Dict[str, Any]) -> bool:
        await self._ensure_file()
        async with self._lock:
            data = await self._read_all_unlocked()
            has_pending = any(
                p.get("status") == "pending"
                and to_int(p.get("user_id"), 0) == to_int(user_id, 0)
                for p in data
            )
            if has_pending:
                return False

            data.append(item)
            await self._write_all_unlocked(data)
            return True

    async def remove(self, predicate) -> None:
        await self._ensure_file()
        async with self._lock:
            data = await self._read_all_unlocked()
            new_data = [x for x in data if not predicate(x)]
            await self._write_all_unlocked(new_data)

    async def find_by_id(self, payment_id: str) -> Optional[Dict[str, Any]]:
        data = await self.read_all()
        for item in data:
            if item.get("payment_id") == payment_id:
                return item
        return None

    async def remove_by_id(self, payment_id: str) -> None:
        await self._ensure_file()
        async with self._lock:
            data = await self._read_all_unlocked()
            new_data = [x for x in data if x.get("payment_id") != payment_id]
            await self._write_all_unlocked(new_data)

    @staticmethod
    def _is_processing_stale(
        item: Dict[str, Any], timeout_seconds: int = PAYMENT_PROCESSING_TIMEOUT_SEC
    ) -> bool:
        status = str(item.get("status", ""))
        if not status.startswith("processing_"):
            return False

        started_raw = str(item.get("processing_started_at", "")).strip()
        if not started_raw:
            return True

        try:
            started_at = datetime.fromisoformat(started_raw)
        except Exception:
            return True

        age_seconds = (datetime.now() - started_at).total_seconds()
        return age_seconds > timeout_seconds

    async def release_stale_processing_payments(
        self, timeout_seconds: int = PAYMENT_PROCESSING_TIMEOUT_SEC
    ) -> int:
        await self._ensure_file()
        async with self._lock:
            data = await self._read_all_unlocked()
            released = 0
            for idx, item in enumerate(data):
                if not self._is_processing_stale(item, timeout_seconds):
                    continue

                reset = dict(item)
                reset["status"] = "pending"
                reset["last_processing_error"] = (
                    "Предыдущая обработка была прервана (таймаут processing)"
                )
                reset["last_processing_error_at"] = datetime.now().isoformat()
                reset.pop("processing_by", None)
                reset.pop("processing_started_at", None)
                data[idx] = reset
                released += 1

            if released > 0:
                await self._write_all_unlocked(data)
            return released

    async def claim_pending_payment(
        self, payment_id: str, moderator_id: int, action: str
    ) -> Optional[Dict[str, Any]]:
        """Атомарно переводит payment из pending в processing_*."""
        await self._ensure_file()
        processing_status = f"processing_{action}"
        async with self._lock:
            data = await self._read_all_unlocked()
            for idx, item in enumerate(data):
                if item.get("payment_id") != payment_id:
                    continue
                current = dict(item)
                if current.get("status") != "pending":
                    if self._is_processing_stale(current):
                        current["status"] = "pending"
                        current.pop("processing_by", None)
                        current.pop("processing_started_at", None)
                    else:
                        return None

                claimed = dict(current)
                claimed["status"] = processing_status
                claimed["processing_by"] = moderator_id
                claimed["processing_started_at"] = datetime.now().isoformat()
                data[idx] = claimed
                await self._write_all_unlocked(data)
                return claimed
        return None

    async def finalize_claimed_payment(
        self,
        payment_id: str,
        moderator_id: int,
        action: str,
        final_status: str,
        extra_fields: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Завершает ранее захваченный payment, если он всё ещё захвачен этим модератором."""
        await self._ensure_file()
        processing_status = f"processing_{action}"
        async with self._lock:
            data = await self._read_all_unlocked()
            for idx, item in enumerate(data):
                if item.get("payment_id") != payment_id:
                    continue
                if item.get("status") != processing_status or to_int(
                    item.get("processing_by"), 0
                ) != to_int(moderator_id, 0):
                    return False

                updated = dict(item)
                updated["status"] = final_status
                updated["processed_by"] = moderator_id
                updated["processed_at"] = datetime.now().isoformat()
                updated.pop("processing_by", None)
                updated.pop("processing_started_at", None)
                if extra_fields:
                    updated.update(extra_fields)
                data[idx] = updated
                await self._write_all_unlocked(data)
                return True
        return False

    async def rollback_claimed_payment(
        self,
        payment_id: str,
        moderator_id: int,
        action: str,
        *,
        error_message: str = "",
    ) -> bool:
        """Возвращает payment в pending, если обработка не завершилась."""
        await self._ensure_file()
        processing_status = f"processing_{action}"
        async with self._lock:
            data = await self._read_all_unlocked()
            for idx, item in enumerate(data):
                if item.get("payment_id") != payment_id:
                    continue
                if item.get("status") != processing_status or to_int(
                    item.get("processing_by"), 0
                ) != to_int(moderator_id, 0):
                    return False

                rolled_back = dict(item)
                rolled_back["status"] = "pending"
                rolled_back.pop("processing_by", None)
                rolled_back.pop("processing_started_at", None)
                if error_message:
                    rolled_back["last_processing_error"] = error_message
                    rolled_back["last_processing_error_at"] = datetime.now().isoformat()
                data[idx] = rolled_back
                await self._write_all_unlocked(data)
                return True
        return False


# --- 3X-UI Panel API ---
class PanelAPI:
    def __init__(self) -> None:
        self.apibase = Config.PANEL_BASE.rstrip("/")
        self.username = Config.PANEL_LOGIN
        self.password = Config.PANEL_PASSWORD
        self.verifyssl = Config.VERIFY_SSL
        self.session: Optional[aiohttp.ClientSession] = None
        self.token: Optional[str] = Config.PANEL_TOKEN or None
        self.logged_in: bool = bool(self.token)
        self.lock = asyncio.Lock()

    async def start(self) -> None:
        connector = aiohttp.TCPConnector(ssl=self.verifyssl)
        timeout = aiohttp.ClientTimeout(total=15)
        cookie_jar = aiohttp.CookieJar(unsafe=True)
        self.session = aiohttp.ClientSession(
            connector=connector, timeout=timeout, cookie_jar=cookie_jar
        )
        if not self.token:
            await self.login()
        else:
            logger.info("Используется PANEL_TOKEN для авторизации 3X-UI")

    async def close(self) -> None:
        if self.session:
            await self.session.close()
            self.session = None

    async def _request_json(self, method: str, url: str, **kwargs):
        if not self.session:
            return 0, {}, ""
        try:
            async with self.session.request(method, url, **kwargs) as resp:
                text = await resp.text()
                data = {}
                if text:
                    try:
                        data = json.loads(text)
                    except Exception:
                        data = {}
                return resp.status, data, text
        except aiohttp.ClientError as e:
            logger.error(f"HTTP ошибка запроса {url}: {e}")
            return 0, {}, ""

    @staticmethod
    def _needs_reauth(status: int, data: Dict[str, Any]) -> bool:
        if status in (401, 403):
            return True
        msg = str(data.get("msg", "") if isinstance(data, dict) else "").lower()
        auth_markers = (
            "login",
            "auth",
            "unauthorized",
            "session",
            "csrf",
            "автор",
            "сесс",
            "войд",
        )
        if status == 404 and any(marker in msg for marker in auth_markers):
            return True
        if (
            status == 200
            and isinstance(data, dict)
            and data.get("success") is False
            and any(marker in msg for marker in auth_markers)
        ):
            return True
        return False

    async def _request_json_with_reauth(self, method: str, url: str, **kwargs):
        status, data, text = await self._request_json(method, url, **kwargs)
        if self._needs_reauth(status, data):
            await self.login()
            status, data, text = await self._request_json(method, url, **kwargs)
        return status, data, text

    async def login(self) -> None:
        async with self.lock:
            if not self.session:
                return
            if self.token:
                self.logged_in = True
                logger.info("PANEL_TOKEN задан: авторизация 3X-UI через Bearer token")
                return
            if not self.username or not self.password:
                self.logged_in = False
                logger.error(
                    "PANEL_LOGIN/PANEL_PASSWORD не заданы, а PANEL_TOKEN отсутствует"
                )
                return
            try:
                url = f"{self.apibase}/login"
                status, data, _ = await self._request_json(
                    "POST",
                    url,
                    json={"username": self.username, "password": self.password},
                )
                if status == 200 and data.get("success"):
                    self.logged_in = True
                    self.token = self.token or None
                    logger.info("Успешная аутентификация в панели 3X-UI")
                else:
                    self.logged_in = False
                    logger.error(
                        f"Ошибка аутентификации 3X-UI: status={status} msg={data.get('msg')}"
                    )
            except Exception as e:
                self.logged_in = False
                logger.error(f"Ошибка при аутентификации 3X-UI: {e}")

    def _headers(self) -> Dict[str, str]:
        headers: Dict[str, str] = {}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        headers["Accept"] = "application/json"
        return headers

    async def ensure_auth(self) -> None:
        if not self.logged_in:
            await self.login()

    async def get_inbounds(self) -> Optional[Dict[str, Any]]:
        await self.ensure_auth()
        url = f"{self.apibase}/panel/api/inbounds/list"
        status, data, _ = await self._request_json_with_reauth(
            "GET", url, headers=self._headers()
        )
        if status == 200 and data.get("success"):
            obj = data.get("obj") or []
            logger.info(f"Получено {len(obj)} inbounds")
            return data

        logger.error(
            f"Ошибка API getInbounds: url={url} status={status} msg={data.get('msg')}"
        )
        return None

    @staticmethod
    def _is_base_email(email: str, base_email: str) -> bool:
        if not email or not base_email:
            return False
        return email.endswith(base_email)

    @staticmethod
    def _quote_path(value: Any) -> str:
        return quote(str(value or ""), safe="")

    @staticmethod
    def _client_rows_from_response(data: Dict[str, Any]) -> List[Dict[str, Any]]:
        obj = data.get("obj") or []
        if isinstance(obj, dict):
            obj = obj.get("items") or []
        return obj if isinstance(obj, list) else []

    @staticmethod
    def _normalize_client_row(row: Dict[str, Any]) -> Dict[str, Any]:
        item = dict(row)
        traffic = item.get("traffic") if isinstance(item.get("traffic"), dict) else {}
        item["up"] = to_int(traffic.get("up", item.get("up", 0)), 0)
        item["down"] = to_int(traffic.get("down", item.get("down", 0)), 0)
        item["total"] = to_int(item.get("totalGB", item.get("total", 0)), 0)
        item["expiryTime"] = to_int(item.get("expiryTime", 0), 0)
        item["enable"] = bool(item.get("enable", traffic.get("enable", True)))
        item["clientObj"] = dict(row)
        return item

    @staticmethod
    def _client_payload_for_update(client: Dict[str, Any]) -> Dict[str, Any]:
        ignored = {"traffic", "inboundIds", "createdAt", "updatedAt", "clientObj"}
        client_uuid = None
        for candidate in (client.get("id"), client.get("uuid")):
            try:
                client_uuid = str(uuid.UUID(str(candidate)))
                break
            except Exception:
                continue

        payload: Dict[str, Any] = {}
        for key, value in client.items():
            if key in ignored or key in {"id", "uuid"}:
                continue
            payload[key] = value
        if client_uuid:
            payload["id"] = client_uuid
        return payload

    @staticmethod
    def _inbound_label(inbound: Dict[str, Any]) -> str:
        parts = [
            inbound.get("remark"),
            inbound.get("tag"),
            inbound.get("name"),
        ]
        return " ".join(str(part) for part in parts if part)

    def _inbound_matches_servers(
        self, inbound: Dict[str, Any], servers: Optional[List[str]]
    ) -> bool:
        selected_servers = normalize_servers(servers)
        if not selected_servers:
            return True

        inbound_label = self._inbound_label(inbound)
        for server in selected_servers:
            for token in get_server_match_tokens(server):
                if token_matches_inbound_label(inbound_label, token):
                    return True
        return False

    def _filter_inbounds_for_servers(
        self, inbounds: List[Dict[str, Any]], servers: Optional[List[str]]
    ) -> List[Dict[str, Any]]:
        return [
            inbound
            for inbound in inbounds
            if inbound.get("enable", False)
            and self._inbound_matches_servers(inbound, servers)
        ]

    async def get_matching_inbound_ids(
        self, servers: Optional[List[str]]
    ) -> Optional[List[int]]:
        inbounds = await self.get_inbounds()
        if not inbounds or not inbounds.get("success"):
            logger.error("Не удалось получить inbounds для подбора локаций")
            return None

        enabled_inbounds = self._filter_inbounds_for_servers(
            inbounds.get("obj", []), servers
        )
        inbound_ids = [to_int(inbound.get("id"), 0) for inbound in enabled_inbounds]
        return [inbound_id for inbound_id in inbound_ids if inbound_id > 0]

    async def get_clients(self) -> Optional[Dict[str, Any]]:
        await self.ensure_auth()
        url = f"{self.apibase}/panel/api/clients/list"
        status, data, _ = await self._request_json_with_reauth(
            "GET", url, headers=self._headers()
        )
        if status == 200 and data.get("success"):
            logger.info(
                f"Получено {len(self._client_rows_from_response(data))} clients"
            )
            return data

        logger.error(
            f"Ошибка API clients/list: url={url} status={status} msg={data.get('msg')}"
        )
        return None

    async def get_client_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        await self.ensure_auth()
        url = f"{self.apibase}/panel/api/clients/get/{self._quote_path(email)}"
        status, data, _ = await self._request_json_with_reauth(
            "GET", url, headers=self._headers()
        )
        if status == 200 and data.get("success"):
            obj = data.get("obj") or {}
            if isinstance(obj, dict) and isinstance(obj.get("client"), dict):
                client = dict(obj.get("client") or {})
                client["inboundIds"] = obj.get("inboundIds") or client.get(
                    "inboundIds", []
                )
                return client
            if isinstance(obj, dict):
                return obj
        return None

    async def find_clients_by_base_email(self, base_email: str) -> List[Dict[str, Any]]:
        ok, result = await self.find_clients_by_base_email_safe(base_email)
        if not ok:
            return []
        return result

    async def find_clients_by_base_email_safe(
        self, base_email: str
    ) -> Tuple[bool, List[Dict[str, Any]]]:
        clients_data = await self.get_clients()
        if not clients_data or not clients_data.get("success"):
            return False, []

        result: List[Dict[str, Any]] = []
        for row in self._client_rows_from_response(clients_data):
            email = str(row.get("email") or "")
            if self._is_base_email(email, base_email):
                result.append(self._normalize_client_row(row))
        return True, result

    async def find_clients_full_by_email(self, base_email: str) -> List[Dict[str, Any]]:
        ok, result = await self.find_clients_full_by_email_safe(base_email)
        if not ok:
            return []
        return result

    async def find_clients_full_by_email_safe(
        self, base_email: str
    ) -> Tuple[bool, List[Dict[str, Any]]]:
        ok, result = await self.find_clients_by_base_email_safe(base_email)
        if not ok:
            return False, []
        logger.info(f"Найдено {len(result)} клиентов по base_email='{base_email}'")
        return True, result

    async def create_client(
        self,
        email: str,
        limit_ip: int,
        total_gb: int,
        days: int = 30,
        servers: Optional[List[str]] = None,
        tg_id: int = 0,
        inbound_ids: Optional[List[int]] = None,
    ) -> Optional[Dict[str, Any]]:
        await self.ensure_auth()
        expiry_ms = int((time.time() + days * 86400) * 1000)
        total_bytes = int(total_gb * BYTES_IN_GB)
        sub_id = f"user_{uuid.uuid4().hex[:12]}"

        if inbound_ids is None:
            inbound_ids = await self.get_matching_inbound_ids(servers)
            if inbound_ids is None:
                logger.error("Не удалось получить inbounds для создания клиента")
                return None
        else:
            inbound_ids = [to_int(inbound_id, 0) for inbound_id in inbound_ids]
            inbound_ids = [inbound_id for inbound_id in inbound_ids if inbound_id > 0]

        if not inbound_ids:
            logger.error(
                f"Нет включённых inbound для создания клиента по локациям: {format_servers(servers)}"
            )
            return None

        client = {
            "email": email,
            "enable": True,
            "flow": "",
            "limitIp": max(0, int(limit_ip)),
            "totalGB": total_bytes,
            "expiryTime": expiry_ms,
            "subId": sub_id,
            "tgId": max(0, int(tg_id or 0)),
        }
        payload = {"client": client, "inboundIds": inbound_ids}

        url = f"{self.apibase}/panel/api/clients/add"
        status, data, text = await self._request_json_with_reauth(
            "POST", url, headers=self._headers(), json=payload
        )

        if status in (200, 201) and data.get("success"):
            client["inboundIds"] = inbound_ids
            logger.info(
                f"Клиент {email} создан через clients/add в inboundIds={inbound_ids}"
            )
            return client

        logger.error(
            f"Ошибка clients/add для {email}: status={status} msg={data.get('msg')}"
        )
        if text:
            logger.error(text)
        return None

    async def create_client_in_inbound(
        self,
        inbound_id: int,
        email: str,
        limit_ip: int,
        total_gb: int,
        expiry_ms: int,
        sub_id: str,
    ) -> Optional[Dict[str, Any]]:
        await self.ensure_auth()

        total_bytes = int(total_gb * BYTES_IN_GB)
        existing = await self.get_client_by_email(email)
        if existing:
            url = f"{self.apibase}/panel/api/clients/{self._quote_path(email)}/attach"
            status, data, text = await self._request_json_with_reauth(
                "POST",
                url,
                headers=self._headers(),
                json={"inboundIds": [inbound_id]},
            )
            if status in (200, 201) and data.get("success"):
                existing["inboundIds"] = sorted(
                    set(list(existing.get("inboundIds") or []) + [inbound_id])
                )
                logger.info(f"Клиент {email} привязан к inbound {inbound_id}")
                return existing
            logger.error(
                f"Ошибка clients/attach inbound {inbound_id}: status={status} msg={data.get('msg')}"
            )
            if text:
                logger.error(text)
            return None

        client = {
            "email": email,
            "enable": True,
            "flow": "",
            "limitIp": max(0, int(limit_ip)),
            "totalGB": total_bytes,
            "expiryTime": expiry_ms,
            "subId": sub_id,
        }
        payload = {"client": client, "inboundIds": [inbound_id]}
        url = f"{self.apibase}/panel/api/clients/add"
        status, data, text = await self._request_json_with_reauth(
            "POST", url, headers=self._headers(), json=payload
        )

        if status in (200, 201) and data.get("success"):
            client["inboundIds"] = [inbound_id]
            logger.info(f"Клиент {email} успешно создан в inbound {inbound_id}")
            return client

        logger.error(
            f"Ошибка clients/add inbound {inbound_id}: status={status} msg={data.get('msg')}"
        )
        if text:
            logger.error(text)
        return None

    async def delete_client(self, base_email: str) -> bool:
        await self.ensure_auth()
        clients_ok, clients = await self.find_clients_full_by_email_safe(base_email)
        if not clients_ok:
            logger.error(
                f"Не удалось удалить клиентов для base_email='{base_email}': панель недоступна"
            )
            return False

        if not clients:
            logger.info(
                f"Клиенты с частью email '{base_email}' не найдены, ничего не удаляем"
            )
            return True

        success_count = 0
        seen_emails = set()
        for client in clients:
            email = str(client.get("email") or "")
            if not email or email in seen_emails:
                continue
            seen_emails.add(email)

            delete_url = (
                f"{self.apibase}/panel/api/clients/del/{self._quote_path(email)}"
            )
            status, data, text = await self._request_json_with_reauth(
                "POST", delete_url, headers=self._headers()
            )

            if status == 200 and data.get("success"):
                logger.info(f"Клиент email={email} успешно удалён через clients/del")
                success_count += 1
            else:
                logger.error(
                    f"Ошибка удаления клиента email={email}: status={status} msg={data.get('msg')}"
                )
                if text:
                    logger.error(text)

        return success_count == len(seen_emails)

    async def extend_client_expiry(self, base_email: str, add_days: int) -> bool:
        await self.ensure_auth()
        clients = await self.find_clients_full_by_email(base_email)
        if not clients:
            return False

        success = False
        seen_emails = set()
        for c in clients:
            email = str(c.get("email") or "")
            if not email or email in seen_emails:
                continue
            seen_emails.add(email)

            client_obj = (
                await self.get_client_by_email(email) or c.get("clientObj") or c
            )
            if not isinstance(client_obj, dict):
                continue

            current_expiry = to_int(
                client_obj.get("expiryTime", c.get("expiryTime")), 0
            )
            if current_expiry and current_expiry > 0:
                new_expiry = int(current_expiry + add_days * 86400 * 1000)
            else:
                new_expiry = int((time.time() + add_days * 86400) * 1000)

            client_obj["expiryTime"] = new_expiry
            payload = self._client_payload_for_update(client_obj)
            update_url = (
                f"{self.apibase}/panel/api/clients/update/{self._quote_path(email)}"
            )
            status, data, text = await self._request_json_with_reauth(
                "POST", update_url, headers=self._headers(), json=payload
            )

            if status in (200, 201) and data.get("success"):
                success = True
            else:
                logger.error(
                    f"Ошибка clients/update email={email}: status={status} msg={data.get('msg')}"
                )
                if text:
                    logger.error(text)

        return success

    async def get_client_stats(self, base_email: str) -> List[Dict[str, Any]]:
        return await self.find_clients_by_base_email(base_email)

    async def get_client_stats_safe(
        self, base_email: str
    ) -> Tuple[bool, List[Dict[str, Any]]]:
        return await self.find_clients_by_base_email_safe(base_email)


# --- FSM States ---
class BanUserState(StatesGroup):
    waiting_for_user_id = State()
    waiting_for_ban_reason = State()


class UnbanUserState(StatesGroup):
    waiting_for_user_id = State()
    waiting_for_unban_reason = State()


class BroadcastState(StatesGroup):
    waiting_for_broadcast_type = State()
    waiting_for_message = State()


class TrustScoreState(StatesGroup):
    waiting_for_user_id = State()
    waiting_for_amount = State()


class CustomTariffState(StatesGroup):
    waiting_for_gb = State()
    waiting_for_ip = State()
    waiting_for_days = State()
    waiting_for_locations = State()
    waiting_for_confirm = State()


# --- Утилиты ---
BOT_USERNAME = ""


def kb(rows: List[List[Dict[str, str]]]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(**button) for button in row] for row in rows
        ]
    )


def support_keyboard(include_main: bool = True) -> InlineKeyboardMarkup:
    rows: List[List[Dict[str, str]]] = []
    if Config.SUPPORT_URL and Config.SUPPORT_URL.strip():
        rows.append([{"text": "Поддержка", "url": Config.SUPPORT_URL}])
    if include_main:
        rows.append([{"text": "Главная", "callback_data": "start"}])
    if not rows:
        rows = [[{"text": "Главная", "callback_data": "start"}]]
    return kb(rows)


def build_main_keyboard(
    is_admin: bool, has_active_subscription: bool
) -> List[List[Dict[str, str]]]:
    rows: List[List[Dict[str, str]]] = [
        [{"text": "Тарифы", "callback_data": "subs"}],
    ]

    if is_admin or not has_active_subscription:
        rows.append([{"text": "Купить подписку", "callback_data": "buy"}])

    rows.extend(
        [
            [{"text": "Моя подписка", "callback_data": "mysub"}],
            [{"text": "Реферальная система", "callback_data": "ref"}],
        ]
    )

    if is_admin:
        rows.extend(
            [
                [{"text": "Ожидающие платежи", "callback_data": "pay_await"}],
                [{"text": "Отправить уведомление", "callback_data": "broadcast"}],
                [{"text": "Инструменты отладки", "callback_data": "debug_menu"}],
            ]
        )

    social_row: List[Dict[str, str]] = []
    if Config.YOUTUBE_URL:
        social_row.append({"text": "YouTube", "url": Config.YOUTUBE_URL})
    if Config.TELEGRAM_URL:
        social_row.append({"text": "Telegram", "url": Config.TELEGRAM_URL})
    if Config.TIKTOK_URL:
        social_row.append({"text": "TikTok", "url": Config.TIKTOK_URL})
    if social_row:
        rows.append(social_row)

    bottom_row: List[Dict[str, str]] = []
    if Config.QNA_URL:
        bottom_row.append({"text": "Q&A", "url": Config.QNA_URL})
    if Config.PRIVACY_POLICY_URL:
        bottom_row.append(
            {"text": "Политика конфиденциальности", "url": Config.PRIVACY_POLICY_URL}
        )
    if bottom_row:
        rows.append(bottom_row)

    if Config.SITE_URL:
        rows.append([{"text": "Наш сайт", "url": Config.SITE_URL}])

    if Config.SUPPORT_URL:
        rows.append([{"text": "Поддержка", "url": Config.SUPPORT_URL}])

    return rows


def generate_ref_code() -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(8))


def get_ref_link(ref_code: str) -> str:
    if BOT_USERNAME:
        return f"https://t.me/{BOT_USERNAME}?start={ref_code}"
    return f"https://t.me/?start={ref_code}"


async def safe_send_message(
    bot: Bot,
    user_id: int,
    message: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
):
    try:
        await bot.send_message(
            user_id, message, parse_mode=ParseMode.HTML, reply_markup=reply_markup
        )
    except TelegramBadRequest as e:
        logger.warning(
            f"HTML parse error for user {user_id}: {e}. Trying escaped HTML then plain text."
        )
        try:
            await bot.send_message(
                user_id,
                html.escape(message),
                parse_mode=ParseMode.HTML,
                reply_markup=reply_markup,
            )
        except Exception:
            try:
                await bot.send_message(user_id, message, reply_markup=reply_markup)
            except Exception as e2:
                logger.error(f"Ошибка отправки plain message {user_id}: {e2}")
    except Exception as e:
        logger.error(f"Ошибка отправки сообщения {user_id}: {str(e)}")


async def notify_admins(
    message: str, reply_markup: Optional[InlineKeyboardMarkup] = None
):
    for admin_id in Config.ADMIN_USER_IDS:
        await safe_send_message(bot, admin_id, message, reply_markup=reply_markup)


async def notify_user(
    user_id: int, message: str, reply_markup: Optional[InlineKeyboardMarkup] = None
):
    await safe_send_message(bot, user_id, message, reply_markup=reply_markup)


async def smart_answer(event, text, reply_markup=None, delete_origin=False):
    try:
        if isinstance(event, Message):
            await event.answer(text, reply_markup=reply_markup)
        elif isinstance(event, CallbackQuery):
            if event.message:
                await event.message.answer(text, reply_markup=reply_markup)
                if delete_origin:
                    try:
                        await event.message.delete()
                    except Exception:
                        pass
            try:
                await event.answer()
            except Exception:
                pass
    except Exception as e:
        logger.error(f"Ошибка в smart_answer: {e}")


def is_admin_user(user_id: int) -> bool:
    return user_id in ADMIN_USER_ID_SET


def main_menu_keyboard() -> InlineKeyboardMarkup:
    return kb([[{"text": "Главная", "callback_data": "start"}]])


async def deny_admin_only(event) -> None:
    await smart_answer(
        event,
        "⛔ <b>Эта команда доступна только администраторам!</b>",
        reply_markup=main_menu_keyboard(),
        delete_origin=True,
    )


async def ensure_admin_access(event, *, silent: bool = False) -> bool:
    user = getattr(event, "from_user", None)
    user_id = getattr(user, "id", 0)
    if is_admin_user(user_id):
        return True
    if not silent:
        await deny_admin_only(event)
    return False


def cancel_only_keyboard() -> InlineKeyboardMarkup:
    return kb([[{"text": "Отмена", "callback_data": "cancel"}]])


async def ensure_custom_tariff_access(
    event, state: Optional[FSMContext] = None
) -> bool:
    if custom_tariff_enabled():
        return True

    if state:
        await state.clear()

    if isinstance(event, CallbackQuery):
        await event.answer(CUSTOM_TARIFF_UNAVAILABLE_TEXT, show_alert=True)
    elif isinstance(event, Message):
        await event.answer(
            CUSTOM_TARIFF_UNAVAILABLE_TEXT, reply_markup=main_menu_keyboard()
        )
    return False


def custom_locations_keyboard(selected_servers: Any) -> InlineKeyboardMarkup:
    selected = set(normalize_servers(selected_servers))
    rows: List[List[Dict[str, str]]] = []
    for location in get_custom_locations():
        code = str(location.get("code") or "")
        if not code:
            continue
        mark = "✓" if code in selected else "□"
        price = format_number(parse_float_value(location.get("price_per_day_rub"), 0.0))
        rows.append(
            [
                {
                    "text": f"{mark} {location.get('label', code)} +{price} ₽/день",
                    "callback_data": f"custom:loc:{code}",
                }
            ]
        )

    rows.append([{"text": "Готово", "callback_data": "custom:locations_done"}])
    rows.append([{"text": "Отмена", "callback_data": "cancel"}])
    return kb(rows)


def build_custom_locations_text(selected_servers: Any) -> str:
    selected = normalize_servers(selected_servers)
    selected_text = format_servers(selected) if selected else "не выбраны"
    return (
        "<b>Кастомный тариф</b>\n\n"
        "Шаг 4/4: выберите одну или несколько локаций.\n\n"
        f"Выбрано: <b>{selected_text}</b>"
    )


async def show_custom_locations_picker(event, state: FSMContext) -> None:
    data = await state.get_data()
    selected = normalize_servers(data.get("custom_servers"))
    text = build_custom_locations_text(selected)
    reply_markup = custom_locations_keyboard(selected)

    if isinstance(event, CallbackQuery) and event.message:
        try:
            await event.message.edit_text(
                text, reply_markup=reply_markup, parse_mode=ParseMode.HTML
            )
            await event.answer()
            return
        except Exception as e:
            logger.warning(f"Не удалось обновить выбор локаций: {e}")

    await smart_answer(event, text, reply_markup=reply_markup, delete_origin=True)


async def show_custom_summary(event, state: FSMContext) -> None:
    data = await state.get_data()
    traffic_gb = to_int(data.get("custom_traffic_gb"), 0)
    ip_limit = to_int(data.get("custom_ip_limit"), 0)
    duration_days = to_int(data.get("custom_duration_days"), 0)
    servers = normalize_servers(data.get("custom_servers"))

    if not is_valid_custom_limits(
        traffic_gb, ip_limit, duration_days
    ) or not is_valid_custom_servers(servers):
        await state.clear()
        await smart_answer(
            event,
            "❌ Не удалось сформировать кастомный тариф. Начните заново.",
            reply_markup=main_menu_keyboard(),
            delete_origin=True,
        )
        return

    base_total = int(
        round(
            calculate_custom_tariff_total(traffic_gb, ip_limit, duration_days, servers)
        )
    )
    user_id = event.from_user.id
    trust_score = await db.get_trust_score(user_id)
    final_price, discount_percent = apply_trust_discount(float(base_total), trust_score)
    final_price_int = max(0, int(final_price))
    plan_name = build_custom_plan_name(traffic_gb, ip_limit, duration_days, servers)
    location_daily_total = sum(get_location_price_per_day(server) for server in servers)

    await state.update_data(
        custom_base_amount=base_total,
        custom_final_amount=final_price_int,
        custom_discount_percent=discount_percent,
        custom_plan_name=plan_name,
    )
    await state.set_state(CustomTariffState.waiting_for_confirm)

    ip_part = "IP × D"
    if abs(Config.CUSTOM_TARIFF_IP_DAY_COEF - 1.0) > 1e-9:
        ip_part = f"IP × D × {format_number(Config.CUSTOM_TARIFF_IP_DAY_COEF)}"
    formula_text = (
        f"total = {format_number(Config.CUSTOM_TARIFF_BASE_PRICE)} + "
        f"GB × {format_number(Config.CUSTOM_TARIFF_GB_COEF)} + "
        f"{ip_part} + LOC × D"
    )
    text = (
        "<b>Кастомный тариф</b>\n\n"
        "Параметры:\n"
        f"• Трафик: <b>{traffic_gb} ГБ</b>\n"
        f"• Устройства: <b>{ip_limit}</b>\n"
        f"• Срок: <b>{duration_days} дней</b>\n"
        f"• Локации: <b>{format_servers(servers)}</b> "
        f"(+{format_number(location_daily_total)} ₽/день)\n\n"
        f"Формула: <code>{formula_text}</code>\n"
        f"Стоимость без скидки: <b>{base_total} ₽</b>\n"
    )
    if discount_percent > 0:
        text += (
            f"Скидка по trust score: <b>-{discount_percent}%</b>\n"
            f"Итоговая стоимость: <b>{final_price_int} ₽</b>\n"
        )
    else:
        text += f"Итоговая стоимость: <b>{final_price_int} ₽</b>\n"

    if is_admin_user(user_id):
        text += (
            "\nДля администраторов кастомный тариф оформляется в тестовом режиме, "
            "без подтверждения оплаты."
        )
        reply_markup = kb(
            [
                [
                    {
                        "text": "Создать тестовую подписку",
                        "callback_data": "custom:confirm_payment",
                    }
                ],
                [{"text": "Отмена", "callback_data": "cancel"}],
            ]
        )
    else:
        text += "\n\nДалее откроется согласие с офертой, затем реквизиты оплаты."
        reply_markup = kb(
            [
                [
                    {
                        "text": "Продолжить",
                        "callback_data": f"custom:show_offer:{user_id}",
                    }
                ],
                [{"text": "Отмена", "callback_data": "cancel"}],
            ]
        )

    await smart_answer(event, text, reply_markup=reply_markup, delete_origin=True)


def format_payment_time(timestamp: Any) -> str:
    raw = str(timestamp or "")
    if not raw:
        return "-"
    try:
        return datetime.fromisoformat(raw).strftime("%d.%m.%Y %H:%M")
    except Exception:
        return raw


def build_custom_plan_from_payment(
    payment: Dict[str, Any],
) -> Tuple[Optional[Dict[str, Any]], str]:
    custom_plan = payment.get("custom_plan")
    if not isinstance(custom_plan, dict):
        return None, "Некорректные параметры кастомного тарифа"

    traffic_gb = to_int(custom_plan.get("traffic_gb"), 0)
    ip_limit = to_int(custom_plan.get("ip_limit"), 0)
    duration_days = to_int(custom_plan.get("duration_days"), 0)
    servers = normalize_servers(custom_plan.get("servers"))
    if not is_valid_custom_limits(traffic_gb, ip_limit, duration_days):
        return None, "Параметры кастомного тарифа вне допустимого диапазона"
    if not is_valid_custom_servers(servers):
        return None, "Некорректный список локаций кастомного тарифа"

    plan_name = str(custom_plan.get("plan_name") or "").strip()
    plan = build_custom_plan(
        traffic_gb,
        ip_limit,
        duration_days,
        servers=servers,
        plan_name=plan_name or None,
    )
    return plan, ""


def build_pending_payment_text(payment: Dict[str, Any]) -> str:
    payment_id = payment.get("payment_id", "")
    user_id = payment.get("user_id", 0)
    plan_id = payment.get("plan_id", "")
    amount = payment.get("amount", 0)
    plan_type = str(payment.get("plan_type", "catalog"))

    plan_name = str(payment.get("plan_name") or "").strip()
    custom_details = ""
    if plan_type == "custom":
        plan, _ = build_custom_plan_from_payment(payment)
        if plan:
            plan_name = plan_name or plan.get("name", "Кастомный")
            custom_details = (
                f"\n⚙️ <b>Параметры:</b> "
                f"{plan.get('traffic_gb', 0)} ГБ, "
                f"{plan.get('ip_limit', 0)} IP, "
                f"{plan.get('duration_days', 0)} дн., "
                f"{format_servers(plan.get('servers'))}"
            )
        else:
            plan_name = plan_name or "Кастомный"
    else:
        plan, _ = get_purchasable_catalog_plan(str(plan_id))
        if plan:
            plan_name = plan_name or plan.get("name", plan_id)
            custom_details = (
                f"\n🌍 <b>Локации:</b> {format_servers(plan.get('servers'))}"
            )
        elif not plan_name:
            plan_name = str(plan_id)

    return (
        f"📋 <b>Платеж ID:</b> <code>{payment_id}</code>\n"
        f"👤 <b>Пользователь:</b> <code>{user_id}</code>\n"
        f"📦 <b>Тариф:</b> {plan_name}\n"
        f"{custom_details}\n"
        f"💰 <b>Сумма:</b> {amount} ₽\n"
        f"🕐 <b>Время:</b> {format_payment_time(payment.get('timestamp'))}"
    )


def build_pending_payment_keyboard(payment_id: str) -> InlineKeyboardMarkup:
    return kb(
        [
            [
                {
                    "text": "✅ Подтвердить",
                    "callback_data": f"pay_await_accept:{payment_id}",
                },
                {
                    "text": "❌ Отклонить",
                    "callback_data": f"pay_await_reject:{payment_id}",
                },
            ],
            [{"text": "Главная", "callback_data": "start"}],
        ]
    )


async def claim_pending_payment_or_alert(
    event: CallbackQuery, payment_id: str, action: str
) -> Optional[Dict[str, Any]]:
    moderator_id = event.from_user.id
    payment = await json_db.claim_pending_payment(payment_id, moderator_id, action)
    if not payment:
        await event.answer(
            "❌ Платеж не найден, уже обработан или сейчас обрабатывается другим администратором.",
            show_alert=True,
        )
        return None
    return payment


async def finalize_claimed_payment_or_alert(
    event: CallbackQuery, payment_id: str, action: str, final_status: str
) -> bool:
    moderator_id = event.from_user.id
    success = await json_db.finalize_claimed_payment(
        payment_id, moderator_id, action, final_status
    )
    if not success:
        await event.answer(
            "❌ Не удалось завершить обработку платежа: его состояние уже изменилось.",
            show_alert=True,
        )
        return False
    return True


async def rollback_claimed_payment(
    event: CallbackQuery, payment_id: str, action: str, *, error_message: str
) -> None:
    moderator_id = event.from_user.id
    rolled_back = await json_db.rollback_claimed_payment(
        payment_id,
        moderator_id,
        action,
        error_message=error_message,
    )
    if not rolled_back:
        logger.warning(
            f"Не удалось откатить платеж payment_id={payment_id} в pending после ошибки обработки"
        )


async def verify_payment_final_status(payment_id: str, expected_status: str) -> bool:
    """Проверяет что платеж находится в ожидаемом финальном статусе.
    Используется для защиты от race condition при модерации платежей."""
    payment = await json_db.find_by_id(payment_id)
    if not payment:
        logger.warning(f"Платеж {payment_id} не найден при проверке финального статуса")
        return False
    status = payment.get("status", "")
    is_valid = status == expected_status
    if not is_valid:
        logger.warning(
            f"Платеж {payment_id}: ожидалась статус={expected_status}, получен статус={status}"
        )
    return is_valid


async def append_payment_decision_label(
    message: Optional[Message], status_label: str
) -> None:
    if not message:
        return
    current_text = message.text or ""
    new_text = f"{current_text}\n\n{status_label}" if current_text else status_label
    await message.edit_text(new_text, parse_mode="HTML")


def build_tariffs_text(plans: Optional[List[Dict[str, Any]]] = None) -> str:
    plans = plans if plans is not None else get_all_active()
    if not plans:
        text = "<b>Тарифы VPN</b>\n\nФиксированные тарифы временно недоступны."
        if custom_tariff_enabled():
            text += "\n\n" + build_custom_tariff_info_block()
        return text

    text = "<b>Тарифы VPN</b>\n\n"
    for idx, plan in enumerate(plans, 1):
        price = plan.get("price_rub", 0)
        duration = int(plan.get("duration_days", 30))
        if price == 0:
            price_line = f"Бесплатно на {duration} дня"
        elif duration == 30:
            price_line = f"{price} ₽/мес"
        else:
            price_line = f"{price} ₽ / {duration} дней"
        text += (
            f"{idx}. <b>{plan.get('name', plan.get('id'))}</b>\n"
            f"   Стоимость: {price_line}\n"
            f"   Устройства: до {plan.get('ip_limit', 0)}\n"
            f"   Трафик: до {format_traffic(plan.get('traffic_gb', 0))}\n"
        )
        servers = get_plan_servers(plan)
        if servers:
            text += f"   Локации: {format_servers(servers)}\n"
        text += "\n"

    if custom_tariff_enabled():
        text += build_custom_tariff_info_block() + "\n\n"

    text += "Список стран и серверов будет расширяться."
    return text


def build_buy_text(
    plans: Optional[List[Dict[str, Any]]] = None, *, for_admin: bool = False
) -> str:
    plans = plans if plans is not None else get_all_active()
    if not plans:
        text = (
            "<b>Покупка подписки VPN</b>\n\n"
            "Фиксированные тарифы временно недоступны."
        )
        if custom_tariff_enabled():
            text += "\n\nДоступен кастомный тариф по кнопке ниже."
        return text

    text = "<b>Покупка подписки VPN</b>\n\nВыберите тариф:\n\n"
    for idx, plan in enumerate(plans, 1):
        price = plan.get("price_rub", 0)
        duration = int(plan.get("duration_days", 30))
        if price == 0:
            price_line = f"Бесплатно на {duration} дня"
        elif duration == 30:
            price_line = f"{price} ₽/мес"
        else:
            price_line = f"{price} ₽ / {duration} дней"
        servers = get_plan_servers(plan)
        servers_text = f" — {format_servers(servers)}" if servers else ""
        text += (
            f"{idx}. <b>{plan.get('name', plan.get('id'))}</b> — "
            f"{price_line}{servers_text}\n"
        )

    if custom_tariff_enabled():
        text += "\n\nКастомный тариф доступен отдельной кнопкой ниже списка."

    if for_admin:
        text += "\nДля администраторов доступны тестовые подключения без оплаты."
    else:
        text += "\nПосле выбора тарифа откроются согласие с офертой и шаг оплаты."
    return text


async def get_visible_plans(user_id: int, *, for_admin: bool) -> List[Dict[str, Any]]:
    plans = get_all_active()
    if for_admin:
        return [p for p in plans if not is_trial_plan(p)]

    user = await db.get_user(user_id)
    trial_used = bool(user.get("trial_used")) if user else False
    has_subscription = bool(user.get("has_subscription")) if user else False

    visible: List[Dict[str, Any]] = []
    for plan in plans:
        if is_trial_plan(plan) and (trial_used or has_subscription):
            continue
        visible.append(plan)
    return visible


def active_subscription_keyboard() -> InlineKeyboardMarkup:
    return kb(
        [
            [{"text": "Моя подписка", "callback_data": "mysub"}],
            [{"text": "Главная", "callback_data": "start"}],
        ]
    )


def inactive_subscription_actions_keyboard() -> InlineKeyboardMarkup:
    rows: List[List[Dict[str, str]]] = [
        [{"text": "Купить подписку", "callback_data": "buy"}]
    ]
    if Config.SUPPORT_URL:
        rows.append([{"text": "Поддержка", "url": f"{Config.SUPPORT_URL}"}])
    rows.append([{"text": "Главная", "callback_data": "start"}])
    return kb(rows)


async def show_active_subscription_guard(event) -> None:
    text = "⚠️ У вас уже есть активная подписка.\n\n" "Новый заказ сейчас недоступен."
    await smart_answer(
        event,
        text,
        reply_markup=active_subscription_keyboard(),
        delete_origin=True,
    )


def build_subscription_cleanup_message(
    reason: str,
    *,
    trust_before: Optional[int] = None,
    trust_after: Optional[int] = None,
    trust_delta: int = 0,
) -> str:
    if trust_before is not None and trust_after is not None:
        trust_line = build_trust_change_line(trust_delta, trust_before, trust_after)
    elif reason == "traffic_exhausted":
        trust_line = (
            "Очки доверия: <b>штраф применён</b> "
            f"(до {TRUST_SCORE_PENALTY_TRAFFIC_EXHAUSTED} очков)."
        )
    else:
        trust_line = "Очки доверия: <b>без изменений</b>."

    if reason == "traffic_exhausted":
        return (
            "⚠️ <b>Пакет трафика исчерпан.</b>\n\n"
            "Ваша подписка была удалена. "
            "Купите новую подписку или обратитесь в поддержку.\n\n"
            f"{trust_line}"
        )
    if reason == "expired":
        return (
            "⏰ <b>Срок подписки истёк.</b>\n\n"
            "Ваша подписка была удалена. "
            "Купите новую подписку или обратитесь в поддержку.\n\n"
            f"{trust_line}"
        )
    return (
        "⚠️ <b>Подписка больше неактивна.</b>\n\n"
        "Ваша подписка была удалена. "
        "Купите новую подписку или обратитесь в поддержку.\n\n"
        f"{trust_line}"
    )


async def get_subscription_state(user_id: int) -> Dict[str, Any]:
    user = await db.get_user(user_id)
    if not user:
        return {"status": "no_user", "panel_available": True}

    sub_id = normalize_sub_id(user.get("vpn_url"))
    if not sub_id:
        return {"status": "no_subscription", "panel_available": True}

    base_email = build_base_email(user_id)
    panel_available, clients = await panel.get_client_stats_safe(base_email)
    if not panel_available:
        return {
            "status": "panel_unavailable",
            "panel_available": False,
            "sub_id": sub_id,
        }

    if not clients:
        return {
            "status": "missing_on_panel",
            "panel_available": True,
            "sub_id": sub_id,
        }

    now_ms = int(time.time() * 1000)
    expiry_times = [to_int(c.get("expiryTime"), 0) for c in clients]
    positive_expiry = [x for x in expiry_times if x > 0]
    max_expiry = max(positive_expiry) if positive_expiry else 0

    used_bytes = 0
    for client in clients:
        used_bytes += max(0, to_int(client.get("up"), 0))
        used_bytes += max(0, to_int(client.get("down"), 0))

    traffic_gb = max(0.0, to_float(user.get("traffic_gb"), 0.0))
    traffic_bytes = int(traffic_gb * BYTES_IN_GB)
    traffic_exhausted = traffic_bytes > 0 and used_bytes >= traffic_bytes
    expired = bool(max_expiry and max_expiry <= now_ms)

    status = "active"
    if expired:
        status = "expired"
    elif traffic_exhausted:
        status = "traffic_exhausted"

    return {
        "status": status,
        "panel_available": True,
        "sub_id": sub_id,
        "clients": clients,
        "max_expiry": max_expiry,
        "used_bytes": used_bytes,
        "used_gb": used_bytes / BYTES_IN_GB,
        "traffic_gb": traffic_gb,
        "traffic_bytes": traffic_bytes,
    }


async def cleanup_subscription(
    user_id: int, reason: str, *, notify_user_about_cleanup: bool
) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "success": False,
        "trust_before": None,
        "trust_after": None,
        "trust_delta": 0,
    }

    deleted_on_panel = await panel.delete_client(build_base_email(user_id))
    if not deleted_on_panel:
        logger.warning(
            f"Не удалось удалить подписку user_id={user_id} из 3X-UI. reason={reason}"
        )
        return result

    # Применить штраф за очки доверия при истощении трафика
    if reason == "traffic_exhausted":
        changed, trust_before, trust_after, trust_delta = await apply_trust_score_delta(
            user_id, -TRUST_SCORE_PENALTY_TRAFFIC_EXHAUSTED
        )
        if changed:
            result["trust_before"] = trust_before
            result["trust_after"] = trust_after
            result["trust_delta"] = trust_delta
            logger.info(
                f"Штраф {trust_delta} очков за истощение трафика для user_id={user_id} "
                f"(было {trust_before}, стало {trust_after})"
            )
        else:
            logger.error(
                f"Не удалось применить штраф trust score для user_id={user_id}, reason={reason}"
            )
    else:
        current_trust = await db.get_trust_score(user_id)
        result["trust_before"] = current_trust
        result["trust_after"] = current_trust

    await db.remove_subscription(user_id)
    if notify_user_about_cleanup:
        await notify_user(
            user_id,
            build_subscription_cleanup_message(
                reason,
                trust_before=result["trust_before"],
                trust_after=result["trust_after"],
                trust_delta=result["trust_delta"],
            ),
            reply_markup=inactive_subscription_actions_keyboard(),
        )
    result["success"] = True
    return result


async def ensure_subscription_state(
    user_id: int, *, notify_user_about_cleanup: bool = False
) -> Dict[str, Any]:
    state = await get_subscription_state(user_id)
    status = state.get("status")

    if status in {"expired", "traffic_exhausted", "missing_on_panel"}:
        cleanup_result = await cleanup_subscription(
            user_id,
            status,
            notify_user_about_cleanup=notify_user_about_cleanup,
        )
        state["cleanup_success"] = bool(cleanup_result.get("success"))
        state["cleanup_trust_before"] = cleanup_result.get("trust_before")
        state["cleanup_trust_after"] = cleanup_result.get("trust_after")
        state["cleanup_trust_delta"] = cleanup_result.get("trust_delta", 0)
    else:
        state["cleanup_success"] = False

    return state


async def create_subscription(
    user_id: int,
    plan: Dict[str, Any],
    *,
    extra_days: int = 0,
    days_override: Optional[int] = None,
    plan_suffix: Optional[str] = None,
    earn_trust: bool = True,
) -> Optional[str]:
    if not plan:
        return None

    pending_days = await db.get_bonus_days_pending(user_id)
    if days_override is None:
        days = int(plan.get("duration_days", 30)) + extra_days + pending_days
    else:
        days = int(days_override) + pending_days

    if days <= 0:
        days = 1

    plan_servers = get_plan_servers(plan)
    inbound_ids = await panel.get_matching_inbound_ids(plan_servers)
    if inbound_ids is None:
        return None
    if not inbound_ids:
        logger.error(
            f"Нельзя создать подписку user_id={user_id}: нет inbound для локаций {format_servers(plan_servers)}"
        )
        return None

    base_email = build_base_email(user_id)
    deleted_existing = await panel.delete_client(base_email)
    if not deleted_existing:
        logger.error(
            f"Не удалось очистить старую подписку user_id={user_id} перед созданием новой"
        )
        return None

    client = await panel.create_client(
        email=base_email,
        limit_ip=int(plan.get("ip_limit", 0)),
        total_gb=int(plan.get("traffic_gb", 0)),
        days=days,
        servers=plan_servers,
        tg_id=user_id,
        inbound_ids=inbound_ids,
    )

    if not client:
        return None

    sub_id = normalize_sub_id(client.get("subId", f"user_{user_id}"))
    if not sub_id:
        sub_id = f"user_{user_id}"

    plan_name = plan.get("name", plan.get("id", ""))
    if plan_suffix:
        plan_name = f"{plan_name}{plan_suffix}"

    await db.set_subscription(
        user_id=user_id,
        plan_text=plan_name,
        ip_limit=int(plan.get("ip_limit", 0)),
        traffic_gb=int(plan.get("traffic_gb", 0)),
        vpn_url=sub_id,
        plan_servers=plan_servers,
    )

    if pending_days > 0:
        await db.clear_bonus_days_pending(user_id)

    if earn_trust:
        # Начислить очки доверия: 5% от стоимости подписки
        price = to_float(plan.get("price_rub", 0), 0.0)
        earned_points = int((price * TRUST_SCORE_EARN_PERCENT) / 100)
        if earned_points > 0:
            await db.add_trust_score(user_id, earned_points)
            logger.info(
                f"Начислено {earned_points} очков доверия для user_id={user_id} (подписка {plan.get('name', plan.get('id'))})"
            )

    return build_subscription_url(sub_id)


async def is_active_subscription(
    user_id: int, *, notify_user_about_cleanup: bool = False
) -> bool:
    state = await ensure_subscription_state(
        user_id, notify_user_about_cleanup=notify_user_about_cleanup
    )
    status = state.get("status")
    if status == "active":
        return True
    if status == "panel_unavailable" and state.get("sub_id"):
        # Не создаем второй заказ, пока панель недоступна и в БД есть активный sub_id.
        return True
    return False


async def reward_referrer(referrer_id: int, bonus_days: int) -> None:
    ref_user = await db.get_user(referrer_id)
    if not ref_user:
        return

    pending = await db.get_bonus_days_pending(referrer_id)
    total_bonus = bonus_days + pending

    base_email = build_base_email(referrer_id)
    has_active = await is_active_subscription(referrer_id)

    if has_active:
        success = await panel.extend_client_expiry(base_email, total_bonus)
        if success:
            if pending > 0:
                await db.clear_bonus_days_pending(referrer_id)
            await notify_user(
                referrer_id,
                f"🎉 Вам начислено {total_bonus} дней по реферальной программе!",
            )
            return

        await db.add_bonus_days_pending(referrer_id, bonus_days)
        await notify_admins(
            f"⚠️ Не удалось продлить подписку реферера {referrer_id}. Бонус {bonus_days} дней сохранен в ожидании."
        )
        return

    min_plan = get_minimal_by_price()
    if not min_plan:
        await db.add_bonus_days_pending(referrer_id, bonus_days)
        await notify_admins(
            f"⚠️ Нет доступных тарифов для выдачи бонуса рефереру {referrer_id}. Бонус сохранен."
        )
        return

    vpn_url = await create_subscription(
        referrer_id,
        min_plan,
        days_override=bonus_days,
        plan_suffix=" (реферальный бонус)",
        earn_trust=False,
    )

    if vpn_url:
        await notify_user(
            referrer_id,
            f"🎉 Вам выдана бесплатная подписка на {total_bonus} дней по реферальной программе!\n\nURL:\n<code>{vpn_url}</code>",
        )
    else:
        await db.add_bonus_days_pending(referrer_id, bonus_days)
        await notify_admins(
            f"⚠️ Не удалось выдать бесплатную подписку рефереру {referrer_id}. Бонус сохранен."
        )


# --- Фоновые задачи ---
async def cleanup_stale_payments() -> int:
    """Освобождает платежи которые зависли в processing более чем на PAYMENT_PROCESSING_TIMEOUT_SEC.
    Используется при завершении бота для предотвращения deadlock зависших платежей."""
    try:
        released = await json_db.release_stale_processing_payments()
        if released > 0:
            logger.info(f"Освобождено {released} зависших платежей при завершении бота")
        return released
    except Exception as e:
        logger.error(f"Ошибка при освобождении зависших платежей: {e}")
        return 0


async def check_expired_subscriptions():
    while True:
        try:
            subscribed_users = await db.get_subscribed_user_ids()

            for user_id in subscribed_users:
                state = await ensure_subscription_state(
                    user_id, notify_user_about_cleanup=True
                )
                status = state.get("status")
                if status == "panel_unavailable":
                    logger.warning(
                        f"Проверка подписки user_id={user_id} отложена: панель недоступна"
                    )
                if status in {
                    "expired",
                    "traffic_exhausted",
                    "missing_on_panel",
                } and not state.get("cleanup_success"):
                    logger.warning(
                        f"Не удалось завершить cleanup подписки user_id={user_id}. reason={status}"
                    )

            await asyncio.sleep(3600)
        except Exception as e:
            logger.error(f"Ошибка проверки подписок: {e}")
            await asyncio.sleep(60)


async def cleanup_old_payments():
    while True:
        try:
            cutoff_time = datetime.now() - timedelta(days=30)

            def should_remove(payment: Dict[str, Any]) -> bool:
                if payment.get("status") not in ("accepted", "rejected"):
                    return False

                processed_at = payment.get("processed_at")
                if not processed_at:
                    return False

                try:
                    dt = datetime.fromisoformat(processed_at)
                    return dt < cutoff_time
                except Exception:
                    return False

            await json_db.remove(should_remove)

            await asyncio.sleep(259200)
        except Exception as e:
            logger.error(f"Ошибка очистки платежей: {e}")
            await asyncio.sleep(3600)


class SSLUpdateTask:
    @staticmethod
    async def run():
        while True:
            try:
                await asyncio.sleep(432000)  # 5 days = 432000 seconds

                if not Config.SSH_HOST or not Config.SSH_USER:
                    logger.warning("SSH настройки не заданы, пропуск обновления SSL")
                    continue

                logger.info("Начинаем обновление SSL через SSH")

                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

                try:
                    if Config.SSH_PASSWORD:
                        ssh.connect(
                            Config.SSH_HOST,
                            username=Config.SSH_USER,
                            password=Config.SSH_PASSWORD,
                        )
                    elif Config.SSH_KEY_PATH:
                        ssh.connect(
                            Config.SSH_HOST,
                            username=Config.SSH_USER,
                            key_filename=Config.SSH_KEY_PATH,
                        )
                    else:
                        raise ValueError("Не заданы SSH пароль или ключ")

                    # Получить интерактивную оболочку
                    shell = ssh.invoke_shell()
                    await asyncio.sleep(1)  # Подождать инициализации

                    # Команды для обновления SSL
                    commands = [
                        "service nginx stop",
                        "x-ui",
                        "19",  # Управление сертификатами
                        "6",  # Получение сертификата для IP
                        "y",  # Подтверждение
                        "",  # Пустая строка для пропуска IPv6
                        "80",  # Порт 80
                    ]

                    # Отправить команды
                    for cmd in commands:
                        shell.send(cmd + "\n")
                        await asyncio.sleep(5)  # Пауза между командами

                    # Дополнительное ожидание для получения SSL (дольше)
                    await asyncio.sleep(60)  # 1 минута на получение сертификата

                    # Включить nginx обратно
                    shell.send("service nginx start\n")
                    await asyncio.sleep(5)

                    # Закрыть соединение
                    shell.close()
                    ssh.close()

                    logger.info("SSL обновление выполнено успешно")

                    # Notify admins
                    for admin_id in Config.ADMIN_USER_IDS:
                        try:
                            await notify_user(
                                admin_id,
                                "🔄 <b>SSL-сертификат обновлён</b>\n\nОбновление выполнено автоматически.",
                            )
                        except Exception as e:
                            logger.error(f"Не удалось уведомить админа {admin_id}: {e}")

                except Exception as e:
                    logger.error(f"Ошибка SSH подключения или выполнения команд: {e}")
                    ssh.close()
                    raise e

            except Exception as e:
                logger.error(f"Ошибка обновления SSL: {e}")
                # Notify admins about failure
                for admin_id in Config.ADMIN_USER_IDS:
                    try:
                        await notify_user(
                            admin_id, f"❌ <b>Ошибка обновления SSL</b>\n\n{e}"
                        )
                    except Exception:
                        pass
                await asyncio.sleep(3600)  # Wait 1 hour before retry


# --- Инициализация ---
BOT_TOKEN_FOR_INIT = (
    Config.BOT_TOKEN if is_valid_bot_token_format(Config.BOT_TOKEN) else "0:invalid"
)
bot = Bot(
    token=BOT_TOKEN_FOR_INIT, default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)
dp.callback_query.middleware(CallbackAnswerMiddleware())

db = Database(Config.DATA_FILE)
json_db = JSONStorage(Config.DATA_AWAIT)
panel = PanelAPI()


# --- Middleware ---
async def ban_middleware(handler, event, data):
    if isinstance(event, Message):
        user_id = event.from_user.id
    elif isinstance(event, CallbackQuery):
        user_id = event.from_user.id
    else:
        return await handler(event, data)

    user_data = await db.get_user(user_id)
    if user_data and user_data.get("banned"):
        ban_reason = user_data.get("ban_reason", "Не указана")
        support_kb = support_keyboard(include_main=True)
        if isinstance(event, Message):
            await event.answer(
                "⛔ <b>Ваш аккаунт заблокирован!</b>\n\n"
                f"Причина: {ban_reason}\n\n"
                "Ваши очки доверия были обнулены.\n\n"
                "Если вы считаете, что это ошибка, пожалуйста, свяжитесь с поддержкой.",
                reply_markup=support_kb,
            )
        elif isinstance(event, CallbackQuery):
            if event.message:
                await event.message.answer(
                    "⛔ <b>Ваш аккаунт заблокирован!</b>\n\n"
                    f"Причина: {ban_reason}\n\n"
                    "Ваши очки доверия были обнулены.\n\n"
                    "Если вы считаете, что это ошибка, пожалуйста, свяжитесь с поддержкой.",
                    reply_markup=support_kb,
                )
            await event.answer("⛔ Ваш аккаунт заблокирован.", show_alert=True)
        return None

    return await handler(event, data)


router.message.middleware(ban_middleware)
router.callback_query.middleware(ban_middleware)


# --- Обработчики команд ---
# --- start ---
@router.message(Command("start"))
@router.callback_query(F.data == "start")
async def cmd_start(event, state: FSMContext):
    await state.clear()

    if isinstance(event, Message):
        user_id = event.from_user.id
        parts = event.text.strip().split(maxsplit=1) if event.text else []
        ref_code = parts[1] if len(parts) > 1 else ""
    else:
        user_id = event.from_user.id
        ref_code = ""

    await db.add_user(user_id)
    await db.ensure_ref_code(user_id)

    if ref_code:
        ref_user = await db.get_user_by_ref_code(ref_code)
        if ref_user and ref_user.get("user_id") != user_id:
            await db.set_ref_by(user_id, int(ref_user.get("user_id")))

    total_users = await db.get_total_users()
    banned_users = await db.get_banned_users_count()
    subs_ids = await db.get_subscribed_user_ids()
    active_vpns = len(subs_ids)
    has_active_subscription = False
    if not is_admin_user(user_id):
        has_active_subscription = await is_active_subscription(
            user_id, notify_user_about_cleanup=True
        )

    if is_admin_user(user_id):
        text = (
            "👑 <b>Добро пожаловать, администратор!</b>\n\n"
            f"Всего пользователей: <b>{total_users}</b>\n"
            f"Активных VPN: <b>{active_vpns}</b>\n"
            f"Заблокированных пользователей: <b>{banned_users}</b>"
        )
        keyboard = build_main_keyboard(is_admin=True, has_active_subscription=False)
    else:
        text = (
            "👋 <b>Добро пожаловать в VPN бот!</b>\n\n"
            f"Всего пользователей: <b>{total_users}</b>\n"
            f"Активных VPN: <b>{active_vpns}</b>"
        )
        keyboard = build_main_keyboard(
            is_admin=False, has_active_subscription=has_active_subscription
        )

    await smart_answer(event, text, reply_markup=kb(keyboard), delete_origin=True)


# --- cancel ---
@router.callback_query(F.data == "cancel")
async def cmd_cancel(event, state: FSMContext):
    await state.clear()
    await cmd_start(event, state)


# --- subs ---
@router.callback_query(F.data == "subs")
async def cmd_subs(event):
    user_id = event.from_user.id
    await db.add_user(user_id)
    is_admin = is_admin_user(user_id)
    plans = await get_visible_plans(user_id, for_admin=is_admin)
    text = build_tariffs_text(plans)

    if is_admin:
        keyboard = [
            [{"text": "Подтверждение платежей", "callback_data": "pay_await"}],
            [{"text": "Купить подписку", "callback_data": "buy"}],
            [{"text": "Моя подписка", "callback_data": "mysub"}],
            [{"text": "Главная", "callback_data": "start"}],
        ]
    else:
        has_active_subscription = await is_active_subscription(
            user_id, notify_user_about_cleanup=True
        )
        keyboard: List[List[Dict[str, str]]] = []
        if not has_active_subscription:
            keyboard.append([{"text": "Купить подписку", "callback_data": "buy"}])
        keyboard.extend(
            [
                [{"text": "Моя подписка", "callback_data": "mysub"}],
                [{"text": "Главная", "callback_data": "start"}],
            ]
        )

    await smart_answer(event, text, reply_markup=kb(keyboard), delete_origin=True)


# --- buy ---
@router.callback_query(F.data == "buy")
async def cmd_buy(event):
    user_id = event.from_user.id
    await db.add_user(user_id)
    is_admin = is_admin_user(user_id)
    if not is_admin and await is_active_subscription(
        user_id, notify_user_about_cleanup=True
    ):
        await show_active_subscription_guard(event)
        return

    plans = await get_visible_plans(user_id, for_admin=is_admin)
    text = build_buy_text(plans, for_admin=is_admin)

    keyboard = [[{"text": "Наши тарифы", "callback_data": "subs"}]]
    if is_admin:
        for plan in plans:
            keyboard.append(
                [
                    {
                        "text": f"Тест {plan.get('name', plan.get('id'))}",
                        "callback_data": f"test:{plan.get('id')}",
                    }
                ]
            )
    else:
        for plan in plans:
            if is_trial_plan(plan):
                keyboard.append(
                    [
                        {
                            "text": plan.get("name", plan.get("id")),
                            "callback_data": "trial:trial",
                        }
                    ]
                )
            else:
                keyboard.append(
                    [
                        {
                            "text": plan.get("name", plan.get("id")),
                            "callback_data": f"buy:{plan.get('id')}",
                        }
                    ]
                )

    if custom_tariff_enabled():
        custom_button_text = "Кастомный тариф (тест)" if is_admin else "Кастомный тариф"
        keyboard.append([{"text": custom_button_text, "callback_data": "custom:start"}])

    keyboard.append([{"text": "Главная", "callback_data": "start"}])

    await smart_answer(event, text, reply_markup=kb(keyboard), delete_origin=True)


@router.callback_query(F.data == "custom:start")
async def cmd_custom_start(event: CallbackQuery, state: FSMContext):
    if not await ensure_custom_tariff_access(event, state):
        return

    user_id = event.from_user.id
    await db.add_user(user_id)
    if not is_admin_user(user_id) and await is_active_subscription(
        user_id, notify_user_about_cleanup=True
    ):
        await show_active_subscription_guard(event)
        return
    if not get_custom_locations():
        await smart_answer(
            event,
            "❌ Кастомный тариф временно недоступен: локации не настроены.",
            reply_markup=main_menu_keyboard(),
            delete_origin=True,
        )
        return

    min_gb, max_gb = custom_gb_bounds()
    await state.clear()
    await state.set_state(CustomTariffState.waiting_for_gb)
    await smart_answer(
        event,
        "<b>Кастомный тариф</b>\n\n"
        f"Шаг 1/4: введите объём трафика в ГБ ({min_gb}-{max_gb}).\n"
        "Пример: <code>500</code>",
        reply_markup=cancel_only_keyboard(),
        delete_origin=True,
    )


@router.message(CustomTariffState.waiting_for_gb)
async def process_custom_gb(event: Message, state: FSMContext):
    if not await ensure_custom_tariff_access(event, state):
        return

    text_value = (event.text or "").strip()
    if text_value.lower() in ("отмена", "cancel", "/cancel"):
        await state.clear()
        await cmd_start(event, state)
        return

    if not text_value.isdigit():
        await event.answer(
            "❌ Значение трафика должно быть целым числом в ГБ.",
            reply_markup=cancel_only_keyboard(),
        )
        return

    traffic_gb = int(text_value)
    min_gb, max_gb = custom_gb_bounds()
    if not (min_gb <= traffic_gb <= max_gb):
        await event.answer(
            f"❌ Трафик должен быть в диапазоне {min_gb}-{max_gb} ГБ.",
            reply_markup=cancel_only_keyboard(),
        )
        return

    min_ip, max_ip = custom_ip_bounds()
    await state.update_data(custom_traffic_gb=traffic_gb)
    await state.set_state(CustomTariffState.waiting_for_ip)
    await event.answer(
        "<b>Кастомный тариф</b>\n\n"
        f"Шаг 2/4: введите лимит IP ({min_ip}-{max_ip}).\n"
        "Пример: <code>3</code>",
        reply_markup=cancel_only_keyboard(),
    )


@router.message(CustomTariffState.waiting_for_ip)
async def process_custom_ip(event: Message, state: FSMContext):
    if not await ensure_custom_tariff_access(event, state):
        return

    text_value = (event.text or "").strip()
    if text_value.lower() in ("отмена", "cancel", "/cancel"):
        await state.clear()
        await cmd_start(event, state)
        return

    if not text_value.isdigit():
        await event.answer(
            "❌ Лимит IP должен быть целым числом.",
            reply_markup=cancel_only_keyboard(),
        )
        return

    ip_limit = int(text_value)
    min_ip, max_ip = custom_ip_bounds()
    if not (min_ip <= ip_limit <= max_ip):
        await event.answer(
            f"❌ Лимит IP должен быть в диапазоне {min_ip}-{max_ip}.",
            reply_markup=cancel_only_keyboard(),
        )
        return

    min_days, max_days = custom_days_bounds()
    await state.update_data(custom_ip_limit=ip_limit)
    await state.set_state(CustomTariffState.waiting_for_days)
    await event.answer(
        "<b>Кастомный тариф</b>\n\n"
        f"Шаг 3/4: введите срок подписки в днях ({min_days}-{max_days}).\n"
        "Пример: <code>30</code>",
        reply_markup=cancel_only_keyboard(),
    )


@router.message(CustomTariffState.waiting_for_days)
async def process_custom_days(event: Message, state: FSMContext):
    if not await ensure_custom_tariff_access(event, state):
        return

    text_value = (event.text or "").strip()
    if text_value.lower() in ("отмена", "cancel", "/cancel"):
        await state.clear()
        await cmd_start(event, state)
        return

    if not text_value.isdigit():
        await event.answer(
            "❌ Срок должен быть целым числом дней.",
            reply_markup=cancel_only_keyboard(),
        )
        return

    duration_days = int(text_value)
    min_days, max_days = custom_days_bounds()
    if not (min_days <= duration_days <= max_days):
        await event.answer(
            f"❌ Срок должен быть в диапазоне {min_days}-{max_days} дней.",
            reply_markup=cancel_only_keyboard(),
        )
        return

    data = await state.get_data()
    traffic_gb = to_int(data.get("custom_traffic_gb"), 0)
    ip_limit = to_int(data.get("custom_ip_limit"), 0)
    if not is_valid_custom_limits(traffic_gb, ip_limit, duration_days):
        await state.clear()
        await event.answer(
            "❌ Не удалось сформировать кастомный тариф. Начните заново.",
            reply_markup=main_menu_keyboard(),
            delete_origin=True,
        )
        return

    await state.update_data(custom_duration_days=duration_days, custom_servers=[])
    await state.set_state(CustomTariffState.waiting_for_locations)
    await show_custom_locations_picker(event, state)


@router.message(CustomTariffState.waiting_for_locations)
async def process_custom_locations_text(event: Message, state: FSMContext):
    if not await ensure_custom_tariff_access(event, state):
        return

    text_value = (event.text or "").strip()
    if text_value.lower() in ("отмена", "cancel", "/cancel"):
        await state.clear()
        await cmd_start(event, state)
        return

    await event.answer(
        "Выберите локации кнопками в сообщении выше или нажмите <b>Отмена</b>.",
        reply_markup=cancel_only_keyboard(),
    )


@router.callback_query(
    CustomTariffState.waiting_for_locations, F.data.startswith("custom:loc:")
)
async def cmd_custom_toggle_location(event: CallbackQuery, state: FSMContext):
    if not await ensure_custom_tariff_access(event, state):
        return

    code = normalize_server_code(event.data.rsplit(":", 1)[-1])
    if not get_location_by_code(code):
        await event.answer("❌ Локация недоступна", show_alert=True)
        return

    data = await state.get_data()
    selected = normalize_servers(data.get("custom_servers"))
    if code in selected:
        selected = [server for server in selected if server != code]
    else:
        selected.append(code)

    await state.update_data(custom_servers=selected)
    await show_custom_locations_picker(event, state)


@router.callback_query(
    CustomTariffState.waiting_for_locations, F.data == "custom:locations_done"
)
async def cmd_custom_locations_done(event: CallbackQuery, state: FSMContext):
    if not await ensure_custom_tariff_access(event, state):
        return

    data = await state.get_data()
    selected = normalize_servers(data.get("custom_servers"))
    if not selected:
        await event.answer("Выберите хотя бы одну локацию.", show_alert=True)
        return

    await show_custom_summary(event, state)


@router.message(CustomTariffState.waiting_for_confirm)
async def process_custom_confirm_text(event: Message, state: FSMContext):
    if not await ensure_custom_tariff_access(event, state):
        return

    text_value = (event.text or "").strip()
    if text_value.lower() in ("отмена", "cancel", "/cancel"):
        await state.clear()
        await cmd_start(event, state)
        return

    await event.answer(
        "Используйте кнопки в сообщении выше или нажмите <b>Отмена</b>.",
        reply_markup=cancel_only_keyboard(),
    )


@router.callback_query(
    CustomTariffState.waiting_for_confirm, F.data.startswith("custom:show_offer:")
)
async def cmd_custom_show_offer(event: CallbackQuery, state: FSMContext):
    if not await ensure_custom_tariff_access(event, state):
        return

    parts = event.data.split(":")
    if len(parts) < 3:
        await event.answer("❌ Ошибка обработки запроса", show_alert=True)
        return

    try:
        user_id = int(parts[2])
    except ValueError:
        await event.answer(
            "❌ Некорректный идентификатор пользователя", show_alert=True
        )
        return

    if user_id != event.from_user.id:
        await event.answer("❌ Ошибка: неверный пользователь", show_alert=True)
        return

    if is_admin_user(user_id):
        await event.answer(
            "Для администраторов этот шаг не требуется.", show_alert=True
        )
        return

    await show_offer_agreement(
        event,
        continue_callback_data=f"custom:show_payment:{user_id}",
    )


@router.callback_query(
    CustomTariffState.waiting_for_confirm, F.data.startswith("custom:show_payment:")
)
async def cmd_custom_show_payment(event: CallbackQuery, state: FSMContext):
    if not await ensure_custom_tariff_access(event, state):
        return

    parts = event.data.split(":")
    if len(parts) < 3:
        await event.answer("❌ Ошибка обработки запроса", show_alert=True)
        return

    try:
        user_id = int(parts[2])
    except ValueError:
        await event.answer(
            "❌ Некорректный идентификатор пользователя", show_alert=True
        )
        return

    if user_id != event.from_user.id:
        await event.answer("❌ Ошибка: неверный пользователь", show_alert=True)
        return

    if is_admin_user(user_id):
        await event.answer(
            "Для администраторов этот шаг не требуется.", show_alert=True
        )
        return

    if await is_active_subscription(user_id, notify_user_about_cleanup=True):
        await state.clear()
        await show_active_subscription_guard(event)
        return

    data = await state.get_data()
    traffic_gb = to_int(data.get("custom_traffic_gb"), 0)
    ip_limit = to_int(data.get("custom_ip_limit"), 0)
    duration_days = to_int(data.get("custom_duration_days"), 0)
    servers = normalize_servers(data.get("custom_servers"))
    amount = to_int(data.get("custom_final_amount"), -1)
    plan_name = str(data.get("custom_plan_name") or "").strip()

    if (
        amount < 0
        or not is_valid_custom_limits(traffic_gb, ip_limit, duration_days)
        or not is_valid_custom_servers(servers)
    ):
        await state.clear()
        await smart_answer(
            event,
            "❌ Не удалось подготовить платеж. Создайте кастомный тариф заново.",
            reply_markup=main_menu_keyboard(),
            delete_origin=True,
        )
        return

    if not plan_name:
        plan_name = build_custom_plan_name(traffic_gb, ip_limit, duration_days, servers)

    text = (
        "<b>Оплата кастомного тарифа</b>\n\n"
        f"Тариф: <b>{plan_name}</b>\n"
        f"Локации: <b>{format_servers(servers)}</b>\n"
        f"Сумма к оплате: <b>{amount} ₽</b>\n\n"
        f"Переведите {amount} ₽ по номеру карты: <code>{Config.PAYMENT_CARD_NUMBER}</code>.\n\n"
        "После перевода нажмите кнопку подтверждения."
    )
    keyboard = kb(
        [
            [
                {
                    "text": "Подтвердить оплату",
                    "callback_data": "custom:confirm_payment",
                }
            ],
            [{"text": "Отмена", "callback_data": "cancel"}],
        ]
    )
    await smart_answer(event, text, reply_markup=keyboard, delete_origin=True)


@router.callback_query(
    CustomTariffState.waiting_for_confirm, F.data == "custom:confirm_payment"
)
async def cmd_custom_confirm_payment(event: CallbackQuery, state: FSMContext):
    if not await ensure_custom_tariff_access(event, state):
        return

    user_id = event.from_user.id
    if not is_admin_user(user_id) and await is_active_subscription(
        user_id, notify_user_about_cleanup=True
    ):
        await state.clear()
        await show_active_subscription_guard(event)
        return

    data = await state.get_data()
    traffic_gb = to_int(data.get("custom_traffic_gb"), 0)
    ip_limit = to_int(data.get("custom_ip_limit"), 0)
    duration_days = to_int(data.get("custom_duration_days"), 0)
    servers = normalize_servers(data.get("custom_servers"))
    amount = to_int(data.get("custom_final_amount"), -1)
    base_amount = to_int(data.get("custom_base_amount"), -1)
    plan_name = str(data.get("custom_plan_name") or "").strip()

    if (
        amount < 0
        or base_amount < 0
        or not is_valid_custom_limits(traffic_gb, ip_limit, duration_days)
        or not is_valid_custom_servers(servers)
    ):
        await state.clear()
        await smart_answer(
            event,
            "❌ Не удалось подготовить платеж. Создайте кастомный тариф заново.",
            reply_markup=main_menu_keyboard(),
            delete_origin=True,
        )
        return

    if not plan_name:
        plan_name = build_custom_plan_name(traffic_gb, ip_limit, duration_days, servers)

    if is_admin_user(user_id):
        custom_plan = build_custom_plan(
            traffic_gb,
            ip_limit,
            duration_days,
            servers=servers,
            plan_name=plan_name,
        )
        vpn_url = await create_subscription(
            user_id,
            custom_plan,
            plan_suffix=" (тест)",
            earn_trust=False,
        )
        await state.clear()

        if vpn_url:
            text = (
                "<b>Тестовая подписка создана</b>\n\n"
                f"Тариф: <b>{plan_name} (тест)</b>\n"
                f"Устройства: <b>до {ip_limit}</b>\n"
                f"Трафик: <b>{format_traffic(traffic_gb)}</b>\n"
                f"Локации: <b>{format_servers(servers)}</b>\n"
                f"Срок: <b>{format_duration(duration_days)}</b>\n\n"
                f"URL для подключения:\n<code>{vpn_url}</code>"
            )
        else:
            text = "❌ Не удалось создать тестовую подписку."

        await smart_answer(
            event,
            text,
            reply_markup=kb(
                [
                    [{"text": "Моя подписка", "callback_data": "mysub"}],
                    [{"text": "Главная", "callback_data": "start"}],
                ]
            ),
            delete_origin=True,
        )
        return

    payment_id = f"pay_{user_id}_{int(time.time())}"
    payment_data = {
        "payment_id": payment_id,
        "user_id": user_id,
        "plan_id": "custom",
        "plan_type": "custom",
        "plan_name": plan_name,
        "amount": amount,
        "timestamp": datetime.now().isoformat(),
        "status": "pending",
        "custom_plan": {
            "traffic_gb": traffic_gb,
            "ip_limit": ip_limit,
            "duration_days": duration_days,
            "servers": servers,
            "price_rub": base_amount,
            "plan_name": plan_name,
        },
    }

    added = await json_db.add_pending_for_user(user_id, payment_data)
    await state.clear()
    if not added:
        await smart_answer(
            event,
            "У вас уже есть заявка на подтверждение платежа.\n\n"
            "Дождитесь проверки администратором.",
            reply_markup=main_menu_keyboard(),
            delete_origin=True,
        )
        return

    await smart_answer(
        event,
        "<b>Запрос на подтверждение платежа по кастомному тарифу получен.</b>\n\n"
        "После проверки платежа вы получите уведомление о статусе подписки.",
        reply_markup=main_menu_keyboard(),
        delete_origin=True,
    )


@router.callback_query(F.data.startswith("custom:"))
async def cmd_custom_unknown(event: CallbackQuery, state: FSMContext):
    if not await ensure_custom_tariff_access(event, state):
        return
    await event.answer("❌ Неизвестная команда кастомного тарифа.", show_alert=True)


@router.callback_query(F.data.startswith("buy:"))
async def cmd_buy_plan(event: CallbackQuery):
    user_id = event.from_user.id
    if not is_admin_user(user_id) and await is_active_subscription(
        user_id, notify_user_about_cleanup=True
    ):
        await show_active_subscription_guard(event)
        return

    plan_id = event.data.split(":", 1)[1]
    plan, error = get_purchasable_catalog_plan(plan_id)
    if not plan:
        await event.answer(error, show_alert=True)
        return

    await show_offer_agreement(
        event,
        continue_callback_data=f"show_payment:{plan.get('id')}:{user_id}",
    )


async def show_offer_agreement(
    event: CallbackQuery, *, continue_callback_data: str
) -> None:
    """Показывает окно с публичной офертой и кнопкой согласия."""
    text = (
        "<b>Подтвердите согласие с публичной офертой</b>\n\n"
        "Перед продолжением оплаты ознакомьтесь с публичной офертой и подтвердите согласие."
    )

    keyboard: List[List[Dict[str, str]]] = []
    if Config.PUBLIC_OFFER_URL:
        keyboard.append(
            [
                {
                    "text": "Публичная оферта",
                    "url": Config.PUBLIC_OFFER_URL,
                }
            ]
        )

    keyboard.append(
        [
            {"text": "Отмена", "callback_data": "cancel"},
            {
                "text": "Я согласен",
                "callback_data": continue_callback_data,
            },
        ]
    )

    await smart_answer(event, text, reply_markup=kb(keyboard), delete_origin=True)


@router.callback_query(F.data.startswith("show_payment:"))
async def cmd_show_payment_details(event: CallbackQuery):
    """Показывает реквизиты для оплаты и кнопку 'Подтвердить оплату'."""
    parts = event.data.split(":")
    if len(parts) < 3:
        await event.answer("❌ Ошибка обработки запроса", show_alert=True)
        return

    plan_id = parts[1]
    try:
        user_id = int(parts[2])
    except ValueError:
        await event.answer(
            "❌ Некорректный идентификатор пользователя", show_alert=True
        )
        return

    if user_id != event.from_user.id:
        await event.answer("❌ Ошибка: неверный пользователь", show_alert=True)
        return

    plan, error = get_purchasable_catalog_plan(plan_id)
    if not plan:
        await event.answer(error, show_alert=True)
        return

    price = to_float(plan.get("price_rub", 0), 0.0)
    duration = int(plan.get("duration_days", 30))
    if duration == 30:
        price_line = (
            f"{int(price)} ₽/мес" if price.is_integer() else f"{price:.2f} ₽/мес"
        )
    else:
        price_line = (
            f"{int(price)} ₽/{duration} дней"
            if price.is_integer()
            else f"{price:.2f} ₽/{duration} дней"
        )

    # Получить очки доверия и применить скидку
    trust_score = await db.get_trust_score(user_id)
    final_price, discount_percent = apply_trust_discount(price, trust_score)
    final_price_int = int(final_price)

    # Построить текст с учётом скидки
    text = (
        "<b>Покупка тарифа VPN</b>\n\n"
        f"Вы выбрали тариф <b>{plan.get('name', plan_id)}</b> за {price_line}.\n\n"
    )
    servers = get_plan_servers(plan)
    if servers:
        text += f"Локации: <b>{format_servers(servers)}</b>\n\n"

    if discount_percent > 0:
        original_price = int(price) if price.is_integer() else price
        text += (
            f"Итого (с учётом скидки): <s>{original_price} ₽</s> <b>{final_price_int} ₽</b> "
            f"(скидка -{discount_percent}%)\n\n"
        )
    else:
        text += f"Итого: <b>{int(price) if price.is_integer() else price} ₽</b>\n\n"

    text += (
        f"Для оплаты переведите {final_price_int} ₽ по номеру карты: <code>{Config.PAYMENT_CARD_NUMBER}</code>.\n"
        "После оплаты нажмите кнопку ниже для подтверждения платежа."
    )

    keyboard = [
        [
            {
                "text": "Подтвердить оплату",
                "callback_data": f"confirm_payment:{plan_id}:{user_id}",
            }
        ],
        [{"text": "Отмена", "callback_data": "cancel"}],
    ]

    await smart_answer(event, text, reply_markup=kb(keyboard), delete_origin=True)


@router.callback_query(F.data.startswith("test:"))
async def cmd_test_plan(event: CallbackQuery):
    user_id = event.from_user.id
    if not is_admin_user(user_id):
        await event.answer(
            "⛔ Эта функция доступна только администраторам!", show_alert=True
        )
        return

    plan_id = event.data.split(":", 1)[1]
    plan, error = get_purchasable_catalog_plan(plan_id)
    if not plan:
        await event.answer(error, show_alert=True)
        return

    vpn_url = await create_subscription(
        user_id,
        plan,
        plan_suffix=" (тест)",
        earn_trust=False,
    )

    if vpn_url:
        text = (
            "✅ <b>Тестовая подписка успешно создана!</b>\n\n"
            f"Тариф: <b>{plan.get('name', plan_id)} (тест)</b>\n"
            f"IP-адреса: <b>до {plan.get('ip_limit', 0)}</b>\n"
            f"Трафик: <b>{format_traffic(plan.get('traffic_gb', 0))}</b>\n"
            f"Локации: <b>{format_servers(plan.get('servers'))}</b>\n"
            f"Срок: <b>{format_duration(int(plan.get('duration_days', 30)))}</b>\n\n"
            f"URL для подключения:\n<code>{vpn_url}</code>"
        )
    else:
        text = "❌ <b>Ошибка создания тестовой подписки!</b>"

    keyboard = [
        [{"text": "Моя подписка", "callback_data": "mysub"}],
        [{"text": "Главная", "callback_data": "start"}],
    ]

    await smart_answer(event, text, reply_markup=kb(keyboard), delete_origin=True)


@router.callback_query(F.data.startswith("trial:"))
async def cmd_trial_plan(event: CallbackQuery):
    user_id = event.from_user.id
    if is_admin_user(user_id):
        await event.answer(
            "⛔ Пробный тариф доступен только пользователям!", show_alert=True
        )
        return
    if await is_active_subscription(user_id, notify_user_about_cleanup=True):
        await show_active_subscription_guard(event)
        return

    plan_id = event.data.split(":", 1)[1] if ":" in event.data else "trial"
    plan = get_by_id(plan_id)
    if not plan or not plan.get("active", True) or not is_trial_plan(plan):
        await event.answer("❌ Пробный тариф не найден или недоступен", show_alert=True)
        return

    await db.add_user(user_id)
    user = await db.get_user(user_id)
    trial_used = bool(user.get("trial_used")) if user else False
    has_subscription = bool(user.get("has_subscription")) if user else False

    if trial_used or has_subscription:
        text = "⚠️ Пробный тариф доступен только один раз для новых пользователей."
        keyboard = [
            [{"text": "Моя подписка", "callback_data": "mysub"}],
            [{"text": "Главная", "callback_data": "start"}],
        ]
        await smart_answer(event, text, reply_markup=kb(keyboard), delete_origin=True)
        return

    vpn_url = await create_subscription(
        user_id,
        plan,
        plan_suffix=" (пробный)",
        earn_trust=False,
    )

    if vpn_url:
        await db.mark_trial_used(user_id)
        text = (
            "✅ <b>Пробная подписка успешно создана!</b>\n\n"
            f"Тариф: <b>{plan.get('name', plan_id)} (пробный)</b>\n"
            f"IP-адреса: <b>до {plan.get('ip_limit', 0)}</b>\n"
            f"Трафик: <b>{format_traffic(plan.get('traffic_gb', 0))}</b>\n"
            f"Локации: <b>{format_servers(plan.get('servers'))}</b>\n"
            f"Срок: <b>{format_duration(int(plan.get('duration_days', 30)))}</b>\n\n"
            f"URL для подключения:\n<code>{vpn_url}</code>"
        )
    else:
        text = "❌ <b>Ошибка создания пробной подписки!</b>"

    keyboard = [
        [{"text": "Моя подписка", "callback_data": "mysub"}],
        [{"text": "Главная", "callback_data": "start"}],
    ]

    await smart_answer(event, text, reply_markup=kb(keyboard), delete_origin=True)


@router.callback_query(F.data.startswith("confirm_payment:"))
async def cmd_confirm_payment(event: CallbackQuery):
    parts = event.data.split(":")
    if len(parts) < 3:
        await event.answer("❌ Ошибка обработки платежа", show_alert=True)
        return

    plan_id = parts[1]
    try:
        user_id = int(parts[2])
    except Exception:
        await event.answer("❌ Ошибка обработки платежа", show_alert=True)
        return

    if user_id != event.from_user.id:
        await event.answer("❌ Ошибка обработки платежа", show_alert=True)
        return

    plan, error = get_purchasable_catalog_plan(plan_id)
    if not plan:
        await event.answer(error, show_alert=True)
        return

    if await is_active_subscription(user_id, notify_user_about_cleanup=True):
        await show_active_subscription_guard(event)
        return

    price = to_float(plan.get("price_rub", 0), 0.0)
    trust_score = await db.get_trust_score(user_id)
    amount = max(0, int(apply_trust_discount(price, trust_score)[0]))

    payment_id = f"pay_{user_id}_{int(time.time())}"
    payment_data = {
        "payment_id": payment_id,
        "user_id": user_id,
        "plan_id": plan_id,
        "plan_type": "catalog",
        "plan_name": plan.get("name", plan_id),
        "amount": amount,
        "timestamp": datetime.now().isoformat(),
        "status": "pending",
    }

    added = await json_db.add_pending_for_user(user_id, payment_data)
    if not added:
        text = (
            "🕒 У вас уже есть заявка на подтверждение платежа.\n\n"
            "Дождитесь проверки администратором."
        )
        keyboard = [[{"text": "Главная", "callback_data": "start"}]]
        await smart_answer(event, text, reply_markup=kb(keyboard), delete_origin=True)
        return

    text = (
        "🕒 <b>Ваш запрос на подтверждение платежа получен!</b>\n\n"
        "После проверки платежа вы получите уведомление о статусе вашей подписки."
    )
    keyboard = [[{"text": "Главная", "callback_data": "start"}]]
    await smart_answer(event, text, reply_markup=kb(keyboard), delete_origin=True)


# --- mysub ---
@router.callback_query(F.data == "mysub")
async def cmd_mysub(event):
    user_id = event.from_user.id

    if is_admin_user(user_id):
        text = (
            "👤 <b>Ваша подписка VPN</b>\n\n"
            "Тариф: <b>Admin</b>\n"
            "Остаток трафика: <b>Безлимит</b>\n"
            "IP-адреса: <b>Безлимит</b>\n"
            "Срок действия: <b>Безлимит</b>\n\n"
            "URL для подключения:\n"
            f"{Config.SUB_PANEL_BASE}Admin"
        )
        keyboard = [[{"text": "Главная", "callback_data": "start"}]]
        await smart_answer(event, text, reply_markup=kb(keyboard), delete_origin=True)
        return

    state = await ensure_subscription_state(user_id, notify_user_about_cleanup=False)
    status = state.get("status")
    user_data = await db.get_user(user_id)

    if status in {"expired", "traffic_exhausted", "missing_on_panel"} and state.get(
        "cleanup_success"
    ):
        await smart_answer(
            event,
            build_subscription_cleanup_message(
                status,
                trust_before=to_int(state.get("cleanup_trust_before"), 0),
                trust_after=to_int(state.get("cleanup_trust_after"), 0),
                trust_delta=to_int(state.get("cleanup_trust_delta"), 0),
            ),
            reply_markup=inactive_subscription_actions_keyboard(),
            delete_origin=True,
        )
        return

    if not user_data or not normalize_sub_id(user_data.get("vpn_url")):
        text = "👤 <b>Ваша подписка VPN</b>\n\nУ вас нет активной подписки."
        keyboard = [
            [{"text": "Купить подписку", "callback_data": "buy"}],
            [{"text": "Главная", "callback_data": "start"}],
        ]
        await smart_answer(event, text, reply_markup=kb(keyboard), delete_origin=True)
        return

    plan_text = user_data.get("plan_text", "Неизвестно")
    plan_servers = get_user_plan_servers(user_data)
    ip_limit = to_int(user_data.get("ip_limit"), 0)
    traffic_gb = max(0.0, to_float(user_data.get("traffic_gb"), 0.0))
    sub_url = build_subscription_url(user_data.get("vpn_url"))
    trust_score = to_int(user_data.get("trust_score"), 0)
    discount_percent = calculate_discount_percent(trust_score)

    if status == "active":
        used_gb = to_float(state.get("used_gb"), 0.0)
        max_expiry = to_int(state.get("max_expiry"), 0)
        if traffic_gb > 0:
            remaining_gb = max(0.0, traffic_gb - used_gb)
            traffic_line = f"Остаток трафика: <b>{remaining_gb:.1f} ГБ из {traffic_gb:.0f} ГБ</b>\n"
        else:
            traffic_line = "Остаток трафика: <b>Безлимит</b>\n"

        if max_expiry > 0:
            expiry_date = datetime.fromtimestamp(max_expiry / 1000).strftime(
                "%d.%m.%Y %H:%M"
            )
        else:
            expiry_date = "не указана"

        text = (
            "👤 <b>Ваша подписка VPN</b>\n\n"
            f"Тариф: <b>{plan_text}</b>\n"
            f"{traffic_line}"
            f"IP-адреса: <b>до {ip_limit}</b>\n"
            f"Локации: <b>{format_servers(plan_servers)}</b>\n"
            f"Срок действия: <b>до {expiry_date}</b>\n\n"
            f"Ваши очки доверия: <b>{trust_score}/100</b>\n"
            f"Ваша скидка: <b>{discount_percent}%</b>\n\n"
            "URL для подключения:\n"
            f"<code>{sub_url}</code>"
        )
    else:
        text = (
            "👤 <b>Ваша подписка VPN</b>\n\n"
            f"Тариф: <b>{plan_text}</b>\n"
            f"IP-адреса: <b>до {ip_limit}</b>\n"
            f"Трафик: <b>{format_traffic(traffic_gb)}</b>\n"
            f"Локации: <b>{format_servers(plan_servers)}</b>\n"
            f"Ваши очки доверия: <b>{trust_score}/100</b>\n"
            f"Ваша скидка: <b>{discount_percent}%</b>\n\n"
            "URL для подключения:\n"
            f"<code>{sub_url}</code>\n\n"
            "<i>Статистика подписки временно недоступна, попробуйте позже.</i>"
        )

    keyboard = [
        [{"text": "Рефералка", "callback_data": "ref"}],
        [{"text": "Главная", "callback_data": "start"}],
    ]
    await smart_answer(event, text, reply_markup=kb(keyboard), delete_origin=True)


# --- ref ---
@router.callback_query(F.data == "ref")
async def cmd_ref(event):
    user_id = event.from_user.id
    await db.add_user(user_id)
    ref_code = await db.ensure_ref_code(user_id)
    if not ref_code:
        text = "❌ Не удалось сгенерировать реферальный код."
        keyboard = [[{"text": "Главная", "callback_data": "start"}]]
        await smart_answer(event, text, reply_markup=kb(keyboard), delete_origin=True)
        return

    total_refs = await db.count_referrals(user_id)
    paid_refs = await db.count_referrals_paid(user_id)

    link = get_ref_link(ref_code)

    text = (
        "🤝 <b>Реферальная система VPN</b>\n\n"
        f"Ваш диплинк для приглашения друзей:\n<code>{link}</code>\n\n"
        f"Всего приглашено: <b>{total_refs}</b>\n"
        f"Оплатили подписку: <b>{paid_refs}</b>\n\n"
        f"Бонус за приглашение: <b>{Config.REF_BONUS_DAYS} дней</b> вам и другу после первой оплаты."
    )
    keyboard = [[{"text": "Главная", "callback_data": "start"}]]
    await smart_answer(event, text, reply_markup=kb(keyboard), delete_origin=True)


# --- ban ---
@router.callback_query(F.data == "ban")
async def cmd_ban(event, state: FSMContext):
    if not await ensure_admin_access(event):
        return

    text = "⛔ <b>Введите ID пользователя для блокировки</b>"
    keyboard = [
        [{"text": "Отмена", "callback_data": "cancel"}],
    ]
    await smart_answer(event, text, reply_markup=kb(keyboard), delete_origin=True)
    await state.set_state(BanUserState.waiting_for_user_id)


@router.message(BanUserState.waiting_for_user_id)
async def process_ban_user_id(event: Message, state: FSMContext):
    text_value = event.text.strip()
    if text_value.lower() in ("отмена", "cancel", "/cancel"):
        await state.clear()
        await cmd_start(event, state)
        return

    if not text_value.isdigit():
        await event.answer("❌ ID пользователя должен быть числом! Попробуйте снова:")
        return

    user_id_to_ban = int(text_value)

    if is_admin_user(user_id_to_ban):
        await event.answer("❌ Нельзя заблокировать администратора!")
        return

    await state.update_data(user_id_to_ban=user_id_to_ban)
    text = f"⛔ <b>Введите причину блокировки для пользователя ID {user_id_to_ban}</b>"
    keyboard = [
        [{"text": "Отмена", "callback_data": "cancel"}],
    ]
    await smart_answer(event, text, reply_markup=kb(keyboard), delete_origin=True)
    await state.set_state(BanUserState.waiting_for_ban_reason)


@router.message(BanUserState.waiting_for_ban_reason)
async def process_ban_reason(event: Message, state: FSMContext):
    text_value = event.text.strip()
    if text_value.lower() in ("отмена", "cancel", "/cancel"):
        await state.clear()
        await cmd_start(event, state)
        return

    data = await state.get_data()
    user_id_to_ban = data.get("user_id_to_ban")
    ban_reason = text_value

    success = await db.ban_user(user_id_to_ban, ban_reason)
    await state.clear()

    if success:
        # Удалить подписку при блокировке
        await cleanup_subscription(
            user_id_to_ban, f"banned: {ban_reason}", notify_user_about_cleanup=False
        )

        text = (
            f"⛔ <b>Пользователь ID {user_id_to_ban} заблокирован по причине:</b>\n"
            f"{ban_reason}"
        )

        try:
            await notify_user(
                user_id_to_ban,
                "⛔ <b>Ваш аккаунт заблокирован!</b>\n\n"
                f"Причина: {ban_reason}\n\n"
                "Ваши очки доверия были обнулены.\n\n"
                "Если вы считаете, что это ошибка, пожалуйста, свяжитесь с поддержкой.",
                reply_markup=support_keyboard(include_main=True),
            )
        except Exception:
            pass
    else:
        text = f"❌ <b>Ошибка при блокировке пользователя ID {user_id_to_ban}</b>"

    keyboard = [
        [{"text": "Разблокировать", "callback_data": "unban"}],
        [{"text": "Главная", "callback_data": "start"}],
    ]
    await smart_answer(event, text, reply_markup=kb(keyboard), delete_origin=True)


# --- unban ---
@router.callback_query(F.data == "unban")
async def cmd_unban(event, state: FSMContext):
    if not await ensure_admin_access(event):
        return

    text = "⛔ <b>Введите ID пользователя для разблокировки</b>"
    keyboard = [
        [{"text": "Отмена", "callback_data": "cancel"}],
    ]
    await smart_answer(event, text, reply_markup=kb(keyboard), delete_origin=True)
    await state.set_state(UnbanUserState.waiting_for_user_id)


@router.message(UnbanUserState.waiting_for_user_id)
async def process_unban_user_id(event: Message, state: FSMContext):
    text_value = event.text.strip()
    if text_value.lower() in ("отмена", "cancel", "/cancel"):
        await state.clear()
        await cmd_start(event, state)
        return

    if not text_value.isdigit():
        await event.answer("❌ ID пользователя должен быть числом! Попробуйте снова:")
        return

    user_id_to_unban = int(text_value)
    await state.update_data(user_id_to_unban=user_id_to_unban)

    text = f"⛔ <b>Введите причину разблокировки для пользователя ID {user_id_to_unban}</b>"
    keyboard = [
        [{"text": "Отмена", "callback_data": "cancel"}],
    ]
    await smart_answer(event, text, reply_markup=kb(keyboard), delete_origin=True)
    await state.set_state(UnbanUserState.waiting_for_unban_reason)


@router.message(UnbanUserState.waiting_for_unban_reason)
async def process_unban_reason(event: Message, state: FSMContext):
    text_value = event.text.strip()
    if text_value.lower() in ("отмена", "cancel", "/cancel"):
        await state.clear()
        await cmd_start(event, state)
        return

    data = await state.get_data()
    user_id_to_unban = data.get("user_id_to_unban")
    unban_reason = text_value

    success = await db.unban_user(user_id_to_unban)
    await state.clear()

    if success:
        text = (
            f"✅ <b>Пользователь ID {user_id_to_unban} разблокирован по причине:</b>\n"
            f"{unban_reason}"
        )

        try:
            await notify_user(
                user_id_to_unban,
                "✅ <b>Ваш аккаунт разблокирован!</b>\n\n"
                f"Причина: {unban_reason}\n\n"
                "Добро пожаловать обратно!",
            )
        except Exception:
            pass
    else:
        text = f"❌ <b>Ошибка при разблокировке пользователя ID {user_id_to_unban}</b>"

    keyboard = [[{"text": "Главная", "callback_data": "start"}]]
    await smart_answer(event, text, reply_markup=kb(keyboard), delete_origin=True)


# --- broadcast ---
@router.callback_query(F.data == "broadcast")
async def cmd_broadcast(event, state: FSMContext):
    if not await ensure_admin_access(event):
        return

    text = "📢 <b>Отправка уведомления</b>\n\nВыберите тип рассылки:"
    keyboard = [
        [
            {
                "text": "Всем зарегистрированным (кроме заблокированных)",
                "callback_data": "broadcast_all",
            }
        ],
        [
            {
                "text": "Только активным подписчикам",
                "callback_data": "broadcast_active",
            }
        ],
        [{"text": "Отмена", "callback_data": "cancel"}],
    ]
    await smart_answer(event, text, reply_markup=kb(keyboard), delete_origin=True)
    await state.set_state(BroadcastState.waiting_for_broadcast_type)


@router.callback_query(BroadcastState.waiting_for_broadcast_type)
async def process_broadcast_type(event: CallbackQuery, state: FSMContext):
    if event.data == "cancel":
        await state.clear()
        await cmd_start(event, state)
        return
    elif event.data == "start":
        await state.clear()
        await cmd_start(event, state)
        return

    broadcast_type = event.data
    await state.update_data(broadcast_type=broadcast_type)

    type_text = (
        "всем зарегистрированным (кроме заблокированных)"
        if broadcast_type == "broadcast_all"
        else "активным подписчикам"
    )
    text = f"📢 <b>Отправка уведомления {type_text}</b>\n\nВведите текст сообщения:"
    keyboard = [
        [{"text": "Отмена", "callback_data": "cancel"}],
    ]
    await smart_answer(event, text, reply_markup=kb(keyboard), delete_origin=True)
    await state.set_state(BroadcastState.waiting_for_message)


@router.message(BroadcastState.waiting_for_message)
async def process_broadcast_message(event: Message, state: FSMContext):
    # Проверить что это текстовое сообщение
    if not event.text or not isinstance(event.text, str):
        await event.answer(
            "❌ Для рассылки нужно отправить текстовое сообщение, а не стикер, фото или видео."
        )
        return

    text_value = event.text.strip()
    if not text_value:
        await event.answer("❌ Текст рассылки не должен быть пустым.")
        return

    if text_value.lower() in ("отмена", "cancel", "/cancel"):
        await state.clear()
        await cmd_start(event, state)
        return

    data = await state.get_data()
    broadcast_type = data.get("broadcast_type")
    message = text_value

    if broadcast_type == "broadcast_all":
        user_ids = await db.get_all_non_banned_user_ids()
    elif broadcast_type == "broadcast_active":
        user_ids = await db.get_subscribed_user_ids()
    else:
        await event.answer("❌ Ошибка: неизвестный тип рассылки")
        await state.clear()
        return

    await state.clear()

    sent_count = 0
    failed_count = 0

    for user_id in user_ids:
        try:
            await notify_user(user_id, message)
            sent_count += 1
        except Exception as e:
            logger.error(f"Failed to send broadcast to {user_id}: {e}")
            failed_count += 1

    type_text = (
        "всем зарегистрированным (кроме заблокированных)"
        if broadcast_type == "broadcast_all"
        else "активным подписчикам"
    )
    text = (
        f"📢 <b>Рассылка завершена</b>\n\n"
        f"Тип: {type_text}\n"
        f"Отправлено: {sent_count}\n"
        f"Ошибок: {failed_count}"
    )
    keyboard = [[{"text": "Главная", "callback_data": "start"}]]
    await smart_answer(event, text, reply_markup=kb(keyboard), delete_origin=True)


# --- debug_menu ---
@router.callback_query(F.data == "debug_menu")
async def cmd_debug_menu(event):
    if not await ensure_admin_access(event):
        return

    text = "🔧 <b>Инструменты отладки</b>\n\nВыберите действие:"
    keyboard = [
        [{"text": "Заблокировать пользователя", "callback_data": "ban"}],
        [{"text": "Разблокировать пользователя", "callback_data": "unban"}],
        [{"text": "Начислить очки доверия", "callback_data": "debug_trust_add"}],
        [{"text": "Снять очки доверия", "callback_data": "debug_trust_remove"}],
        [{"text": "Нормализация подписок", "callback_data": "debug_normalize"}],
        [{"text": "Главная", "callback_data": "start"}],
    ]
    await smart_answer(event, text, reply_markup=kb(keyboard), delete_origin=True)


@router.callback_query(F.data == "debug_trust_add")
async def cmd_debug_trust_add(event: CallbackQuery, state: FSMContext):
    if not await ensure_admin_access(event):
        return

    await state.set_state(TrustScoreState.waiting_for_user_id)
    await state.update_data(action="add")

    text = "🔧 <b>Начисление очков доверия</b>\n\nВведите ID пользователя, которому нужно начислить очки."
    keyboard = [[{"text": "Отмена", "callback_data": "cancel"}]]
    await smart_answer(event, text, reply_markup=kb(keyboard), delete_origin=True)


@router.callback_query(F.data == "debug_trust_remove")
async def cmd_debug_trust_remove(event: CallbackQuery, state: FSMContext):
    if not await ensure_admin_access(event):
        return

    await state.set_state(TrustScoreState.waiting_for_user_id)
    await state.update_data(action="remove")

    text = "🔧 <b>Снятие очков доверия</b>\n\nВведите ID пользователя, у которого нужно снять очки."
    keyboard = [[{"text": "Отмена", "callback_data": "cancel"}]]
    await smart_answer(event, text, reply_markup=kb(keyboard), delete_origin=True)


@router.message(TrustScoreState.waiting_for_user_id)
async def process_trust_user_id(event: Message, state: FSMContext):
    text_value = event.text.strip() if event.text else ""
    if text_value.lower() in ("отмена", "cancel", "/cancel"):
        await state.clear()
        await cmd_start(event, state)
        return

    if not text_value.isdigit():
        await event.answer("❌ ID пользователя должен быть числом.")
        return

    user_id_to_adjust = int(text_value)
    await state.update_data(user_id_to_adjust=user_id_to_adjust)

    data = await state.get_data()
    action = data.get("action")
    action_text = "начислить" if action == "add" else "снять"

    text = (
        f"🔧 <b>{action_text.capitalize()} очков доверия</b>\n\n"
        f"Введите количество очков, которые нужно {action_text} пользователю {user_id_to_adjust}."
    )
    keyboard = [[{"text": "Отмена", "callback_data": "cancel"}]]
    await smart_answer(event, text, reply_markup=kb(keyboard), delete_origin=True)
    await state.set_state(TrustScoreState.waiting_for_amount)


@router.message(TrustScoreState.waiting_for_amount)
async def process_trust_amount(event: Message, state: FSMContext):
    text_value = event.text.strip() if event.text else ""
    if text_value.lower() in ("отмена", "cancel", "/cancel"):
        await state.clear()
        await cmd_start(event, state)
        return

    if not text_value.isdigit():
        await event.answer(
            "❌ Количество очков должно быть положительным целым числом."
        )
        return

    amount = int(text_value)
    if amount <= 0:
        await event.answer("❌ Количество очков должно быть больше нуля.")
        return

    if amount > TRUST_SCORE_MAX:
        await event.answer(f"❌ Количество не может превышать {TRUST_SCORE_MAX}.")
        return

    data = await state.get_data()
    action = data.get("action")
    user_id_to_adjust = data.get("user_id_to_adjust")

    if not user_id_to_adjust or action not in {"add", "remove"}:
        await state.clear()
        await event.answer("❌ Ошибка состояния. Попробуйте снова.")
        return

    # Проверить текущий скор для предупреждения
    current_score = await db.get_trust_score(user_id_to_adjust)
    future_score = current_score + (amount if action == "add" else -amount)

    if future_score < TRUST_SCORE_MIN:
        await event.answer(
            f"❌ Операция приведет к отрицательному балансу. Текущий скор: {current_score}."
        )
        return

    if future_score > TRUST_SCORE_MAX:
        await event.answer(
            f"❌ Операция приведет к превышению максимума ({TRUST_SCORE_MAX}). Текущий скор: {current_score}."
        )
        return

    delta = amount if action == "add" else -amount
    result = await db.add_trust_score(user_id_to_adjust, delta)
    final_score = await db.get_trust_score(user_id_to_adjust)
    actual_delta = final_score - current_score

    await state.clear()

    if result:
        action_text = "начислено" if action == "add" else "снято"
        text = (
            f"✅ Очки доверия успешно {action_text}.\n\n"
            f"Пользователь: <code>{user_id_to_adjust}</code>\n"
            f"Было: <b>{current_score}</b> → стало: <b>{final_score}</b>\n"
            f"Изменение: <b>{actual_delta:+d}</b>\n"
            f"Скидка: <b>{calculate_discount_percent(final_score)}%</b>"
        )
        admin_id = event.from_user.id
        admin_username = (event.from_user.username or "").strip()
        admin_identity = f"ID <code>{admin_id}</code>"
        if admin_username:
            admin_identity += f", username <code>@{admin_username}</code>"

        admin_action = "начислил" if actual_delta >= 0 else "снял"
        try:
            await notify_user(
                user_id_to_adjust,
                "<b>Изменение очков доверия</b>\n\n"
                f"Администратор ({admin_identity}) {admin_action} вам "
                f"<b>{abs(actual_delta)}</b> очков доверия.\n"
                f"Было: <b>{current_score}</b> → стало: <b>{final_score}</b>.\n"
                f"Текущая скидка: <b>{calculate_discount_percent(final_score)}%</b>.",
            )
        except Exception:
            pass
    else:
        text = "❌ Не удалось изменить очки доверия. Проверьте состояние бота и попробуйте снова."

    keyboard = [[{"text": "Главная", "callback_data": "start"}]]
    await smart_answer(event, text, reply_markup=kb(keyboard), delete_origin=True)


@router.callback_query(F.data == "debug_normalize")
async def cmd_debug_normalize(event):
    if not await ensure_admin_access(event, silent=True):
        return

    text = "🔧 <b>Нормализация подписок...</b>"
    await smart_answer(event, text, delete_origin=True)

    report = {
        "expired_cleaned": 0,
        "traffic_exceeded_cleaned": 0,
        "missing_recovered": 0,
        "errors": 0,
    }

    # 1. Проверить истёкшие подписки и удалить
    db_subs = await db.get_subscribed_user_ids()
    for uid in db_subs:
        state = await get_subscription_state(uid)
        if state.get("status") == "expired":
            cleanup_result = await cleanup_subscription(
                uid, "expired", notify_user_about_cleanup=True
            )
            if cleanup_result.get("success"):
                report["expired_cleaned"] += 1
            else:
                report["errors"] += 1

    # 2. Проверить трафик и удалить превысившие
    for uid in db_subs:
        state = await get_subscription_state(uid)
        if state.get("status") == "traffic_exhausted":
            cleanup_result = await cleanup_subscription(
                uid, "traffic_exhausted", notify_user_about_cleanup=True
            )
            if cleanup_result.get("success"):
                report["traffic_exceeded_cleaned"] += 1
            else:
                report["errors"] += 1

    # 3. Восстановить отсутствующие подписки
    # Получить все уникальные sub_id из БД
    db_subs = await db.get_subscribed_user_ids()  # Обновить список после очистки
    sub_ids = []
    user_by_sub = {}
    expiry_by_sub = {}
    for uid in db_subs:
        user = await db.get_user(uid)
        if user and user.get("vpn_url"):
            sub_id = normalize_sub_id(user.get("vpn_url"))
            if sub_id:
                sub_ids.append(sub_id)
                user_by_sub[sub_id] = user
                # Получить expiry из существующих клиентов
                state = await get_subscription_state(uid)
                if state.get("max_expiry"):
                    expiry_by_sub[sub_id] = state["max_expiry"]

    # Получить все enabled inbound
    inbounds_data = await panel.get_inbounds()
    if inbounds_data and inbounds_data.get("success"):
        enabled_inbounds = [
            i for i in inbounds_data.get("obj", []) if i.get("enable", False)
        ]

        # Для каждого inbound проверить наличие всех sub_id
        for inbound in enabled_inbounds:
            inbound_id = inbound.get("id")
            client_stats = inbound.get("clientStats", [])

            # Собрать set существующих subId в этом inbound
            existing_sub_ids = {
                normalize_sub_id(c.get("subId", ""))
                for c in client_stats
                if c.get("subId")
            }

            for sub_id in sub_ids:
                if sub_id not in existing_sub_ids:
                    # Нужно добавить клиента в этот inbound
                    user = user_by_sub.get(sub_id)
                    if not user:
                        continue

                    plan_text = user.get("plan_text", "basic")
                    ip_limit = user.get("ip_limit", 1)
                    traffic_gb = user.get("traffic_gb", 10)
                    plan_servers = get_user_plan_servers(user)
                    if plan_servers and not panel._inbound_matches_servers(
                        inbound, plan_servers
                    ):
                        continue

                    # Найти тариф
                    tariff = get_by_name(plan_text)
                    if not tariff and isinstance(plan_text, str):
                        base_plan_text = plan_text.split(" (", 1)[0].strip()
                        if base_plan_text:
                            tariff = get_by_name(base_plan_text)

                    # Рассчитать expiry
                    bonus_days = user.get("bonus_days_pending", 0)
                    if sub_id in expiry_by_sub:
                        expiry_ms = expiry_by_sub[sub_id]
                    else:
                        # Если нет существующих, используем дни из БД
                        duration_days = (
                            to_int(tariff.get("duration_days"), 30) if tariff else 30
                        )
                        total_days = duration_days + bonus_days
                        expiry_ms = int((time.time() + total_days * 86400) * 1000)

                    # Создать клиента в конкретном inbound
                    try:
                        client_data = await panel.create_client_in_inbound(
                            inbound_id=inbound_id,
                            email=build_base_email(user["user_id"]),
                            limit_ip=ip_limit,
                            total_gb=traffic_gb,
                            expiry_ms=expiry_ms,
                            sub_id=sub_id,
                        )
                        if client_data:
                            report["missing_recovered"] += 1
                        else:
                            report["errors"] += 1
                    except Exception as e:
                        report["errors"] += 1
                        logger.error(
                            f"Exception recovering {sub_id} in {inbound_id}: {e}"
                        )

    # Отправить отчёт админу
    report_text = (
        f"🔧 <b>Отчёт нормализации подписок</b>\n\n"
        f"Удалено истёкших: {report['expired_cleaned']}\n"
        f"Удалено превысивших трафик: {report['traffic_exceeded_cleaned']}\n"
        f"Восстановлено отсутствующих: {report['missing_recovered']}\n"
        f"Ошибок: {report['errors']}"
    )

    if all(v == 0 for v in report.values()):
        report_text = (
            "✅ <b>Нормализация завершена</b>\n\nПроблем не найдено, всё в порядке."
        )

    keyboard = [[{"text": "Главная", "callback_data": "start"}]]
    await smart_answer(
        event, report_text, reply_markup=kb(keyboard), delete_origin=True
    )


# --- pay_await ---
@router.callback_query(F.data == "pay_await")
async def cmd_pay_await(event):
    if not await ensure_admin_access(event):
        return

    payments = await json_db.read_all()
    pending = [p for p in payments if p.get("status") == "pending"]

    if not pending:
        await smart_answer(
            event,
            "🕒 <b>Нет пользователей, ожидающих подтверждения платежа</b>",
            reply_markup=main_menu_keyboard(),
            delete_origin=True,
        )
        return

    await smart_answer(
        event,
        "🕒 <b>Пользователи, ожидающие подтверждения платежа:</b>",
        delete_origin=True,
    )

    for payment in pending:
        payment_id = str(payment.get("payment_id", ""))
        if not payment_id:
            continue
        payment_text = build_pending_payment_text(payment)
        reply_markup = build_pending_payment_keyboard(payment_id)

        if isinstance(event, Message):
            await event.answer(payment_text, reply_markup=reply_markup)
        elif isinstance(event, CallbackQuery) and event.message:
            await event.message.answer(payment_text, reply_markup=reply_markup)


@router.callback_query(F.data.startswith("pay_await_accept:"))
async def cmd_pay_await_accept(event: CallbackQuery):
    if not await ensure_admin_access(event):
        return

    payment_id = event.data.split(":", 1)[1]
    payment = await claim_pending_payment_or_alert(event, payment_id, "accept")
    if not payment:
        return

    user_id = to_int(payment.get("user_id"), 0)
    if user_id <= 0:
        await rollback_claimed_payment(
            event,
            payment_id,
            "accept",
            error_message="Некорректный user_id во время подтверждения платежа",
        )
        await event.answer("❌ Некорректный пользователь в платеже", show_alert=True)
        return

    plan_type = str(payment.get("plan_type", "catalog"))
    plan_id = str(payment.get("plan_id", ""))
    if plan_type == "custom":
        plan, error = build_custom_plan_from_payment(payment)
    else:
        plan, error = get_purchasable_catalog_plan(plan_id)

    if not plan:
        await rollback_claimed_payment(
            event,
            payment_id,
            "accept",
            error_message=error or "Тариф не найден во время подтверждения платежа",
        )
        await event.answer(f"❌ {error or 'Тариф не найден'}", show_alert=True)
        return

    plan_name = str(payment.get("plan_name") or "").strip()
    if not plan_name:
        plan_name = plan.get("name", plan_id or "custom")

    user_data = await db.get_user(user_id)
    ref_by = user_data.get("ref_by") if user_data else None
    ref_rewarded = user_data.get("ref_rewarded") if user_data else None

    bonus_days_for_user = 0
    if ref_by and not ref_rewarded:
        bonus_days_for_user = Config.REF_BONUS_DAYS

    trust_before_payment = await db.get_trust_score(user_id)
    vpn_url = await create_subscription(
        user_id,
        plan,
        extra_days=bonus_days_for_user,
    )

    if vpn_url:
        trust_after_payment = await db.get_trust_score(user_id)
        trust_delta_payment = trust_after_payment - trust_before_payment
        trust_change_line = build_trust_change_line(
            trust_delta_payment, trust_before_payment, trust_after_payment
        )

        await db.set_has_subscription(user_id)
        finalized = await finalize_claimed_payment_or_alert(
            event, payment_id, "accept", "accepted"
        )
        if not finalized:
            return

        # Дополнительная защита: проверить что платеж действительно в финальном статусе
        is_really_finalized = await verify_payment_final_status(payment_id, "accepted")
        if not is_really_finalized:
            logger.error(
                f"Платеж {payment_id} не в финальном статусе 'accepted' после финализации. "
                f"Это может указывать на race condition. Подписка уже выдана user_id={user_id}"
            )
            await event.answer(
                "⚠️ Платеж обработан, но возникло предупреждение. Проверьте статус платежа.",
                show_alert=True,
            )
            return

        bonus_text = ""
        if bonus_days_for_user > 0:
            bonus_text = f"\nБонус: <b>+{bonus_days_for_user} дней</b>"

        try:
            await notify_user(
                user_id,
                "✅ <b>Ваш платеж подтвержден!</b>\n\n"
                f"Тариф: <b>{plan_name}</b>\n"
                f"IP-адреса: <b>до {plan.get('ip_limit', 0)}</b>\n"
                f"Трафик: <b>{format_traffic(plan.get('traffic_gb', 0))}</b>\n"
                f"Локации: <b>{format_servers(plan.get('servers'))}</b>\n"
                f"Срок: <b>{format_duration(int(plan.get('duration_days', 30)) + bonus_days_for_user)}</b>"
                f"{bonus_text}\n"
                f"{trust_change_line}\n\n"
                f"URL для подключения:\n<code>{vpn_url}</code>\n\n"
                "Спасибо за покупку! 🎉",
            )
        except Exception:
            pass

        # Выдать бонус рефереру только если он не был выдан (atomic check)
        if ref_by and not ref_rewarded:
            # Перепроверить что ref_rewarded еще false перед выдачей бонуса
            fresh_user_data = await db.get_user(user_id)
            if not fresh_user_data.get("ref_rewarded"):
                await reward_referrer(ref_by, Config.REF_BONUS_DAYS)
                marked = await db.mark_ref_rewarded(user_id)
                if not marked:
                    logger.error(
                        f"Не удалось отметить ref_rewarded для user_id={user_id}. "
                        f"Рефереру {ref_by} может быть выдан бонус дважды."
                    )
            else:
                logger.warning(
                    f"ref_rewarded уже установлен для user_id={user_id}, пропущена выдача бонуса рефереру"
                )

        await event.answer(f"✅ Платеж {payment_id} подтвержден!", show_alert=True)
        await append_payment_decision_label(event.message, "✅ <b>ПОДТВЕРЖДЕНО</b>")
    else:
        await rollback_claimed_payment(
            event,
            payment_id,
            "accept",
            error_message="Не удалось создать VPN-подписку",
        )
        await event.answer(
            f"❌ Ошибка создания VPN для платежа {payment_id}", show_alert=True
        )


@router.callback_query(F.data.startswith("pay_await_reject:"))
async def cmd_pay_await_reject(event: CallbackQuery):
    if not await ensure_admin_access(event):
        return

    payment_id = event.data.split(":", 1)[1]
    payment = await claim_pending_payment_or_alert(event, payment_id, "reject")
    if not payment:
        return

    user_id = to_int(payment.get("user_id"), 0)
    finalized = await finalize_claimed_payment_or_alert(
        event, payment_id, "reject", "rejected"
    )
    if not finalized:
        return

    # Дополнительная защита: проверить что платеж действительно в финальном статусе
    is_really_finalized = await verify_payment_final_status(payment_id, "rejected")
    if not is_really_finalized:
        logger.error(
            f"Платеж {payment_id} не в финальном статусе 'rejected' после финализации. "
            f"Это может указывать на race condition."
        )
        await event.answer(
            "⚠️ Платеж обработан, но возникло предупреждение. Проверьте статус платежа.",
            show_alert=True,
        )

    if user_id > 0:
        changed, trust_before_reject, trust_after_reject, trust_delta_reject = (
            await apply_trust_score_delta(
                user_id, -TRUST_SCORE_PENALTY_PAYMENT_REJECTED
            )
        )
        if changed:
            logger.info(
                f"Штраф {trust_delta_reject} очков за отклонение платежа для user_id={user_id} "
                f"(было {trust_before_reject}, стало {trust_after_reject})"
            )
        else:
            logger.error(
                f"Не удалось применить штраф trust score при отклонении платежа для user_id={user_id}"
            )
    else:
        logger.error(
            f"Некорректный user_id в отклоненном платеже payment_id={payment_id}"
        )

    if user_id > 0:
        try:
            await notify_user(
                user_id,
                "❌ <b>Ваш платеж отклонен!</b>\n\n"
                "Пожалуйста, проверьте:\n"
                "1. Правильность суммы платежа\n"
                "2. Актуальность данных карты\n"
                "3. Успешность перевода в банковском приложении\n\n"
                f"{build_trust_change_line(trust_delta_reject, trust_before_reject, trust_after_reject)}\n\n"
                "Если вы уверены, что все сделали правильно, свяжитесь с поддержкой.",
                reply_markup=support_keyboard(include_main=True),
            )
        except Exception:
            pass

    await event.answer(f"❌ Платеж {payment_id} отклонен!", show_alert=True)
    await append_payment_decision_label(event.message, "❌ <b>ОТКЛОНЕНО</b>")


# --- Запуск ---
async def main():
    global BOT_USERNAME
    background_tasks: List[asyncio.Task] = []

    try:
        load_tariffs()
    except Exception as e:
        logger.critical(f"Не удалось загрузить тарифы: {e}")
        sys.exit(1)

    if not is_valid_bot_token_format(Config.BOT_TOKEN):
        logger.critical("BOT_TOKEN не настроен! Установите его в .env")
        sys.exit(1)
    if not Config.PANEL_BASE:
        logger.critical("PANEL_BASE не настроен! Установите адрес 3X-UI в .env")
        sys.exit(1)
    if not Config.PANEL_TOKEN and not (Config.PANEL_LOGIN and Config.PANEL_PASSWORD):
        logger.critical(
            "Не настроена авторизация 3X-UI: задайте PANEL_TOKEN или PANEL_LOGIN/PANEL_PASSWORD"
        )
        sys.exit(1)

    polling_task = None
    try:
        await db.connect()
        await panel.start()

        me = await bot.get_me()
        BOT_USERNAME = me.username or ""

        for admin_id in Config.ADMIN_USER_IDS:
            await safe_send_message(bot, admin_id, "🟢 <b>Бот успешно запущен!</b>")

        background_tasks.append(asyncio.create_task(check_expired_subscriptions()))
        background_tasks.append(asyncio.create_task(cleanup_old_payments()))
        background_tasks.append(asyncio.create_task(SSLUpdateTask.run()))

        polling_task = asyncio.create_task(dp.start_polling(bot))
        await polling_task
    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info("Остановка бота по запросу пользователя")
    finally:
        # Сначала остановить polling
        if polling_task and not polling_task.done():
            polling_task.cancel()
            try:
                await asyncio.wait_for(
                    asyncio.gather(polling_task, return_exceptions=True), timeout=2.0
                )
            except asyncio.TimeoutError:
                logger.warning("Polling task не остановилась в течение 2 секунд")

        # Отменить только фоновые задачи
        for task in background_tasks:
            if not task.done():
                task.cancel()

        # Дать время на выполнение отмены
        if background_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*background_tasks, return_exceptions=True),
                    timeout=3.0,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "Некоторые фоновые задачи не завершились в течение 3 секунд"
                )

        # Выполнить cleanup
        try:
            for admin_id in Config.ADMIN_USER_IDS:
                await safe_send_message(bot, admin_id, "🔴 <b>Бот остановлен!</b>")
        except Exception as e:
            logger.error(f"Ошибка при отправке уведомления админам: {e}")

        # Освободить зависшие платежи
        try:
            await cleanup_stale_payments()
        except Exception as e:
            logger.error(f"Ошибка при очистке зависших платежей: {e}")

        try:
            await panel.close()
        except Exception as e:
            logger.error(f"Ошибка при закрытии panel: {e}")

        try:
            await db.close()
        except Exception as e:
            logger.error(f"Ошибка при закрытии db: {e}")

        try:
            if bot.session:
                await bot.session.close()
        except Exception as e:
            logger.error(f"Ошибка при закрытии bot session: {e}")


if __name__ == "__main__":
    asyncio.run(main())
