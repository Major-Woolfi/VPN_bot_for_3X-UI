import asyncio
import html
import json
import logging
import os
import random
import secrets
import string
import sys
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import aiofiles
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


class Config:
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
    ADMIN_USER_IDS: List[int] = [
        int(x.strip())
        for x in os.getenv("ADMIN_USER_IDS", "").split(",")
        if x.strip()
    ]
    PAYMENT_CARD_NUMBER: str = os.getenv("PAYMENT_CARD_NUMBER", "")
    PANEL_BASE: str = os.getenv("PANEL_BASE", "").rstrip("/")
    SUB_PANEL_BASE: str = os.getenv("SUB_PANEL_BASE", "")
    PANEL_LOGIN: str = os.getenv("PANEL_LOGIN", "")
    PANEL_PASSWORD: str = os.getenv("PANEL_PASSWORD", "")
    VERIFY_SSL: bool = str_to_bool(os.getenv("VERIFY_SSL", "true"))
    DATA_DIR: str = os.getenv("DATA_DIR", "/data")
    DATA_FILE: str = os.getenv("DATA_FILE", os.path.join(DATA_DIR, "users.db"))
    DATA_AWAIT: str = os.getenv(
        "DATA_AWAIT", os.path.join(DATA_DIR, "await_payments.json")
    )
    SITE_URL: str = os.getenv("SITE_URL", "")
    TG_CHANNEL: str = os.getenv("TG_CHANNEL", "")
    SUPPORT_URL: str = os.getenv("SUPPORT_URL", "")
    REF_BONUS_DAYS: int = int(os.getenv("REF_BONUS_DAYS", "7"))


try:
    os.makedirs(Config.DATA_DIR, exist_ok=True)
except Exception as e:
    logger.warning(f"Не удалось создать папку DATA_DIR={Config.DATA_DIR}: {e}")


# --- Тарифы ---
TARIFFS_PATH = os.path.join(BASE_DIR, "data", "tarifs.json")
TARIFFS_ALL: List[Dict[str, Any]] = []
TARIFFS_ACTIVE: List[Dict[str, Any]] = []
TARIFFS_BY_ID: Dict[str, Dict[str, Any]] = {}


def load_tariffs() -> None:
    global TARIFFS_ALL, TARIFFS_ACTIVE, TARIFFS_BY_ID

    if not os.path.exists(TARIFFS_PATH):
        raise FileNotFoundError(f"Файл тарифов не найден: {TARIFFS_PATH}")

    with open(TARIFFS_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    plans = data.get("plans") or []
    if not isinstance(plans, list):
        raise ValueError("tarifs.json должен содержать список plans")

    for plan in plans:
        if "active" not in plan:
            plan["active"] = True

    TARIFFS_ALL = plans
    TARIFFS_ACTIVE = [p for p in plans if p.get("active", True)]
    TARIFFS_ACTIVE.sort(
        key=lambda p: (p.get("sort", 9999), p.get("price_rub", 0))
    )
    TARIFFS_BY_ID = {p.get("id"): p for p in plans if p.get("id")}


def get_all_active() -> List[Dict[str, Any]]:
    return list(TARIFFS_ACTIVE)


def get_by_id(plan_id: str) -> Optional[Dict[str, Any]]:
    return TARIFFS_BY_ID.get(plan_id)


def is_trial_plan(plan: Optional[Dict[str, Any]]) -> bool:
    if not plan:
        return False
    return plan.get("id") == "trial" or plan.get("price_rub", 0) == 0


def get_minimal_by_price() -> Optional[Dict[str, Any]]:
    if not TARIFFS_ACTIVE:
        return None
    eligible = [p for p in TARIFFS_ACTIVE if not is_trial_plan(p)]
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
            await self.conn.execute(
                """
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
                    ip_limit INTEGER DEFAULT 0,
                    traffic_gb INTEGER DEFAULT 0,
                    vpn_url TEXT DEFAULT ''
                )
                """
            )
            await self.conn.commit()

            await self.conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_ref_code ON users(ref_code)"
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

    async def ban_user(self, user_id: int, reason: str = "") -> bool:
        return await self.update_user(user_id, banned=True, ban_reason=reason)

    async def unban_user(self, user_id: int) -> bool:
        return await self.update_user(user_id, banned=False, ban_reason="")

    async def set_subscription(
        self, user_id: int, plan_text: str, ip_limit: int, vpn_url: str, traffic_gb: int
    ) -> bool:
        return await self.update_user(
            user_id=user_id,
            plan_text=plan_text,
            ip_limit=ip_limit,
            vpn_url=vpn_url,
            traffic_gb=traffic_gb,
        )

    async def remove_subscription(self, user_id: int) -> bool:
        return await self.update_user(
            user_id=user_id, plan_text="", ip_limit=0, vpn_url="", traffic_gb=0
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

    async def read_all(self) -> List[Dict[str, Any]]:
        await self._ensure_file()
        async with self._lock:
            async with aiofiles.open(self.path, "r", encoding="utf-8") as f:
                content = await f.read()
        if not content:
            return []
        try:
            data = json.loads(content)
            return data if isinstance(data, list) else []
        except Exception:
            return []

    async def write_all(self, data: List[Dict[str, Any]]) -> None:
        await self._ensure_file()
        async with self._lock:
            async with aiofiles.open(self.path, "w", encoding="utf-8") as f:
                await f.write(json.dumps(data, ensure_ascii=False, indent=2))

    async def add(self, item: Dict[str, Any]) -> None:
        data = await self.read_all()
        data.append(item)
        await self.write_all(data)

    async def remove(self, predicate) -> None:
        data = await self.read_all()
        new_data = [x for x in data if not predicate(x)]
        await self.write_all(new_data)

    async def find_by_id(self, payment_id: str) -> Optional[Dict[str, Any]]:
        data = await self.read_all()
        for item in data:
            if item.get("payment_id") == payment_id:
                return item
        return None

    async def remove_by_id(self, payment_id: str) -> None:
        await self.remove(lambda x: x.get("payment_id") == payment_id)


# --- 3X-UI Panel API ---
class PanelAPI:
    def __init__(self) -> None:
        self.apibase = Config.PANEL_BASE.rstrip("/")
        self.username = Config.PANEL_LOGIN
        self.password = Config.PANEL_PASSWORD
        self.verifyssl = Config.VERIFY_SSL
        self.session: Optional[aiohttp.ClientSession] = None
        self.token: Optional[str] = None
        self.logged_in: bool = False
        self.lock = asyncio.Lock()

    async def start(self) -> None:
        connector = aiohttp.TCPConnector(ssl=self.verifyssl)
        timeout = aiohttp.ClientTimeout(total=15)
        cookie_jar = aiohttp.CookieJar(unsafe=True)
        self.session = aiohttp.ClientSession(
            connector=connector, timeout=timeout, cookie_jar=cookie_jar
        )
        await self.login()

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
        if status in (401, 403, 404):
            return True
        if status == 200 and isinstance(data, dict) and data.get("success") is False:
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
            try:
                url = f"{self.apibase}/login"
                status, data, _ = await self._request_json(
                    "POST",
                    url,
                    json={"username": self.username, "password": self.password},
                )
                if status == 200 and data.get("success"):
                    token = data.get("token")
                    if not token and isinstance(data.get("data"), dict):
                        token = data["data"].get("token")
                    if not token and isinstance(data.get("obj"), dict):
                        token = data["obj"].get("token")

                    self.token = token
                    self.logged_in = True
                    if not self.token:
                        logger.warning(
                            "Аутентификация успешна, но token не найден в ответе"
                        )
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
        return {"Authorization": f"Bearer {self.token}"} if self.token else {}

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

    def _parse_inbound_clients(self, inbound: Dict[str, Any]) -> List[Dict[str, Any]]:
        clients: List[Dict[str, Any]] = []
        settings = inbound.get("settings")

        if isinstance(settings, str):
            try:
                settings_obj = json.loads(settings)
                s_clients = settings_obj.get("clients") or []
                if isinstance(s_clients, list):
                    clients.extend(s_clients)
            except Exception:
                pass
        elif isinstance(settings, dict):
            s_clients = settings.get("clients") or []
            if isinstance(s_clients, list):
                clients.extend(s_clients)

        protocol = inbound.get("protocol", "")
        for client in clients:
            client["protocol"] = protocol

        return clients

    @staticmethod
    def _is_base_email(email: str, base_email: str) -> bool:
        if not email or not base_email:
            return False
        return email.endswith(base_email)

    async def find_clients_by_base_email(self, base_email: str) -> List[Dict[str, Any]]:
        inbounds = await self.get_inbounds()
        if not inbounds or not inbounds.get("success"):
            return []

        result = []
        for inbound in inbounds.get("obj", []):
            inbound_id = inbound.get("id")
            for stat in inbound.get("clientStats", []) or []:
                email = stat.get("email", "")
                if self._is_base_email(email, base_email):
                    stat["inboundId"] = inbound_id
                    result.append(stat)
        return result

    async def find_clients_full_by_email(self, base_email: str) -> List[Dict[str, Any]]:
        inbounds = await self.get_inbounds()
        if not inbounds or not inbounds.get("success"):
            return []

        result: List[Dict[str, Any]] = []
        for inbound in inbounds.get("obj", []):
            inbound_id = inbound.get("id")
            protocol = inbound.get("protocol", "").lower()
            client_stats = inbound.get("clientStats", []) or []
            clients = self._parse_inbound_clients(inbound)

            for stat in client_stats:
                email = stat.get("email", "") or ""
                if not self._is_base_email(email, base_email):
                    continue

                client_id = None
                password = None
                sub_id = None
                client_obj = None

                for c in clients:
                    c_email = c.get("email", "") or ""
                    if c_email == email:
                        client_id = c.get("id") or c.get("clientId")
                        password = c.get("password")
                        sub_id = c.get("subId")
                        client_obj = c
                        break

                item = dict(stat)
                item["inboundId"] = inbound_id
                item["clientId"] = client_id
                item["password"] = password
                item["subId"] = sub_id
                item["protocol"] = protocol
                item["clientObj"] = client_obj
                result.append(item)

        logger.info(f"Найдено {len(result)} клиентов по base_email='{base_email}'")
        return result

    async def create_client(
        self,
        email: str,
        limit_ip: int,
        total_gb: int,
        days: int = 30,
    ) -> Optional[Dict[str, Any]]:
        await self.ensure_auth()
        inbounds = await self.get_inbounds()
        if not inbounds or not inbounds.get("success"):
            logger.error("Не удалось получить inbounds для создания клиента")
            return None

        enabled_inbounds = [
            i for i in inbounds.get("obj", []) if i.get("enable", False)
        ]

        if not enabled_inbounds:
            logger.error("Нет включённых inbound для создания клиента")
            return None

        expiry_ms = int((time.time() + days * 86400) * 1000)
        total_bytes = int(total_gb * 1073741824)
        sub_id = f"user{random.randint(100000, 999999)}"
        created_inbounds = []
        last_client = None

        for inbound in enabled_inbounds:
            inbound_id = inbound.get("id")
            protocol = inbound.get("protocol", "").lower()
            prefix = "".join(
                secrets.choice(string.ascii_letters + string.digits) for _ in range(2)
            )

            if protocol == "trojan":
                client = {
                    "password": secrets.token_urlsafe(12),
                    "email": f"{prefix}{email}",
                    "enable": True,
                    "flow": "",
                    "limitIp": limit_ip,
                    "totalGB": total_bytes,
                    "expiryTime": expiry_ms,
                    "subId": sub_id,
                }
            else:
                client = {
                    "id": str(uuid.uuid4()),
                    "email": f"{prefix}{email}",
                    "enable": True,
                    "flow": "",
                    "limitIp": limit_ip,
                    "totalGB": total_bytes,
                    "expiryTime": expiry_ms,
                    "subId": sub_id,
                }

            payload = {
                "id": inbound_id,
                "settings": json.dumps({"clients": [client]}, ensure_ascii=False),
            }

            url = f"{self.apibase}/panel/api/inbounds/addClient"
            status, data, text = await self._request_json_with_reauth(
                "POST", url, headers=self._headers(), json=payload
            )

            if status in (200, 201) and data.get("success"):
                logger.info(
                    f"Клиент {email} успешно создан в inbound {inbound_id} ({protocol})"
                )
                created_inbounds.append(inbound_id)
                client["protocol"] = protocol
                last_client = client
            else:
                logger.error(
                    f"Ошибка addClient inbound {inbound_id}: status={status} msg={data.get('msg')}"
                )
                if text:
                    logger.error(text)

        if created_inbounds and last_client:
            return last_client
        return None

    async def delete_client(self, base_email: str) -> bool:
        await self.ensure_auth()
        clients = await self.find_clients_full_by_email(base_email)

        if not clients:
            logger.info(
                f"Клиенты с частью email '{base_email}' не найдены, ничего не удаляем"
            )
            return True

        success_count = 0

        for c in clients:
            inbound_id = c.get("inboundId")
            client_id = c.get("clientId")
            password = c.get("password")
            protocol = c.get("protocol", "").lower()
            email = c.get("email", "")

            if not inbound_id:
                logger.error(f"Пропускаем клиента email={email}: нет inboundId")
                continue

            if protocol == "trojan":
                delete_id = password
            else:
                delete_id = client_id

            if not delete_id:
                logger.error(f"Пропускаем клиента email={email}: нет delete_id")
                continue

            delete_url = (
                f"{self.apibase}/panel/api/inbounds/{inbound_id}/delClient/{delete_id}"
            )
            status, data, text = await self._request_json_with_reauth(
                "POST", delete_url, headers=self._headers()
            )

            if status == 200 and data.get("success"):
                logger.info(
                    f"Клиент email={email} (inboundId={inbound_id}, protocol={protocol}) успешно удалён"
                )
                success_count += 1
            else:
                logger.error(
                    f"Ошибка удаления клиента email={email} inbound={inbound_id}: status={status} msg={data.get('msg')}"
                )
                if text:
                    logger.error(text)

        return success_count > 0

    async def extend_client_expiry(self, base_email: str, add_days: int) -> bool:
        await self.ensure_auth()
        clients = await self.find_clients_full_by_email(base_email)
        if not clients:
            return False

        success = False
        update_url = f"{self.apibase}/panel/api/inbounds/updateClient"
        for c in clients:
            inbound_id = c.get("inboundId")
            client_obj = c.get("clientObj")
            if not inbound_id or not isinstance(client_obj, dict):
                continue

            current_expiry = c.get("expiryTime", 0) or 0
            if current_expiry and current_expiry > 0:
                new_expiry = int(current_expiry + add_days * 86400 * 1000)
            else:
                new_expiry = int((time.time() + add_days * 86400) * 1000)

            client_obj["expiryTime"] = new_expiry

            payload = {
                "id": inbound_id,
                "settings": json.dumps({"clients": [client_obj]}, ensure_ascii=False),
            }

            status, data, text = await self._request_json_with_reauth(
                "POST", update_url, headers=self._headers(), json=payload
            )

            if status in (200, 201) and data.get("success"):
                success = True
            else:
                logger.error(
                    f"Ошибка updateClient inbound {inbound_id}: status={status} msg={data.get('msg')}"
                )
                if text:
                    logger.error(text)

        return success

    async def get_client_stats(self, base_email: str) -> List[Dict[str, Any]]:
        return await self.find_clients_by_base_email(base_email)


# --- FSM States ---
class BanUserState(StatesGroup):
    waiting_for_user_id = State()
    waiting_for_ban_reason = State()


class UnbanUserState(StatesGroup):
    waiting_for_user_id = State()
    waiting_for_unban_reason = State()


# --- Утилиты ---
BOT_USERNAME = ""


def kb(rows: List[List[Dict[str, str]]]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(**button) for button in row] for row in rows
        ]
    )


def support_keyboard(include_main: bool = True) -> InlineKeyboardMarkup:
    rows = [[{"text": "Поддержка", "url": f"{Config.SUPPORT_URL}"}]]
    if include_main:
        rows.append([{"text": "Главная", "callback_data": "start"}])
    return kb(rows)


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


async def notify_admins(message: str, reply_markup: Optional[InlineKeyboardMarkup] = None):
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


def build_tariffs_text(plans: Optional[List[Dict[str, Any]]] = None) -> str:
    plans = plans if plans is not None else get_all_active()
    if not plans:
        return "🔒 <b>Тарифы VPN</b>\n\nТарифы временно недоступны."

    text = "🔒 <b>Тарифы VPN</b>\n\n"
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
            f"{idx}. <b>{plan.get('name', plan.get('id'))}</b> - {price_line}\n"
            f"- до {plan.get('ip_limit', 0)} устройств\n"
            f"- до {format_traffic(plan.get('traffic_gb', 0))} трафика\n"
        )
        if plan.get("description"):
            text += f"- {plan.get('description')}\n"
        text += "\n"

    text += (
        "В будущем количество серверов и стран будет увеличено, а также со временем появится WhiteList!"
    )
    return text


def build_buy_text(plans: Optional[List[Dict[str, Any]]] = None) -> str:
    plans = plans if plans is not None else get_all_active()
    if not plans:
        return "💳 <b>Купить подписку VPN</b>\n\nТарифы временно недоступны."

    text = "💳 <b>Купить подписку VPN</b>\n\nВыберите тариф для покупки:\n"
    for idx, plan in enumerate(plans, 1):
        price = plan.get("price_rub", 0)
        duration = int(plan.get("duration_days", 30))
        if price == 0:
            price_line = f"Бесплатно на {duration} дня"
        elif duration == 30:
            price_line = f"{price} ₽/мес"
        else:
            price_line = f"{price} ₽ / {duration} дней"
        text += f"{idx}. <b>{plan.get('name', plan.get('id'))}</b> - {price_line}\n"

    text += "\nПосле выбора тарифа вы будете перенаправлены на оплату."
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


async def create_subscription(
    user_id: int,
    plan: Dict[str, Any],
    *,
    extra_days: int = 0,
    days_override: Optional[int] = None,
    plan_suffix: Optional[str] = None,
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

    base_email = f"user_{user_id}@vpn.example"
    await panel.delete_client(base_email)

    client = await panel.create_client(
        email=base_email,
        limit_ip=int(plan.get("ip_limit", 0)),
        total_gb=int(plan.get("traffic_gb", 0)),
        days=days,
    )

    if not client:
        return None

    vpn_url = f"{Config.SUB_PANEL_BASE}{client.get('subId', 'user_' + str(user_id))}"
    plan_name = plan.get("name", plan.get("id", ""))
    if plan_suffix:
        plan_name = f"{plan_name}{plan_suffix}"

    await db.set_subscription(
        user_id=user_id,
        plan_text=plan_name,
        ip_limit=int(plan.get("ip_limit", 0)),
        traffic_gb=int(plan.get("traffic_gb", 0)),
        vpn_url=vpn_url,
    )

    if pending_days > 0:
        await db.clear_bonus_days_pending(user_id)

    return vpn_url


async def is_active_subscription(user_id: int) -> bool:
    user = await db.get_user(user_id)
    if not user or not user.get("vpn_url"):
        return False

    base_email = f"user_{user_id}@vpn.example"
    clients = await panel.find_clients_by_base_email(base_email)
    if not clients:
        await db.remove_subscription(user_id)
        return False

    expiry_times = [c.get("expiryTime", 0) for c in clients]
    max_expiry = max(expiry_times) if expiry_times else 0
    if max_expiry and max_expiry < int(time.time() * 1000):
        await db.remove_subscription(user_id)
        return False

    return True


async def reward_referrer(referrer_id: int, bonus_days: int) -> None:
    ref_user = await db.get_user(referrer_id)
    if not ref_user:
        return

    pending = await db.get_bonus_days_pending(referrer_id)
    total_bonus = bonus_days + pending

    base_email = f"user_{referrer_id}@vpn.example"
    has_active = bool(ref_user.get("vpn_url"))

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
        days_override=total_bonus,
        plan_suffix=" (реферальный бонус)",
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
async def check_expired_subscriptions():
    while True:
        try:
            subscribed_users = await db.get_subscribed_user_ids()
            now_ms = int(time.time() * 1000)

            for user_id in subscribed_users:
                user_data = await db.get_user(user_id)
                if not user_data:
                    continue

                base_email = f"user_{user_id}@vpn.example"
                clients = await panel.find_clients_by_base_email(base_email)
                if not clients:
                    continue

                expiry_times = [c.get("expiryTime", 0) for c in clients]
                positive_expiry = [
                    x for x in expiry_times if isinstance(x, int) and x > 0
                ]
                min_expiry = min(positive_expiry) if positive_expiry else 0

                if min_expiry and min_expiry < now_ms:
                    await panel.delete_client(base_email)
                    await db.remove_subscription(user_id)
                    await notify_user(user_id, "⏰ Ваша подписка истекла!")

            await asyncio.sleep(3600)
        except Exception as e:
            logger.error(f"Ошибка проверки подписок: {e}")
            await asyncio.sleep(60)


async def cleanup_old_payments():
    while True:
        try:
            payments = await json_db.read_all()
            cutoff_time = datetime.now() - timedelta(days=30)
            new_payments = []

            for payment in payments:
                if payment.get("status") in ("accepted", "rejected"):
                    processed_at = payment.get("processed_at")
                    if processed_at:
                        try:
                            dt = datetime.fromisoformat(processed_at)
                            if dt >= cutoff_time:
                                new_payments.append(payment)
                        except Exception:
                            new_payments.append(payment)
                    else:
                        new_payments.append(payment)
                else:
                    new_payments.append(payment)

            if len(new_payments) != len(payments):
                await json_db.write_all(new_payments)

            await asyncio.sleep(259200)
        except Exception as e:
            logger.error(f"Ошибка очистки платежей: {e}")
            await asyncio.sleep(3600)


# --- Инициализация ---
bot = Bot(
    token=Config.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML)
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
                "Если вы считаете, что это ошибка, пожалуйста, свяжитесь с поддержкой.",
                reply_markup=support_kb,
            )
        elif isinstance(event, CallbackQuery):
            if event.message:
                await event.message.answer(
                    "⛔ <b>Ваш аккаунт заблокирован!</b>\n\n"
                    f"Причина: {ban_reason}\n\n"
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

    if user_id in Config.ADMIN_USER_IDS:
        text = (
            "👑 <b>Добро пожаловать, администратор!</b>\n\n"
            f"Всего пользователей: <b>{total_users}</b>\n"
            f"Активных VPN: <b>{active_vpns}</b>\n"
            f"Заблокированных пользователей: <b>{banned_users}</b>"
        )
        keyboard = [
            [{"text": "Тарифы", "callback_data": "subs"}],
            [{"text": "Купить подписку", "callback_data": "buy"}],
            [{"text": "Моя подписка", "callback_data": "mysub"}],
            [{"text": "Реферальная система", "callback_data": "ref"}],
            [{"text": "Заблокировать пользователя", "callback_data": "ban"}],
            [{"text": "Разблокировать пользователя", "callback_data": "unban"}],
            [{"text": "Ожидающие платежи", "callback_data": "pay_await"}],
            [{"text": "Поддержка", "url": f"{Config.SUPPORT_URL}"}],
            [{"text": "Наш сайт", "url": f"{Config.SITE_URL}"}],
            [{"text": "Наш канал", "url": f"{Config.TG_CHANNEL}"}],
        ]
    else:
        text = (
            "👋 <b>Добро пожаловать в VPN бот!</b>\n\n"
            f"Всего пользователей: <b>{total_users}</b>\n"
            f"Активных VPN: <b>{active_vpns}</b>"
        )
        keyboard = [
            [{"text": "Тарифы", "callback_data": "subs"}],
            [{"text": "Купить подписку", "callback_data": "buy"}],
            [{"text": "Моя подписка", "callback_data": "mysub"}],
            [{"text": "Реферальная система", "callback_data": "ref"}],
            [{"text": "Поддержка", "url": f"{Config.SUPPORT_URL}"}],
            [{"text": "Наш сайт", "url": f"{Config.SITE_URL}"}],
            [{"text": "Наш канал", "url": f"{Config.TG_CHANNEL}"}],
        ]

    await smart_answer(
        event, text, reply_markup=kb(keyboard), delete_origin=True
    )


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
    plans = await get_visible_plans(
        user_id, for_admin=user_id in Config.ADMIN_USER_IDS
    )
    text = build_tariffs_text(plans)

    if user_id in Config.ADMIN_USER_IDS:
        keyboard = [
            [{"text": "Подтверждение платежей", "callback_data": "pay_await"}],
            [{"text": "Купить подписку", "callback_data": "buy"}],
            [{"text": "Моя подписка", "callback_data": "mysub"}],
            [{"text": "Главная", "callback_data": "start"}],
        ]
    else:
        keyboard = [
            [{"text": "Купить подписку", "callback_data": "buy"}],
            [{"text": "Моя подписка", "callback_data": "mysub"}],
            [{"text": "Главная", "callback_data": "start"}],
        ]

    await smart_answer(
        event, text, reply_markup=kb(keyboard), delete_origin=True
    )


# --- buy ---
@router.callback_query(F.data == "buy")
async def cmd_buy(event):
    user_id = event.from_user.id
    await db.add_user(user_id)
    plans = await get_visible_plans(
        user_id, for_admin=user_id in Config.ADMIN_USER_IDS
    )
    text = build_buy_text(plans)

    keyboard = [[{"text": "Наши тарифы", "callback_data": "subs"}]]

    if user_id in Config.ADMIN_USER_IDS:
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

    keyboard.append([{"text": "Главная", "callback_data": "start"}])

    await smart_answer(
        event, text, reply_markup=kb(keyboard), delete_origin=True
    )


@router.callback_query(F.data.startswith("buy:"))
async def cmd_buy_plan(event: CallbackQuery):
    user_id = event.from_user.id
    plan_id = event.data.split(":", 1)[1]
    plan = get_by_id(plan_id)

    if not plan or not plan.get("active", True):
        await event.answer("❌ Тариф не найден или недоступен", show_alert=True)
        return
    if is_trial_plan(plan):
        await event.answer("⚠️ Пробный тариф оформляется отдельно.", show_alert=True)
        return

    price = plan.get("price_rub", 0)
    duration = int(plan.get("duration_days", 30))
    if duration == 30:
        price_line = f"{price} ₽/мес"
    else:
        price_line = f"{price} ₽/{duration} дней"

    text = (
        "💳 <b>Покупка тарифа VPN</b>\n\n"
        f"Вы выбрали тариф <b>{plan.get('name', plan_id)}</b> за {price_line}.\n"
        f"Для оплаты перевидите {price} ₽ по номеру карты: <code>{Config.PAYMENT_CARD_NUMBER}</code>.\n"
        f"В комментарии к платежу ОБЯЗАТЕЛЬНО укажите этот текст: <code>Пожертвование от {user_id}</code>.\n\n"
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
        [{"text": "Главная", "callback_data": "start"}],
    ]

    await smart_answer(
        event, text, reply_markup=kb(keyboard), delete_origin=True
    )


@router.callback_query(F.data.startswith("test:"))
async def cmd_test_plan(event: CallbackQuery):
    user_id = event.from_user.id
    if user_id not in Config.ADMIN_USER_IDS:
        await event.answer("⛔ Эта функция доступна только администраторам!", show_alert=True)
        return

    plan_id = event.data.split(":", 1)[1]
    plan = get_by_id(plan_id)
    if not plan:
        await event.answer("❌ Тариф не найден", show_alert=True)
        return

    vpn_url = await create_subscription(
        user_id,
        plan,
        plan_suffix=" (тест)",
    )

    if vpn_url:
        text = (
            "✅ <b>Тестовая подписка успешно создана!</b>\n\n"
            f"Тариф: <b>{plan.get('name', plan_id)} (тест)</b>\n"
            f"IP-адреса: <b>до {plan.get('ip_limit', 0)}</b>\n"
            f"Трафик: <b>{format_traffic(plan.get('traffic_gb', 0))}</b>\n"
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
    if user_id in Config.ADMIN_USER_IDS:
        await event.answer("⛔ Пробный тариф доступен только пользователям!", show_alert=True)
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
    )

    if vpn_url:
        await db.mark_trial_used(user_id)
        text = (
            "✅ <b>Пробная подписка успешно создана!</b>\n\n"
            f"Тариф: <b>{plan.get('name', plan_id)} (пробный)</b>\n"
            f"IP-адреса: <b>до {plan.get('ip_limit', 0)}</b>\n"
            f"Трафик: <b>{format_traffic(plan.get('traffic_gb', 0))}</b>\n"
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

    plan = get_by_id(plan_id)
    if not plan:
        await event.answer("❌ Тариф не найден", show_alert=True)
        return

    if await is_active_subscription(user_id):
        text = (
            "⚠️ У вас уже есть активная подписка.\n\n"
            "Вы не можете отправить подтверждение оплаты для новой подписки."
        )
        keyboard = [
            [{"text": "Моя подписка", "callback_data": "mysub"}],
            [{"text": "Главная", "callback_data": "start"}],
        ]
        await smart_answer(event, text, reply_markup=kb(keyboard), delete_origin=True)
        return

    payment_id = f"pay_{user_id}_{int(time.time())}"
    payment_data = {
        "payment_id": payment_id,
        "user_id": user_id,
        "plan_id": plan_id,
        "amount": plan.get("price_rub", 0),
        "timestamp": datetime.now().isoformat(),
        "status": "pending",
    }

    await json_db.add(payment_data)

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

    if user_id in Config.ADMIN_USER_IDS:
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
        await smart_answer(
            event, text, reply_markup=kb(keyboard), delete_origin=True
        )
        return

    user_data = await db.get_user(user_id)
    subs_ids = await db.get_subscribed_user_ids()

    if not user_data or user_id not in subs_ids:
        text = "👤 <b>Ваша подписка VPN</b>\n\nУ вас нет активной подписки."
        keyboard = [
            [{"text": "Купить подписку", "callback_data": "subs"}],
            [{"text": "Главная", "callback_data": "start"}],
        ]
        await smart_answer(
            event, text, reply_markup=kb(keyboard), delete_origin=True
        )
        return

    base_email = f"user_{user_id}@vpn.example"
    client_stats = await panel.get_client_stats(base_email)
    plan_text = user_data.get("plan_text", "Неизвестно")
    ip_limit = user_data.get("ip_limit", 0)
    vpn_url = user_data.get("vpn_url", "")
    traffic_gb = user_data.get("traffic_gb", 0)

    if client_stats:
        used_bytes = 0
        expiry_time = 0

        for client in client_stats:
            used_bytes += client.get("up", 0) + client.get("down", 0)
            client_expiry = client.get("expiryTime", 0)
            if client_expiry > expiry_time:
                expiry_time = client_expiry

        used_gb = used_bytes / 1073741824
        remaining_gb = max(0, traffic_gb - used_gb)

        if expiry_time > 0:
            expiry_date = datetime.fromtimestamp(expiry_time / 1000).strftime(
                "%d.%m.%Y %H:%M"
            )
        else:
            expiry_date = "не указана"

        text = (
            "👤 <b>Ваша подписка VPN</b>\n\n"
            f"Тариф: <b>{plan_text}</b>\n"
            f"Остаток трафика: <b>{remaining_gb:.1f} ГБ из {traffic_gb:.0f} ГБ</b>\n"
            f"IP-адреса: <b>до {ip_limit}</b>\n"
            f"Срок действия: <b>до {expiry_date}</b>\n\n"
            f"URL для подключения:\n"
            f"<code>{vpn_url}</code>"
        )
    else:
        text = (
            "👤 <b>Ваша подписка VPN</b>\n\n"
            f"Тариф: <b>{plan_text}</b>\n"
            f"IP-адреса: <b>до {ip_limit}</b>\n"
            f"Трафик: <b>{format_traffic(traffic_gb)}</b>\n"
            f"URL для подключения:\n"
            f"<code>{vpn_url}</code>\n\n"
            "<i>Статистика трафика временно недоступна</i>"
        )

    keyboard = [
        [{"text": "Рефералка", "callback_data": "ref"}],
        [{"text": "Главная", "callback_data": "start"}],
    ]
    await smart_answer(
        event, text, reply_markup=kb(keyboard), delete_origin=True
    )


# --- ref ---
@router.callback_query(F.data == "ref")
async def cmd_ref(event):
    user_id = event.from_user.id
    await db.add_user(user_id)
    ref_code = await db.ensure_ref_code(user_id)
    if not ref_code:
        text = "❌ Не удалось сгенерировать реферальный код."
        keyboard = [[{"text": "Главная", "callback_data": "start"}]]
        await smart_answer(
            event, text, reply_markup=kb(keyboard), delete_origin=True
        )
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
    user_id = event.from_user.id
    if user_id in Config.ADMIN_USER_IDS:
        text = "⛔ <b>Введите ID пользователя для блокировки</b>"
        keyboard = [
            [{"text": "Отмена", "callback_data": "cancel"}],
            [{"text": "Главная", "callback_data": "start"}],
        ]
        await smart_answer(
            event, text, reply_markup=kb(keyboard), delete_origin=True
        )
        await state.set_state(BanUserState.waiting_for_user_id)
    else:
        text = "⛔ <b>Эта команда доступна только администраторам!</b>"
        keyboard = [[{"text": "Главная", "callback_data": "start"}]]
        await smart_answer(
            event, text, reply_markup=kb(keyboard), delete_origin=True
        )


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

    if user_id_to_ban in Config.ADMIN_USER_IDS:
        await event.answer("❌ Нельзя заблокировать администратора!")
        return

    await state.update_data(user_id_to_ban=user_id_to_ban)
    text = f"⛔ <b>Введите причину блокировки для пользователя ID {user_id_to_ban}</b>"
    keyboard = [
        [{"text": "Отмена", "callback_data": "cancel"}],
        [{"text": "Главная", "callback_data": "start"}],
    ]
    await smart_answer(
        event, text, reply_markup=kb(keyboard), delete_origin=True
    )
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
        text = (
            f"⛔ <b>Пользователь ID {user_id_to_ban} заблокирован по причине:</b>\n"
            f"{ban_reason}"
        )

        try:
            await notify_user(
                user_id_to_ban,
                "⛔ <b>Ваш аккаунт заблокирован!</b>\n\n"
                f"Причина: {ban_reason}\n\n"
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
    await smart_answer(
        event, text, reply_markup=kb(keyboard), delete_origin=True
    )


# --- unban ---
@router.callback_query(F.data == "unban")
async def cmd_unban(event, state: FSMContext):
    user_id = event.from_user.id
    if user_id in Config.ADMIN_USER_IDS:
        text = "⛔ <b>Введите ID пользователя для разблокировки</b>"
        keyboard = [
            [{"text": "Отмена", "callback_data": "cancel"}],
            [{"text": "Главная", "callback_data": "start"}],
        ]
        await smart_answer(
            event, text, reply_markup=kb(keyboard), delete_origin=True
        )
        await state.set_state(UnbanUserState.waiting_for_user_id)
    else:
        text = "⛔ <b>Эта команда доступна только администраторам!</b>"
        keyboard = [[{"text": "Главная", "callback_data": "start"}]]
        await smart_answer(
            event, text, reply_markup=kb(keyboard), delete_origin=True
        )


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
        [{"text": "Главная", "callback_data": "start"}],
    ]
    await smart_answer(
        event, text, reply_markup=kb(keyboard), delete_origin=True
    )
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
    await smart_answer(
        event, text, reply_markup=kb(keyboard), delete_origin=True
    )


# --- pay_await ---
@router.callback_query(F.data == "pay_await")
async def cmd_pay_await(event):
    user_id = event.from_user.id
    if user_id in Config.ADMIN_USER_IDS:
        payments = await json_db.read_all()
        pending = [p for p in payments if p.get("status") == "pending"]

        if not pending:
            text = "🕒 <b>Нет пользователей, ожидающих подтверждения платежа</b>"
            keyboard = [[{"text": "Главная", "callback_data": "start"}]]
            await smart_answer(
                event,
                text,
                reply_markup=kb(keyboard),
                delete_origin=True,
            )
            return

        text = "🕒 <b>Пользователи, ожидающие подтверждения платежа:</b>"
        await smart_answer(event, text, delete_origin=True)

        for payment in pending:
            payment_id = payment.get("payment_id", "")
            p_user_id = payment.get("user_id", 0)
            plan_id = payment.get("plan_id", "")
            amount = payment.get("amount", 0)
            timestamp = payment.get("timestamp", "")

            plan = get_by_id(plan_id)
            plan_name = plan.get("name", plan_id) if plan else plan_id

            try:
                dt = datetime.fromisoformat(timestamp)
                time_str = dt.strftime("%d.%m.%Y %H:%M")
            except Exception:
                time_str = timestamp

            payment_text = (
                f"📋 <b>Платеж ID:</b> <code>{payment_id}</code>\n"
                f"👤 <b>Пользователь:</b> <code>{p_user_id}</code>\n"
                f"📦 <b>Тариф:</b> {plan_name}\n"
                f"💰 <b>Сумма:</b> {amount} ₽\n"
                f"🕐 <b>Время:</b> {time_str}"
            )

            keyboard = [
                [
                    {
                        "text": "✅ Подтвердить",
                        "callback_data": f"pay_await_accept:{payment_id}",
                    },
                    {
                        "text": "❌ Отклонить",
                        "callback_data": f"pay_await_reject:{payment_id}",
                    },
                ]
            ]
            keyboard.append([{"text": "Главная", "callback_data": "start"}])

            if isinstance(event, Message):
                await event.answer(payment_text, reply_markup=kb(keyboard))
            elif isinstance(event, CallbackQuery) and event.message:
                await event.message.answer(payment_text, reply_markup=kb(keyboard))
    else:
        text = "⛔ <b>Эта команда доступна только администраторам!</b>"
        keyboard = [[{"text": "Главная", "callback_data": "start"}]]
        await smart_answer(
            event, text, reply_markup=kb(keyboard), delete_origin=True
        )


@router.callback_query(F.data.startswith("pay_await_accept:"))
async def cmd_pay_await_accept(event: CallbackQuery):
    payment_id = event.data.split(":", 1)[1]
    payment = await json_db.find_by_id(payment_id)

    if not payment or payment.get("status") != "pending":
        await event.answer("❌ Платеж не найден или уже обработан", show_alert=True)
        return

    user_id = payment.get("user_id")
    plan_id = payment.get("plan_id")
    plan = get_by_id(plan_id)

    if not plan:
        await event.answer("❌ Тариф не найден", show_alert=True)
        return

    user_data = await db.get_user(user_id)
    ref_by = user_data.get("ref_by") if user_data else None
    ref_rewarded = user_data.get("ref_rewarded") if user_data else None

    bonus_days_for_user = 0
    if ref_by and not ref_rewarded:
        bonus_days_for_user = Config.REF_BONUS_DAYS

    vpn_url = await create_subscription(
        user_id,
        plan,
        extra_days=bonus_days_for_user,
    )

    if vpn_url:
        await db.set_has_subscription(user_id)
        payment["status"] = "accepted"
        payment["processed_at"] = datetime.now().isoformat()
        await json_db.remove_by_id(payment_id)
        await json_db.add(payment)

        bonus_text = ""
        if bonus_days_for_user > 0:
            bonus_text = f"\nБонус: <b>+{bonus_days_for_user} дней</b>"

        try:
            await notify_user(
                user_id,
                "✅ <b>Ваш платеж подтвержден!</b>\n\n"
                f"Тариф: <b>{plan.get('name', plan_id)}</b>\n"
                f"IP-адреса: <b>до {plan.get('ip_limit', 0)}</b>\n"
                f"Трафик: <b>{format_traffic(plan.get('traffic_gb', 0))}</b>\n"
                f"Срок: <b>{format_duration(int(plan.get('duration_days', 30)) + bonus_days_for_user)}</b>"
                f"{bonus_text}\n\n"
                f"URL для подключения:\n<code>{vpn_url}</code>\n\n"
                "Спасибо за покупку! 🎉",
            )
        except Exception:
            pass

        if ref_by and not ref_rewarded:
            await reward_referrer(ref_by, Config.REF_BONUS_DAYS)
            await db.mark_ref_rewarded(user_id)

        await event.answer(f"✅ Платеж {payment_id} подтвержден!", show_alert=True)

        if event.message:
            new_text = event.message.text + "\n\n✅ <b>ПОДТВЕРЖДЕНО</b>"
            await event.message.edit_text(new_text, parse_mode="HTML")
    else:
        await event.answer(
            f"❌ Ошибка создания VPN для платежа {payment_id}", show_alert=True
        )


@router.callback_query(F.data.startswith("pay_await_reject:"))
async def cmd_pay_await_reject(event: CallbackQuery):
    payment_id = event.data.split(":", 1)[1]
    payment = await json_db.find_by_id(payment_id)

    if not payment or payment.get("status") != "pending":
        await event.answer("❌ Платеж не найден или уже обработан", show_alert=True)
        return

    user_id = payment.get("user_id")
    payment["status"] = "rejected"
    payment["processed_at"] = datetime.now().isoformat()
    await json_db.remove_by_id(payment_id)
    await json_db.add(payment)

    try:
        await notify_user(
            user_id,
            "❌ <b>Ваш платеж отклонен!</b>\n\n"
            "Пожалуйста, проверьте:\n"
            "1. Правильность суммы платежа\n"
            "2. Наличие комментария к платежу\n"
            "3. Актуальность данных карты\n\n"
            "Если вы уверены, что все сделали правильно, свяжитесь с поддержкой.",
            reply_markup=support_keyboard(include_main=True),
        )
    except Exception:
        pass

    await event.answer(f"❌ Платеж {payment_id} отклонен!", show_alert=True)

    if event.message:
        new_text = event.message.text + "\n\n❌ <b>ОТКЛОНЕНО</b>"
        await event.message.edit_text(new_text, parse_mode="HTML")


# --- Запуск ---
async def main():
    global BOT_USERNAME

    try:
        load_tariffs()
    except Exception as e:
        logger.critical(f"Не удалось загрузить тарифы: {e}")
        sys.exit(1)

    if not Config.BOT_TOKEN or Config.BOT_TOKEN == "YOUR_BOT_TOKEN":
        logger.critical("BOT_TOKEN не настроен! Установите его в .env")
        sys.exit(1)

    try:
        await db.connect()
        await panel.start()

        me = await bot.get_me()
        BOT_USERNAME = me.username or ""

        for admin_id in Config.ADMIN_USER_IDS:
            await safe_send_message(bot, admin_id, "🟢 <b>Бот успешно запущен!</b>")

        asyncio.create_task(check_expired_subscriptions())
        asyncio.create_task(cleanup_old_payments())

        await dp.start_polling(bot)
    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info("Остановка бота по запросу пользователя")
    finally:
        for admin_id in Config.ADMIN_USER_IDS:
            await safe_send_message(bot, admin_id, "🔴 <b>Бот остановлен!</b>")
        await panel.close()
        await db.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
