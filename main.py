import asyncio
import html
import json
import logging
import os
import random
import secrets
import requests
import sys
import sqlite3
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, CallbackQuery
from aiogram.utils.callback_answer import CallbackAnswerMiddleware


# --- Логирование ---
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            f"logs/{datetime.now().strftime('%Y-%m-%d')}.log", encoding="utf-8"
        ),
    ],
)
logger = logging.getLogger(__name__)


# --- Конфиг ---
class Config:
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN")
    ADMIN_USER_IDS: List[int] = [
        int(x.strip()) for x in os.getenv("ADMIN_USER_IDS", "").split(",")
    ]
    PAYMENT_CARD_NUMBER: str = os.getenv("PAYMENT_CARD_NUMBER", "")
    PANEL_BASE: str = os.getenv("PANEL_BASE", "")
    SUB_PANEL_BASE: str = os.getenv("SUB_PANEL_BASE", "")
    PANEL_LOGIN: str = os.getenv("PANEL_LOGIN", "")
    PANEL_PASSWORD: str = os.getenv("PANEL_PASSWORD", "")
    VERIFY_SSL: bool = True
    DATA_DIR: str = os.getenv("DATA_DIR", "data")
    DATA_FILE: str = os.getenv("DATA_FILE", os.path.join(DATA_DIR, "users.db"))
    DATA_AWAIT: str = os.getenv(
        "DATA_AWAIT", os.path.join(DATA_DIR, "await_payments.json")
    )
    SITE_URL: str = os.getenv("SITE_URL", "")
    TG_CHANNEL: str = os.getenv("TG_CHANNEL", "")
    SUPPORT_URL: str = os.getenv("SUPPORT_URL", "")


os.makedirs(Config.DATA_DIR, exist_ok=True)


# --- FSM States ---
class BanUserState(StatesGroup):
    waiting_for_user_id = State()
    waiting_for_ban_reason = State()


class UnbanUserState(StatesGroup):
    waiting_for_user_id = State()
    waiting_for_unban_reason = State()


# --- SQLite ---
class Database:
    def __init__(self, db_path: str = Config.DATA_FILE):
        self.db_path = db_path
        self.init_db()

    def get_connection(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def init_db(self):
        with self.get_connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    join_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    banned BOOLEAN DEFAULT FALSE,
                    ban_reason TEXT DEFAULT '',
                    ref_code TEXT UNIQUE,
                    ref_by INTEGER,
                    plan_text TEXT DEFAULT '',
                    ip_limit INTEGER DEFAULT 0,
                    traffic_gb INTEGER DEFAULT 0,
                    vpn_url TEXT DEFAULT ''
                )
            """
            )
            conn.commit()

    async def add_user(self, user_id: int) -> bool:
        with self.get_connection() as conn:
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,)
                )
                conn.commit()
                return True
            except Exception as e:
                logger.error(f"Ошибка добавления пользователя {user_id}: {e}")
                return False

    async def get_user(self, user_id: int) -> Optional[Dict]:
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    async def update_user(self, user_id: int, **kwargs) -> bool:
        if not kwargs:
            return False

        set_clause = ", ".join([f"{key} = ?" for key in kwargs.keys()])
        values = list(kwargs.values())
        values.append(user_id)

        with self.get_connection() as conn:
            try:
                conn.execute(f"UPDATE users SET {set_clause} WHERE user_id = ?", values)
                conn.commit()
                return True
            except Exception as e:
                logger.error(f"Ошибка обновления пользователя {user_id}: {e}")
                return False

    async def get_total_users(self) -> int:
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM users")
            return cursor.fetchone()[0]

    async def get_banned_users_count(self) -> int:
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM users WHERE banned = TRUE")
            return cursor.fetchone()[0]

    async def get_banned_user_ids(self) -> List[int]:
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT user_id FROM users WHERE banned = TRUE")
            return [row[0] for row in cursor.fetchall()]

    async def get_subscribed_user_ids(self) -> List[int]:
        with self.get_connection() as conn:
            cursor = conn.execute(
                "SELECT user_id FROM users WHERE vpn_url != '' AND vpn_url IS NOT NULL"
            )
            return [row[0] for row in cursor.fetchall()]

    async def ban_user(self, user_id: int, reason: str = "") -> bool:
        with self.get_connection() as conn:
            try:
                conn.execute(
                    "UPDATE users SET banned = TRUE, ban_reason = ? WHERE user_id = ?",
                    (reason, user_id),
                )
                conn.commit()
                return True
            except Exception as e:
                logger.error(f"Ошибка блокировки пользователя {user_id}: {e}")
                return False

    async def unban_user(self, user_id: int) -> bool:
        with self.get_connection() as conn:
            try:
                conn.execute(
                    "UPDATE users SET banned = FALSE, ban_reason = '' WHERE user_id = ?",
                    (user_id,),
                )
                conn.commit()
                return True
            except Exception as e:
                logger.error(f"Ошибка разблокировки пользователя {user_id}: {e}")
                return False

    async def set_subscription(self, user_id, plan_text, ip_limit, vpn_url, traffic_gb):
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


# --- .json ---
class JSONDatabase:
    def __init__(self, path: str):
        self.path = path
        self._lock = asyncio.Lock()
        if not os.path.exists(self.path):
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump([], f)

    async def read_all(self):
        async with self._lock:
            with open(self.path, "r", encoding="utf-8") as f:
                try:
                    return json.load(f)
                except Exception:
                    return []

    async def write_all(self, data):
        async with self._lock:
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

    async def add(self, item):
        data = await self.read_all()
        data.append(item)
        await self.write_all(data)

    async def remove(self, predicate):
        data = await self.read_all()
        new = [x for x in data if not predicate(x)]
        await self.write_all(new)

    async def find_by_id(self, payment_id: str):
        data = await self.read_all()
        for item in data:
            if item.get("payment_id") == payment_id:
                return item
        return None

    async def remove_by_id(self, payment_id: str):
        await self.remove(lambda x: x.get("payment_id") == payment_id)


# --- 3X-UI Panel API ---
class PanelAPI:
    def __init__(self):
        self.apibase = Config.PANEL_BASE.rstrip("/")
        self.username = Config.PANEL_LOGIN
        self.password = Config.PANEL_PASSWORD
        self.verifyssl = Config.VERIFY_SSL
        self.session = requests.Session()
        self.session.verify = Config.VERIFY_SSL
        self.token: Optional[str] = None
        self.login()

    def login(self) -> None:
        try:
            url = f"{self.apibase}/login"
            resp = self.session.post(
                url,
                json={"username": self.username, "password": self.password},
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json()
                if data.get("success"):
                    self.token = data.get("token")
                    logger.info("Успешная аутентификация в панели 3X-UI")
                else:
                    logger.error(f"Ошибка аутентификации 3X-UI: {data.get('msg')}")
            else:
                logger.error(f"HTTP ошибка аутентификации 3X-UI: {resp.status_code}")
        except Exception as e:
            logger.error(f"Ошибка при аутентификации 3X-UI: {e}")

    def _headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"} if self.token else {}

    def ensureauth(self) -> None:
        if not self.token:
            self.login()

    def getinbounds(self) -> Optional[Dict[str, Any]]:
        self.ensureauth()
        try:
            url = f"{self.apibase}/xui/api/inbounds/list"
            resp = self.session.get(url, headers=self._headers(), timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("success"):
                    obj = data.get("obj") or []
                    logger.info(f"Получено {len(obj)} inbounds")
                    return data
                else:
                    logger.error(f"Ошибка API getInbounds: {data.get('msg')}")
                    return None
            else:
                logger.error(f"HTTP ошибка getInbounds: {resp.status_code}")
                return None
        except Exception as e:
            logger.error(f"Ошибка getInbounds: {e}")
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

    def find_clients_by_base_email(self, base_email: str) -> List[Dict[str, Any]]:
        inbounds = self.getinbounds()
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

    def create_client(
        self,
        email: str,
        limit_ip: int,
        total_gb: int,
        days: int = 30,
    ) -> Optional[Dict[str, Any]]:
        self.ensureauth()
        inbounds = self.getinbounds()
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
        total_bytes = total_gb * 1073741824
        sub_id = f"user{random.randint(100000, 999999)}"
        created_inbounds = []

        for inbound in enabled_inbounds:
            inbound_id = inbound.get("id")
            protocol = inbound.get("protocol", "").lower()

            if protocol == "trojan":
                client = {
                    "password": secrets.token_urlsafe(12),
                    "email": secrets.token_urlsafe(1) + email,
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
                    "email": secrets.token_urlsafe(1) + email,
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

            try:
                url = f"{self.apibase}/panel/api/inbounds/addClient"
                logger.info(
                    f"Вызов addClient для inbound {inbound_id} ({protocol}): {url}"
                )
                resp = self.session.post(
                    url,
                    headers=self._headers(),
                    json=payload,
                    timeout=10,
                )
                logger.info(f"addClient HTTP status: {resp.status_code}")
                if resp.status_code in (200, 201):
                    data = resp.json()
                    if data.get("success"):
                        logger.info(
                            f"Клиент {email} успешно создан в inbound {inbound_id} ({protocol})"
                        )
                        created_inbounds.append(inbound_id)
                        client["protocol"] = protocol
                    else:
                        logger.error(
                            f"Ошибка API addClient для inbound {inbound_id}: {data.get('msg')}"
                        )
                        if resp.text:
                            logger.error(resp.text)
                else:
                    logger.error(
                        f"HTTP ошибка при создании клиента в inbound {inbound_id}: {resp.status_code}"
                    )
                    if resp.text:
                        logger.error(resp.text)
            except Exception as e:
                logger.error(f"Ошибка при создании клиента в inbound {inbound_id}: {e}")

        if created_inbounds:
            return client
        else:
            return None

    def find_clients_full_by_email(self, base_email: str) -> List[Dict[str, Any]]:
        inbounds = self.getinbounds()
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
                if base_email.lower() not in email.lower():
                    continue

                client_id = None
                password = None
                sub_id = None

                for c in clients:
                    c_email = c.get("email", "") or ""
                    if c_email == email:
                        client_id = c.get("id") or c.get("clientId")
                        password = c.get("password")
                        sub_id = c.get("subId")
                        break

                item = dict(stat)
                item["inboundId"] = inbound_id
                item["clientId"] = client_id
                item["password"] = password
                item["subId"] = sub_id
                item["protocol"] = protocol
                result.append(item)

        logger.info(f"Найдено {len(result)} клиентов по base_email='{base_email}'")
        return result

    def delete_client(self, base_email: str) -> bool:
        self.ensureauth()
        clients = self.find_clients_full_by_email(base_email)

        if not clients:
            logger.info(
                f"Клиенты с частью email '{base_email}' не найдены, ничего не удаляем"
            )
            return True

        logger.info(f"Будем удалять {len(clients)} клиентов")
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
                delete_type = "password"
            else:
                delete_id = client_id
                delete_type = "clientId"

            if not delete_id:
                logger.error(f"Пропускаем клиента email={email}: нет {delete_type}")
                continue

            delete_url = (
                f"{self.apibase}/panel/api/inbounds/{inbound_id}/delClient/{delete_id}"
            )

            logger.debug(f"DELETE URL = {delete_url} (protocol={protocol})")
            try:
                resp = self.session.post(
                    delete_url,
                    headers=self._headers(),
                    timeout=10,
                )
            except requests.RequestException as e:
                logger.error(
                    f"Ошибка HTTP при удалении клиента "
                    f"email={email} inbound={inbound_id}: {e}"
                )
                continue

            if resp.status_code == 200:
                try:
                    data = resp.json()
                except ValueError:
                    data = {}
                if data.get("success"):
                    logger.info(
                        f"Клиент email={email} "
                        f"(inboundId={inbound_id}, protocol={protocol}) успешно удалён"
                    )
                    success_count += 1
                else:
                    logger.error(
                        f"API success=False при удалении клиента "
                        f"email={email} inbound={inbound_id}: {data.get('msg')}"
                    )
            else:
                logger.error(
                    f"HTTP {resp.status_code} при удалении клиента "
                    f"email={email} inbound={inbound_id}: {resp.text}"
                )

        return success_count > 0

    def get_client_stats(self, email: str) -> List[Dict[str, Any]]:
        return self.find_clients_by_base_email(email)

    def _is_base_email(self, email: str, base_email: str) -> bool:
        if not email or len(email) < 3:
            return False
        return email[2:] == base_email


# --- Утилиты ---
async def safe_send_message(bot: Bot, user_id: int, message: str):
    try:
        await bot.send_message(user_id, message, parse_mode=ParseMode.HTML)
    except TelegramBadRequest as e:
        logger.warning(
            f"HTML parse error for user {user_id}: {e}. Trying escaped HTML then plain text."
        )
        try:
            await bot.send_message(
                user_id, html.escape(message), parse_mode=ParseMode.HTML
            )
        except Exception:
            try:
                await bot.send_message(user_id, message)
            except Exception as e2:
                logger.error(f"Ошибка отправки plain message {user_id}: {e2}")
    except Exception as e:
        logger.error(f"Ошибка отправки сообщения {user_id}: {str(e)}")


async def notify_admins(message: str):
    for admin_id in Config.ADMIN_USER_IDS:
        await safe_send_message(bot, admin_id, message)


async def notify_user(user_id: int, message: str):
    await safe_send_message(bot, user_id, message)


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


async def check_expired_subscriptions():
    while True:
        try:
            subscribed_users = await db.get_subscribed_user_ids()
            for user_id in subscribed_users:
                user_data = await db.get_user(user_id)
                if not user_data:
                    continue

                base_email = f"user_{user_id}@vpn.com"
                clients = panel.find_clients_by_base_email(base_email)

                for c in clients:
                    if c.get("expiryTime", 0) < int(time.time() * 1000):
                        panel.delete_client(base_email)
                        await db.remove_subscription(user_id)
                        break
                if clients:
                    expiry_time = clients.get("expiryTime", 0)
                    if expiry_time > 0 and expiry_time < int(time.time() * 1000):
                        await db.remove_subscription(user_id)
                        await notify_user(user_id, "⏰ Ваша подписка истекла!")

            await asyncio.sleep(3600)  # 1 час
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
                        except:
                            new_payments.append(payment)
                    else:
                        new_payments.append(payment)
                else:
                    new_payments.append(payment)

            if len(new_payments) != len(payments):
                await json_db.write_all(new_payments)

            await asyncio.sleep(259200)  # 3 дня
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
db = Database()
json_db = JSONDatabase(Config.DATA_AWAIT)
panel = PanelAPI()


# --- Middleware ---
async def middleware(handler, event, data):
    banned_user_ids = await db.get_banned_user_ids()
    if not banned_user_ids:
        return await handler(event, data)

    if isinstance(event, Message):
        user_id = event.from_user.id
    elif isinstance(event, CallbackQuery):
        user_id = event.from_user.id
    else:
        return await handler(event, data)

    if user_id in banned_user_ids:
        user_data = await db.get_user(user_id)
        ban_reason = (
            user_data.get("ban_reason", "Не указана") if user_data else "Не указана"
        )
        if isinstance(event, Message):
            await event.answer(
                "⛔ <b>Ваш аккаунт заблокирован!</b>\n\n"
                f"Причина: {ban_reason}\n\n"
                "Если вы считаете, что это ошибка, пожалуйста, свяжитесь с поддержкой."
            )
        return None
    else:
        return await handler(event, data)


router.message.middleware(middleware)
router.callback_query.middleware(middleware)


# --- Обработчики команд ---


# --- start ---
@router.message(Command("start"))
@router.callback_query(F.data == "start")
async def cmd_start(event):
    user_id = event.from_user.id
    await db.add_user(user_id)
    total_users = await db.get_total_users()
    banned_users = await db.get_banned_users_count()
    subs_IDS = await db.get_subscribed_user_ids()
    active_vpns = len(subs_IDS)

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
        event, text, reply_markup={"inline_keyboard": keyboard}, delete_origin=True
    )


# --- subs ---
@router.message(Command("subs"))
@router.callback_query(F.data == "subs")
async def cmd_subs(event):
    user_id = event.from_user.id
    # TODO: Переделайте под свои тарифы и нужды, но не забудьте что требуется тогда переделать и обработчики покупки!
    text = (
        "🔒 <b>Тарифы VPN</b>\n\n"
        "1. <b>Базовый</b> - 100 ₽/мес\n"
        "- до 2 устройств\n"
        "- до 100 ГБ трафика\n"
        "- NL\n\n"
        "2. <b>Стандарт</b> - 200 ₽/мес\n"
        "- до 5 устройств\n"
        "- до 500 ГБ трафика\n"
        "- NL\n\n"
        "3. <b>Премиум</b> - 300 ₽/мес\n"
        "- до 10 устройств\n"
        "- до 2 ТБ трафика\n"
        "- NL\n\n"
        "В будущем количество серверов и стран будет увеличено, а также со временем появится WhiteList!"
    )

    if user_id in Config.ADMIN_USER_IDS:
        keyboard = [
            [{"text": "Подтверждение платежей", "callback_data": "pay_await"}],
            [{"text": "Тест покупки", "callback_data": "buy"}],
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
        event, text, reply_markup={"inline_keyboard": keyboard}, delete_origin=True
    )


# --- buy ---
@router.message(Command("buy"))
@router.callback_query(F.data == "buy")
async def cmd_buy(event):
    user_id = event.from_user.id
    text = (
        "💳 <b>Купить подписку VPN</b>\n\n"
        "Выберите тариф для покупки:\n"
        "1. <b>Базовый</b> - 100 ₽/мес\n"
        "2. <b>Стандарт</b> - 200 ₽/мес\n"
        "3. <b>Премиум</b> - 300 ₽/мес\n\n"
        "После выбора тарифа вы будете перенаправлены на оплату."
    )

    if user_id in Config.ADMIN_USER_IDS:
        keyboard = [
            [{"text": "Наши тарифы", "callback_data": "subs"}],
            [{"text": "Базовый", "callback_data": "test_basic"}],
            [{"text": "Стандарт", "callback_data": "test_standard"}],
            [{"text": "Премиум", "callback_data": "test_premium"}],
            [{"text": "Отмена", "callback_data": "start"}],
        ]
    else:
        keyboard = [
            [{"text": "Наши тарифы", "callback_data": "subs"}],
            [{"text": "Базовый", "callback_data": "buy_basic"}],
            [{"text": "Стандарт", "callback_data": "buy_standard"}],
            [{"text": "Премиум", "callback_data": "buy_premium"}],
            [{"text": "Отмена", "callback_data": "start"}],
        ]

    await smart_answer(
        event, text, reply_markup={"inline_keyboard": keyboard}, delete_origin=True
    )


@router.callback_query(F.data == "test_basic")
async def cmd_test_basic(event: CallbackQuery):
    user_id = event.from_user.id
    if user_id not in Config.ADMIN_USER_IDS:
        await event.answer(
            "⛔ Эта функция доступна только администраторам!", show_alert=True
        )
        return

    email = f"user_{user_id}@vpn.com"
    panel.delete_client(base_email=email)
    client = panel.create_client(email=email, limit_ip=2, total_gb=100, days=30)

    if client:
        vpn_url = f"{Config.SUB_PANEL_BASE}{client.get('subId', 'test')}"
        await db.set_subscription(
            user_id=user_id,
            plan_text="Базовый (тест)",
            ip_limit=2,
            vpn_url=vpn_url,
            traffic_gb=100,
        )
        text = (
            "✅ <b>Тестовая подписка успешно создана!</b>\n\n"
            "Тариф: <b>Базовый (тест)</b>\n"
            "IP-адреса: <b>до 2</b>\n"
            "Трафик: <b>100 ГБ</b>\n"
            "Срок: <b>30 дней</b>\n\n"
            f"URL для подключения:\n<code>{vpn_url}</code>"
        )
    else:
        text = "❌ <b>Ошибка создания тестовой подписки!</b>"

    keyboard = [
        [{"text": "Моя подписка", "callback_data": "mysub"}],
        [{"text": "Главная", "callback_data": "start"}],
    ]
    await smart_answer(
        event, text, reply_markup={"inline_keyboard": keyboard}, delete_origin=True
    )


@router.callback_query(F.data == "test_standard")
async def cmd_test_standard(event: CallbackQuery):
    user_id = event.from_user.id
    if user_id not in Config.ADMIN_USER_IDS:
        await event.answer(
            "⛔ Эта функция доступна только администраторам!", show_alert=True
        )
        return

    email = f"user_{user_id}@vpn.com"
    panel.delete_client(base_email=email)
    client = panel.create_client(email=email, limit_ip=5, total_gb=500, days=30)

    if client:
        vpn_url = f"{Config.SUB_PANEL_BASE}{client.get('subId', 'test')}"
        await db.set_subscription(
            user_id=user_id,
            plan_text="Стандарт (тест)",
            ip_limit=5,
            vpn_url=vpn_url,
            traffic_gb=500,
        )
        text = (
            "✅ <b>Тестовая подписка успешно создана!</b>\n\n"
            "Тариф: <b>Стандарт (тест)</b>\n"
            "IP-адреса: <b>до 5</b>\n"
            "Трафик: <b>500 ГБ</b>\n"
            "Срок: <b>30 дней</b>\n\n"
            f"URL для подключения:\n<code>{vpn_url}</code>"
        )
    else:
        text = "❌ <b>Ошибка создания тестовой подписки!</b>"

    keyboard = [
        [{"text": "Моя подписка", "callback_data": "mysub"}],
        [{"text": "Главная", "callback_data": "start"}],
    ]
    await smart_answer(
        event, text, reply_markup={"inline_keyboard": keyboard}, delete_origin=True
    )


@router.callback_query(F.data == "test_premium")
async def cmd_test_premium(event: CallbackQuery):
    user_id = event.from_user.id
    if user_id not in Config.ADMIN_USER_IDS:
        await event.answer(
            "⛔ Эта функция доступна только администраторам!", show_alert=True
        )
        return

    email = f"user_{user_id}@vpn.com"
    panel.delete_client(base_email=email)
    client = panel.create_client(email=email, limit_ip=10, total_gb=2048, days=30)

    if client:
        vpn_url = f"{Config.SUB_PANEL_BASE}{client.get('subId', 'test')}"
        await db.set_subscription(
            user_id=user_id,
            plan_text="Премиум (тест)",
            ip_limit=10,
            vpn_url=vpn_url,
            traffic_gb=2048,
        )
        text = (
            "✅ <b>Тестовая подписка успешно создана!</b>\n\n"
            "Тариф: <b>Премиум (тест)</b>\n"
            "IP-адреса: <b>до 10</b>\n"
            "Трафик: <b>2 ТБ</b>\n"
            "Срок: <b>30 дней</b>\n\n"
            f"URL для подключения:\n<code>{vpn_url}</code>"
        )
    else:
        text = "❌ <b>Ошибка создания тестовой подписки!</b>"

    keyboard = [
        [{"text": "Моя подписка", "callback_data": "mysub"}],
        [{"text": "Главная", "callback_data": "start"}],
    ]
    await smart_answer(
        event, text, reply_markup={"inline_keyboard": keyboard}, delete_origin=True
    )


@router.callback_query(F.data == "buy_basic")
async def cmd_buy_basic(event):
    user_id = event.from_user.id
    text = (
        "💳 <b>Покупка базового тарифа VPN</b>\n\n"
        "Вы выбрали тариф <b>Базовый</b> за 100 ₽/мес.\n"
        f"Для оплаты перевидите 100 ₽ по номеру карты: <code>{Config.PAYMENT_CARD_NUMBER}</code>.\n"
        f"В комментарии к платежу ОБЯЗАТЕЛЬНО укажите этот текст: <code>Пожертвование от {user_id}</code>.\n\n"
        "После оплаты нажмите кнопку ниже для подтверждения платежа."
    )
    keyboard = [
        [
            {
                "text": "Подтвердить оплату",
                "callback_data": f"confirm_payment:basic:{user_id}:100",
            }
        ],
        [{"text": "Отмена", "callback_data": "start"}],
    ]
    await smart_answer(
        event, text, reply_markup={"inline_keyboard": keyboard}, delete_origin=True
    )


@router.callback_query(F.data == "buy_standard")
async def cmd_buy_standard(event):
    user_id = event.from_user.id
    text = (
        "💳 <b>Покупка стандартного тарифа VPN</b>\n\n"
        "Вы выбрали тариф <b>Стандарт</b> за 200 ₽/мес.\n"
        f"Для оплаты перевидите 200 ₽ по номеру карты: <code>{Config.PAYMENT_CARD_NUMBER}</code>.\n"
        f"В комментарии к платежу ОБЯЗАТЕЛЬНО укажите этот текст: <code>Пожертвование от {user_id}</code>.\n\n"
        "После оплаты нажмите кнопку ниже для подтверждения платежа."
    )
    keyboard = [
        [
            {
                "text": "Подтвердить оплату",
                "callback_data": f"confirm_payment:standard:{user_id}:200",
            }
        ],
        [{"text": "Отмена", "callback_data": "start"}],
    ]
    await smart_answer(
        event, text, reply_markup={"inline_keyboard": keyboard}, delete_origin=True
    )


@router.callback_query(F.data == "buy_premium")
async def cmd_buy_premium(event):
    user_id = event.from_user.id
    text = (
        "💳 <b>Покупка премиум тарифа VPN</b>\n\n"
        "Вы выбрали тариф <b>Премиум</b> за 300 ₽/мес.\n"
        f"Для оплаты перевидите 300 ₽ по номеру карты: <code>{Config.PAYMENT_CARD_NUMBER}</code>.\n"
        f"В комментарии к платежу ОБЯЗАТЕЛЬНО укажите этот текст: <code>Пожертвование от {user_id}</code>.\n\n"
        "После оплаты нажмите кнопку ниже для подтверждения платежа."
    )
    keyboard = [
        [
            {
                "text": "Подтвердить оплату",
                "callback_data": f"confirm_payment:premium:{user_id}:300",
            }
        ],
        [{"text": "Отмена", "callback_data": "start"}],
    ]
    await smart_answer(
        event, text, reply_markup={"inline_keyboard": keyboard}, delete_origin=True
    )


@router.callback_query(F.data.startswith("confirm_payment:"))
async def cmd_confirm_payment(event: CallbackQuery):
    parts = event.data.split(":")
    if len(parts) < 4:
        await event.answer("❌ Ошибка обработки платежа", show_alert=True)
        return

    plan_type = parts[1]
    user_id = int(parts[2])
    amount = parts[3]

    payment_id = f"pay_{user_id}_{int(time.time())}"
    payment_data = {
        "payment_id": payment_id,
        "user_id": user_id,
        "plan_type": plan_type,
        "amount": amount,
        "timestamp": datetime.now().isoformat(),
        "status": "pending",
    }

    await json_db.add(payment_data)

    text = (
        "🕒 <b>Ваш запрос на подтверждение платежа получен!</b>\n\n"
        "После проверки платежа вы получите уведомление о статусе вашей подписки."
    )
    keyboard = [[{"text": "Главная", "callback_data": "start"}]]
    await smart_answer(
        event, text, reply_markup={"inline_keyboard": keyboard}, delete_origin=True
    )


# --- mysub ---
@router.message(Command("mysub"))
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
            event, text, reply_markup={"inline_keyboard": keyboard}, delete_origin=True
        )
        return

    user_data = await db.get_user(user_id)
    subs_IDS = await db.get_subscribed_user_ids()

    if not user_data or user_id not in subs_IDS:
        text = "👤 <b>Ваша подписка VPN</b>\n\nУ вас нет активной подписки."
        keyboard = [
            [{"text": "Купить подписку", "callback_data": "subs"}],
            [{"text": "Главная", "callback_data": "start"}],
        ]
        await smart_answer(
            event, text, reply_markup={"inline_keyboard": keyboard}, delete_origin=True
        )
        return

    base_email = f"user_{user_id}@vpn.com"
    client_stats = panel.get_client_stats(base_email)
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
            f"Трафик: <b>{traffic_gb} ГБ</b>\n"
            f"URL для подключения:\n"
            f"<code>{vpn_url}</code>\n\n"
            "<i>Статистика трафика временно недоступна</i>"
        )

    keyboard = [
        [{"text": "Рефералка", "callback_data": "ref"}],
        [{"text": "Главная", "callback_data": "start"}],
    ]
    await smart_answer(
        event, text, reply_markup={"inline_keyboard": keyboard}, delete_origin=True
    )


# --- ref ---
@router.message(Command("ref"))
@router.callback_query(F.data == "ref")
async def cmd_ref(event):
    text = "🤝 <b>Реферальная система VPN временно не доступна</b>"
    keyboard = [[{"text": "Главная", "callback_data": "start"}]]
    await smart_answer(
        event, text, reply_markup={"inline_keyboard": keyboard}, delete_origin=True
    )


# --- ban ---
@router.message(Command("ban"))
@router.callback_query(F.data == "ban")
async def cmd_ban(event, state: FSMContext):
    user_id = event.from_user.id
    if user_id in Config.ADMIN_USER_IDS:
        text = "⛔ <b>Введите ID пользователя для блокировки</b>"
        keyboard = [[{"text": "Отмена", "callback_data": "start"}]]
        await smart_answer(
            event, text, reply_markup={"inline_keyboard": keyboard}, delete_origin=True
        )
        await state.set_state(BanUserState.waiting_for_user_id)
    else:
        text = "⛔ <b>Эта команда доступна только администраторам!</b>"
        keyboard = [[{"text": "Главная", "callback_data": "start"}]]
        await smart_answer(
            event, text, reply_markup={"inline_keyboard": keyboard}, delete_origin=True
        )


@router.message(BanUserState.waiting_for_user_id)
async def process_ban_user_id(event: Message, state: FSMContext):
    user_id_to_ban = event.text.strip()

    if not user_id_to_ban.isdigit():
        await event.answer("❌ ID пользователя должен быть числом! Попробуйте снова:")
        return

    user_id_to_ban = int(user_id_to_ban)

    if user_id_to_ban in Config.ADMIN_USER_IDS:
        await event.answer("❌ Нельзя заблокировать администратора!")
        return

    await state.update_data(user_id_to_ban=user_id_to_ban)
    text = f"⛔ <b>Введите причину блокировки для пользователя ID {user_id_to_ban}</b>"
    keyboard = [[{"text": "Отмена", "callback_data": "start"}]]
    await smart_answer(
        event, text, reply_markup={"inline_keyboard": keyboard}, delete_origin=True
    )
    await state.set_state(BanUserState.waiting_for_ban_reason)


@router.message(BanUserState.waiting_for_ban_reason)
async def process_ban_reason(event: Message, state: FSMContext):
    data = await state.get_data()
    user_id_to_ban = data.get("user_id_to_ban")
    ban_reason = event.text.strip()

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
                f"⛔ <b>Ваш аккаунт заблокирован!</b>\n\n"
                f"Причина: {ban_reason}\n\n"
                "Если вы считаете, что это ошибка, пожалуйста, свяжитесь с поддержкой.",
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
        event, text, reply_markup={"inline_keyboard": keyboard}, delete_origin=True
    )


# --- unban ---
@router.message(Command("unban"))
@router.callback_query(F.data == "unban")
async def cmd_unban(event, state: FSMContext):
    user_id = event.from_user.id
    if user_id in Config.ADMIN_USER_IDS:
        text = "⛔ <b>Введите ID пользователя для разблокировки</b>"
        keyboard = [[{"text": "Отмена", "callback_data": "start"}]]
        await smart_answer(
            event, text, reply_markup={"inline_keyboard": keyboard}, delete_origin=True
        )
        await state.set_state(UnbanUserState.waiting_for_user_id)
    else:
        text = "⛔ <b>Эта команда доступна только администраторам!</b>"
        keyboard = [[{"text": "Главная", "callback_data": "start"}]]
        await smart_answer(
            event, text, reply_markup={"inline_keyboard": keyboard}, delete_origin=True
        )


@router.message(UnbanUserState.waiting_for_user_id)
async def process_unban_user_id(event: Message, state: FSMContext):
    user_id_to_unban = event.text.strip()

    if not user_id_to_unban.isdigit():
        await event.answer("❌ ID пользователя должен быть числом! Попробуйте снова:")
        return

    user_id_to_unban = int(user_id_to_unban)
    await state.update_data(user_id_to_unban=user_id_to_unban)

    text = f"⛔ <b>Введите причину разблокировки для пользователя ID {user_id_to_unban}</b>"
    keyboard = [[{"text": "Отмена", "callback_data": "start"}]]
    await smart_answer(
        event, text, reply_markup={"inline_keyboard": keyboard}, delete_origin=True
    )
    await state.set_state(UnbanUserState.waiting_for_unban_reason)


@router.message(UnbanUserState.waiting_for_unban_reason)
async def process_unban_reason(event: Message, state: FSMContext):
    data = await state.get_data()
    user_id_to_unban = data.get("user_id_to_unban")
    unban_reason = event.text.strip()

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
                f"✅ <b>Ваш аккаунт разблокирован!</b>\n\n"
                f"Причина: {unban_reason}\n\n"
                "Добро пожаловать обратно!",
            )
        except Exception:
            pass
    else:
        text = f"❌ <b>Ошибка при разблокировке пользователя ID {user_id_to_unban}</b>"

    keyboard = [[{"text": "Главная", "callback_data": "start"}]]
    await smart_answer(
        event, text, reply_markup={"inline_keyboard": keyboard}, delete_origin=True
    )


# --- pay_await ---
@router.message(Command("pay_await"))
@router.callback_query(F.data == "pay_await")
async def cmd_pay_await(event):
    user_id = event.from_user.id
    if user_id in Config.ADMIN_USER_IDS:
        payments = await json_db.read_all()

        if not payments:
            text = "🕒 <b>Нет пользователей, ожидающих подтверждения платежа</b>"
            keyboard = [[{"text": "Главная", "callback_data": "start"}]]
            await smart_answer(
                event,
                text,
                reply_markup={"inline_keyboard": keyboard},
                delete_origin=True,
            )
            return

        text = "🕒 <b>Пользователи, ожидающие подтверждения платежа:</b>"
        await smart_answer(event, text, delete_origin=True)

        for payment in payments:
            if payment.get("status") != "pending":
                continue

            payment_id = payment.get("payment_id", "")
            user_id = payment.get("user_id", 0)
            plan_type = payment.get("plan_type", "")
            amount = payment.get("amount", 0)
            timestamp = payment.get("timestamp", "")

            try:
                dt = datetime.fromisoformat(timestamp)
                time_str = dt.strftime("%d.%m.%Y %H:%M")
            except:
                time_str = timestamp

            plan_names = {
                "basic": "Базовый",
                "standard": "Стандарт",
                "premium": "Премиум",
            }
            plan_name = plan_names.get(plan_type, plan_type)

            payment_text = (
                f"📋 <b>Платеж ID:</b> <code>{payment_id}</code>\n"
                f"👤 <b>Пользователь:</b> <code>{user_id}</code>\n"
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

            if isinstance(event, Message):
                await event.answer(
                    payment_text, reply_markup={"inline_keyboard": keyboard}
                )
            elif isinstance(event, CallbackQuery) and event.message:
                await event.message.answer(
                    payment_text, reply_markup={"inline_keyboard": keyboard}
                )
    else:
        text = "⛔ <b>Эта команда доступна только администраторам!</b>"
        keyboard = [[{"text": "Главная", "callback_data": "start"}]]
        await smart_answer(
            event, text, reply_markup={"inline_keyboard": keyboard}, delete_origin=True
        )


@router.callback_query(F.data.startswith("pay_await_accept:"))
async def cmd_pay_await_accept(event: CallbackQuery):
    payment_id = event.data.split(":")[1]
    payment = await json_db.find_by_id(payment_id)

    if not payment or payment.get("status") != "pending":
        await event.answer("❌ Платеж не найден или уже обработан", show_alert=True)
        return

    user_id = payment.get("user_id")
    plan_type = payment.get("plan_type")

    plan_params = {
        "basic": {"name": "Базовый", "ip_limit": 2, "traffic_gb": 100, "amount": 100},
        "standard": {
            "name": "Стандарт",
            "ip_limit": 5,
            "traffic_gb": 500,
            "amount": 200,
        },
        "premium": {
            "name": "Премиум",
            "ip_limit": 10,
            "traffic_gb": 2048,
            "amount": 300,
        },
    }

    params = plan_params.get(plan_type, plan_params["basic"])
    email = f"user_{user_id}@vpn.com"
    panel.delete_client(base_email=email)
    client = panel.create_client(
        email=email, limit_ip=params["ip_limit"], total_gb=params["traffic_gb"], days=30
    )

    if client:
        vpn_url = (
            f"{Config.SUB_PANEL_BASE}{client.get('subId', 'user_' + str(user_id))}"
        )
        await db.set_subscription(
            user_id=user_id,
            plan_text=params["name"],
            ip_limit=params["ip_limit"],
            traffic_gb=params["traffic_gb"],
            vpn_url=vpn_url,
        )
        payment["status"] = "accepted"
        payment["processed_at"] = datetime.now().isoformat()
        await json_db.remove_by_id(payment_id)
        await json_db.add(payment)

        try:
            await notify_user(
                user_id,
                f"✅ <b>Ваш платеж подтвержден!</b>\n\n"
                f"Тариф: <b>{params['name']}</b>\n"
                f"IP-адреса: <b>до {params['ip_limit']}</b>\n"
                f"Трафик: <b>{params['traffic_gb']} ГБ</b>\n"
                f"Срок: <b>30 дней</b>\n\n"
                f"URL для подключения:\n<code>{vpn_url}</code>\n\n"
                "Спасибо за покупку! 🎉",
            )
        except Exception:
            pass

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
    payment_id = event.data.split(":")[1]
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
        )
    except Exception:
        pass

    await event.answer(f"❌ Платеж {payment_id} отклонен!", show_alert=True)

    if event.message:
        new_text = event.message.text + "\n\n❌ <b>ОТКЛОНЕНО</b>"
        await event.message.edit_text(new_text, parse_mode="HTML")


# --- Запуск ---
async def main():
    logger.info(f"Admin IDs: {Config.ADMIN_USER_IDS}")
    logger.info("Запуск VPN бота...")

    if not Config.BOT_TOKEN or Config.BOT_TOKEN == "YOUR_BOT_TOKEN":
        logger.critical("BOT_TOKEN не настроен! Установите его в классе Config")
        sys.exit(1)

    try:
        for admin_id in Config.ADMIN_USER_IDS:
            await safe_send_message(bot, admin_id, "🟢 <b>Бот успешно запущен!</b>\n")

        asyncio.create_task(check_expired_subscriptions())
        asyncio.create_task(cleanup_old_payments())

        await dp.start_polling(bot)
    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info("Остановка бота по запросу пользователя")
    finally:
        for admin_id in Config.ADMIN_USER_IDS:
            await safe_send_message(bot, admin_id, "🔴 <b>Бот остановлен!</b>\n\n")
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
