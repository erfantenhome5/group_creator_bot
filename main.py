import asyncio
import hashlib
import json
import logging
import os
import random
import re
import shutil
import traceback
import uuid  # Added for generating random usernames
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import sentry_sdk
from cryptography.fernet import Fernet, InvalidToken
from dotenv import load_dotenv
from sentry_sdk.integrations.logging import LoggingIntegration
from sentry_sdk.types import Event, Hint
from telethon import Button, TelegramClient, errors, events, types
from telethon.extensions import markdown
from telethon.sessions import StringSession
from telethon.tl import functions
from telethon.tl.functions.channels import CreateChannelRequest, InviteToChannelRequest
from telethon.tl.functions.messages import (GetAllStickersRequest,
                                            GetStickerSetRequest,
                                            ImportChatInviteRequest,
                                            SendReactionRequest)
from telethon.tl.types import (InputStickerSetID, Message, PeerChannel,
                               ReactionEmoji)

# --- Basic Logging Setup ---
logging.basicConfig(
    level=logging.INFO, # Changed to INFO for production
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bot_activity.log"),
        logging.StreamHandler()
    ]
)
LOGGER = logging.getLogger(__name__)

# --- Environment Loading ---
load_dotenv()
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")
SENTRY_DSN = os.getenv("SENTRY_DSN")
ADMIN_USER_ID = os.getenv("ADMIN_USER_ID")
MASTER_PASSWORD_HASH = os.getenv("MASTER_PASSWORD_HASH")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")


if not all([API_ID, API_HASH, BOT_TOKEN, ENCRYPTION_KEY, ADMIN_USER_ID]):
    raise ValueError("Missing required environment variables. Ensure API_ID, API_HASH, BOT_TOKEN, ENCRYPTION_KEY, and ADMIN_USER_ID are set.")

API_ID = int(API_ID)
ADMIN_USER_ID = int(ADMIN_USER_ID)

SESSIONS_DIR = Path(os.getenv("SESSIONS_DIR", "sessions"))
SESSIONS_DIR.mkdir(exist_ok=True, parents=True)

# --- Custom Markdown for Spoilers ---
class CustomMarkdown:
    @staticmethod
    def parse(text):
        text, entities = markdown.parse(text)
        for i, e in enumerate(entities):
            if isinstance(e, types.MessageEntityTextUrl):
                if e.url == 'spoiler':
                    entities[i] = types.MessageEntitySpoiler(e.offset, e.length)
                elif e.url.startswith('emoji/'):
                    entities[i] = types.MessageEntityCustomEmoji(e.offset, e.length, int(e.url.split('/')[1]))
        return text, entities

    @staticmethod
    def unparse(text, entities):
        for i, e in enumerate(entities or []):
            if isinstance(e, types.MessageEntityCustomEmoji):
                entities[i] = types.MessageEntityTextUrl(e.offset, e.length, f'emoji/{e.document_id}')
            if isinstance(e, types.MessageEntitySpoiler):
                entities[i] = types.MessageEntityTextUrl(e.offset, e.length, 'spoiler')
        return markdown.unparse(text, entities)


# --- Global Proxy Loading Function ---
def load_proxies_from_file(proxy_file_path: str) -> List[Dict]:
    proxy_list = []
    try:
        with open(proxy_file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                try:
                    host, port = line.split(':', 1)
                    proxy_list.append({
                        'proxy_type': 'http',
                        'addr': host,
                        'port': int(port)
                    })
                except ValueError:
                    LOGGER.warning(f"Skipping malformed proxy line: {line}. Expected format is IP:PORT.")
        LOGGER.info(f"Loaded {len(proxy_list)} proxies from {proxy_file_path}.")
    except FileNotFoundError:
        LOGGER.warning(f"Proxy file '{proxy_file_path}' not found.")
    return proxy_list

# --- Centralized Configuration ---
class Config:
    """Holds all configurable values and UI strings for the bot."""
    # Bot Settings
    MAX_CONCURRENT_WORKERS = 5
    GROUPS_TO_CREATE = 5
    MIN_SLEEP_SECONDS = 300
    MAX_SLEEP_SECONDS = 900
    PROXY_FILE = "proxy.txt"
    PROXY_TIMEOUT = 15

    # --- UI Text & Buttons (All in Persian) ---
    BTN_MANAGE_ACCOUNTS = "👤 مدیریت حساب‌ها"
    BTN_SERVER_STATUS = "📊 وضعیت سرور"
    BTN_HELP = "ℹ️ راهنما"
    BTN_ADD_ACCOUNT = "➕ افزودن حساب (API)"
    BTN_ADD_ACCOUNT_SELENIUM = "✨ افزودن حساب (مرورگر امن)"
    BTN_BACK = "⬅️ بازگشت"
    BTN_START_PREFIX = "🟢 شروع برای"
    BTN_STOP_PREFIX = "⏹️ توقف برای"
    BTN_DELETE_PREFIX = "🗑️ حذف"
    BTN_SET_KEYWORDS = "📝 تنظیم کلمات کلیدی AI"
    BTN_SET_CONVERSATION_ACCOUNTS = "🗣️ تنظیم حساب‌های گفتگو"
    BTN_JOIN_VIA_LINK = "🔗 عضویت با لینک"
    BTN_EXPORT_LINKS = "🔗 صدور لینک‌های گروه"

    # --- Messages (All in Persian) ---
    MSG_WELCOME = "**🤖 به ربات سازنده گروه خوش آمدید!**"
    MSG_ACCOUNT_MENU_HEADER = "👤 **مدیریت حساب‌ها**\n\nاز این منو می‌توانید حساب‌های خود را مدیریت کرده و عملیات ساخت گروه را برای هرکدام آغاز یا متوقف کنید."
    MSG_HELP_TEXT = (
        "**راهنمای جامع ربات**\n\n"
        "این ربات به شما اجازه می‌دهد تا با چندین حساب تلگرام به صورت همزمان گروه‌های جدید بسازید.\n\n"
        f"**{BTN_MANAGE_ACCOUNTS}**\n"
        "در این بخش می‌توانید حساب‌های خود را مدیریت کنید:\n"
        f"  - `{BTN_ADD_ACCOUNT}`: یک شماره تلفن جدید با روش API اضافه کنید.\n"
        f"  - `{BTN_ADD_ACCOUNT_SELENIUM}`: یک شماره تلفن جدید با روش شبیه‌سازی مرورگر اضافه کنید (امنیت بالاتر).\n"
        f"  - `{BTN_START_PREFIX} [نام حساب]`: عملیات ساخت گروه را برای حساب مشخص شده آغاز می‌کند.\n"
        f"  - `{BTN_STOP_PREFIX} [نام حساب]`: عملیات در حال اجرا برای یک حساب را متوقف می‌کند.\n"
        f"  - `{BTN_DELETE_PREFIX} [نام حساب]`: یک حساب و تمام اطلاعات آن را برای همیشه حذف می‌کند.\n\n"
        f"**{BTN_JOIN_VIA_LINK}**\n"
        "یکی از حساب‌های خود را با استفاده از لینک دعوت در یک یا چند گروه/کانال عضو کنید.\n\n"
        f"**{BTN_EXPORT_LINKS}**\n"
        "لینک‌های دعوت تمام گروه‌هایی که توسط یک حساب خاص ساخته شده را دریافت کنید.\n\n"
        f"**{BTN_SET_KEYWORDS}**\n"
        "کلمات کلیدی مورد نظر خود را برای تولید محتوای هوش مصنوعی تنظیم کنید.\n\n"
        f"**{BTN_SET_CONVERSATION_ACCOUNTS}**\n"
        "حساب‌هایی که باید در گروه‌های جدید به گفتگو بپردازند را مشخص کنید.\n\n"
        f"**{BTN_SERVER_STATUS}**\n"
        "این گزینه اطلاعات لحظه‌ای درباره وضعیت ربات را نمایش می‌دهد."
    )
    MSG_PROMPT_MASTER_PASSWORD = "🔑 لطفاً برای دسترسی به ربات، رمز عبور اصلی را وارد کنید:"
    MSG_INCORRECT_MASTER_PASSWORD = "❌ رمز عبور اشتباه است. لطفاً دوباره تلاش کنید."
    MSG_BROWSER_RUNNING = "⏳ در حال آماده‌سازی مرورگر امن... این کار ممکن است چند لحظه طول بکشد."
    MSG_PROMPT_KEYWORDS = "📝 لطفاً کلمات کلیدی مورد نظر خود را برای تولید محتوای هوش مصنوعی وارد کنید. کلمات را با کاما (,) از هم جدا کنید.\n\nمثال: موفقیت, بازاریابی, ارز دیجیتال, فروش آنلاین"
    MSG_KEYWORDS_SET = "✅ کلمات کلیدی شما با موفقیت ذخیره شد."
    MSG_PROMPT_CONVERSATION_ACCOUNTS = "🗣️ لطفاً نام مستعار حساب‌هایی که می‌خواهید در گفتگوها شرکت کنند را وارد کنید. نام‌ها را با کاما (,) از هم جدا کنید.\n\nاین حساب‌ها در گروه‌های جدید ساخته شده با یکدیگر گفتگو خواهند کرد. برای غیرفعال کردن این ویژگی، این بخش را خالی بگذارید."
    MSG_CONVERSATION_ACCOUNTS_SET = "✅ حساب‌های گفتگو با موفقیت ذخیره شدند."
    MSG_AWAITING_APPROVAL = "⏳ درخواست دسترسی شما برای ادمین ارسال شد. لطفاً منتظر تایید بمانید."
    MSG_USER_APPROVED = "✅ درخواست شما تایید شد! برای شروع /start را بزنید."
    MSG_USER_DENIED = "❌ متاسفانه درخواست دسترسی شما رد شد."
    MSG_PROMPT_JOIN_ACCOUNT = "👤 لطفاً حسابی که می‌خواهید با آن عضو شوید را انتخاب کنید:"
    MSG_PROMPT_EXPORT_ACCOUNT = "📤 لطفاً حسابی که می‌خواهید لینک‌های آن را استخراج کنید، انتخاب نمایید:"
    MSG_PROMPT_JOIN_LINK_MULTIPLE = "🔗 لطفاً یک یا چند لینک دعوت را ارسال کنید. هر لینک را در یک خط جدید وارد کنید:"
    MSG_JOIN_SUMMARY = "🏁 **گزارش عضویت برای `{account_name}`:**\n\n✅ **موفق:** {success_count}\n❌ **ناموفق:** {fail_count}\n\n{fail_details}"
    MSG_EXPORTING_LINKS = "⏳ در حال استخراج لینک‌های دعوت برای حساب `{account_name}`... این عملیات ممکن است کمی طول بکشد."
    MSG_EXPORT_SUCCESS = "✅ لینک‌های دعوت با موفقیت استخراج شدند و در فایل زیر برای شما ارسال شد."
    MSG_EXPORT_FAIL = "❌ خطایی در استخراج لینک‌ها رخ داد یا این حساب گروهی نساخته است."


class SessionManager:
    """Manages encrypted user session files."""
    def __init__(self, fernet: Fernet, directory: Path):
        self._fernet = fernet
        self._dir = directory
        self._user_sessions_dir = self._dir / "user_sessions"
        self._user_sessions_dir.mkdir(exist_ok=True)

    def _get_user_dir(self, user_id: int) -> Path:
        user_dir = self._user_sessions_dir / str(user_id)
        user_dir.mkdir(exist_ok=True)
        return user_dir

    def get_user_accounts(self, user_id: int) -> List[str]:
        user_dir = self._get_user_dir(user_id)
        return [f.stem for f in user_dir.glob("*.session")]

    def save_session_string(self, user_id: int, name: str, session_string: str) -> None:
        user_dir = self._get_user_dir(user_id)
        session_file = user_dir / f"{name}.session"
        encrypted_session = self._fernet.encrypt(session_string.encode())
        session_file.write_bytes(encrypted_session)

    def load_session_string(self, user_id: int, name: str) -> Optional[str]:
        user_dir = self._get_user_dir(user_id)
        session_file = user_dir / f"{name}.session"
        if not session_file.exists():
            return None
        try:
            encrypted_session = session_file.read_bytes()
            decrypted_session = self._fernet.decrypt(encrypted_session)
            return decrypted_session.decode()
        except (InvalidToken, IOError):
            LOGGER.error(f"Could not load or decrypt session for {name} of user {user_id}.")
            return None

    def delete_session_file(self, user_id: int, name: str) -> bool:
        user_dir = self._get_user_dir(user_id)
        session_file = user_dir / f"{name}.session"
        if session_file.exists():
            session_file.unlink()
            return True
        return False


class GroupCreatorBot:
    """A class to encapsulate the bot's logic for managing multiple accounts."""

    def __init__(self, session_manager) -> None:
        """Initializes the bot instance and the encryption engine."""
        self.bot = TelegramClient('bot_session', API_ID, API_HASH)
        self.user_sessions: Dict[int, Dict[str, Any]] = {}
        self.active_workers: Dict[str, asyncio.Task] = {}
        self.worker_semaphore = asyncio.Semaphore(Config.MAX_CONCURRENT_WORKERS)
        self.counts_file = SESSIONS_DIR / "group_counts.json"
        self.group_counts = self._load_group_counts()
        self.proxies = load_proxies_from_file(Config.PROXY_FILE)
        self.account_proxy_file = SESSIONS_DIR / "account_proxies.json"
        self.account_proxies = self._load_account_proxies()
        self.known_users_file = SESSIONS_DIR / "known_users.json"
        self.known_users = self._load_known_users()
        self.pending_users_file = SESSIONS_DIR / "pending_users.json"
        self.pending_users = self._load_pending_users()
        self.created_groups_file = SESSIONS_DIR / "created_groups.json"
        self.created_groups = self._load_created_groups()
        self.active_workers_file = SESSIONS_DIR / "active_workers.json"
        self.active_workers_state = self._load_active_workers_state()
        self.keywords_file = SESSIONS_DIR / "keywords.json"
        self.user_keywords = self._load_user_keywords()
        self.conversation_accounts_file = SESSIONS_DIR / "conversation_accounts.json"
        self.conversation_accounts = self._load_conversation_accounts()
        try:
            fernet = Fernet(ENCRYPTION_KEY.encode())
            self.session_manager = session_manager(fernet, SESSIONS_DIR)
        except (ValueError, TypeError):
            raise ValueError("Invalid ENCRYPTION_KEY. Please generate a valid key.")

        self._initialize_sentry()

    def _initialize_sentry(self):
        if not SENTRY_DSN:
            return

        def before_send_hook(event: Event, hint: Hint) -> Optional[Event]:
            if 'log_record' in hint:
                log_record = hint['log_record']
                if log_record.levelno <= logging.DEBUG and log_record.name.startswith('telethon'):
                    message = log_record.getMessage()
                    noisy_patterns = [
                        "Assigned msg_id", "Encrypting", "Encrypted messages put in a queue",
                        "Waiting for messages to send", "Handling pong", "Receiving items from the network",
                        "Handling gzipped data", "Handling update", "Handling RPC result",
                        "stopped chain of propagation"
                    ]
                    for pattern in noisy_patterns:
                        if pattern in message:
                            return None
            return event

        sentry_logging = LoggingIntegration(
            level=logging.INFO,
            event_level=logging.ERROR
        )

        sentry_options = {
            "dsn": SENTRY_DSN,
            "integrations": [sentry_logging],
            "traces_sample_rate": 1.0,
            "before_send": before_send_hook,
        }

        if self.proxies:
            sentry_proxy = random.choice(self.proxies)
            proxy_url = f"http://{sentry_proxy['addr']}:{sentry_proxy['port']}"
            sentry_options["http_proxy"] = proxy_url
            sentry_options["https_proxy"] = proxy_url
            LOGGER.info(f"Sentry will use proxy: {sentry_proxy['addr']}:{sentry_proxy['port']}")
        else:
            LOGGER.info("Sentry will not use a proxy (none found).")

        sentry_sdk.init(**sentry_options)
        LOGGER.info("Sentry initialized for error reporting.")

    # --- Proxy Helpers ---
    def _load_account_proxies(self) -> Dict[str, Dict]:
        if not self.account_proxy_file.exists():
            return {}
        try:
            with self.account_proxy_file.open("r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            LOGGER.error("Could not read or parse account_proxies.json. Starting with empty assignments.")
            return {}

    def _save_account_proxies(self) -> None:
        try:
            with self.account_proxy_file.open("w") as f:
                json.dump(self.account_proxies, f, indent=4)
        except IOError:
            LOGGER.error("Could not save account_proxies.json.")

    def _get_available_proxy(self) -> Optional[Dict]:
        if not self.proxies:
            return None
        assigned_proxy_keys = {
            (p['addr'], p['port'])
            for p in self.account_proxies.values() if p
        }
        for proxy in self.proxies:
            proxy_key = (proxy['addr'], proxy['port'])
            if proxy_key not in assigned_proxy_keys:
                LOGGER.info(f"Found available proxy: {proxy['addr']}:{proxy['port']}")
                return proxy
        LOGGER.warning("All proxies are currently assigned. No available proxy found.")
        return None

    # --- Data Store Helpers ---
    def _load_json_file(self, file_path: Path, default_type: Any = {}) -> Any:
        if not file_path.exists():
            return default_type
        try:
            with file_path.open("r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            LOGGER.error(f"Could not read or parse {file_path.name}. Starting with empty data.")
            return default_type

    def _save_json_file(self, data: Any, file_path: Path) -> None:
        try:
            with file_path.open("w") as f:
                json.dump(data, f, indent=4)
        except IOError:
            LOGGER.error(f"Could not save {file_path.name}.")

    def _load_group_counts(self) -> Dict[str, int]:
        return self._load_json_file(self.counts_file, {})

    def _save_group_counts(self) -> None:
        self._save_json_file(self.group_counts, self.counts_file)

    def _load_user_keywords(self) -> Dict[str, List[str]]:
        return self._load_json_file(self.keywords_file, {})

    def _save_user_keywords(self) -> None:
        self._save_json_file(self.user_keywords, self.keywords_file)

    def _load_conversation_accounts(self) -> Dict[str, List[str]]:
        return self._load_json_file(self.conversation_accounts_file, {})

    def _save_conversation_accounts(self) -> None:
        self._save_json_file(self.conversation_accounts, self.conversation_accounts_file)

    def _load_known_users(self) -> List[int]:
        return self._load_json_file(self.known_users_file, [])

    def _save_known_users(self) -> None:
        self._save_json_file(self.known_users, self.known_users_file)

    def _load_pending_users(self) -> List[int]:
        return self._load_json_file(self.pending_users_file, [])

    def _save_pending_users(self) -> None:
        self._save_json_file(self.pending_users, self.pending_users_file)

    def _load_created_groups(self) -> Dict[str, Dict]:
        return self._load_json_file(self.created_groups_file, {})

    def _save_created_groups(self) -> None:
        self._save_json_file(self.created_groups, self.created_groups_file)

    def _get_group_count(self, worker_key: str) -> int:
        return self.group_counts.get(worker_key, 0)

    def _set_group_count(self, worker_key: str, count: int) -> None:
        self.group_counts[worker_key] = count
        self._save_group_counts()

    def _remove_group_count(self, worker_key: str) -> None:
        if worker_key in self.group_counts:
            del self.group_counts[worker_key]
            self._save_group_counts()

    def _load_active_workers_state(self) -> Dict[str, Dict]:
        return self._load_json_file(self.active_workers_file, {})

    def _save_active_workers_state(self) -> None:
        self._save_json_file(self.active_workers_state, self.active_workers_file)

    async def _broadcast_message(self, message_text: str):
        LOGGER.info(f"Broadcasting message to {len(self.known_users)} users.")
        for user_id in self.known_users:
            try:
                await self.bot.send_message(user_id, message_text)
                await asyncio.sleep(0.1)
            except (errors.UserIsBlockedError, errors.InputUserDeactivatedError):
                LOGGER.warning(f"User {user_id} has blocked the bot or is deactivated. Cannot send message.")
            except Exception as e:
                LOGGER.error(f"Error sending message to {user_id}: {e}")

    async def _create_login_client(self, proxy: Optional[Dict]) -> Optional[TelegramClient]:
        session = StringSession()
        device_params = random.choice([{'device_model': 'iPhone 14 Pro Max', 'system_version': '17.5.1'}, {'device_model': 'Samsung Galaxy S24 Ultra', 'system_version': 'SDK 34'}])

        try:
            proxy_info = f"with proxy {proxy['addr']}:{proxy['port']}" if proxy else "without proxy"
            LOGGER.debug(f"Attempting login connection {proxy_info}")
            client = TelegramClient(session, API_ID, API_HASH, proxy=proxy, timeout=Config.PROXY_TIMEOUT, **device_params)
            client.parse_mode = CustomMarkdown() # Apply custom parser
            await client.connect()
            return client
        except Exception as e:
            LOGGER.error(f"Login connection {proxy_info} failed: {e}")
            return None

    async def _create_worker_client(self, session_string: str, proxy: Optional[Dict]) -> Optional[TelegramClient]:
        session = StringSession(session_string)
        device_params = random.choice([{'device_model': 'iPhone 14 Pro Max', 'system_version': '17.5.1'}, {'device_model': 'Samsung Galaxy S24 Ultra', 'system_version': 'SDK 34'}])

        client = TelegramClient(
            session, API_ID, API_HASH, proxy=proxy, timeout=Config.PROXY_TIMEOUT,
            device_model=device_params['device_model'], system_version=device_params['system_version']
        )
        client.parse_mode = CustomMarkdown() # Apply custom parser

        try:
            proxy_info = f"with proxy {proxy['addr']}:{proxy['port']}" if proxy else "without proxy"
            LOGGER.debug(f"Attempting worker connection {proxy_info}")
            await client.connect()
            LOGGER.info(f"Worker connected successfully {proxy_info}")
            return client
        except Exception as e:
            LOGGER.error(f"Worker connection {proxy_info} failed: {e}")
            sentry_sdk.capture_exception(e)
            if isinstance(e, errors.AuthKeyUnregisteredError):
                raise
            return None

    async def _send_request_with_reconnect(self, client: TelegramClient, request: Any, account_name: str) -> Any:
        try:
            if not client.is_connected():
                LOGGER.warning(f"Client for '{account_name}' is disconnected. Reconnecting...")
                await client.connect()
                if client.is_connected():
                    LOGGER.info(f"Client for '{account_name}' reconnected successfully.")
                else:
                    LOGGER.error(f"Failed to reconnect client for '{account_name}'.")
                    raise ConnectionError("Client reconnection failed.")
            return await client(request)
        except ConnectionError as e:
            LOGGER.error(f"Connection error for '{account_name}' even after check: {e}")
            sentry_sdk.capture_exception(e)
            raise
        except Exception as e:
            LOGGER.error(f"Unexpected error sending request for '{account_name}': {e}")
            sentry_sdk.capture_exception(e)
            raise

    def _build_main_menu(self) -> List[List[Button]]:
        return [
            [Button.text(Config.BTN_MANAGE_ACCOUNTS), Button.text(Config.BTN_JOIN_VIA_LINK)],
            [Button.text(Config.BTN_EXPORT_LINKS)],
            [Button.text(Config.BTN_SET_KEYWORDS), Button.text(Config.BTN_SET_CONVERSATION_ACCOUNTS)],
            [Button.text(Config.BTN_SERVER_STATUS), Button.text(Config.BTN_HELP)],
        ]

    def _build_accounts_menu(self, user_id: int) -> List[List[Button]]:
        accounts = self.session_manager.get_user_accounts(user_id)
        keyboard = []
        if not accounts:
            keyboard.append([Button.text("هنوز هیچ حسابی اضافه نشده است.")])
        else:
            for acc_name in accounts:
                worker_key = f"{user_id}:{acc_name}"
                if worker_key in self.active_workers:
                    keyboard.append([Button.text(f"{Config.BTN_STOP_PREFIX} {acc_name}")])
                else:
                    keyboard.append([
                        Button.text(f"{Config.BTN_START_PREFIX} {acc_name}"),
                        Button.text(f"{Config.BTN_DELETE_PREFIX} {acc_name}")
                    ])
        keyboard.append([
            Button.text(Config.BTN_ADD_ACCOUNT),
            Button.text(Config.BTN_ADD_ACCOUNT_SELENIUM)
        ])
        keyboard.append([Button.text(Config.BTN_BACK)])
        return keyboard

    def _prepare_spoiler_text(self, text: str) -> str:
        """Converts ||spoiler|| syntax to [spoiler](spoiler) for custom markdown."""
        return re.sub(r'\|\|(.*?)\|\|', r'[\1](spoiler)', text)

    async def _generate_persian_messages(self, user_id: int) -> List[str]:
        if not GEMINI_API_KEY:
            LOGGER.warning("GEMINI_API_KEY not set. Skipping message generation.")
            return []

        keywords = self.user_keywords.get(str(user_id), ["زندگی", "خدا", "موفقیت"])
        prompt = (
            f"ایجاد ۲۰ پیام یا نقل قول منحصر به فرد و عمیق به زبان فارسی. "
            f"این پیام‌ها باید درباره این موضوعات باشند: {', '.join(keywords)}. "
            "گاهی اوقات، از سینتکس ||کلمه یا عبارت|| برای مخفی کردن (اسپویلر) بخشی از متن استفاده کن. "
            "لطفا پاسخ را در قالب یک آرایه JSON از رشته‌ها برگردانید. مثال: "
            '["پیام اول", "پیام دوم حاوی ||متن مخفی|| است", ...]'
            "فقط و فقط آرایه JSON را بدون هیچ متن اضافی، توضیحات یا قالب‌بندی دیگری برگردانید."
        )
        api_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"
        payload = {"contents": [{"parts": [{"text": prompt}]}], "generationConfig": {"responseMimeType": "application/json"}}
        headers = {'Content-Type': 'application/json'}

        LOGGER.info("Attempting to generate messages from Gemini using server IP.")

        try:
            async with httpx.AsyncClient(timeout=40.0) as client:
                response = await client.post(api_url, json=payload, headers=headers)
                response.raise_for_status()
                data = response.json()

                if data.get("candidates") and data["candidates"][0].get("content", {}).get("parts"):
                    json_text = data["candidates"][0]["content"]["parts"][0]["text"]
                    messages = json.loads(json_text)
                    if isinstance(messages, list) and all(isinstance(item, str) for item in messages):
                        LOGGER.info(f"Successfully generated {len(messages)} messages from Gemini.")
                        return messages
                    else:
                        LOGGER.warning(f"Gemini API returned an unexpected format inside JSON: {messages}")
                else:
                    LOGGER.warning(f"Unexpected Gemini API response structure: {data}")

        except httpx.RequestError as e:
            LOGGER.error(f"Failed to connect to Gemini API: {e}.")
            sentry_sdk.capture_exception(e)
        except json.JSONDecodeError as e:
            LOGGER.error(f"Error decoding JSON from Gemini response: {e}.")
            sentry_sdk.capture_exception(e)
        except Exception as e:
            LOGGER.error(f"An unexpected error occurred during message generation: {e}.")
            sentry_sdk.capture_exception(e)

        return []

    async def _ensure_entity_cached(self, client: TelegramClient, group_id: int, account_name: str, retries: int = 5, delay: int = 5) -> bool:
        for i in range(retries):
            try:
                # Attempt to get the entity
                await client.get_entity(PeerChannel(group_id))
                # Trigger updates to help cache synchronization
                await client.get_dialogs(limit=1)
                LOGGER.info(f"Account '{account_name}' successfully cached entity for group {group_id}.")
                return True
            except ValueError as e:
                LOGGER.warning(f"Attempt {i+1}/{retries}: Account '{account_name}' could not find entity for group {group_id}. Retrying in {delay}s. Error: {e}")
                await asyncio.sleep(delay)
            except Exception as e:
                LOGGER.error(f"Unexpected error while ensuring entity cached for '{account_name}': {e}")
                sentry_sdk.capture_exception(e)
                return False
        LOGGER.error(f"Account '{account_name}' failed to cache entity for group {group_id} after {retries} retries.")
        return False

    async def _simulate_conversation(self, user_id: int, group_id: int, clients_with_meta: List[Dict]):
        if len(clients_with_meta) < 2:
            LOGGER.warning(f"Not enough clients to simulate conversation in group {group_id}.")
            return

        try:
            chat_messages = await self._generate_persian_messages(user_id)
            if not chat_messages:
                LOGGER.warning("Failed to generate messages for conversation simulation.")
                return

            num_messages_to_send = random.randint(5, 15)
            LOGGER.info(f"Starting to send {num_messages_to_send} messages in group {group_id}.")
            last_message = None

            for i in range(num_messages_to_send):
                sender_info = random.choice(clients_with_meta)
                sender_client = sender_info['client']
                sender_account_name = sender_info['account_name']

                try:
                    # Use PeerChannel to be explicit
                    group_entity = await sender_client.get_entity(PeerChannel(group_id))
                    
                    # Decide whether to send a text or a sticker
                    if random.random() < 0.15: # 15% chance to send a sticker
                        try:
                            sticker_sets = await sender_client(GetAllStickersRequest(0))
                            if sticker_sets.sets:
                                chosen_set = random.choice(sticker_sets.sets)
                                stickers = await sender_client(GetStickerSetRequest(
                                    stickerset=InputStickerSetID(id=chosen_set.id, access_hash=chosen_set.access_hash),
                                    hash=0
                                ))
                                if stickers.documents:
                                    last_message = await sender_client.send_file(group_entity, random.choice(stickers.documents))
                                    LOGGER.info(f"Account '{sender_account_name}' sent a sticker to group {group_id}.")
                                else: # Fallback to text if sticker set is empty
                                    continue
                            else: # Fallback to text if no sticker sets
                                continue
                        except Exception as e:
                            LOGGER.warning(f"Could not send sticker for {sender_account_name}: {e}")
                            continue # Skip to next message on sticker error
                    else:
                        message_text = random.choice(chat_messages)
                        chat_messages.remove(message_text)
                        formatted_text = self._prepare_spoiler_text(message_text)
                        last_message = await sender_client.send_message(group_entity, formatted_text)
                        LOGGER.info(f"Account '{sender_account_name}' sent message {i+1}/{num_messages_to_send} to group {group_id}.")

                    # Chance for another user to react
                    if last_message and random.random() < 0.4: # 40% chance to react
                        await asyncio.sleep(random.uniform(5, 15)) # Delay before reacting
                        # Ensure there's at least one other client to react
                        other_clients = [c for c in clients_with_meta if c['client'] != sender_client]
                        if other_clients:
                            reactor_info = random.choice(other_clients)
                            reactor_client = reactor_info['client']
                            reaction = random.choice(['❤️', '👍', '😂', '🔥', '👏', '😁'])
                            try:
                                await reactor_client(SendReactionRequest(
                                    peer=group_entity,
                                    msg_id=last_message.id,
                                    reaction=[ReactionEmoji(emoticon=reaction)]
                                ))
                                LOGGER.info(f"Account '{reactor_info['account_name']}' reacted with {reaction} in group {group_id}.")
                            except Exception as e:
                                LOGGER.warning(f"Could not send reaction from {reactor_info['account_name']}: {e}")

                except Exception as e:
                    LOGGER.error(f"Account '{sender_account_name}' failed to send simulation message to {group_id}: {e}")
                await asyncio.sleep(random.uniform(15, 45))
        except Exception as e:
            LOGGER.error(f"Unexpected error during conversation simulation: {e}")


    async def run_group_creation_worker(self, user_id: int, account_name: str, user_client: TelegramClient) -> None:
        worker_key = f"{user_id}:{account_name}"
        temp_clients = []
        try:
            async with self.worker_semaphore:
                LOGGER.info(f"Worker for {worker_key} started.")

                participant_clients_meta = []
                participant_names = self.conversation_accounts.get(str(user_id), [])
                other_participant_names = [name for name in participant_names if name != account_name]

                for name in other_participant_names:
                    session_str = self.session_manager.load_session_string(user_id, name)
                    if not session_str: continue
                    proxy = self.account_proxies.get(f"{user_id}:{name}")
                    client = await self._create_worker_client(session_str, proxy)
                    if client:
                        temp_clients.append(client)
                        me = await client.get_me()
                        p_account_name = me.first_name or me.username or f"ID:{me.id}"
                        participant_clients_meta.append({'client': client, 'user_id': user_id, 'account_name': p_account_name})

                me = await user_client.get_me()
                u_account_name = me.first_name or me.username or f"ID:{me.id}"
                all_clients_meta = [{'client': user_client, 'user_id': user_id, 'account_name': u_account_name}] + participant_clients_meta

                for i in range(Config.GROUPS_TO_CREATE):
                    try:
                        current_semester = self._get_group_count(worker_key) + 1
                        group_title = f"collage Semester {current_semester}"
                        create_result = await self._send_request_with_reconnect(
                            user_client, CreateChannelRequest(title=group_title, about="Official group.", megagroup=True), account_name
                        )
                        new_supergroup = create_result.chats[0]
                        LOGGER.info(f"Successfully created supergroup '{new_supergroup.title}' (ID: {new_supergroup.id}).")
                        # Correctly associate the group with the worker key
                        self.created_groups[str(new_supergroup.id)] = {"owner_worker_key": worker_key, "last_simulated": 0}
                        self._save_created_groups()

                        users_to_invite = [await p_meta['client'].get_me() for p_meta in participant_clients_meta]
                        if users_to_invite:
                            LOGGER.info(f"Creator '{account_name}' inviting {len(users_to_invite)} users.")
                            await self._send_request_with_reconnect(
                                user_client, InviteToChannelRequest(channel=new_supergroup.id, users=users_to_invite), account_name
                            )
                        
                        # Ensure all clients have the group entity cached before proceeding
                        successful_clients_meta = []
                        ensure_tasks = [self._ensure_entity_cached(meta['client'], new_supergroup.id, meta['account_name']) for meta in all_clients_meta]
                        results = await asyncio.gather(*ensure_tasks)
                        
                        for idx, meta in enumerate(all_clients_meta):
                            if results[idx]:
                                successful_clients_meta.append(meta)
                            else:
                                LOGGER.warning(f"Account '{meta['account_name']}' failed to cache group entity and will not participate.")
                                # Disconnect clients that failed and are temporary
                                if meta['client'] in temp_clients and meta['client'].is_connected():
                                    await meta['client'].disconnect()


                        if len(successful_clients_meta) < 2:
                             LOGGER.warning(f"Not enough clients ({len(successful_clients_meta)}) could cache the group. Aborting conversation for group {new_supergroup.id}.")
                        else:
                            await self._simulate_conversation(user_id, new_supergroup.id, successful_clients_meta)

                        self._set_group_count(worker_key, current_semester)
                        await self.bot.send_message(user_id, f"📊 [{account_name}] Group '{group_title}' created. ({i+1}/{Config.GROUPS_TO_CREATE})")
                        await asyncio.sleep(random.randint(Config.MIN_SLEEP_SECONDS, Config.MAX_SLEEP_SECONDS))

                    except errors.AuthKeyUnregisteredError as e:
                        LOGGER.error(f"Auth key unregistered for '{account_name}'. Deleting session.")
                        sentry_sdk.capture_exception(e)
                        self.session_manager.delete_session_file(user_id, account_name)
                        await self.bot.send_message(user_id, f"🚨 Session for `{account_name}` revoked. Account removed.")
                        break
                    except Exception as e:
                        LOGGER.error(f"Worker error for {worker_key}", exc_info=True)
                        sentry_sdk.capture_exception(e)
                        await self.bot.send_message(user_id, "❌ Unexpected Error. Check logs.")
                        break
        except asyncio.CancelledError:
            LOGGER.info(f"Task for {worker_key} was cancelled.")
            await self.bot.send_message(user_id, f"⏹️ Operation for `{account_name}` stopped.")
        finally:
            LOGGER.info(f"Worker for {worker_key} finished. Disconnecting clients.")
            if worker_key in self.active_workers:
                del self.active_workers[worker_key]
                self.active_workers_state.pop(worker_key, None)
                self._save_active_workers_state()
            for client in temp_clients:
                if client.is_connected():
                    await client.disconnect()
            if user_client and user_client.is_connected():
                await user_client.disconnect()

    async def _run_scheduled_simulation(self, user_id: int, group_id: int):
        clients_with_meta = []
        clients_to_disconnect = []
        try:
            participant_names = self.conversation_accounts.get(str(user_id), [])
            if len(participant_names) < 2:
                LOGGER.warning(f"[Scheduler] Not enough accounts for user {user_id} to simulate.")
                return

            for acc_name in participant_names:
                session_str = self.session_manager.load_session_string(user_id, acc_name)
                if not session_str: continue
                proxy = self.account_proxies.get(f"{user_id}:{acc_name}")
                client = await self._create_worker_client(session_str, proxy)
                if client:
                    clients_to_disconnect.append(client)
                    me = await client.get_me()
                    p_account_name = me.first_name or me.username or f"ID:{me.id}"
                    clients_with_meta.append({'client': client, 'user_id': user_id, 'account_name': p_account_name})

            if len(clients_with_meta) >= 2:
                await self._simulate_conversation(user_id, group_id, clients_with_meta)
            else:
                LOGGER.warning(f"[Scheduler] Not enough clients could connect for user {user_id}.")

        except Exception as e:
            LOGGER.error(f"[Scheduler] Error for group {group_id}: {e}", exc_info=True)
            sentry_sdk.capture_exception(e)
        finally:
            LOGGER.info(f"[Scheduler] Disconnecting {len(clients_to_disconnect)} clients for group {group_id}.")
            for client in clients_to_disconnect:
                if client.is_connected():
                    await client.disconnect()

    async def on_login_success(self, event: events.NewMessage.Event, user_client: TelegramClient) -> None:
        user_id = event.sender_id
        account_name = self.user_sessions[user_id]['account_name']
        worker_key = f"{user_id}:{account_name}"
        self.session_manager.save_session_string(user_id, account_name, user_client.session.save())
        assigned_proxy = self.user_sessions[user_id].get('login_proxy')
        self.account_proxies[worker_key] = assigned_proxy
        self._save_account_proxies()
        if assigned_proxy:
            proxy_addr = f"{assigned_proxy['addr']}:{assigned_proxy['port']}"
            LOGGER.info(f"Login proxy {proxy_addr} assigned to account '{account_name}'.")
        else:
            LOGGER.info(f"Account '{account_name}' logged in directly and will run without a proxy.")
        if user_client and user_client.is_connected():
            await user_client.disconnect()
            LOGGER.info(f"Login client for user {user_id} ('{account_name}') disconnected successfully.")
        if 'client' in self.user_sessions[user_id]:
            del self.user_sessions[user_id]['client']
        if 'login_proxy' in self.user_sessions[user_id]:
            del self.user_sessions[user_id]['login_proxy']
        self.user_sessions[user_id]['state'] = 'authenticated'
        await self.bot.send_message(user_id, f"✅ Account `{account_name}` added successfully!")
        await self._send_accounts_menu(event)

    async def _start_handler(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        if user_id not in self.known_users and user_id != ADMIN_USER_ID:
            self.user_sessions[user_id] = {'state': 'awaiting_master_password'}
            await event.reply(Config.MSG_PROMPT_MASTER_PASSWORD, buttons=Button.clear())
        else:
            self.user_sessions[user_id] = {'state': 'authenticated'}
            await event.reply(Config.MSG_WELCOME, buttons=self._build_main_menu())
        raise events.StopPropagation

    async def _send_accounts_menu(self, event: events.NewMessage.Event) -> None:
        accounts_keyboard = self._build_accounts_menu(event.sender_id)
        await event.reply(Config.MSG_ACCOUNT_MENU_HEADER, buttons=accounts_keyboard)

    async def _manage_accounts_handler(self, event: events.NewMessage.Event) -> None:
        await self._send_accounts_menu(event)
        raise events.StopPropagation

    async def _server_status_handler(self, event: events.NewMessage.Event) -> None:
        active_count = len(self.active_workers)
        max_workers = Config.MAX_CONCURRENT_WORKERS
        status_text = f"**📊 Server Status**\n\n"
        status_text += f"**Active Workers:** {active_count} / {max_workers}\n"
        if active_count > 0:
            status_text += "\n**Accounts in Operation:**\n"
            for worker_key in self.active_workers.keys():
                _, acc_name = worker_key.split(":", 1)
                proxy_info = self.account_proxies.get(worker_key)
                proxy_str = f" (Proxy: {proxy_info['addr']})" if proxy_info else ""
                status_text += f"- `{acc_name}`{proxy_str}\n"
        else:
            status_text += "\nℹ️ No accounts are currently in operation."
        await event.reply(status_text, buttons=self._build_main_menu())
        raise events.StopPropagation

    async def _help_handler(self, event: events.NewMessage.Event) -> None:
        await event.reply(Config.MSG_HELP_TEXT, buttons=self._build_main_menu())
        raise events.StopPropagation

    async def _set_keywords_handler(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        self.user_sessions[user_id]['state'] = 'awaiting_keywords'
        await event.reply(Config.MSG_PROMPT_KEYWORDS, buttons=[[Button.text(Config.BTN_BACK)]])

    async def _set_conv_accs_handler(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        self.user_sessions[user_id]['state'] = 'awaiting_conv_accounts'
        user_accounts = self.session_manager.get_user_accounts(user_id)
        if user_accounts:
            accounts_list_str = "\n".join(f"- `{acc}`" for acc in user_accounts)
            prompt_message = f"{Config.MSG_PROMPT_CONVERSATION_ACCOUNTS}\n\n**حساب‌های موجود شما:**\n{accounts_list_str}"
        else:
            prompt_message = f"{Config.MSG_PROMPT_CONVERSATION_ACCOUNTS}\n\n**شما هنوز حسابی اضافه نکرده‌اید.**"
        await event.reply(prompt_message, buttons=[[Button.text(Config.BTN_BACK)]])

    async def _join_via_link_handler(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        accounts = self.session_manager.get_user_accounts(user_id)
        if not accounts:
            await event.reply("❌ شما هیچ حسابی برای عضویت ندارید. ابتدا یک حساب اضافه کنید.")
            return

        self.user_sessions[user_id]['state'] = 'awaiting_join_account_selection'
        buttons = [[Button.text(acc)] for acc in accounts]
        buttons.append([Button.text(Config.BTN_BACK)])
        await event.reply(Config.MSG_PROMPT_JOIN_ACCOUNT, buttons=buttons)

    async def _export_links_handler(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        accounts = self.session_manager.get_user_accounts(user_id)
        if not accounts:
            await event.reply("❌ شما هیچ حسابی برای استخراج لینک ندارید. ابتدا یک حساب اضافه کنید.")
            return

        self.user_sessions[user_id]['state'] = 'awaiting_export_account_selection'
        buttons = [[Button.text(acc)] for acc in accounts]
        buttons.append([Button.text(Config.BTN_BACK)])
        await event.reply(Config.MSG_PROMPT_EXPORT_ACCOUNT, buttons=buttons)

    async def _admin_command_handler(self, event: events.NewMessage.Event, handler: callable):
        if event.sender_id != ADMIN_USER_ID:
            await event.reply("❌ You are not authorized to use this command.")
            return
        await handler(event)

    async def _debug_test_proxies_handler(self, event: events.NewMessage.Event) -> None:
        LOGGER.info(f"Admin {event.sender_id} initiated silent proxy test.")
        if not self.proxies:
            LOGGER.debug("Proxy test: No proxies found in file.")
            await self.bot.send_message(event.sender_id, "⚠️ No proxies found in the file to test.")
            return
        await self.bot.send_message(event.sender_id, "🧪 Starting silent proxy test... Results will be in system logs.")
        LOGGER.debug("--- PROXY TEST START ---")
        for proxy in self.proxies:
            proxy_addr = f"{proxy['addr']}:{proxy['port']}"
            client = None
            try:
                device_params = random.choice([{'device_model': 'iPhone 14 Pro Max', 'system_version': '17.5.1'}, {'device_model': 'Samsung Galaxy S24 Ultra', 'system_version': 'SDK 34'}])
                LOGGER.debug(f"Testing proxy: {proxy['addr']} with device: {device_params}")
                client = TelegramClient(StringSession(), API_ID, API_HASH, proxy=proxy, timeout=Config.PROXY_TIMEOUT, **device_params)
                await client.connect()
                if client.is_connected():
                    LOGGER.info(f"  ✅ SUCCESS: {proxy_addr}")
            except Exception as e:
                LOGGER.warning(f"  ❌ FAILURE ({type(e).__name__}): {proxy_addr} - {e}")
            finally:
                if client and client.is_connected():
                    await client.disconnect()
        LOGGER.debug("--- DIRECT CONNECTION TEST ---")
        client = None
        try:
            device_params = random.choice([{'device_model': 'iPhone 14 Pro Max', 'system_version': '17.5.1'}, {'device_model': 'Samsung Galaxy S24 Ultra', 'system_version': 'SDK 34'}])
            LOGGER.debug(f"Testing direct connection with device: {device_params}")
            client = TelegramClient(StringSession(), API_ID, API_HASH, timeout=Config.PROXY_TIMEOUT, **device_params)
            await client.connect()
            if client.is_connected():
                LOGGER.info("  ✅ SUCCESS: Direct Connection")
        except Exception as e:
            LOGGER.warning(f"  ❌ FAILURE ({type(e).__name__}): Direct Connection - {e}")
        finally:
            if client and client.is_connected():
                await client.disconnect()
        LOGGER.info("Silent proxy test finished.")
        await self.bot.send_message(event.sender_id, "🏁 Silent proxy test finished. Check system logs for results.")
        raise events.StopPropagation

    async def _clean_sessions_handler(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        LOGGER.info(f"Admin {user_id} initiated session cleanup.")
        try:
            async with self.bot.conversation(user_id, timeout=30) as conv:
                await conv.send_message("⚠️ **WARNING:** This will delete all user sessions, counters, proxy assignments, and stop all running workers. Please confirm by sending `confirm` within 30 seconds.")
                response = await conv.get_response()
                if response.text.lower() != 'confirm':
                    await conv.send_message("❌ Operation cancelled.")
                    return
        except asyncio.TimeoutError:
            await self.bot.send_message(user_id, "❌ Confirmation timed out. Operation cancelled.")
            return
        msg = await self.bot.send_message(user_id, "🧹 Cleaning sessions and stopping workers...")
        stopped_workers = []
        if self.active_workers:
            LOGGER.info("Stopping all active workers before session cleanup.")
            for worker_key, task in list(self.active_workers.items()):
                task.cancel()
                stopped_workers.append(worker_key.split(":", 1)[1])
            self.active_workers.clear()
            await asyncio.sleep(1)
        report = ["**📝 Cleanup Report:**\n"]
        if stopped_workers:
            report.append(f"⏹️ **Stopped Workers:** {', '.join(f'`{name}`' for name in stopped_workers)}\n")
        deleted_files_count = 0
        if SESSIONS_DIR.exists():
            for item in SESSIONS_DIR.iterdir():
                if item.name != 'bot_session.session':
                    try:
                        if item.is_file():
                            item.unlink()
                            deleted_files_count += 1
                            LOGGER.debug(f"Deleted file: {item.name}")
                    except OSError as e:
                        LOGGER.error(f"Failed to delete file {item}: {e}")
        self.group_counts.clear()
        self.account_proxies.clear()
        self.known_users.clear()
        self.user_keywords.clear()
        self.pending_users.clear()
        self.created_groups.clear()
        self.conversation_accounts.clear()
        self._save_user_keywords()
        self._save_pending_users()
        self._save_created_groups()
        self._save_conversation_accounts()
        report.append(f"🗑️ **Deleted Data Files:** {deleted_files_count} files\n")
        LOGGER.info(f"Deleted {deleted_files_count} data files from {SESSIONS_DIR}.")
        folders_to_clean = ["selenium_sessions", "api_sessions", "telethon_sessions"]
        for folder_name in folders_to_clean:
            folder_path = Path(folder_name)
            if folder_path.exists() and folder_path.is_dir():
                try:
                    shutil.rmtree(folder_path)
                    report.append(f"📁 **Deleted Folder:** `{folder_name}`\n")
                    LOGGER.info(f"Deleted folder: {folder_name}")
                except OSError as e:
                    LOGGER.error(f"Failed to delete folder {folder_path}: {e}")
        report.append("\n✅ Cleanup completed successfully.")
        await msg.edit(''.join(report))
        raise events.StopPropagation

    async def _test_sentry_handler(self, event: events.NewMessage.Event) -> None:
        LOGGER.info(f"Admin {event.sender_id} initiated Sentry test.")
        await event.reply("🧪 Sending a test exception to Sentry. Please check your Sentry dashboard.")
        try:
            1 / 0
        except Exception as e:
            sentry_sdk.capture_exception(e)
            await event.reply("✅ Test exception sent to Sentry!")

    async def _initiate_login_flow(self, event: events.NewMessage.Event) -> None:
        self.user_sessions[event.sender_id] = {'state': 'awaiting_phone'}
        await event.reply('📞 Please send the phone number for the new account in international format (e.g., `+15551234567`).', buttons=Button.clear())

    async def _initiate_selenium_login_flow(self, event: events.NewMessage.Event) -> None:
        await event.reply(Config.MSG_BROWSER_RUNNING)
        await asyncio.sleep(2)
        await self._initiate_login_flow(event)

    async def _message_router(self, event: events.NewMessage.Event) -> None:
        if not isinstance(getattr(event, 'message', None), Message) or not event.message.text:
            return
        text = event.message.text
        user_id = event.sender_id
        if user_id not in self.known_users and user_id != ADMIN_USER_ID:
            if user_id in self.pending_users:
                await event.reply(Config.MSG_AWAITING_APPROVAL)
                return
            await self._handle_master_password(event)
            return
        session = self.user_sessions.get(user_id, {})
        state = session.get('state')

        if text == Config.BTN_BACK and state in ['awaiting_keywords', 'awaiting_conv_accounts', 'awaiting_join_account_selection', 'awaiting_join_link', 'awaiting_export_account_selection']:
            self.user_sessions[user_id]['state'] = 'authenticated'
            await self._start_handler(event)
            return

        if state == 'awaiting_keywords':
            await self._handle_keywords_input(event)
            return
        if state == 'awaiting_conv_accounts':
            await self._handle_conv_accounts_input(event)
            return
        if state == 'awaiting_join_account_selection':
            await self._handle_join_account_selection(event)
            return
        if state == 'awaiting_join_link':
            await self._handle_join_link_input(event)
            return
        if state == 'awaiting_export_account_selection':
            await self._handle_export_account_selection(event)
            return

        login_flow_states = ['awaiting_phone', 'awaiting_code', 'awaiting_password', 'awaiting_account_name']
        if state in login_flow_states:
            if text == Config.BTN_BACK:
                self.user_sessions[user_id]['state'] = 'authenticated'
                await self._send_accounts_menu(event)
                return
            state_map = {
                'awaiting_phone': self._handle_phone_input,
                'awaiting_code': self._handle_code_input,
                'awaiting_password': self._handle_password_input,
                'awaiting_account_name': self._handle_account_name_input
            }
            await state_map[state](event)
            return

        if state != 'authenticated':
            await self._start_handler(event)
            return

        admin_routes = {
            "/debug_proxies": self._debug_test_proxies_handler,
            "/clean_sessions": self._clean_sessions_handler,
            "/test_sentry": self._test_sentry_handler,
        }
        if text in admin_routes:
            await self._admin_command_handler(event, admin_routes[text])
            return

        route_map = {
            Config.BTN_MANAGE_ACCOUNTS: self._manage_accounts_handler,
            Config.BTN_HELP: self._help_handler,
            Config.BTN_BACK: self._start_handler,
            Config.BTN_ADD_ACCOUNT: self._initiate_login_flow,
            Config.BTN_ADD_ACCOUNT_SELENIUM: self._initiate_selenium_login_flow,
            Config.BTN_SERVER_STATUS: self._server_status_handler,
            Config.BTN_SET_KEYWORDS: self._set_keywords_handler,
            Config.BTN_SET_CONVERSATION_ACCOUNTS: self._set_conv_accs_handler,
            Config.BTN_JOIN_VIA_LINK: self._join_via_link_handler,
            Config.BTN_EXPORT_LINKS: self._export_links_handler,
        }
        handler = route_map.get(text)
        if handler:
            await handler(event)
            return

        start_match = re.match(rf"{re.escape(Config.BTN_START_PREFIX)} (.*)", text)
        if start_match:
            await self._start_process_handler(event, start_match.group(1))
            return

        stop_match = re.match(rf"{re.escape(Config.BTN_STOP_PREFIX)} (.*)", text)
        if stop_match:
            await self._cancel_worker_handler(event, stop_match.group(1))
            return

        delete_match = re.match(rf"{re.escape(Config.BTN_DELETE_PREFIX)} (.*)", text)
        if delete_match:
            await self._delete_account_handler(event, delete_match.group(1))
            return

    async def _start_process_handler(self, event: events.NewMessage.Event, account_name: str) -> None:
        user_id = event.sender_id
        worker_key = f"{user_id}:{account_name}"
        if worker_key in self.active_workers:
            await event.reply('⏳ An operation for this account is already in progress.')
            return
        session_str = self.session_manager.load_session_string(user_id, account_name)
        if not session_str:
            await event.reply('❌ No session found for this account. Please delete and add it again.')
            return
        await event.reply(f'🚀 Preparing to start operation for account `{account_name}`...')
        user_client = None
        try:
            assigned_proxy = self.account_proxies.get(worker_key)
            user_client = await self._create_worker_client(session_str, assigned_proxy)
            if not user_client:
                await event.reply(f'❌ Failed to connect to Telegram for account `{account_name}`.')
                return
            if await user_client.is_user_authorized():
                task = asyncio.create_task(self.run_group_creation_worker(user_id, account_name, user_client))
                self.active_workers[worker_key] = task
                self.active_workers_state[worker_key] = {"user_id": user_id, "account_name": account_name}
                self._save_active_workers_state()
                await self._send_accounts_menu(event)
            else:
                self.session_manager.delete_session_file(user_id, account_name)
                self._remove_group_count(worker_key)
                await event.reply(f'⚠️ Session for `{account_name}` has expired. Please add it again.')
        except errors.AuthKeyUnregisteredError as e:
            LOGGER.error(f"Auth key is unregistered for '{account_name}'. Deleting session.")
            sentry_sdk.capture_exception(e)
            self.session_manager.delete_session_file(user_id, account_name)
            self._remove_group_count(worker_key)
            await event.reply(f"🚨 Session for `{account_name}` revoked. Account removed.")
            await self._send_accounts_menu(event)
        except Exception as e:
            LOGGER.error(f"Error starting process for {worker_key}", exc_info=True)
            sentry_sdk.capture_exception(e)
            await event.reply(f'❌ An error occurred while connecting to `{account_name}`.')
        finally:
            if user_client and not self.active_workers.get(worker_key):
                if user_client.is_connected():
                    await user_client.disconnect()

    async def _cancel_worker_handler(self, event: events.NewMessage.Event, account_name: str) -> None:
        user_id = event.sender_id
        worker_key = f"{user_id}:{account_name}"
        if worker_key in self.active_workers:
            task = self.active_workers[worker_key]
            task.cancel()
            LOGGER.info(f"User initiated cancellation for worker {worker_key}.")
            try:
                await task
            except asyncio.CancelledError:
                LOGGER.info(f"Worker task {worker_key} successfully cancelled.")
            await self._send_accounts_menu(event)
        else:
            await event.reply(f"ℹ️ No active operation for `{account_name}`.")

    async def _delete_account_handler(self, event: events.NewMessage.Event, account_name: str) -> None:
        user_id = event.sender_id
        worker_key = f"{user_id}:{account_name}"
        if worker_key in self.active_workers:
            self.active_workers[worker_key].cancel()
            LOGGER.info(f"Worker for {worker_key} cancelled due to account deletion.")
        if self.session_manager.delete_session_file(user_id, account_name):
            self._remove_group_count(worker_key)
            if worker_key in self.account_proxies:
                del self.account_proxies[worker_key]
                self._save_account_proxies()
            await event.reply(f"✅ Account `{account_name}` deleted successfully.")
        else:
            await event.reply(f"✅ Account `{account_name}` removed (session did not exist).")
        await self._send_accounts_menu(event)

    async def _handle_master_password(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        if user_id == ADMIN_USER_ID:
            self.user_sessions[user_id] = {'state': 'authenticated'}
            if user_id not in self.known_users:
                self.known_users.append(user_id)
                self._save_known_users()
            await event.reply(Config.MSG_WELCOME, buttons=self._build_main_menu())
            return
        hashed_input = hashlib.sha256(event.message.text.strip().encode()).hexdigest()
        if hashed_input == MASTER_PASSWORD_HASH:
            if user_id not in self.pending_users:
                self.pending_users.append(user_id)
                self._save_pending_users()
                approval_buttons = [[Button.inline("✅ Approve", f"approve_{user_id}"), Button.inline("❌ Deny", f"deny_{user_id}")]]
                await self.bot.send_message(ADMIN_USER_ID, f"🔔 New user access request from ID: `{user_id}`", buttons=approval_buttons)
            await event.reply(Config.MSG_AWAITING_APPROVAL)
        else:
            await event.reply(Config.MSG_INCORRECT_MASTER_PASSWORD)
        raise events.StopPropagation

    async def _handle_keywords_input(self, event: events.NewMessage.Event) -> None:
        user_id = str(event.sender_id)
        keywords_text = event.message.text.strip()
        if keywords_text:
            keywords = [kw.strip() for kw in keywords_text.split(',')]
            self.user_keywords[user_id] = keywords
            self._save_user_keywords()
            await event.reply(Config.MSG_KEYWORDS_SET, buttons=self._build_main_menu())
        else:
            await event.reply("❌ ورودی نامعتبر است. لطفاً حداقل یک کلمه کلیدی وارد کنید.", buttons=[[Button.text(Config.BTN_BACK)]])
        self.user_sessions[event.sender_id]['state'] = 'authenticated'
        raise events.StopPropagation

    async def _handle_conv_accounts_input(self, event: events.NewMessage.Event) -> None:
        user_id = str(event.sender_id)
        input_text = event.message.text.strip()

        if not input_text:
            self.conversation_accounts[user_id] = []
            self._save_conversation_accounts()
            await event.reply(Config.MSG_CONVERSATION_ACCOUNTS_SET, buttons=self._build_main_menu())
            self.user_sessions[event.sender_id]['state'] = 'authenticated'
            raise events.StopPropagation

        all_user_accounts = self.session_manager.get_user_accounts(int(user_id))
        provided_accounts = [acc.strip() for acc in input_text.split(',')]
        invalid_accounts = [acc for acc in provided_accounts if acc not in all_user_accounts]

        if invalid_accounts:
            await event.reply(f"❌ حساب‌های زیر یافت نشدند یا متعلق به شما نیستند: `{'`, `'.join(invalid_accounts)}`\n\nلطفاً دوباره تلاش کنید.", buttons=[[Button.text(Config.BTN_BACK)]])
            return

        self.conversation_accounts[user_id] = provided_accounts
        self._save_conversation_accounts()
        await event.reply(Config.MSG_CONVERSATION_ACCOUNTS_SET, buttons=self._build_main_menu())
        self.user_sessions[event.sender_id]['state'] = 'authenticated'
        raise events.StopPropagation

    async def _handle_join_account_selection(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        account_name = event.message.text.strip()
        user_accounts = self.session_manager.get_user_accounts(user_id)

        if account_name not in user_accounts:
            await event.reply("❌ حساب انتخاب شده نامعتبر است. لطفاً از دکمه‌ها استفاده کنید.")
            return

        self.user_sessions[user_id]['join_account_name'] = account_name
        self.user_sessions[user_id]['state'] = 'awaiting_join_link'
        await event.reply(Config.MSG_PROMPT_JOIN_LINK_MULTIPLE, buttons=[[Button.text(Config.BTN_BACK)]])

    async def _handle_join_link_input(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        text = event.message.text.strip()
        # Split by newlines or commas and filter out empty strings
        links = [link.strip() for link in re.split(r'[\n,]+', text) if link.strip()]
        
        if not links:
            await event.reply("❌ لینکی وارد نشده است. لطفاً حداقل یک لینک ارسال کنید.")
            return

        account_name = self.user_sessions[user_id].get('join_account_name')
        if not account_name:
            await event.reply("خطای داخلی رخ داده است. لطفاً از ابتدا شروع کنید.", buttons=self._build_main_menu())
            self.user_sessions[user_id]['state'] = 'authenticated'
            return

        session_str = self.session_manager.load_session_string(user_id, account_name)
        if not session_str:
            await event.reply(f"❌ نشست برای حساب `{account_name}` یافت نشد.", buttons=self._build_main_menu())
            self.user_sessions[user_id]['state'] = 'authenticated'
            return

        await event.reply(f"⏳ در حال تلاش برای عضویت حساب `{account_name}` در {len(links)} لینک...")
        
        client = None
        success_count = 0
        fail_count = 0
        fail_details_list = []
        try:
            proxy = self.account_proxies.get(f"{user_id}:{account_name}")
            client = await self._create_worker_client(session_str, proxy)
            if not client:
                await event.reply(f"❌ اتصال به حساب `{account_name}` ناموفق بود.", buttons=self._build_main_menu())
                return

            for i, link in enumerate(links):
                match = re.search(r'(?:t\.me/joinchat/|\+)([a-zA-Z0-9_-]+)', link)
                if not match:
                    fail_count += 1
                    fail_details_list.append(f"- `{link}` (فرمت نامعتبر)")
                    continue
                
                invite_hash = match.group(1)
                try:
                    await client(ImportChatInviteRequest(invite_hash))
                    success_count += 1
                    LOGGER.info(f"Account '{account_name}' successfully joined chat with link {link}.")
                except Exception as e:
                    fail_count += 1
                    fail_details_list.append(f"- `{link}` ({e.__class__.__name__})")
                    LOGGER.warning(f"Account '{account_name}' failed to join {link}: {e}")
                
                # Add a delay to avoid getting limited by Telegram
                if i < len(links) - 1:
                    await asyncio.sleep(random.uniform(5, 15))

            fail_details = "\n".join(fail_details_list) if fail_details_list else "موردی یافت نشد."
            summary_msg = Config.MSG_JOIN_SUMMARY.format(
                account_name=account_name,
                success_count=success_count,
                fail_count=fail_count,
                fail_details=f"**جزئیات خطاها:**\n{fail_details}" if fail_count > 0 else ""
            )
            await event.reply(summary_msg, buttons=self._build_main_menu())

        except Exception as e:
            LOGGER.error(f"Unexpected error during multi-join for '{account_name}': {e}", exc_info=True)
            sentry_sdk.capture_exception(e)
            await event.reply(f"❌ یک خطای پیش‌بینی نشده در حین عملیات رخ داد.", buttons=self._build_main_menu())
        finally:
            if client and client.is_connected():
                await client.disconnect()
            self.user_sessions[user_id]['state'] = 'authenticated'

    async def _handle_export_account_selection(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        account_name = event.message.text.strip()
        
        if account_name not in self.session_manager.get_user_accounts(user_id):
            await event.reply("❌ حساب انتخاب شده نامعتبر است. لطفاً از دکمه‌ها استفاده کنید.")
            return

        await event.reply(Config.MSG_EXPORTING_LINKS.format(account_name=account_name))
        
        worker_key_to_find = f"{user_id}:{account_name}"
        owned_group_ids = [
            int(gid) for gid, data in self.created_groups.items()
            if data.get("owner_worker_key") == worker_key_to_find
        ]

        if not owned_group_ids:
            await event.reply(Config.MSG_EXPORT_FAIL.format(account_name=account_name), buttons=self._build_main_menu())
            self.user_sessions[user_id]['state'] = 'authenticated'
            return

        session_str = self.session_manager.load_session_string(user_id, account_name)
        client = None
        links = []
        try:
            proxy = self.account_proxies.get(worker_key_to_find)
            client = await self._create_worker_client(session_str, proxy)
            if not client:
                await event.reply(f"❌ اتصال به حساب `{account_name}` ناموفق بود.", buttons=self._build_main_menu())
                return

            for group_id in owned_group_ids:
                try:
                    link = await client.export_chat_invite_link(group_id)
                    links.append(link)
                except Exception as e:
                    LOGGER.warning(f"Could not export link for group {group_id} with account {account_name}: {e}")
                    links.append(f"Error exporting for group ID {group_id}: {e.__class__.__name__}")
            
            if links:
                file_path = SESSIONS_DIR / f"invite_links_{account_name}_{user_id}.txt"
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write("\n".join(links))
                
                await self.bot.send_file(user_id, file_path, caption=Config.MSG_EXPORT_SUCCESS)
                os.remove(file_path) # Clean up the file after sending
            else:
                await event.reply(Config.MSG_EXPORT_FAIL.format(account_name=account_name))

        except Exception as e:
            LOGGER.error(f"Unexpected error during link export for '{account_name}': {e}", exc_info=True)
            sentry_sdk.capture_exception(e)
            await event.reply(Config.MSG_EXPORT_FAIL.format(account_name=account_name))
        finally:
            if client and client.is_connected():
                await client.disconnect()
            self.user_sessions[user_id]['state'] = 'authenticated'
            await self._start_handler(event)


    async def _handle_phone_input(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        phone_number = event.message.text.strip()
        if not re.match(r'^\+\d{10,}$', phone_number):
            await event.reply(
                '❌ **Invalid phone number format.**\n'
                'Please enter the full number in international format (e.g., `+15551234567`).',
                buttons=[[Button.text(Config.BTN_BACK)]]
            )
            return
        self.user_sessions[user_id]['phone'] = phone_number
        selected_proxy = self._get_available_proxy()
        self.user_sessions[user_id]['login_proxy'] = selected_proxy
        user_client = None
        try:
            user_client = await self._create_login_client(selected_proxy)
            if not user_client:
                proxy_msg = f" with proxy {selected_proxy['addr']}:{selected_proxy['port']}" if selected_proxy else " directly"
                await event.reply(f'❌ Failed to connect to Telegram{proxy_msg}. Please try again later.')
                return
            self.user_sessions[user_id]['client'] = user_client
            sent_code = await user_client.send_code_request(self.user_sessions[user_id]['phone'])
            self.user_sessions[user_id]['phone_code_hash'] = sent_code.phone_code_hash
            self.user_sessions[user_id]['state'] = 'awaiting_code'
            await event.reply('💬 A login code has been sent. Please send it here.', buttons=[[Button.text(Config.BTN_BACK)]])
        except Exception as e:
            LOGGER.error(f"Phone input error for {user_id}", exc_info=True)
            sentry_sdk.capture_exception(e)
            self.user_sessions[user_id]['state'] = 'awaiting_phone'
            await event.reply(
                '❌ **Error:** Invalid phone number or issue sending code. Please try again with the international format (+countrycode) or cancel.',
                buttons=[[Button.text(Config.BTN_BACK)]]
            )
        finally:
            if user_client and self.user_sessions.get(user_id, {}).get('state') != 'awaiting_code':
                 if user_client.is_connected():
                    await user_client.disconnect()

    async def _handle_code_input(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        user_client = self.user_sessions[user_id]['client']
        try:
            await user_client.sign_in(self.user_sessions[user_id]['phone'], code=event.message.text.strip(), phone_code_hash=self.user_sessions[user_id].get('phone_code_hash'))
            self.user_sessions[user_id]['state'] = 'awaiting_account_name'
            await event.reply('✅ Login successful! Please enter a nickname for this account (e.g., `Main Account` or `Second Number`).', buttons=[[Button.text(Config.BTN_BACK)]])
        except errors.SessionPasswordNeededError:
            self.user_sessions[user_id]['state'] = 'awaiting_password'
            await event.reply('🔑 This account has two-step verification enabled. Please send the password.', buttons=[[Button.text(Config.BTN_BACK)]])
        except errors.PhoneCodeExpiredError:
            try:
                LOGGER.warning(f"Phone code for {user_id} expired. Requesting new code.")
                sent_code = await user_client.send_code_request(self.user_sessions[user_id]['phone'])
                self.user_sessions[user_id]['phone_code_hash'] = sent_code.phone_code_hash
                self.user_sessions[user_id]['state'] = 'awaiting_code'
                await event.reply('⚠️ The code expired. A new code has been sent. Please enter the new code.', buttons=[[Button.text(Config.BTN_BACK)]])
            except Exception as e:
                LOGGER.error(f"Failed to resend code for {user_id} after expiration: {e}", exc_info=True)
                sentry_sdk.capture_exception(e)
                self.user_sessions[user_id]['state'] = 'awaiting_phone'
                await event.reply('❌ **Error:** The previous code expired and resending failed. Please enter the phone number again.', buttons=[[Button.text(Config.BTN_BACK)]])
        except Exception as e:
            LOGGER.error(f"Code input error for {user_id}", exc_info=True)
            sentry_sdk.capture_exception(e)
            self.user_sessions[user_id]['state'] = 'awaiting_phone'
            await event.reply('❌ **Error:** The code is invalid. Please enter the phone number again.', buttons=[[Button.text(Config.BTN_BACK)]])

    async def _handle_password_input(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        try:
            await self.user_sessions[user_id]['client'].sign_in(password=event.message.text.strip())
            self.user_sessions[user_id]['state'] = 'awaiting_account_name'
            await event.reply('✅ Login successful! Please enter a nickname for this account (e.g., `Main Account` or `Second Number`).', buttons=[[Button.text(Config.BTN_BACK)]])
        except Exception as e:
            LOGGER.error(f"Password input error for {user_id}", exc_info=True)
            sentry_sdk.capture_exception(e)
            self.user_sessions[user_id]['state'] = 'awaiting_password'
            await event.reply('❌ **Error:** Incorrect password. Please try again.', buttons=[[Button.text(Config.BTN_BACK)]])

    async def _handle_account_name_input(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        account_name = event.message.text.strip()
        if not account_name:
            await event.reply("❌ Nickname cannot be empty. Please enter a name.", buttons=[[Button.text(Config.BTN_BACK)]])
            return
        if account_name in self.session_manager.get_user_accounts(user_id):
            await event.reply(f"❌ You already have an account with the nickname `{account_name}`. Please choose another name.", buttons=[[Button.text(Config.BTN_BACK)]])
            return
        self.user_sessions[user_id]['account_name'] = account_name
        user_client = self.user_sessions[user_id]['client']
        await self.on_login_success(event, user_client)

    async def _approval_handler(self, event: events.CallbackQuery.Event):
        if event.sender_id != ADMIN_USER_ID:
            await event.answer("You are not authorized to perform this action.")
            return
        data = event.data.decode('utf-8')
        action, user_id_str = data.split('_')
        user_id = int(user_id_str)
        if action == "approve":
            if user_id in self.pending_users:
                self.pending_users.remove(user_id)
                self.known_users.append(user_id)
                self._save_pending_users()
                self._save_known_users()
                await event.edit(f"✅ User `{user_id}` has been approved.")
                await self.bot.send_message(user_id, Config.MSG_USER_APPROVED)
                LOGGER.info(f"Admin approved user {user_id}.")
            else:
                await event.edit(f"⚠️ User `{user_id}` was not found in the pending list.")
        elif action == "deny":
            if user_id in self.pending_users:
                self.pending_users.remove(user_id)
                self._save_pending_users()
                await event.edit(f"❌ User `{user_id}` has been denied.")
                await self.bot.send_message(user_id, Config.MSG_USER_DENIED)
                LOGGER.info(f"Admin denied user {user_id}.")
            else:
                await event.edit(f"⚠️ User `{user_id}` was not found in the pending list.")

    async def _daily_conversation_scheduler(self):
        while True:
            await asyncio.sleep(3600)
            now_ts = datetime.now().timestamp()
            groups_to_simulate = []
            for group_id, data in self.created_groups.items():
                last_simulated_ts = data.get("last_simulated", 0)
                if (now_ts - last_simulated_ts) > 86400:
                    # Find the user_id from the worker key
                    owner_worker_key = data.get("owner_worker_key")
                    if owner_worker_key:
                        owner_user_id_str = owner_worker_key.split(':', 1)[0]
                        groups_to_simulate.append((int(group_id), int(owner_user_id_str)))

            if not groups_to_simulate:
                continue
            LOGGER.info(f"Daily scheduler found {len(groups_to_simulate)} groups needing conversation simulation.")
            for group_id, owner_id in groups_to_simulate:
                asyncio.create_task(self._run_scheduled_simulation(owner_id, group_id))
                self.created_groups[str(group_id)]["last_simulated"] = now_ts
                await asyncio.sleep(5)
            self._save_created_groups()

    def register_handlers(self) -> None:
        self.bot.add_event_handler(self._start_handler, events.NewMessage(pattern='/start'))
        self.bot.add_event_handler(self._message_router, events.NewMessage)
        self.bot.add_event_handler(self._approval_handler, events.CallbackQuery)

    async def run(self) -> None:
        self.register_handlers()
        LOGGER.info("Starting bot...")
        try:
            await self.bot.start(bot_token=BOT_TOKEN)
            LOGGER.info("Bot service started successfully.")
            self.bot.loop.create_task(self._daily_conversation_scheduler())
            for worker_key, worker_data in self.active_workers_state.items():
                user_id = worker_data["user_id"]
                account_name = worker_data["account_name"]
                LOGGER.info(f"Resuming worker for account '{account_name}' after restart.")
                try:
                    await self.bot.send_message(
                        user_id,
                        f"⚠️ The bot has restarted. Please manually start the process again for account `{account_name}` from the 'Manage Accounts' menu."
                    )
                except Exception:
                    pass
            if self.known_users:
                await self._broadcast_message("✅ Bot has started successfully and is now online.")
            await self.bot.run_until_disconnected()
        finally:
            LOGGER.info("Bot service is shutting down. Disconnecting main bot client.")
            if self.bot.is_connected():
                await self.bot.disconnect()


if __name__ == "__main__":
    bot_instance = GroupCreatorBot(SessionManager)
    asyncio.run(bot_instance.run())
