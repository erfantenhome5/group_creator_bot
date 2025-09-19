import asyncio
import hashlib
import json
import logging
import os
import random
import re
import shutil
import traceback
import uuid
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
import sys
import time

import sentry_sdk
from cryptography.fernet import Fernet, InvalidToken
from dotenv import load_dotenv
from sentry_sdk.integrations.logging import LoggingIntegration
from sentry_sdk.types import Event, Hint
from telethon import Button, TelegramClient, errors, events, types, sessions
from telethon.extensions import markdown
from telethon.tl.functions.account import UpdatePasswordSettingsRequest
from telethon.tl.functions.channels import (CreateChannelRequest, GetParticipantRequest,
                                            InviteToChannelRequest, LeaveChannelRequest)
from telethon.tl.functions.messages import (ExportChatInviteRequest,
                                            GetAllStickersRequest,
                                            GetStickerSetRequest,
                                            ImportChatInviteRequest,
                                            SearchStickerSetsRequest)
from telethon.tl.types import (ChannelParticipantCreator, ChannelParticipantsAdmins,
                               InputStickerSetID, InputStickerSetShortName, Message,
                               PeerChannel)

# --- Basic Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
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
    """[MODIFIED] Loads proxies from a file, now supporting IP:PORT and IP:PORT:USER:PASS formats."""
    proxy_list = []
    try:
        with open(proxy_file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                
                parts = line.split(':')
                proxy_info = {'proxy_type': 'http'}

                try:
                    if len(parts) == 2:
                        proxy_info['addr'] = parts[0]
                        proxy_info['port'] = int(parts[1])
                    elif len(parts) == 4:
                        proxy_info['addr'] = parts[0]
                        proxy_info['port'] = int(parts[1])
                        proxy_info['username'] = parts[2]
                        proxy_info['password'] = parts[3]
                    else:
                        LOGGER.warning(f"Skipping malformed proxy line: {line}. Expected IP:PORT or IP:PORT:USER:PASS.")
                        continue
                    
                    proxy_list.append(proxy_info)

                except (ValueError, IndexError):
                    LOGGER.warning(f"Skipping malformed proxy line: {line}. Could not parse correctly.")
        LOGGER.info(f"Loaded {len(proxy_list)} proxies from {proxy_file_path}.")
    except FileNotFoundError:
        LOGGER.warning(f"Proxy file '{proxy_file_path}' not found.")
    return proxy_list

# --- Centralized Configuration ---
class Config:
    """Holds all configurable values and UI strings for the bot."""
    # Bot Settings
    MAX_CONCURRENT_WORKERS = 50
    GROUPS_TO_CREATE = 50
    MIN_SLEEP_SECONDS = 144
    MAX_SLEEP_SECONDS = 288
    PROXY_FILE = "proxy.txt"
    PROXY_TIMEOUT = 15
    DAILY_MESSAGE_LIMIT_PER_GROUP = 20
    MESSAGE_SEND_DELAY_MIN = 1
    MESSAGE_SEND_DELAY_MAX = 5
    GROUP_HEALTH_CHECK_INTERVAL_SECONDS = 604800 # 7 days

    RANDOM_MESSAGES = [
        "سلام دوستان!", "چه خبر؟", "کسی اینجا هست؟", "🤔", "👍", "عالیه!",
        "موافقم.", "جالبه.", "چه روز خوبی!", "امیدوارم همگی خوب باشید.",
        "کسی نظری نداره؟", "من برگشtem.", "موضوع بحث چیه؟", "خیلی جالبه!",
        "بعدا صحبت می کنیم.",
    ]

    USER_AGENTS = [
        {'device_model': 'iPhone 15 Pro Max', 'system_version': '17.5.1'},
        {'device_model': 'Samsung Galaxy S24 Ultra', 'system_version': 'SDK 34'},
        {'device_model': 'iPhone 14 Pro', 'system_version': '17.4.1'},
        {'device_model': 'Google Pixel 8 Pro', 'system_version': 'SDK 34'},
        {'device_model': 'Samsung Galaxy Z Fold 5', 'system_version': 'SDK 33'},
        {'device_model': 'iPhone 13', 'system_version': '16.6'},
        {'device_model': 'Xiaomi 13T Pro', 'system_version': 'SDK 33'}
    ]

    # --- UI Text & Buttons (All in Persian) ---
    BTN_MANAGE_ACCOUNTS = "👤 مدیریت حساب‌ها"
    BTN_SERVER_STATUS = "📊 وضعیت سرور"
    BTN_HELP = "ℹ️ راهنما"
    BTN_SETTINGS = "⚙️ تنظیمات"
    BTN_ADD_ACCOUNT = "➕ افزودن حساب (API)"
    BTN_ADD_ACCOUNT_SELENIUM = "✨ افزودن حساب (مرورگر امن)"
    BTN_BACK = "⬅️ بازگشت"
    BTN_START_PREFIX = "🟢 شروع برای"
    BTN_STOP_PREFIX = "⏹️ توقف برای"
    BTN_DELETE_PREFIX = "🗑️ حذف"
    BTN_SET_STICKERS = "🎨 تنظیم استیکرها"
    BTN_SET_CONVERSATION_ACCOUNTS = "🗣️ تنظیم حساب‌های گفتگو"
    BTN_JOIN_VIA_LINK = "🔗 عضویت با لینک"
    BTN_EXPORT_LINKS = "🔗 صدور لینک‌های گروه"
    BTN_FORCE_CONVERSATION = "💬 شروع مکالمه دستی"
    BTN_STOP_FORCE_CONVERSATION = "⏹️ توقف مکالمه دستی"
    BTN_MANUAL_HEALTH_CHECK = "🩺 بررسی سلامت گروه‌ها"
    BTN_MESSAGE_ALL_GROUPS = "💬 پیام دار کردن همه گروه ها"
    BTN_GET_CODE = "📲 دریافت کد"
    BTN_CHANGE_2FA_YES = "✅ بله، تغییر بده"
    BTN_CHANGE_2FA_NO = "❌ خیر، دست نزن"

    # --- Messages (All in Persian) ---
    MSG_WELCOME = "**🤖 به ربات سازنده گروه خوش آمدید!**"
    MSG_ACCOUNT_MENU_HEADER = "👤 **مدیریت حساب‌ها**\n\nاز این منو می‌توانید حساب‌های خود را مدیریت کرده و عملیات ساخت گروه را برای هرکدام آغاز یا متوقف کنید."
    MSG_HELP_TEXT = (
        "**راهنمای جامع ربات**\n\n"
        "این ربات به شما اجازه می‌دهد تا با چندین حساب تلگرام به صورت همزمان گروه‌های جدید بسازید.\n\n"
        "**دستورات ادمین:**\n"
        "- `/broadcast [message]`: ارسال پیام همگانی به تمام کاربران.\n"
        "- `/set_user_limit [user_id] [limit]`: تنظیم محدودیت ورکر برای یک کاربر.\n"
        "- `/export_all_links`: دریافت فایل متنی حاوی لینک تمام گروه‌های ساخته شده.\n"
        "- `/send_random_links [count]`: ارسال تعدادی لینک گروه تصادفی به یک کاربر تصادفی.\n\n"
        f"**{BTN_MANAGE_ACCOUNTS}**\n"
        "در این بخش می‌توانید حساب‌های خود را مدیریت کنید:\n"
        f"  - `{BTN_ADD_ACCOUNT}`: یک شماره تلفن جدید با روش API اضافه کنید.\n"
        f"  - `{BTN_ADD_ACCOUNT_SELENIUM}`: یک شماره تلفن جدید با روش شبیه‌سازی مرورگر اضافه کنید (امنیت بالاتر).\n"
        f"  - `{BTN_START_PREFIX} [نام حساب]`: عملیات ساخت گروه را برای حساب مشخص شده آغاز می‌کند.\n"
        f"  - `{BTN_STOP_PREFIX} [نام حساب]`: عملیات در حال اجرا برای یک حساب را متوقف می‌کند.\n"
        f"  - `{BTN_DELETE_PREFIX} [نام حساب]`: یک حساب و تمام اطلاعات آن را برای همیشه حذف می‌کند.\n\n"
        f"**{BTN_GET_CODE}**\n"
        "کد ورود به یکی از حساب‌های ذخیره شده خود را دریافت کنید. این برای زمانی مفید است که می‌خواهید با آن حساب در دستگاه دیگری وارد شوید.\n\n"
        f"**{BTN_JOIN_VIA_LINK}**\n"
        "یکی از حساب‌های خود را با استفاده از لینک دعوت در یک یا چند گروه/کانال عضو کنید.\n\n"
        f"**{BTN_EXPORT_LINKS}**\n"
        "لینک‌های دعوت تمام گروه‌هایی که توسط یک حساب خاص ساخته شده را دریافت کنید.\n\n"
        f"**{BTN_FORCE_CONVERSATION}**\n"
        "مکالمه را به صورت دستی در تمام گروه‌های ساخته شده توسط یک حساب خاص فعال کنید.\n\n"
        f"**{BTN_STOP_FORCE_CONVERSATION}**\n"
        "یک مکالمه دستی در حال اجرا را متوقف کنید.\n\n"
        f"**{BTN_SET_STICKERS}**\n"
        "بسته‌های استیکر مورد علاقه خود را برای استفاده در گفتگوها تنظیم کنید.\n\n"
        f"**{BTN_SET_CONVERSATION_ACCOUNTS}**\n"
        "حساب‌هایی که باید در گروه‌های جدید به گفتگو بپردازند را مشخص کنید.\n\n"
        f"**{BTN_SERVER_STATUS}**\n"
        "این گزینه اطلاعات لحظه‌ای درباره وضعیت ربات را نمایش می‌دهد.\n\n"
        f"**{BTN_MANUAL_HEALTH_CHECK} (Admin Only)**\n"
        "این گزینه یک بررسی کامل و فوری روی تمام گروه‌های ساخته شده انجام می‌دهد تا از سلامت آنها اطمینان حاصل شود."
    )
    MSG_PROMPT_MASTER_PASSWORD = "🔑 لطفاً برای دسترسی به ربات، رمز عبور اصلی را وارد کنید:"
    MSG_INCORRECT_MASTER_PASSWORD = "❌ رمز عبور اشتباه است. لطفاً دوباره تلاش کنید."
    MSG_BROWSER_RUNNING = "⏳ در حال آماده‌سازی مرورگر امن... این کار ممکن است چند لحظه طول بکشد."
    MSG_PROMPT_STICKERS = "🎨 لطفاً نام کوتاه یک یا چند بسته استیکر را وارد کنید. نام‌ها را با کاما (,) از هم جدا کنید.\n\nمثال: AnimatedStickers, Cats"
    MSG_STICKERS_SET = "✅ بسته‌های استیکر شما با موفقیت ذخیره شد."
    MSG_PROMPT_CONVERSATION_ACCOUNTS = "🗣️ لطفاً نام مستعار حساب‌هایی که می‌خواهید در گفتگوها شرکت کنند را وارد کنید. نام‌ها را با کاما (,) از هم جدا کنید.\n\nاین حساب‌ها در گروه‌های جدید ساخته شده با یکدیگر گفتگو خواهند کرد. برای غیرفعال کردن این ویژگی، این بخش را خالی بگذارید."
    MSG_CONVERSATION_ACCOUNTS_SET = "✅ حساب‌های گفتگو با موفقیت ذخیره شدند."
    MSG_AWAITING_APPROVAL = "⏳ درخواست دسترسی شما برای ادمین ارسال شد. لطفاً منتظر تایید بمانید."
    MSG_USER_APPROVED = "✅ درخواست شما تایید شد! برای شروع /start را بزنید."
    MSG_USER_DENIED = "❌ متاسفانه درخواست دسترسی شما رد شد."
    MSG_PROMPT_JOIN_ACCOUNT = "👤 لطفاً حسابی که می‌خواهید با آن عضو شوید را انتخاب کنید:"
    MSG_PROMPT_EXPORT_ACCOUNT = "📤 لطفاً حسابی که می‌خواهید لینک‌های آن را استخراج کنید، انتخاب نمایید:"
    MSG_PROMPT_FORCE_CONV_ACCOUNT = "💬 لطفاً حسابی که گروه‌ها را ساخته است، انتخاب کنید تا مکالمه در آنها فعال شود:"
    MSG_PROMPT_NUM_MESSAGES = "🔢 لطفاً تعداد پیام‌هایی که می‌خواهید ارسال شود را وارد کنید (مثلاً: 20):"
    MSG_PROMPT_STOP_FORCE_CONV = "⛔️ کدام مکالمه دستی را می‌خواهید متوقف کنید؟"
    MSG_NO_ACTIVE_FORCE_CONV = "ℹ️ در حال حاضر هیچ مکالمه دستی فعالی وجود ندارد."
    MSG_PROMPT_JOIN_LINK_MULTIPLE = "🔗 لطفاً یک یا چند لینک دعوت را ارسال کنید. هر لینک را در یک خط جدید وارد کنید:"
    MSG_JOIN_SUMMARY = "🏁 **گزارش عضویت برای `{account_name}`:**\n\n✅ **موفق:** {success_count}\n❌ **ناموفق:** {fail_count}\n\n{fail_details}"
    MSG_EXPORTING_LINKS = "⏳ در حال استخراج لینک‌های دعوت برای حساب `{account_name}`... این عملیات ممکن است کمی طول بکشد."
    MSG_EXPORT_SUCCESS = "✅ لینک‌های دعوت با موفقیت استخراج شدند و در فایل زیر برای شما ارسال شد."
    MSG_EXPORT_FAIL = "❌ خطایی در استخراج لینک‌ها رخ داد یا این حساب گروهی نساخته است."
    MSG_FORCE_CONV_STARTED = "✅ فعال‌سازی مکالمه در {count} گروه متعلق به `{account_name}` آغاز شد."
    MSG_FORCE_CONV_STOPPED = "✅ مکالمه دستی برای حساب `{account_name}` متوقف شد."
    MSG_FORCE_CONV_NO_GROUPS = "ℹ️ هیچ گروهی برای فعال‌سازی مکالمه توسط حساب `{account_name}` یافت نشد."
    MSG_HEALTH_CHECK_STARTED = "🩺 بررسی سلامت گروه‌ها آغاز شد... این عملیات در پس‌زمینه انجام می‌شود و ممکن است زمان‌بر باشد. گزارش نهایی پس از اتمام ارسال خواهد شد."
    MSG_HEALTH_CHECK_COMPLETE = "✅ بررسی سلامت گروه‌ها به پایان رسید.\n\n🔧 **گروه‌های تعمیر شده:** {healed_count}\n👥 **گروه‌های پاکسازی شده:** {cleaned_count}\n💬 **گروه‌هایی که پیام دریافت کردند:** {topped_up_count}\n\nبرای جزئیات بیشتر به لاگ‌ها مراجعه کنید."
    MSG_MAINTENANCE_ACTIVE = "⏳ ربات در حال حاضر تحت عملیات بررسی و نگهداری است. لطفاً چند دقیقه دیگر دوباره امتحان کنید."
    MSG_MAINTENANCE_BROADCAST_START = "🔧 **اطلاعیه:** ربات برای بررسی و نگهداری دوره‌ای موقتاً با محدودیت در دسترس خواهد بود. از صبر شما متشکریم."
    MSG_MAINTENANCE_BROADCAST_END = "✅ **اطلاعیه:** عملیات نگهداری ربات به پایان رسید. تمام قابلیت‌ها اکنون در دسترس هستند."
    MSG_MESSAGE_ALL_GROUPS_STARTED = "✅ عملیات ارسال پیام به تمام گروه‌ها آغاز شد. این فرآیند در پس‌زمینه اجرا می‌شود و ممکن است بسیار زمان‌بر باشد."
    MSG_MESSAGE_ALL_GROUPS_COMPLETE = "🏁 عملیات ارسال پیام به تمام گروه‌ها به پایان رسید.\n\n👥 **اکانت‌های پردازش شده:** {accounts_processed}\n💬 **مجموع پیام‌های ارسال شده:** {total_messages_sent}"

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

    def get_all_accounts(self) -> Dict[str, int]:
        """Returns a dictionary of all accounts across all users."""
        all_accounts = {}
        for user_dir in self._user_sessions_dir.iterdir():
            if user_dir.is_dir():
                try:
                    user_id = int(user_dir.name)
                    accounts = [f.stem for f in user_dir.glob("*.session")]
                    for acc_name in accounts:
                        all_accounts[f"{user_id}:{acc_name}"] = user_id
                except ValueError:
                    continue
        return all_accounts

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
        self.active_conversations: Dict[str, asyncio.Task] = {}
        self.active_dm_chats: Dict[str, asyncio.Task] = {}
        self.suggested_code: Optional[str] = None
        self.health_check_lock = asyncio.Lock()
        self.message_all_lock = asyncio.Lock()
        self.config_file = SESSIONS_DIR / "config.json"
        self.config = self._load_json_file(self.config_file, {})
        self.update_config_from_file()

        self.worker_semaphore = asyncio.Semaphore(self.config.get("MAX_CONCURRENT_WORKERS", 50))
        
        self.counts_file = SESSIONS_DIR / "group_counts.json"
        self.group_counts = self._load_group_counts()
        self.daily_counts_file = SESSIONS_DIR / "daily_counts.json"
        self.daily_counts = self._load_daily_counts()
        self.proxies = load_proxies_from_file(self.config.get("PROXY_FILE", "proxy.txt"))
        
        self.account_proxy_file = SESSIONS_DIR / "account_proxies.json"
        self.account_proxies = self._load_account_proxies()
        self.known_users_file = SESSIONS_DIR / "known_users.json"
        self.known_users = self._load_known_users()
        self.banned_users_file = SESSIONS_DIR / "banned_users.json"
        self.banned_users = self._load_banned_users()
        self.pending_users_file = SESSIONS_DIR / "pending_users.json"
        self.pending_users = self._load_pending_users()
        self.created_groups_file = SESSIONS_DIR / "created_groups.json"
        self.created_groups = self._load_created_groups()
        self.active_workers_file = SESSIONS_DIR / "active_workers.json"
        self.active_workers_state = self._load_active_workers_state()
        self.user_sticker_packs_file = SESSIONS_DIR / "user_sticker_packs.json"
        self.user_sticker_packs = self._load_user_sticker_packs()
        self.conversation_accounts_file = SESSIONS_DIR / "conversation_accounts.json"
        self.conversation_accounts = self._load_conversation_accounts()
        self.user_worker_limits_file = SESSIONS_DIR / "user_worker_limits.json"
        self.user_worker_limits = self._load_user_worker_limits()
        self.sticker_sets: Dict[str, Any] = {}
        try:
            fernet = Fernet(ENCRYPTION_KEY.encode())
            self.session_manager = session_manager(fernet, SESSIONS_DIR)
        except (ValueError, TypeError):
            raise ValueError("Invalid ENCRYPTION_KEY. Please generate a valid key.")

    def update_config_from_file(self):
        """Update runtime config attributes from the loaded JSON."""
        self.max_workers = self.config.get("MAX_CONCURRENT_WORKERS", Config.MAX_CONCURRENT_WORKERS)
        self.groups_to_create = self.config.get("GROUPS_TO_CREATE", Config.GROUPS_TO_CREATE)
        self.min_sleep_seconds = self.config.get("MIN_SLEEP_SECONDS", Config.MIN_SLEEP_SECONDS)
        self.max_sleep_seconds = self.config.get("MAX_SLEEP_SECONDS", Config.MAX_SLEEP_SECONDS)
        self.proxy_timeout = self.config.get("PROXY_TIMEOUT", Config.PROXY_TIMEOUT)
        self.daily_message_limit = self.config.get("DAILY_MESSAGE_LIMIT_PER_GROUP", Config.DAILY_MESSAGE_LIMIT_PER_GROUP)
        self.master_password_hash = self.config.get("MASTER_PASSWORD_HASH", os.getenv("MASTER_PASSWORD_HASH"))
        self.health_check_interval = self.config.get("GROUP_HEALTH_CHECK_INTERVAL_SECONDS", Config.GROUP_HEALTH_CHECK_INTERVAL_SECONDS)

    async def _initialize_sentry(self):
        """Initializes Sentry for error reporting, tracing, and logging."""
        sentry_dsn = self.config.get("SENTRY_DSN", SENTRY_DSN)
        if not sentry_dsn:
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
            "dsn": sentry_dsn,
            "integrations": [sentry_logging],
            "traces_sample_rate": 1.0,
            "_experiments": {
                "enable_logs": True,
            },
            "before_send": before_send_hook,
        }
        
        sentry_proxy = random.choice(self.proxies) if self.proxies else None
        if sentry_proxy:
            if 'username' in sentry_proxy and 'password' in sentry_proxy:
                proxy_url = (
                    f"http://{sentry_proxy['username']}:{sentry_proxy['password']}"
                    f"@{sentry_proxy['addr']}:{sentry_proxy['port']}"
                )
            else:
                proxy_url = f"http://{sentry_proxy['addr']}:{sentry_proxy['port']}"
            
            sentry_options["http_proxy"] = proxy_url
            sentry_options["https_proxy"] = proxy_url
            LOGGER.info(f"Sentry will use proxy: {sentry_proxy['addr']}:{sentry_proxy['port']}")
        else:
            LOGGER.info("Sentry will not use a proxy (none found).")

        sentry_sdk.init(**sentry_options)
        LOGGER.info("Sentry initialized for error reporting, tracing, and logging.")

    def _ensure_session(self, user_id: int):
        """
        Ensures a session dictionary exists for the given user_id.
        If a session doesn't exist, it initializes one with a default state.
        """
        if user_id not in self.user_sessions:
            LOGGER.info(f"No session found for user {user_id}. Initializing a new one.")
            if user_id == ADMIN_USER_ID or user_id in self.known_users:
                self.user_sessions[user_id] = {'state': 'authenticated'}
            else:
                self.user_sessions[user_id] = {'state': 'awaiting_master_password'}

    # --- Proxy Helpers ---
    def _load_account_proxies(self) -> Dict[str, Dict]:
        return self._load_json_file(self.account_proxy_file, {})

    def _save_account_proxies(self) -> None:
        self._save_json_file(self.account_proxies, self.account_proxy_file)

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
            with file_path.open("r", encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            LOGGER.error(f"Could not read or parse {file_path.name}. Starting with empty data.")
            return default_type

    def _save_json_file(self, data: Any, file_path: Path) -> None:
        try:
            with file_path.open("w", encoding='utf-8') as f:
                json.dump(data, f, indent=4)
        except IOError:
            LOGGER.error(f"Could not save {file_path.name}.")

    def _load_group_counts(self) -> Dict[str, int]:
        return self._load_json_file(self.counts_file, {})

    def _save_group_counts(self) -> None:
        self._save_json_file(self.group_counts, self.counts_file)
    
    def _load_banned_users(self) -> List[int]:
        return self._load_json_file(self.banned_users_file, [])

    def _save_banned_users(self) -> None:
        self._save_json_file(self.banned_users, self.banned_users_file)

    def _load_daily_counts(self) -> Dict[str, Any]:
        today_str = str(datetime.utcnow().date())
        data = self._load_json_file(self.daily_counts_file, {"date": today_str, "groups": {}})
        if data.get("date") != today_str:
            LOGGER.info("New day detected, resetting daily message counts for all groups.")
            data = {"date": today_str, "groups": {}}
            self._save_json_file(data, self.daily_counts_file)
        return data

    def _get_daily_count_for_group(self, group_id: int) -> int:
        today_str = str(datetime.utcnow().date())
        if self.daily_counts.get("date") != today_str:
            self.daily_counts = self._load_daily_counts()
        return self.daily_counts.get("groups", {}).get(str(group_id), 0)

    def _increment_daily_count_for_group(self, group_id: int):
        count = self._get_daily_count_for_group(group_id)
        group_id_str = str(group_id)
        if "groups" not in self.daily_counts:
            self.daily_counts["groups"] = {}
        self.daily_counts["groups"][group_id_str] = count + 1
        self._save_json_file(self.daily_counts, self.daily_counts_file)

    def _load_user_sticker_packs(self) -> Dict[str, List[str]]:
        return self._load_json_file(self.user_sticker_packs_file, {})

    def _save_user_sticker_packs(self) -> None:
        self._save_json_file(self.user_sticker_packs, self.user_sticker_packs_file)

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

    def _load_user_worker_limits(self) -> Dict[str, int]:
        return self._load_json_file(self.user_worker_limits_file, {})

    def _save_user_worker_limits(self) -> None:
        self._save_json_file(self.user_worker_limits, self.user_worker_limits_file)

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
            except (errors.UserIsBlockedError, errors.InputUserDeactivatedError, errors.rpcerrorlist.UserIsBotError):
                LOGGER.warning(f"User {user_id} has blocked the bot, is deactivated, or is a bot. Cannot send message.")
            except Exception as e:
                LOGGER.error(f"Error sending message to {user_id}: {e}")

    async def _set_user_limit_handler(self, event: events.NewMessage.Event, user_id: int, limit: int):
        """Sets the concurrent worker limit for a specific user."""
        if event.sender_id != ADMIN_USER_ID:
            return
        
        if limit <= 0:
            await event.reply("❌ Limit must be a positive number.")
            return

        self.user_worker_limits[str(user_id)] = limit
        self._save_user_worker_limits()
        await event.reply(f"✅ Worker limit for user `{user_id}` has been set to `{limit}`.")
      

    async def _create_login_client(self, proxy: Optional[Dict]) -> Optional[TelegramClient]:
        session = sessions.StringSession()
        device_params = random.choice(Config.USER_AGENTS)

        try:
            proxy_info = f"with proxy {proxy['addr']}:{proxy['port']}" if proxy else "without proxy (direct connection)"
            LOGGER.debug(f"Attempting login connection {proxy_info}")
            client = TelegramClient(session, API_ID, API_HASH, proxy=proxy, timeout=self.proxy_timeout, **device_params)
            client.parse_mode = CustomMarkdown()
            await client.connect()
            return client
        except Exception as e:
            LOGGER.error(f"Login connection {proxy_info} failed: {e}")
            return None

    async def _create_resilient_login_client(self) -> (Optional[TelegramClient], Optional[Dict]):
        """[NEW] Tries to connect for login using available proxies, falling back to direct connection."""
        proxies_to_try = self.proxies[:]  # Create a copy
        random.shuffle(proxies_to_try)
        
        # Add None to the end to try direct connection last
        proxies_to_try.append(None)

        LOGGER.info(f"Attempting login. Trying up to {len(proxies_to_try)} connection methods.")

        for i, proxy in enumerate(proxies_to_try):
            proxy_info_str = f"proxy {proxy['addr']}:{proxy['port']}" if proxy else "a direct connection"
            LOGGER.info(f"Login attempt {i + 1}/{len(proxies_to_try)} using {proxy_info_str}...")

            client = await self._create_login_client(proxy)
            if client and client.is_connected():
                LOGGER.info(f"Login connection successful using {proxy_info_str}.")
                return client, proxy

        LOGGER.error("All login attempts failed.")
        return None, None

    async def _create_worker_client(self, session_string: str, proxy: Optional[Dict]) -> Optional[TelegramClient]:
        session = sessions.StringSession(session_string)
        device_params = random.choice(Config.USER_AGENTS)

        client = TelegramClient(
            session, API_ID, API_HASH, proxy=proxy, timeout=self.proxy_timeout,
            device_model=device_params['device_model'], system_version=device_params['system_version']
        )
        client.parse_mode = CustomMarkdown()

        try:
            proxy_info = f"with proxy {proxy['addr']}:{proxy['port']}" if proxy else "without proxy"
            LOGGER.debug(f"Attempting worker connection {proxy_info}")
            await client.connect()
            LOGGER.info(f"Worker connected successfully {proxy_info}")
            return client
        except errors.AuthKeyUnregisteredError:
            raise
        except Exception as e:
            LOGGER.error(f"Worker connection {proxy_info} failed: {e}")
            sentry_sdk.capture_exception(e)
            return None

    async def _create_resilient_worker_client(self, user_id: int, account_name: str, session_string: str) -> Optional[TelegramClient]:
        """[MODIFIED] Tries to connect using proxies, and after 3 failures, attempts a direct connection."""
        worker_key = f"{user_id}:{account_name}"
        
        # Get all available proxies and shuffle them
        potential_proxies = self.proxies[:]
        random.shuffle(potential_proxies)

        # Prioritize the currently assigned proxy if it exists
        assigned_proxy = self.account_proxies.get(worker_key)
        if assigned_proxy:
            # Move assigned proxy to the front of the list to try it first
            try:
                # Find and move the exact proxy object if it's in the list
                idx = -1
                for i, p in enumerate(potential_proxies):
                    if p['addr'] == assigned_proxy['addr'] and p['port'] == assigned_proxy['port']:
                        idx = i
                        break
                if idx != -1:
                    potential_proxies.insert(0, potential_proxies.pop(idx))
                else: # if not found (e.g., from an old proxy list), just add it to the front
                    potential_proxies.insert(0, assigned_proxy)
            except Exception:
                potential_proxies.insert(0, assigned_proxy)

        proxies_to_try = potential_proxies
        
        LOGGER.info(f"Attempting connection for '{account_name}'. Trying up to {len(proxies_to_try)} proxies before direct connection.")

        failed_attempts = 0
        
        # Try available proxies, up to a limit of 3 failures
        for proxy in proxies_to_try:
            if failed_attempts >= 3:
                LOGGER.warning(f"[{account_name}] Reached {failed_attempts} failed proxy attempts. Now trying a direct connection.")
                break

            proxy_info_str = f"{proxy['addr']}:{proxy['port']}"
            LOGGER.info(f"[{account_name}] Proxy attempt {failed_attempts + 1}/3 using {proxy_info_str}...")

            client = await self._create_worker_client(session_string, proxy)
            
            if client and client.is_connected():
                LOGGER.info(f"[{account_name}] Successfully connected using proxy {proxy_info_str}.")
                
                # Check if the successful proxy is different from the assigned one before saving
                is_different = True
                if assigned_proxy:
                    if assigned_proxy['addr'] == proxy['addr'] and assigned_proxy['port'] == proxy['port']:
                        is_different = False
                
                if is_different:
                    LOGGER.info(f"Updating assigned proxy for '{account_name}' to {proxy_info_str}.")
                    self.account_proxies[worker_key] = proxy
                    self._save_account_proxies()
                
                return client
            else:
                failed_attempts += 1

        # If all proxy attempts failed or we hit the limit, try a direct connection
        LOGGER.info(f"[{account_name}] All proxies failed or limit reached. Trying a direct connection...")
        client = await self._create_worker_client(session_string, None)
        if client and client.is_connected():
            LOGGER.info(f"[{account_name}] Successfully connected using a direct connection.")
            
            # Since direct connection worked, we can clear any failed proxy assignment
            if self.account_proxies.get(worker_key) is not None:
                LOGGER.info(f"Removing failed proxy assignment for '{account_name}'.")
                self.account_proxies[worker_key] = None
                self._save_account_proxies()
            return client

        LOGGER.error(f"[{account_name}] All connection attempts (proxies and direct) failed.")
        return None

    # ---------- MODIFICATION START: Resilient Login Client ----------
    async def _create_resilient_login_client(self, user_id: int) -> tuple[Optional[TelegramClient], Optional[Dict]]:
        """Tries to connect for login using proxies, and after failures, attempts a direct connection."""
        
        potential_proxies = self.proxies[:]
        random.shuffle(potential_proxies)
        
        # We will also try a direct connection at the end
        proxies_to_try = potential_proxies + [None] 
        
        LOGGER.info(f"Attempting login for user {user_id}. Trying up to {len(proxies_to_try)} connection methods.")

        for i, proxy in enumerate(proxies_to_try):
            proxy_info_str = f"{proxy['addr']}:{proxy['port']}" if proxy else "a direct connection"
            LOGGER.info(f"[Login User {user_id}] Connection attempt {i+1}/{len(proxies_to_try)} using {proxy_info_str}...")

            client = await self._create_login_client(proxy)
            
            if client and client.is_connected():
                LOGGER.info(f"[Login User {user_id}] Successfully connected using {proxy_info_str}.")
                return client, proxy

        LOGGER.error(f"[Login User {user_id}] All login connection attempts failed.")
        return None, None
    # ---------- MODIFICATION END: Resilient Login Client ----------

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
            [Button.text(Config.BTN_FORCE_CONVERSATION), Button.text(Config.BTN_STOP_FORCE_CONVERSATION)],
            [Button.text(Config.BTN_SET_STICKERS), Button.text(Config.BTN_SET_CONVERSATION_ACCOUNTS)],
            [Button.text(Config.BTN_SERVER_STATUS), Button.text(Config.BTN_HELP)],
            [Button.text(Config.BTN_SETTINGS)]
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
        keyboard.append([Button.text(Config.BTN_GET_CODE)])
        keyboard.append([
            Button.text(Config.BTN_ADD_ACCOUNT),
            Button.text(Config.BTN_ADD_ACCOUNT_SELENIUM)
        ])
        keyboard.append([Button.text(Config.BTN_BACK)])
        return keyboard

    def _prepare_spoiler_text(self, text: str) -> str:
        """Converts ||spoiler|| syntax to [spoiler](spoiler) for custom markdown."""
        return re.sub(r'\|\|(.*?)\|\|', r'[\1](spoiler)', text)
    
    def _format_time_delta(self, seconds: float) -> str:
        """Formats a duration in seconds into a human-readable string."""
        if seconds < 0:
            return "0s"
        seconds = int(seconds)
        days, remainder = divmod(seconds, 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        parts = []
        if days > 0:
            parts.append(f"{days}d")
        if hours > 0:
            parts.append(f"{hours}h")
        if minutes > 0:
            parts.append(f"{minutes}m")
        if seconds > 0 or not parts:
            parts.append(f"{seconds}s")
            
        return " ".join(parts)

    async def _get_random_sticker(self, client: TelegramClient, user_id: int) -> Optional[types.Document]:
        """Gets a random sticker from one of the user's configured sticker packs."""
        user_sticker_packs = self.user_sticker_packs.get(str(user_id))
        if not user_sticker_packs:
            return None

        pack_name_to_use = random.choice(user_sticker_packs)

        if pack_name_to_use not in self.sticker_sets:
            try:
                LOGGER.info(f"Loading sticker set '{pack_name_to_use}' for the first time for user {user_id}.")
                sticker_set = await client(GetStickerSetRequest(
                    stickerset=InputStickerSetShortName(short_name=pack_name_to_use),
                    hash=0
                ))
                self.sticker_sets[pack_name_to_use] = sticker_set.documents
            except Exception as e:
                LOGGER.error(f"Could not load sticker set '{pack_name_to_use}' for user {user_id}: {e}")
                if str(user_id) in self.user_sticker_packs and pack_name_to_use in self.user_sticker_packs[str(user_id)]:
                    self.user_sticker_packs[str(user_id)].remove(pack_name_to_use)
                    self._save_user_sticker_packs()
                return None
        
        documents = self.sticker_sets.get(pack_name_to_use)
        return random.choice(documents) if documents else None

    async def _ensure_entity_cached(self, client: TelegramClient, group_id: int, account_name: str, retries: int = 5, delay: int = 1) -> bool:
        """Ensures the client has cached the group entity and is a participant."""
        for attempt in range(retries):
            try:
                if not client.is_connected():
                    await client.connect()

                group_entity = await client.get_entity(PeerChannel(group_id))
                
                me = await client.get_me()
                await client(GetParticipantRequest(channel=group_entity, participant=me))
                
                LOGGER.info(f"Account '{account_name}' successfully verified as participant in group {group_id}.")
                return True
            except errors.rpcerrorlist.UserNotParticipantError:
                LOGGER.warning(f"Attempt {attempt + 1}/{retries}: Account '{account_name}' is not yet a participant in group {group_id}. Retrying in {delay}s.")
                if attempt < retries - 1:
                    await asyncio.sleep(delay)
                else:
                    LOGGER.error(f"Account '{account_name}' failed to confirm participation in group {group_id} after {retries} retries.")
                    return False
            except ValueError as e:
                LOGGER.warning(f"Attempt {attempt + 1}/{retries}: Account '{account_name}' could not find entity for group {group_id}. Retrying in {delay}s. Error: {e}")
                if attempt < retries - 1:
                    await client.get_dialogs(limit=1)
                    await asyncio.sleep(delay)
                else:
                     LOGGER.error(f"Account '{account_name}' failed to cache entity for group {group_id} after {retries} retries.")
                     return False
            except Exception as e:
                LOGGER.error(f"Unexpected error while ensuring entity cached for '{account_name}' in group {group_id}: {e}", exc_info=True)
                sentry_sdk.capture_exception(e)
                return False
        return False

    async def _send_initial_random_messages(self, client: TelegramClient, group_id: int):
        """Sends 10 predefined random messages to a newly created group."""
        try:
            LOGGER.info(f"Sending 10 random messages to new group {group_id}.")
            for i in range(10):
                if self._get_daily_count_for_group(group_id) >= self.daily_message_limit:
                    LOGGER.info(f"Daily message limit reached for group {group_id}. Stopping initial messages.")
                    break
                
                message_text = random.choice(Config.RANDOM_MESSAGES)
                await client.send_message(PeerChannel(group_id), message_text)
                self._increment_daily_count_for_group(group_id)
                LOGGER.info(f"Sent initial message {i + 1}/10 to group {group_id}.")
                await asyncio.sleep(random.uniform(Config.MESSAGE_SEND_DELAY_MIN, Config.MESSAGE_SEND_DELAY_MAX))

            self.created_groups[str(group_id)]["last_simulated"] = datetime.utcnow().timestamp()
            self._save_created_groups()
            LOGGER.info(f"Finished sending initial messages and updated 'last_simulated' for group {group_id}.")
        except (ValueError, errors.rpcerrorlist.ChannelInvalidError) as e:
            LOGGER.error(f"Sending initial messages failed in group {group_id} due to channel error: {e}")
        except Exception as e:
            LOGGER.error(f"Unexpected error during initial message sending for group {group_id}: {e}", exc_info=True)
            sentry_sdk.capture_exception(e)

    async def run_group_creation_worker(self, user_id: int, account_name: str, user_client: TelegramClient) -> None:
        worker_key = f"{user_id}:{account_name}"
        progress_message = None
        try:
            async with self.worker_semaphore:
                LOGGER.info(f"Worker for {worker_key} started.")
                
                avg_sleep_per_group = (self.min_sleep_seconds + self.max_sleep_seconds) / 2
                buffer_for_api_calls = 20
                total_estimated_seconds = (avg_sleep_per_group + buffer_for_api_calls) * self.groups_to_create
                eta_str = self._format_time_delta(total_estimated_seconds)
                
                initial_message = (
                    f"🚀 شروع عملیات ساخت گروه برای حساب `{account_name}`...\n\n"
                    f"⏳ **زمان تخمینی برای ساخت {self.groups_to_create} گروه:** حدودا **{eta_str}**."
                )
                progress_message = await self.bot.send_message(user_id, initial_message)
                
                start_time = datetime.now()
                me = await user_client.get_me()
                owner_id = me.id

                i = 0
                while i < self.groups_to_create:
                    try:
                        current_semester = self._get_group_count(worker_key) + 1
                        group_title = f"collage Semester {current_semester}"
                        create_result = await self._send_request_with_reconnect(
                            user_client, CreateChannelRequest(title=group_title, about="Official group.", megagroup=True), account_name
                        )
                        new_supergroup = create_result.chats[0]
                        LOGGER.info(f"Successfully created supergroup '{new_supergroup.title}' (ID: {new_supergroup.id}).")
                        
                        self.created_groups[str(new_supergroup.id)] = {
                            "owner_worker_key": worker_key, 
                            "owner_id": owner_id,
                            "last_simulated": 0
                        }
                        self._save_created_groups()

                        await self._send_initial_random_messages(user_client, new_supergroup.id)

                        self._set_group_count(worker_key, current_semester)
                        
                        groups_done = i + 1
                        elapsed_time = (datetime.now() - start_time).total_seconds()
                        avg_time_per_group = elapsed_time / groups_done
                        remaining_groups = self.groups_to_create - groups_done
                        estimated_remaining_seconds = remaining_groups * avg_time_per_group
                        eta_str = self._format_time_delta(estimated_remaining_seconds)
                        
                        try:
                            await progress_message.edit(
                                f"📊 [{account_name}] Group '{group_title}' created. "
                                f"({groups_done}/{self.groups_to_create})\n\n"
                                f"⏳ **Estimated time remaining:** {eta_str}"
                            )
                        except errors.MessageNotModifiedError:
                            pass
                        except Exception as e:
                            LOGGER.warning(f"Could not edit progress message: {e}")

                        await asyncio.sleep(random.randint(self.min_sleep_seconds, self.max_sleep_seconds))
                        i += 1

                    except errors.FloodWaitError as e:
                        LOGGER.warning(f"Flood wait error for '{account_name}': waiting for {e.seconds} seconds. Worker will pause and resume.")
                        wait_time_str = self._format_time_delta(e.seconds)
                        try:
                            await progress_message.edit(
                                f"⏳ [{account_name}] Telegram limit hit. Pausing for **{wait_time_str}**. "
                                f"Operation will resume automatically."
                            )
                        except Exception as msg_e:
                            LOGGER.warning(f"Could not edit progress message to show flood wait: {msg_e}")

                        await asyncio.sleep(e.seconds + 60)
                        
                        try:
                             await progress_message.edit(
                                f"✅ [{account_name}] Resuming operation after waiting for Telegram limit."
                            )
                        except Exception as msg_e:
                            LOGGER.warning(f"Could not edit progress message after resuming from flood wait: {msg_e}")
                        
                        continue

                    except errors.AuthKeyUnregisteredError as e:
                        LOGGER.error(f"Auth key unregistered for '{account_name}'. Deleting session.")
                        sentry_sdk.capture_exception(e)
                        self.session_manager.delete_session_file(user_id, account_name)
                        if progress_message: await progress_message.edit(f"🚨 Session for `{account_name}` revoked. Account removed.")
                        break
                    except Exception as e:
                        await self._send_error_explanation(user_id, e)
                        if progress_message:
                            try:
                                await progress_message.edit("❌ عملیات با خطا مواجه شد. لطفا گزارش ارسال شده را بررسی کنید.")
                            except Exception:
                                pass
                        break
                else:
                    if progress_message: await progress_message.edit(f"✅ [{account_name}] Finished creating {self.groups_to_create} groups.")

        except asyncio.CancelledError:
            LOGGER.info(f"Task for {worker_key} was cancelled.")
            if progress_message: await progress_message.edit(f"⏹️ Operation for `{account_name}` stopped.")
        finally:
            LOGGER.info(f"Worker for {worker_key} finished. Disconnecting client.")
            if worker_key in self.active_workers:
                del self.active_workers[worker_key]
                self.active_workers_state.pop(worker_key, None)
                self._save_active_workers_state()
            if user_client and user_client.is_connected():
                await user_client.disconnect()

    async def _run_conversation_task(self, user_id: int, group_id: int, num_messages: Optional[int] = None):
        """ This task is used for manual conversation triggers, not group creation. It will use random messages. """
        clients_to_disconnect = []
        try:
            group_data = self.created_groups.get(str(group_id))
            if not group_data or "owner_id" not in group_data:
                LOGGER.error(f"[Conversation Task] Cannot run for group {group_id}, owner_id is missing.")
                return

            participant_names = self.conversation_accounts.get(str(user_id), [])
            if not participant_names:
                LOGGER.warning(f"[Conversation Task] No conversation accounts set for user {user_id}.")
                return

            first_participant_name = participant_names[0]
            session_str = self.session_manager.load_session_string(user_id, first_participant_name)
            if not session_str: return
            
            client = await self._create_resilient_worker_client(user_id, first_participant_name, session_str)
            if not client: return
            clients_to_disconnect.append(client)
            
            messages_to_send = num_messages or self.daily_message_limit
            LOGGER.info(f"Manually sending {messages_to_send} random messages to group {group_id}.")
            for _ in range(messages_to_send):
                if self._get_daily_count_for_group(group_id) >= self.daily_message_limit:
                    LOGGER.info(f"Daily limit reached for group {group_id} during manual conversation.")
                    break
                message = random.choice(Config.RANDOM_MESSAGES)
                await client.send_message(PeerChannel(group_id), message)
                self._increment_daily_count_for_group(group_id)
                await asyncio.sleep(random.uniform(Config.MESSAGE_SEND_DELAY_MIN, Config.MESSAGE_SEND_DELAY_MAX))

        except asyncio.CancelledError:
            LOGGER.info(f"[Conversation Task] for group {group_id} was cancelled.")
            raise
        except Exception as e:
            LOGGER.error(f"[Conversation Task] Error for group {group_id}: {e}", exc_info=True)
            sentry_sdk.capture_exception(e)
        finally:
            LOGGER.info(f"[Conversation Task] Disconnecting {len(clients_to_disconnect)} clients for group {group_id}.")
            for client in clients_to_disconnect:
                try:
                    if client.is_connected():
                        await client.disconnect()
                except Exception as e:
                    LOGGER.warning(f"Failed to disconnect client for group {group_id}: {e}")

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
        if user_id in self.banned_users:
            await event.reply("❌ You are banned from using this bot.")
            return
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
        if self.health_check_lock.locked() and event.sender_id != ADMIN_USER_ID:
            await event.reply(Config.MSG_MAINTENANCE_ACTIVE)
            return
        await self._send_accounts_menu(event)
        raise events.StopPropagation

    async def _server_status_handler(self, event: events.NewMessage.Event) -> None:
        active_count = len(self.active_workers)
        active_conv_count = len(self.active_conversations)
        active_dm_count = len(self.active_dm_chats)
        
        status_text = f"**📊 Server Status**\n\n"
        status_text += f"**Health Check Active:** {'Yes' if self.health_check_lock.locked() else 'No'}\n"
        status_text += f"**Active Group Creators:** {active_count} / {self.max_workers}\n"
        status_text += f"**Active Manual Conversations:** {active_conv_count}\n"
        
        if event.sender_id == ADMIN_USER_ID:
            status_text += f"**Active Private DM Chats:** {active_dm_count}\n"

        if self.active_workers:
            status_text += "\n**Accounts Creating Groups:**\n"
            for worker_key in self.active_workers.keys():
                status_text += f"- `{worker_key}`\n"

        if self.active_conversations:
            status_text += "\n**Accounts in Manual Conversation:**\n"
            for worker_key in self.active_conversations.keys():
                status_text += f"- `{worker_key}`\n"
        
        if self.active_dm_chats and event.sender_id == ADMIN_USER_ID:
            status_text += "\n**Accounts in Private DM Chat:**\n"
            for chat_key in self.active_dm_chats.keys():
                status_text += f"- `{chat_key}`\n"

        if not any([self.active_workers, self.active_conversations, self.active_dm_chats]):
            status_text += "\nℹ️ No accounts are currently in operation."

        await event.reply(status_text, buttons=self._build_main_menu())
        raise events.StopPropagation

    async def _help_handler(self, event: events.NewMessage.Event) -> None:
        await event.reply(Config.MSG_HELP_TEXT, buttons=self._build_main_menu())
        raise events.StopPropagation

    async def _set_stickers_handler(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        self.user_sessions[user_id]['state'] = 'awaiting_sticker_packs'
        await event.reply(Config.MSG_PROMPT_STICKERS, buttons=[[Button.text(Config.BTN_BACK)]])

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
        if self.health_check_lock.locked() and event.sender_id != ADMIN_USER_ID:
            await event.reply(Config.MSG_MAINTENANCE_ACTIVE)
            return
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
        if self.health_check_lock.locked() and event.sender_id != ADMIN_USER_ID:
            await event.reply(Config.MSG_MAINTENANCE_ACTIVE)
            return
        user_id = event.sender_id
        accounts = self.session_manager.get_user_accounts(user_id)
        if not accounts:
            await event.reply("❌ شما هیچ حسابی برای استخراج لینک ندارید. ابتدا یک حساب اضافه کنید.")
            return

        self.user_sessions[user_id]['state'] = 'awaiting_export_account_selection'
        buttons = [[Button.text(acc)] for acc in accounts]
        buttons.append([Button.text(Config.BTN_BACK)])
        await event.reply(Config.MSG_PROMPT_EXPORT_ACCOUNT, buttons=buttons)

    async def _force_conversation_handler(self, event: events.NewMessage.Event) -> None:
        if self.health_check_lock.locked() and event.sender_id != ADMIN_USER_ID:
            await event.reply(Config.MSG_MAINTENANCE_ACTIVE)
            return
        user_id = event.sender_id
        accounts = self.session_manager.get_user_accounts(user_id)
        if not accounts:
            await event.reply("❌ شما هیچ حسابی برای این کار ندارید. ابتدا یک حساب اضافه کنید.")
            return

        self.user_sessions[user_id]['state'] = 'awaiting_force_conv_account_selection'
        buttons = [[Button.text(acc)] for acc in accounts]
        buttons.append([Button.text(Config.BTN_BACK)])
        await event.reply(Config.MSG_PROMPT_FORCE_CONV_ACCOUNT, buttons=buttons)

    async def _stop_force_conversation_handler(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        active_conv_keys = [key for key in self.active_conversations.keys() if key.startswith(f"{user_id}:")]

        if not active_conv_keys:
            await event.reply(Config.MSG_NO_ACTIVE_FORCE_CONV)
            return

        self.user_sessions[user_id]['state'] = 'awaiting_stop_force_conv_selection'
        buttons = [[Button.text(key.split(":", 1)[1])] for key in active_conv_keys]
        buttons.append([Button.text(Config.BTN_BACK)])
        await event.reply(Config.MSG_PROMPT_STOP_FORCE_CONV, buttons=buttons)

    async def _settings_handler(self, event: events.NewMessage.Event) -> None:
        if event.sender_id != ADMIN_USER_ID:
            return
        
        buttons = [
            [Button.text(Config.BTN_MANUAL_HEALTH_CHECK)],
            [Button.text(Config.BTN_MESSAGE_ALL_GROUPS)],
            [Button.text("Set Worker Limit"), Button.text("Set Group Count")],
            [Button.text("Set Sleep Times"), Button.text("Set Daily Msg Limit")],
            [Button.text("Set Proxy Timeout"), Button.text("Set Master Password")],
            [Button.text("View Config"), Button.text(Config.BTN_BACK)]
        ]
        await event.reply("⚙️ **Admin Settings**\n\nClick a button to change a setting, or use `/set_config KEY value`.", buttons=buttons)

    async def _admin_command_handler(self, event: events.NewMessage.Event) -> None:
        if event.sender_id != ADMIN_USER_ID:
            await event.reply("❌ You are not authorized to use this command.")
            return
        
        self._ensure_session(event.sender_id)
        
        text = event.message.text
        
        pre_approve_match = re.match(r"/pre_approve (\d+)", text)
        ban_match = re.match(r"/ban (\d+)", text)
        unban_match = re.match(r"/unban (\d+)", text)
        set_config_match = re.match(r"/set_config (\w+) (.*)", text, re.DOTALL)
        terminate_match = re.match(r"/terminate_worker (.*)", text)
        restart_match = re.match(r"/restart_worker (.*)", text)
        set_user_limit_match = re.match(r"/set_user_limit (\d+) (\d+)", text)
        broadcast_match = re.match(r"/broadcast (.+)", text, re.DOTALL)
        send_random_links_match = re.match(r"/send_random_links (\d+)", text)

        if pre_approve_match:
            await self._pre_approve_handler(event, int(pre_approve_match.group(1)))
        elif ban_match:
            await self._ban_user_handler(event, int(ban_match.group(1)))
        elif unban_match:
            await self._unban_user_handler(event, int(unban_match.group(1)))
        elif set_config_match:
            await self._set_config_handler(event, set_config_match.group(1), set_config_match.group(2))
        elif terminate_match:
            await self._terminate_worker_handler(event, terminate_match.group(1))
        elif restart_match:
            await self._restart_worker_handler(event, restart_match.group(1))
        elif set_user_limit_match: 
            await self._set_user_limit_handler(event, int(set_user_limit_match.group(1)), int(set_user_limit_match.group(2)))
        elif broadcast_match: 
            await self._broadcast_command_handler(event, broadcast_match.group(1))
        elif send_random_links_match: 
            await self._send_random_links_handler(event, int(send_random_links_match.group(1)))
        elif text == "/list_users":
            await self._list_users_handler(event)
        elif text == "/list_workers":
            await self._list_workers_handler(event)
        elif text == "/list_groups":
            await self._list_groups_handler(event)
        elif text == "/export_all_links": 
            await self._export_all_links_handler(event)
        elif text == "/list_conv_accounts":
            await self._list_conv_accounts_handler(event)
        elif text == "/view_config":
            await self._view_config_handler(event)
        elif text == "/debug_proxies":
            await self._debug_test_proxies_handler(event)
        elif text == "/clean_sessions":
            await self._clean_sessions_handler(event)
        elif text == "/test_sentry":
            await self._test_sentry_handler(event)
        elif text == "/test_self_healing":
            await self._test_self_healing_handler(event)
        elif text == "/dm_chat":
            await self._start_dm_chat_handler(event)
        elif text == "/stop_dm_chat":
            await self._stop_dm_chat_handler(event)
        elif text == "/dm_message":
            await self._start_dm_message_handler(event)
        else:
            await event.reply("Unknown admin command.")
        
    async def _pre_approve_handler(self, event: events.NewMessage.Event, user_id_to_approve: int):
        if user_id_to_approve not in self.known_users:
            self.known_users.append(user_id_to_approve)
            self._save_known_users()
            await event.reply(f"✅ User `{user_id_to_approve}` has been pre-approved.")
        else:
            await event.reply(f"ℹ️ User `{user_id_to_approve}` is already an approved user.")

    async def _ban_user_handler(self, event: events.NewMessage.Event, user_id_to_ban: int):
        if user_id_to_ban == ADMIN_USER_ID:
            await event.reply("❌ You cannot ban the admin.")
            return
        if user_id_to_ban not in self.banned_users:
            self.banned_users.append(user_id_to_ban)
            self._save_banned_users()
            if user_id_to_ban in self.known_users:
                self.known_users.remove(user_id_to_ban)
                self._save_known_users()
            await event.reply(f"🚫 User `{user_id_to_ban}` has been banned.")
        else:
            await event.reply(f"ℹ️ User `{user_id_to_ban}` is already banned.")

    async def _unban_user_handler(self, event: events.NewMessage.Event, user_id_to_unban: int):
        if user_id_to_unban in self.banned_users:
            self.banned_users.remove(user_id_to_unban)
            self._save_banned_users()
            await event.reply(f"✅ User `{user_id_to_unban}` has been unbanned.")
        else:
            await event.reply(f"ℹ️ User `{user_id_to_unban}` is not banned.")
            
    async def _list_users_handler(self, event: events.NewMessage.Event):
        known_list = "\n".join(f"- `{uid}`" for uid in self.known_users) if self.known_users else "None"
        banned_list = "\n".join(f"- `{uid}`" for uid in self.banned_users) if self.banned_users else "None"
        
        message = (
            f"**👥 User Lists**\n\n"
            f"**Approved Users:**\n{known_list}\n\n"
            f"**Banned Users:**\n{banned_list}"
        )
        await event.reply(message)

    async def _set_config_handler(self, event: events.NewMessage.Event, key: str, value: str):
        key = key.upper()
        
        try:
            if '.' in value:
                value = float(value)
            else:
                value = int(value)
        except ValueError:
            pass
        
        self.config[key] = value
        self._save_json_file(self.config, self.config_file)
        self.update_config_from_file()
        
        if key == "MAX_CONCURRENT_WORKERS":
            self.worker_semaphore = asyncio.Semaphore(self.max_workers)

        await event.reply(f"✅ Config key `{key}` has been set to `{value}`.")

    async def _view_config_handler(self, event: events.NewMessage.Event):
        config_str = json.dumps(self.config, indent=2)
        message = f"**🔧 Current Configuration**\n\n```json\n{config_str}\n```"
        await event.reply(message)

    async def _terminate_worker_handler(self, event: events.NewMessage.Event, worker_key: str):
        if worker_key in self.active_workers:
            task = self.active_workers[worker_key]
            task.cancel()
            LOGGER.info(f"Admin initiated termination for worker {worker_key}.")
            try:
                await task
            except asyncio.CancelledError:
                pass
            await event.reply(f"✅ Worker `{worker_key}` has been terminated.")
        else:
            await event.reply(f"❌ No active worker found with key `{worker_key}`.")

    async def _restart_worker_handler(self, event: events.NewMessage.Event, worker_key: str):
        if worker_key in self.active_workers:
            await event.reply(f"🔄 Restarting worker `{worker_key}`...")
            await self._terminate_worker_handler(event, worker_key)
            await asyncio.sleep(2)
            
            try:
                user_id_str, account_name = worker_key.split(":", 1)
                user_id = int(user_id_str)
                await self._start_worker_task(user_id, account_name)
                await event.reply(f"✅ Worker `{worker_key}` restart initiated.")
            except ValueError:
                await event.reply("❌ Invalid worker key format. Use `user_id:account_name`.")
        else:
            await event.reply(f"❌ No active worker found with key `{worker_key}` to restart.")

    async def _list_workers_handler(self, event: events.NewMessage.Event):
        if not self.active_workers:
            await event.reply("ℹ️ No active workers are currently running.")
            return

        message = "**- Active Workers -**\n\n"
        for worker_key, task in self.active_workers.items():
            proxy_info = self.account_proxies.get(worker_key)
            proxy_str = f"Proxy: {proxy_info['addr']}:{proxy_info['port']}" if proxy_info else "Proxy: None"
            message += f"- **Key:** `{worker_key}`\n  - **Status:** {'Running' if not task.done() else 'Finished'}\n  - **{proxy_str}**\n\n"
        
        await event.reply(message)

    async def _list_groups_handler(self, event: events.NewMessage.Event):
        if not self.created_groups:
            await event.reply("ℹ️ No groups have been created by the bot yet.")
            return

        message = "**- Created Groups -**\n\n"
        for group_id, data in self.created_groups.items():
            owner_key = data.get("owner_worker_key", "Unknown")
            message += f"- **Group ID:** `{group_id}`\n  - **Owner Key:** `{owner_key}`\n\n"
        
        if len(message) > 4096:
            try:
                with open("created_groups.txt", "w", encoding="utf-8") as f:
                    f.write(message)
                await self.bot.send_file(event.chat_id, "created_groups.txt", caption="List of created groups.")
                os.remove("created_groups.txt")
            except Exception as e:
                LOGGER.error(f"Failed to send groups list as file: {e}")
                await event.reply("Failed to send the list as a file due to an error.")
        else:
            await event.reply(message)

    async def _send_random_links_handler(self, event: events.NewMessage.Event, num_links: int):
        if event.sender_id != ADMIN_USER_ID:
            return

        if not self.known_users:
            await event.reply("❌ No users available to source links from.")
            return

        source_user_id = random.choice(self.known_users)
        user_accounts = self.session_manager.get_user_accounts(source_user_id)
        if not user_accounts:
            await event.reply(f"❌ Randomly selected user `{source_user_id}` has no accounts. Please try the command again.")
            return

        source_account_name = random.choice(user_accounts)
        owner_key = f"{source_user_id}:{source_account_name}"

        owned_group_ids = [
            int(gid) for gid, data in self.created_groups.items()
            if data.get("owner_worker_key") == owner_key
        ]

        if not owned_group_ids:
            await event.reply(f"❌ Randomly selected account `{owner_key}` has not created any groups. Please try the command again.")
            return

        if len(owned_group_ids) < num_links:
            await event.reply(f"⚠️ Account `{owner_key}` only has {len(owned_group_ids)} groups, but you requested {num_links}. Sending all available links.")
            num_links = len(owned_group_ids)

        selected_group_ids = random.sample(owned_group_ids, num_links)
        await event.reply(f"⏳ Preparing to send you {num_links} random links from account `{owner_key}`...")

        links = []
        client = None
        try:
            session_str = self.session_manager.load_session_string(source_user_id, source_account_name)
            client = await self._create_resilient_worker_client(source_user_id, source_account_name, session_str)

            if not client:
                await event.reply(f"❌ Failed to connect with account `{owner_key}` to generate links.")
                return

            for group_id in selected_group_ids:
                try:
                    group_entity = await client.get_entity(PeerChannel(group_id))
                    result = await client(ExportChatInviteRequest(group_entity))
                    links.append(result.link)
                except Exception as e:
                    LOGGER.warning(f"Could not export link for group {group_id} for random send: {e}")
                    links.append(f"Error exporting link for group ID {group_id}")
                await asyncio.sleep(1)

        except Exception as e:
            await self._send_error_explanation(ADMIN_USER_ID, e)
            await event.reply(f"❌ An error occurred while generating links for `{owner_key}`.")
            return
        finally:
            if client and client.is_connected():
                await client.disconnect()

        if links:
            message_to_admin = f"🔗 Here are {len(links)} random links from account `{owner_key}`:\n\n" + "\n".join(links)
            try:
                await self.bot.send_message(ADMIN_USER_ID, message_to_admin)
                await event.reply(f"✅ Links sent successfully to your private chat.")
            except Exception as e:
                await event.reply(f"❌ Successfully generated links, but failed to send them to you. Reason: {e}")
        else:
            await event.reply(f"❌ Could not generate any valid links for the selected groups from account `{owner_key}`.")

    async def _export_all_links_handler(self, event: events.NewMessage.Event):
        if event.sender_id != ADMIN_USER_ID:
            return

        if not self.created_groups:
            await event.reply("ℹ️ No groups have been created by the bot yet.")
            return

        await event.reply("⏳ **Exporting all links...** This may take a significant amount of time depending on the number of accounts and groups.")

        all_links = []
        groups_by_owner = {}
        for group_id, data in self.created_groups.items():
            owner_key = data.get("owner_worker_key")
            if owner_key:
                groups_by_owner.setdefault(owner_key, []).append(int(group_id))

        for owner_key, group_ids in groups_by_owner.items():
            client = None
            try:
                user_id_str, account_name = owner_key.split(":", 1)
                user_id = int(user_id_str)
                session_str = self.session_manager.load_session_string(user_id, account_name)
                if not session_str:
                    all_links.append(f"\n--- ERROR: Could not load session for {owner_key} ---")
                    continue

                client = await self._create_resilient_worker_client(user_id, account_name, session_str)
                if not client:
                    all_links.append(f"\n--- ERROR: Could not connect as {owner_key} ---")
                    continue
                
                all_links.append(f"\n--- Links for {owner_key} ---")
                for group_id in group_ids:
                    try:
                        group_entity = await client.get_entity(PeerChannel(group_id))
                        result = await client(ExportChatInviteRequest(group_entity))
                        all_links.append(result.link)
                    except Exception as e:
                        LOGGER.warning(f"Could not export link for group {group_id} with account {owner_key}: {e}")
                        all_links.append(f"Error for group ID {group_id}: {e.__class__.__name__}")
                    await asyncio.sleep(1)

            except Exception as e:
                LOGGER.error(f"Failed to process owner {owner_key} for link export: {e}")
                all_links.append(f"\n--- CRITICAL ERROR processing {owner_key}: {e.__class__.__name__} ---")
            finally:
                if client and client.is_connected():
                    await client.disconnect()

        if all_links:
            file_path = SESSIONS_DIR / "all_invite_links.txt"
            with open(file_path, "w", encoding="utf-8") as f:
                f.write("\n".join(all_links))
            
            await self.bot.send_file(ADMIN_USER_ID, file_path, caption="✅ All group invite links have been exported.")
            os.remove(file_path)
        else:
            await event.reply("❌ No links could be exported.")

    async def _list_conv_accounts_handler(self, event: events.NewMessage.Event):
        if not self.conversation_accounts:
            await event.reply("ℹ️ No conversation accounts have been set.")
            return

        message = "**- Conversation Accounts per User -**\n\n"
        for user_id, accounts in self.conversation_accounts.items():
            accounts_str = ", ".join(f"`{acc}`" for acc in accounts) if accounts else "None"
            message += f"- **User ID:** `{user_id}`\n  - **Accounts:** {accounts_str}\n\n"
        
        await event.reply(message)

    # ---------- MODIFICATION START: Proxy Debug Handler ----------
    async def _debug_test_proxies_handler(self, event: events.NewMessage.Event) -> None:
        """[MODIFIED] Tests all proxies and the direct connection, then sends a report."""
        if event.sender_id != ADMIN_USER_ID: return

        LOGGER.info(f"Admin {event.sender_id} initiated proxy test.")
        if not self.proxies:
            await self.bot.send_message(event.sender_id, "⚠️ No proxies found in the file to test. Testing direct connection only.")
        
        msg = await self.bot.send_message(event.sender_id, f"🧪 Starting test for {len(self.proxies)} proxies and direct connection... This may take a moment.")
        
        working_proxies = []
        tested_count = 0
        total_proxies = len(self.proxies)

        LOGGER.info("--- PROXY TEST START ---")
        for proxy in self.proxies:
            client = None
            try:
                device_params = random.choice(Config.USER_AGENTS)
                client = TelegramClient(sessions.StringSession(), API_ID, API_HASH, proxy=proxy, timeout=self.proxy_timeout, **device_params)
                await client.connect()
                if client.is_connected():
                    proxy_line = f"{proxy['addr']}:{proxy['port']}"
                    if 'username' in proxy and 'password' in proxy:
                        proxy_line += f":{proxy['username']}:{proxy['password']}"
                    
                    working_proxies.append(proxy_line)
                    LOGGER.info(f"  ✅ SUCCESS: {proxy['addr']}:{proxy['port']}")
            except Exception as e:
                # Check for proxy authentication error within the generic exception message
                if "407" in str(e) or "Proxy Authentication Required" in str(e):
                    LOGGER.warning(f"  ❌ FAILURE (407 Auth Required): {proxy['addr']}:{proxy['port']}. Check if username/password are needed and correct.")
                else:
                    LOGGER.warning(f"  ❌ FAILURE ({type(e).__name__}): {proxy['addr']}:{proxy['port']} - {e}")
            finally:
                if client and client.is_connected():
                    await client.disconnect()
                
                tested_count += 1
                if tested_count % 10 == 0 or tested_count == total_proxies:
                    try:
                        await msg.edit(f"🧪 Testing proxies... ({tested_count}/{total_proxies})")
                    except errors.MessageNotModifiedError:
                        pass
        
        LOGGER.info("--- DIRECT CONNECTION TEST ---")
        direct_connection_works = False
        client = None
        try:
            device_params = random.choice(Config.USER_AGENTS)
            client = TelegramClient(sessions.StringSession(), API_ID, API_HASH, proxy=None, timeout=self.proxy_timeout, **device_params)
            await client.connect()
            if client.is_connected():
                direct_connection_works = True
                LOGGER.info("  ✅ SUCCESS: Direct connection works.")
        except Exception as e:
            LOGGER.warning(f"  ❌ FAILURE: Direct connection failed. - {e}")
        finally:
            if client and client.is_connected():
                await client.disconnect()
        
        LOGGER.info("Proxy and connection test finished.")

        report_lines = [
            f"✅ Test complete. Found {len(working_proxies)} working proxies out of {total_proxies}."
        ]
        if direct_connection_works:
            report_lines.append("✅ Direct connection to Telegram is working.")
        else:
            report_lines.append("❌ Direct connection to Telegram failed. Proxies may be required.")

        report_caption = "\n".join(report_lines)

        if working_proxies:
            file_path = SESSIONS_DIR / "working_proxies.txt"
            with open(file_path, "w", encoding="utf-8") as f:
                f.write("# This file contains proxies that successfully connected to Telegram during the last test.\n")
                f.write("\n".join(working_proxies))
            
            await self.bot.send_file(
                event.chat_id, 
                file_path, 
                caption=report_caption
            )
            os.remove(file_path)
            if msg:
                await msg.delete()
        else:
            await msg.edit(report_caption)
        
        raise events.StopPropagation
    # ---------- MODIFICATION END: Proxy Debug Handler ----------

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
        self.pending_users.clear()
        self.created_groups.clear()
        self.conversation_accounts.clear()
        self.user_worker_limits.clear()
        self._save_pending_users()
        self._save_created_groups()
        self._save_conversation_accounts()
        self._save_user_worker_limits()
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
        
        user_id = event.sender_id
        
        self._ensure_session(user_id)

        try:
            text = event.message.text
            if user_id in self.banned_users:
                await event.reply("❌ You are banned from using this bot.")
                return

            if text.startswith('/'):
                if user_id == ADMIN_USER_ID:
                    await self._admin_command_handler(event)
                elif text == '/start':
                    await self._start_handler(event)
                else:
                    await event.reply("❌ You are not authorized to use commands.")
                return

            if user_id not in self.known_users and user_id != ADMIN_USER_ID:
                if user_id in self.pending_users:
                    await event.reply(Config.MSG_AWAITING_APPROVAL)
                    return
                await self._handle_master_password(event)
                return
            
            session = self.user_sessions.get(user_id, {})
            state = session.get('state')

            state_handlers = {
                'awaiting_sticker_packs': self._handle_sticker_packs_input,
                'awaiting_conv_accounts': self._handle_conv_accounts_input,
                'awaiting_get_code_selection': self._handle_get_code_selection,
                'awaiting_2fa_choice': self._handle_2fa_choice,
                'awaiting_join_account_selection': self._handle_join_account_selection,
                'awaiting_join_link': self._handle_join_link_input,
                'awaiting_export_account_selection': self._process_export_link_request,
                'awaiting_force_conv_account_selection': self._handle_force_conv_account_selection,
                'awaiting_force_conv_num_messages': self._handle_force_conv_num_messages,
                'awaiting_stop_force_conv_selection': self._handle_stop_force_conv_selection,
                'awaiting_phone': self._handle_phone_input,
                'awaiting_code': self._handle_code_input,
                'awaiting_password': self._handle_password_input,
                'awaiting_account_name': self._handle_account_name_input,
                'awaiting_config_value': self._handle_config_value_input,
                'awaiting_dm_target_id': self._handle_dm_target_id,
                'awaiting_dm_account_selection': self._handle_dm_account_selection,
                'awaiting_dm_persona': self._handle_dm_persona,
                'awaiting_dm_sticker_packs': self._handle_dm_sticker_packs,
                'awaiting_dm_initial_prompt': self._handle_dm_initial_prompt,
                'awaiting_dm_message_account_selection': self._handle_dm_message_account_selection,
                'awaiting_dm_message_target_user': self._handle_dm_message_target_user,
                'awaiting_dm_message_prompt': self._handle_dm_message_prompt,
            }

            if text == Config.BTN_BACK:
                if state in ['awaiting_phone', 'awaiting_code', 'awaiting_password', 'awaiting_account_name', 'awaiting_2fa_choice']:
                    self.user_sessions[user_id]['state'] = 'authenticated'
                    await self._send_accounts_menu(event)
                    return
                elif state in state_handlers:
                    self.user_sessions[user_id]['state'] = 'authenticated'
                    await self._start_handler(event)
                    return

            if state in state_handlers:
                await state_handlers[state](event)
                return

            if state != 'authenticated':
                await self._start_handler(event)
                return

            button_handlers = {
                Config.BTN_MANAGE_ACCOUNTS: self._manage_accounts_handler,
                Config.BTN_HELP: self._help_handler,
                Config.BTN_BACK: self._start_handler,
                Config.BTN_SETTINGS: self._settings_handler,
                Config.BTN_ADD_ACCOUNT: self._initiate_login_flow,
                Config.BTN_ADD_ACCOUNT_SELENIUM: self._initiate_selenium_login_flow,
                Config.BTN_SERVER_STATUS: self._server_status_handler,
                Config.BTN_GET_CODE: self._get_code_handler,
                Config.BTN_SET_STICKERS: self._set_stickers_handler,
                Config.BTN_SET_CONVERSATION_ACCOUNTS: self._set_conv_accs_handler,
                Config.BTN_JOIN_VIA_LINK: self._join_via_link_handler,
                Config.BTN_EXPORT_LINKS: self._export_links_handler,
                Config.BTN_FORCE_CONVERSATION: self._force_conversation_handler,
                Config.BTN_STOP_FORCE_CONVERSATION: self._stop_force_conversation_handler,
                Config.BTN_MANUAL_HEALTH_CHECK: self._manual_health_check_handler,
                Config.BTN_MESSAGE_ALL_GROUPS: self._message_all_groups_handler,
            }
            
            if user_id == ADMIN_USER_ID:
                admin_settings_map = {
                    "Set Worker Limit": "MAX_CONCURRENT_WORKERS",
                    "Set Group Count": "GROUPS_TO_CREATE", 
                    "Set Sleep Times": "MIN_SLEEP_SECONDS,MAX_SLEEP_SECONDS",
                    "Set Daily Msg Limit": "DAILY_MESSAGE_LIMIT_PER_GROUP", 
                    "Set Proxy Timeout": "PROXY_TIMEOUT",
                    "Set Master Password": "MASTER_PASSWORD_HASH",
                    "View Config": "VIEW_CONFIG"
                }
                if text in admin_settings_map:
                    await self._handle_admin_setting_button(event, admin_settings_map[text])
                    return

            handler = button_handlers.get(text)
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
        
        except events.StopPropagation:
            raise
        except Exception as e:
            LOGGER.error(f"An error occurred for user {user_id}", exc_info=True)
            await self._send_error_explanation(user_id, e)

    async def _start_worker_task(self, user_id: int, account_name: str) -> Optional[TelegramClient]:
        """Core logic to initialize and start a group creation worker."""
        worker_key = f"{user_id}:{account_name}"
        session_str = self.session_manager.load_session_string(user_id, account_name)
        if not session_str:
            LOGGER.error(f"No session found for account '{account_name}' of user {user_id}.")
            await self.bot.send_message(user_id, f'❌ No session found for account `{account_name}`. Please delete and add it again.')
            return None

        user_client = None
        try:
            user_client = await self._create_resilient_worker_client(user_id, account_name, session_str)
            if not user_client:
                LOGGER.error(f"Failed to connect to Telegram for account '{account_name}'.")
                await self.bot.send_message(user_id, f'❌ Failed to connect to Telegram for account `{account_name}`.')
                return None

            if await user_client.is_user_authorized():
                task = asyncio.create_task(self.run_group_creation_worker(user_id, account_name, user_client))
                self.active_workers[worker_key] = task
                self.active_workers_state[worker_key] = {"user_id": user_id, "account_name": account_name}
                self._save_active_workers_state()
                LOGGER.info(f"Successfully started worker task for {worker_key}.")
                return user_client
            else:
                LOGGER.warning(f"Session for '{account_name}' has expired. Deleting.")
                self.session_manager.delete_session_file(user_id, account_name)
                self._remove_group_count(worker_key)
                await self.bot.send_message(user_id, f'⚠️ Session for `{account_name}` has expired. Please add it again.')
                if user_client.is_connected():
                    await user_client.disconnect()
                return None
        except errors.AuthKeyUnregisteredError as e:
            LOGGER.error(f"Auth key is unregistered for '{account_name}'. Deleting session.", exc_info=True)
            sentry_sdk.capture_exception(e)
            self.session_manager.delete_session_file(user_id, account_name)
            self._remove_group_count(worker_key)
            await self.bot.send_message(user_id, f"🚨 Session for `{account_name}` revoked. Account removed.")
            if user_client and user_client.is_connected():
                await user_client.disconnect()
            return None
        except Exception as e:
            await self._send_error_explanation(user_id, e)
            if user_client and user_client.is_connected():
                await user_client.disconnect()
            return None

    async def _start_process_handler(self, event: events.NewMessage.Event, account_name: str, from_admin=False) -> None:
        user_id = event.sender_id
        if self.health_check_lock.locked() and user_id != ADMIN_USER_ID:
            await event.reply(Config.MSG_MAINTENANCE_ACTIVE)
            return

        user_limit = self.user_worker_limits.get(str(user_id), self.max_workers)
        current_user_workers = sum(1 for key in self.active_workers if key.startswith(f"{user_id}:"))
        
        if current_user_workers >= user_limit:
            await event.reply(f"❌ شما به حداکثر تعداد ورکر فعال خود ({user_limit}) رسیده‌اید. لطفاً منتظر بمانید تا یکی از عملیات‌ها تمام شود.")
            return

        worker_key = f"{user_id}:{account_name}"
        if worker_key in self.active_workers:
            if not from_admin:
                await event.reply('⏳ An operation for this account is already in progress.')
            return

        if not from_admin:
            await event.reply(f'🚀 Preparing to start operation for account `{account_name}`...')

        client = await self._start_worker_task(user_id, account_name)
        
        if client:
             if not from_admin:
                await self._send_accounts_menu(event)
        else:
             if not from_admin:
                await event.reply(f'❌ Failed to start worker for `{account_name}`. Check logs for details.')
                await self._send_accounts_menu(event)

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
        if hashed_input == self.master_password_hash:
            if user_id not in self.pending_users:
                self.pending_users.append(user_id)
                self._save_pending_users()
                approval_buttons = [[Button.inline("✅ Approve", f"approve_{user_id}"), Button.inline("❌ Deny", f"deny_{user_id}")]]
                await self.bot.send_message(ADMIN_USER_ID, f"🔔 New user access request from ID: `{user_id}`", buttons=approval_buttons)
            await event.reply(Config.MSG_AWAITING_APPROVAL)
        else:
            await event.reply(Config.MSG_INCORRECT_MASTER_PASSWORD)
        raise events.StopPropagation

    async def _handle_sticker_packs_input(self, event: events.NewMessage.Event) -> None:
        user_id = str(event.sender_id)
        packs_text = event.message.text.strip()
        if packs_text:
            packs = [pack.strip() for pack in packs_text.split(',')]
            self.user_sticker_packs[user_id] = packs
            self._save_user_sticker_packs()
            await event.reply(Config.MSG_STICKERS_SET, buttons=self._build_main_menu())
        else:
            self.user_sticker_packs[user_id] = []
            self._save_user_sticker_packs()
            await event.reply("✅ لیست استیکرهای شما پاک شد.", buttons=self._build_main_menu())
        self.user_sessions[event.sender_id]['state'] = 'authenticated'
        raise events.StopPropagation

    async def _get_code_handler(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        accounts = self.session_manager.get_user_accounts(user_id)
        if not accounts:
            await event.reply("❌ شما هیچ حسابی برای دریافت کد ندارید.")
            return

        self.user_sessions[user_id]['state'] = 'awaiting_get_code_selection'
        buttons = [[Button.text(acc)] for acc in accounts]
        buttons.append([Button.text(Config.BTN_BACK)])
        await event.reply("📲 لطفاً حسابی که می‌خواهید کد ورود آن را دریافت کنید، انتخاب نمایید:", buttons=buttons)

    async def _handle_get_code_selection(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        account_name = event.message.text.strip()
        self.user_sessions[user_id]['state'] = 'authenticated'

        if account_name not in self.session_manager.get_user_accounts(user_id):
            await event.reply("❌ حساب انتخاب شده نامعتبر است.")
            await self._send_accounts_menu(event)
            return

        session_str = self.session_manager.load_session_string(user_id, account_name)
        if not session_str:
            await event.reply(f"❌ نشست برای حساب `{account_name}` یافت نشد.")
            await self._send_accounts_menu(event)
            return

        msg = await event.reply(f"⏳ در حال اتصال به حساب `{account_name}`... لطفاً اکنون در دستگاه دیگر خود درخواست کد ورود دهید. ربات به مدت ۶۰ ثانیه منتظر کد خواهد ماند.")
        
        client = None
        code_found = asyncio.Event()

        async def code_handler(event_code):
            LOGGER.info(f"Received a message from Telegram service for {account_name}: {event_code.message.text}")
            
            code_match = re.search(r'(\d[\s-]?\d[\s-]?\d[\s-]?\d[\s-]?\d)', event_code.message.text)
            
            if code_match:
                code = re.sub(r'[\s-]', '', code_match.group(1))
                await self.bot.send_message(user_id, f"✅ **کد ورود برای `{account_name}`:**\n\n`{code}`")
                code_found.set()
            else:
                await self.bot.send_message(user_id, f"ℹ️ **پیام از تلگرام برای `{account_name}` (کد یافت نشد):**\n\n_{event_code.message.text}_")

        try:
            client = await self._create_resilient_worker_client(user_id, account_name, session_str)
            if not client:
                await msg.edit(f"❌ اتصال به حساب `{account_name}` ناموفق بود.")
                await self._send_accounts_menu(event)
                return

            client.add_event_handler(code_handler, events.NewMessage(from_users=777000))
            await client.get_me() 

            await asyncio.wait_for(code_found.wait(), timeout=60.0)
            await msg.edit(f"✅ کد برای `{account_name}` با موفقیت به شما ارسال شد.")

        except asyncio.TimeoutError:
            await msg.edit(f"⌛️ در مدت ۶۰ ثانیه هیچ کدی برای `{account_name}` دریافت نشد.")
        except Exception as e:
            await self._send_error_explanation(user_id, e)
            await msg.edit(f"❌ خطایی هنگام دریافت کد برای `{account_name}` رخ داد.")
        finally:
            if client:
                if client.is_connected():
                    await client.disconnect()
            await self._send_accounts_menu(event)

    async def _handle_2fa_choice(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        choice = event.message.text.strip()
        client = self.user_sessions[user_id].get('client')

        if choice not in [Config.BTN_CHANGE_2FA_YES, Config.BTN_CHANGE_2FA_NO]:
            await event.reply("❌ انتخاب نامعتبر. لطفاً از دکمه‌ها استفاده کنید.")
            return

        if not client:
            await event.reply("❌ خطای داخلی: نشست کلاینت منقضی شده است. لطفاً فرآیند افزودن حساب را از ابتدا شروع کنید.")
            self.user_sessions[user_id]['state'] = 'authenticated'
            await self._send_accounts_menu(event)
            return

        if choice == Config.BTN_CHANGE_2FA_YES:
            current_password = self.user_sessions[user_id].get('current_password')
            new_password = "erfantenhome"
            msg = await event.reply(f"⏳ در حال تغییر رمز تایید دو مرحله‌ای به `{new_password}`...")
            try:
                # Telethon's edit_2fa is deprecated, use direct request
                current_pwd_check = await client(UpdatePasswordSettingsRequest(
                    current_password_hash=b'', # This is complex to get, so we try with empty first
                    new_settings=types.account.PasswordInputSettings(
                        new_password_hash=b'', # Also complex, but we can set it
                        hint=new_password
                    )
                ))
                # This part is complex due to Telethon's password hashing. 
                # A full implementation requires Salted-Random-KDF.
                # For now, we inform the user it's a complex operation.
                await msg.edit("⚠️ تغییر رمز عبور از طریق ربات یک عملیات پیچیده است و ممکن است به طور کامل پشتیبانی نشود. لطفاً به صورت دستی این کار را انجام دهید.")

            except Exception as e:
                await self._send_error_explanation(user_id, e)
                await msg.edit("❌ در هنگام تغییر رمز خطایی رخ داد. ممکن است رمز فعلی اشتباه باشد یا مشکلی در سمت تلگرام وجود داشته باشد.")
        
        self.user_sessions[user_id]['state'] = 'awaiting_account_name'
        await event.reply('✍️ لطفاً یک نام مستعار برای این حساب وارد کنید (مثال: `حساب اصلی` یا `شماره دوم`).', buttons=[[Button.text(Config.BTN_BACK)]])

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
            client = await self._create_resilient_worker_client(user_id, account_name, session_str)
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
            await self._send_error_explanation(user_id, e)
        finally:
            if client and client.is_connected():
                await client.disconnect()
            self.user_sessions[user_id]['state'] = 'authenticated'

    async def _process_export_link_request(self, event: events.NewMessage.Event) -> None:
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
            client = await self._create_resilient_worker_client(user_id, account_name, session_str)
            if not client:
                await event.reply(f"❌ اتصال به حساب `{account_name}` ناموفق بود.", buttons=self._build_main_menu())
                return

            await client.get_dialogs(limit=100)

            for group_id in owned_group_ids:
                try:
                    group_entity = await client.get_entity(PeerChannel(group_id))
                    result = await client(ExportChatInviteRequest(group_entity))
                    links.append(result.link)
                    LOGGER.info(f"Successfully exported link for group {group_id}: {result.link}")
                except ValueError as e:
                    LOGGER.warning(f"Could not resolve entity for group {group_id} with account {account_name}: {e}")
                    links.append(f"Error exporting for group ID {group_id}: Entity not found")
                except Exception as e:
                    LOGGER.warning(f"Could not export link for group {group_id} with account {account_name}: {e}")
                    links.append(f"Error exporting for group ID {group_id}: {e.__class__.__name__}")

            if links:
                file_path = SESSIONS_DIR / f"invite_links_{account_name}_{user_id}.txt"
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write("\n".join(links))

                await self.bot.send_file(user_id, file_path, caption=Config.MSG_EXPORT_SUCCESS)
                os.remove(file_path)
            else:
                await event.reply(Config.MSG_EXPORT_FAIL.format(account_name=account_name))

        except Exception as e:
            await self._send_error_explanation(user_id, e)
        finally:
            if client and client.is_connected():
                await client.disconnect()
            self.user_sessions[user_id]['state'] = 'authenticated'
            await self._start_handler(event)

    async def _handle_force_conv_account_selection(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        account_name = event.message.text.strip()

        if account_name not in self.session_manager.get_user_accounts(user_id):
            await event.reply("❌ حساب انتخاب شده نامعتبر است. لطفاً از دکمه‌ها استفاده کنید.")
            return

        self.user_sessions[user_id]['force_conv_account_name'] = account_name
        self.user_sessions[user_id]['state'] = 'awaiting_force_conv_num_messages'
        await event.reply(Config.MSG_PROMPT_NUM_MESSAGES, buttons=[[Button.text(Config.BTN_BACK)]])

    async def _handle_force_conv_num_messages(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        try:
            num_messages = int(event.message.text.strip())
            if num_messages <= 0:
                raise ValueError
        except (ValueError, TypeError):
            await event.reply("❌ لطفاً یک عدد معتبر و مثبت وارد کنید.", buttons=[[Button.text(Config.BTN_BACK)]])
            return

        account_name = self.user_sessions[user_id].get('force_conv_account_name')
        if not account_name:
            await event.reply("خطای داخلی رخ داده است. لطفاً از ابتدا شروع کنید.", buttons=self._build_main_menu())
            self.user_sessions[user_id]['state'] = 'authenticated'
            return

        worker_key = f"{user_id}:{account_name}"
        if worker_key in self.active_conversations:
            await event.reply(f"❌ مکالمه دستی برای حساب `{account_name}` از قبل فعال است. ابتدا آن را متوقف کنید.", buttons=self._build_main_menu())
            self.user_sessions[user_id]['state'] = 'authenticated'
            return

        owned_group_ids = [
            int(gid) for gid, data in self.created_groups.items()
            if data.get("owner_worker_key") == worker_key
        ]

        if not owned_group_ids:
            await event.reply(Config.MSG_FORCE_CONV_NO_GROUPS.format(account_name=account_name), buttons=self._build_main_menu())
            self.user_sessions[user_id]['state'] = 'authenticated'
            return

        await event.reply(Config.MSG_FORCE_CONV_STARTED.format(count=len(owned_group_ids), account_name=account_name), buttons=self._build_main_menu())

        async def conversation_runner():
            try:
                tasks = [self._run_conversation_task(user_id, group_id, num_messages=num_messages) for group_id in owned_group_ids]
                await asyncio.gather(*tasks)
            except asyncio.CancelledError:
                LOGGER.info(f"Force conversation task for {worker_key} was cancelled.")
            finally:
                if worker_key in self.active_conversations:
                    del self.active_conversations[worker_key]
                    LOGGER.info(f"Force conversation task for {worker_key} finished and cleaned up.")

        task = asyncio.create_task(conversation_runner())
        self.active_conversations[worker_key] = task
        self.user_sessions[user_id]['state'] = 'authenticated'

    async def _handle_stop_force_conv_selection(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        account_name = event.message.text.strip()
        worker_key = f"{user_id}:{account_name}"

        if worker_key in self.active_conversations:
            task = self.active_conversations[worker_key]
            task.cancel()
            await event.reply(Config.MSG_FORCE_CONV_STOPPED.format(account_name=account_name), buttons=self._build_main_menu())
        else:
            await event.reply(f"❌ مکالمه دستی فعالی برای حساب `{account_name}` یافت نشد.", buttons=self._build_main_menu())

        self.user_sessions[user_id]['state'] = 'authenticated'


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
        
        # [MODIFIED] Use resilient login method
        msg = await event.reply("⏳ Trying to connect to Telegram using available methods...")
        user_client, selected_proxy = await self._create_resilient_login_client()

        if not user_client:
            await msg.edit('❌ Failed to connect to Telegram using all available proxies and a direct connection. Please check your network and proxy list, then try again.')
            self.user_sessions[user_id]['state'] = 'awaiting_phone' # Reset state
            return

        # Store the successful proxy (or None for direct) in the session
        self.user_sessions[user_id]['login_proxy'] = selected_proxy
        
        try:
            self.user_sessions[user_id]['client'] = user_client
            sent_code = await user_client.send_code_request(self.user_sessions[user_id]['phone'])
            self.user_sessions[user_id]['phone_code_hash'] = sent_code.phone_code_hash
            self.user_sessions[user_id]['state'] = 'awaiting_code'
            await msg.edit('💬 A login code has been sent. Please send it here.', buttons=[[Button.text(Config.BTN_BACK)]])
        except Exception as e:
            await self._send_error_explanation(user_id, e)
            self.user_sessions[user_id]['state'] = 'awaiting_phone'
            # Clean up client if it exists but something else failed
            if user_client and user_client.is_connected():
                await user_client.disconnect()


    async def _handle_code_input(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        user_client = self.user_sessions[user_id]['client']
        try:
            await user_client.sign_in(self.user_sessions[user_id]['phone'], code=event.message.text.strip(), phone_code_hash=self.user_sessions[user_id].get('phone_code_hash'))
            
            self.user_sessions[user_id]['current_password'] = None 
            self.user_sessions[user_id]['state'] = 'awaiting_2fa_choice'
            await event.reply(
                '🔐 **تایید دو مرحله‌ای**\n\nآیا می‌خواهید رمز تایید دو مرحله‌ای این حساب را به `erfantenhome` تنظیم کنید؟',
                buttons=[
                    [Button.text(Config.BTN_CHANGE_2FA_YES)],
                    [Button.text(Config.BTN_CHANGE_2FA_NO)],
                    [Button.text(Config.BTN_BACK)]
                ]
            )

        except errors.SessionPasswordNeededError:
            self.user_sessions[user_id]['state'] = 'awaiting_password'
            await event.reply('🔑 این حساب دارای تایید دو مرحله‌ای است. لطفاً رمز عبور را ارسال کنید.', buttons=[[Button.text(Config.BTN_BACK)]])
        except errors.PhoneCodeExpiredError:
            try:
                LOGGER.warning(f"Phone code for {user_id} expired. Requesting new code.")
                sent_code = await user_client.send_code_request(self.user_sessions[user_id]['phone'])
                self.user_sessions[user_id]['phone_code_hash'] = sent_code.phone_code_hash
                self.user_sessions[user_id]['state'] = 'awaiting_code'
                await event.reply('⚠️ The code expired. A new code has been sent. Please enter the new code.', buttons=[[Button.text(Config.BTN_BACK)]])
            except Exception as e:
                await self._send_error_explanation(user_id, e)
                self.user_sessions[user_id]['state'] = 'awaiting_phone'
        except Exception as e:
            await self._send_error_explanation(user_id, e)
            self.user_sessions[user_id]['state'] = 'awaiting_phone'

    async def _handle_password_input(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        password = event.message.text.strip()
        try:
            await self.user_sessions[user_id]['client'].sign_in(password=password)
            
            self.user_sessions[user_id]['current_password'] = password
            self.user_sessions[user_id]['state'] = 'awaiting_2fa_choice'
            await event.reply(
                '🔐 **تایید دو مرحله‌ای**\n\nآیا می‌خواهید رمز تایید دو مرحله‌ای این حساب را به `erfantenhome` تغییر دهید؟',
                buttons=[
                    [Button.text(Config.BTN_CHANGE_2FA_YES)],
                    [Button.text(Config.BTN_CHANGE_2FA_NO)],
                    [Button.text(Config.BTN_BACK)]
                ]
            )
        except Exception as e:
            await self._send_error_explanation(user_id, e)
            self.user_sessions[user_id]['state'] = 'awaiting_password'

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

    async def _handle_admin_setting_button(self, event: events.NewMessage.Event, config_key: str):
        user_id = event.sender_id

        if config_key == "VIEW_CONFIG":
            await self._view_config_handler(event)
            return
        
        if config_key == "MIN_SLEEP_SECONDS,MAX_SLEEP_SECONDS":
             prompt_message = f"Please enter the new min and max sleep times, separated by a comma (e.g., `300,900`).\nCurrent: `{self.min_sleep_seconds},{self.max_sleep_seconds}`"
        elif config_key == "MASTER_PASSWORD_HASH":
             prompt_message = f"Please enter the new **plain text** master password. It will be hashed automatically before saving."
        else:
            current_value = self.config.get(config_key, "Not Set")
            prompt_message = f"Please enter the new value for `{config_key}`.\nCurrent: `{current_value}`"

        self.user_sessions[user_id]['state'] = 'awaiting_config_value'
        self.user_sessions[user_id]['config_key_to_set'] = config_key
        await event.reply(prompt_message, buttons=[[Button.text(Config.BTN_BACK)]])

    async def _handle_config_value_input(self, event: events.NewMessage.Event):
        user_id = event.sender_id
        session = self.user_sessions.get(user_id, {})
        key = session.get('config_key_to_set')
        value_str = event.message.text.strip()

        if not key:
            await event.reply("An internal error occurred. Please try again.", buttons=self._build_main_menu())
            session['state'] = 'authenticated'
            return

        if key == "MIN_SLEEP_SECONDS,MAX_SLEEP_SECONDS":
            try:
                min_val, max_val = map(int, value_str.split(','))
                self.config["MIN_SLEEP_SECONDS"] = min_val
                self.config["MAX_SLEEP_SECONDS"] = max_val
                await event.reply(f"✅ Sleep times set to min `{min_val}` and max `{max_val}`.")
            except (ValueError, TypeError):
                await event.reply("❌ Invalid format. Please provide two numbers separated by a comma (e.g., `300,900`).")
                return
        elif key == "MASTER_PASSWORD_HASH":
            hashed_value = hashlib.sha256(value_str.encode()).hexdigest()
            self.config[key] = hashed_value
            await event.reply(f"✅ `{key}` has been updated.")
        else:
            try:
                if '.' in value_str:
                    value = float(value_str)
                else:
                    value = int(value_str)
            except ValueError:
                value = value_str
            
            self.config[key] = value
            await event.reply(f"✅ Config key `{key}` has been set to `{value}`.")

        self._save_json_file(self.config, self.config_file)
        self.update_config_from_file()
        if key == "MAX_CONCURRENT_WORKERS":
            self.worker_semaphore = asyncio.Semaphore(self.max_workers)

        session['state'] = 'authenticated'
        session.pop('config_key_to_set', None)
        await self._settings_handler(event)

    async def _start_dm_chat_handler(self, event: events.NewMessage.Event):
        user_id = event.sender_id
        if user_id != ADMIN_USER_ID:
            return
        self.user_sessions[user_id]['state'] = 'awaiting_dm_target_id'
        await event.reply("👤 Please enter the **User ID** or **username** of the target you want to start a DM chat with.", buttons=[[Button.text(Config.BTN_BACK)]])

    async def _handle_dm_target_id(self, event: events.NewMessage.Event):
        user_id = event.sender_id
        target_id = event.text.strip()
        
        try:
            target_entity = int(target_id)
        except ValueError:
            target_entity = target_id.lstrip('@')

        self.user_sessions[user_id]['dm_target'] = target_entity
        self.user_sessions[user_id]['state'] = 'awaiting_dm_account_selection'
        
        all_accounts = self.session_manager.get_all_accounts()
        if not all_accounts:
            await event.reply("❌ No accounts from any user are connected to the bot.")
            self.user_sessions[user_id]['state'] = 'authenticated'
            return

        buttons = [[Button.text(full_account_key)] for full_account_key in all_accounts.keys()]
        buttons.append([Button.text(Config.BTN_BACK)])
        await event.reply("🤖 Please select the account that will initiate the DM chat (format is `UserID:AccountName`).", buttons=buttons)

    async def _handle_dm_account_selection(self, event: events.NewMessage.Event):
        user_id = event.sender_id
        full_account_key = event.text.strip()
        all_accounts = self.session_manager.get_all_accounts()

        if full_account_key not in all_accounts:
            await event.reply("❌ Invalid account selected. Please use the buttons.")
            return
        
        try:
            dm_user_id_str, dm_account_name = full_account_key.split(":", 1)
            dm_user_id = int(dm_user_id_str)
        except ValueError:
            await event.reply("❌ Invalid account format selected. Please try again.")
            self.user_sessions[user_id]['state'] = 'authenticated'
            await self._start_handler(event)
            return

        self.user_sessions[user_id]['dm_user_id'] = dm_user_id
        self.user_sessions[user_id]['dm_account_name'] = dm_account_name
        self.user_sessions[user_id]['state'] = 'awaiting_dm_initial_prompt'
        await event.reply("✍️ Please provide the initial message to send to the target user.", buttons=[[Button.text(Config.BTN_BACK)]])

    async def _handle_dm_initial_prompt(self, event: events.NewMessage.Event):
        user_id = event.sender_id
        initial_message = event.text.strip()
        session_data = self.user_sessions.get(user_id, {})
        
        account_name = session_data.get('dm_account_name')
        dm_user_id = session_data.get('dm_user_id')
        target_entity = session_data.get('dm_target')

        if not all([account_name, dm_user_id, target_entity, initial_message]):
            await event.reply("❌ An internal error occurred (missing DM data). Please start over.")
            self.user_sessions[user_id]['state'] = 'authenticated'
            return

        await event.reply(f"🚀 Starting DM chat from `{account_name}` to `{target_entity}`...")
        
        client = None
        try:
            session_str = self.session_manager.load_session_string(dm_user_id, account_name)
            client = await self._create_resilient_worker_client(dm_user_id, account_name, session_str)
            if not client:
                await event.reply("❌ Failed to connect with the selected account.")
                return

            await client.send_message(target_entity, initial_message)
            await event.reply("✅ Initial DM sent successfully!")
            LOGGER.info(f"DM sent from '{account_name}' to '{target_entity}'.")

        except Exception as e:
            await self._send_error_explanation(user_id, e)
        finally:
            if client and client.is_connected():
                await client.disconnect()
            
            session_data.pop('dm_target', None)
            session_data.pop('dm_account_name', None)
            session_data.pop('dm_user_id', None)
            session_data['state'] = 'authenticated'

    async def _handle_dm_persona(self, event: events.NewMessage.Event):
        await event.reply("This feature is not available.")
        self.user_sessions[event.sender_id]['state'] = 'authenticated'

    async def _handle_dm_sticker_packs(self, event: events.NewMessage.Event):
        await event.reply("This feature is not available.")
        self.user_sessions[event.sender_id]['state'] = 'authenticated'

    async def _stop_dm_chat_handler(self, event: events.NewMessage.Event):
        await event.reply("DM chat stopping functionality is not available.")

    async def _start_dm_message_handler(self, event: events.NewMessage.Event):
        await event.reply("This feature is not available.")

    async def _handle_dm_message_account_selection(self, event: events.NewMessage.Event):
        await event.reply("This feature is not available.")

    async def _handle_dm_message_target_user(self, event: events.NewMessage.Event):
        await event.reply("This feature is not available.")

    async def _handle_dm_message_prompt(self, event: events.NewMessage.Event):
        await event.reply("This feature is not available.")

    async def _approval_handler(self, event: events.CallbackQuery.Event):
        user_id = event.sender_id
        data = event.data.decode('utf-8')

        if user_id != ADMIN_USER_ID:
            await event.answer("You are not authorized to perform this action.")
            return

        if data.startswith("approve_") or data.startswith("deny_"):
            try:
                action, user_id_str = data.split('_', 1)
                user_id_to_act_on = int(user_id_str)
            except ValueError:
                await event.edit("⚠️ Invalid callback data.")
                return

            if action == "approve":
                if user_id_to_act_on in self.pending_users:
                    self.pending_users.remove(user_id_to_act_on)
                    self.known_users.append(user_id_to_act_on)
                    self._save_pending_users()
                    self._save_known_users()
                    await event.edit(f"✅ User `{user_id_to_act_on}` has been approved.")
                    await self.bot.send_message(user_id_to_act_on, Config.MSG_USER_APPROVED)
                    LOGGER.info(f"Admin approved user {user_id_to_act_on}.")
                else:
                    await event.edit(f"⚠️ User `{user_id_to_act_on}` was not found in the pending list.")
            elif action == "deny":
                if user_id_to_act_on in self.pending_users:
                    self.pending_users.remove(user_id_to_act_on)
                    self._save_pending_users()
                    await event.edit(f"❌ User `{user_id_to_act_on}` has been denied.")
                    await self.bot.send_message(user_id_to_act_on, Config.MSG_USER_DENIED)
                    LOGGER.info(f"Admin denied user {user_id_to_act_on}.")
                else:
                    await event.edit(f"⚠️ User `{user_id_to_act_on}` was not found in the pending list.")

    async def _group_maintenance_scheduler_task(self):
        while True:
            await asyncio.sleep(self.health_check_interval)
            LOGGER.info("[Scheduler] Running periodic group health check...")
            await self.run_group_health_check(triggered_by="Scheduler")

    async def _manual_health_check_handler(self, event: events.NewMessage.Event):
        if event.sender_id != ADMIN_USER_ID:
            return
        
        await event.reply(Config.MSG_HEALTH_CHECK_STARTED)
        asyncio.create_task(self.run_group_health_check(triggered_by=f"Admin ({event.sender_id})"))

    async def run_group_health_check(self, triggered_by: str):
        if self.health_check_lock.locked():
            LOGGER.warning(f"Health check triggered by {triggered_by} but another check is already in progress. Skipping.")
            return

        async with self.health_check_lock:
            if triggered_by.startswith("Admin"):
                await self._broadcast_message(Config.MSG_MAINTENANCE_BROADCAST_START)

            LOGGER.info(f"--- Group Health Check Started (Trigger: {triggered_by}) ---")
            
            healed_count = 0
            cleaned_count = 0
            topped_up_count = 0
            
            all_accounts = self.session_manager.get_all_accounts()

            for owner_key, user_id in all_accounts.items():
                owner_client = None
                try:
                    user_id_str, account_name = owner_key.split(":", 1)
                    
                    session_str = self.session_manager.load_session_string(user_id, account_name)
                    if not session_str:
                        LOGGER.warning(f"[Health Check] No session for owner {owner_key}, skipping their groups.")
                        continue
                    
                    owner_client = await self._create_resilient_worker_client(user_id, account_name, session_str)
                    if not owner_client:
                        LOGGER.error(f"[Health Check] Failed to connect as owner {owner_key}, skipping their groups.")
                        continue
                    
                    me = await owner_client.get_me()
                    my_id = me.id

                    LOGGER.info(f"[Health Check] Discovering and healing groups for owner {owner_key}.")
                    async for dialog in owner_client.iter_dialogs():
                        if not (dialog.is_group and dialog.entity.megagroup):
                            continue

                        group_id = dialog.id
                        group_id_str = str(dialog.id)
                        
                        is_known = group_id_str in self.created_groups
                        title_matches = dialog.title.startswith("collage Semester ")

                        if not (is_known or title_matches):
                            continue

                        if is_known and "owner_id" in self.created_groups[group_id_str]:
                            owner_id = self.created_groups[group_id_str]["owner_id"]
                        else:
                            LOGGER.info(f"[Health Check] Group {group_id} is legacy or newly discovered. Finding creator...")
                            creator_id = None
                            try:
                                async for p in owner_client.iter_participants(dialog.entity, filter=ChannelParticipantsAdmins):
                                    if isinstance(p.participant, ChannelParticipantCreator):
                                        creator_id = p.id
                                        break
                                if creator_id:
                                    LOGGER.info(f"Found creator for group {group_id}: {creator_id}. Updating records.")
                                    self.created_groups[group_id_str] = {
                                        "owner_worker_key": owner_key,
                                        "owner_id": creator_id,
                                        "last_simulated": self.created_groups.get(group_id_str, {}).get("last_simulated", 0)
                                    }
                                    owner_id = creator_id
                                    healed_count += 1
                                    self._save_created_groups()
                                else:
                                    LOGGER.warning(f"Could not find a creator for group {group_id}. Skipping.")
                                    continue
                            except Exception as e:
                                LOGGER.error(f"Error finding creator for group {group_id}: {e}")
                                continue

                        try:
                            participants = await owner_client.get_participants(dialog.entity, limit=200)
                            if len(participants) > 1:
                                LOGGER.info(f"[Health Check] Group {group_id} has {len(participants)} members. Cleaning up...")
                                for p in participants:
                                    if p.id != owner_id:
                                        try:
                                            await owner_client.kick_participant(dialog.entity, p)
                                            LOGGER.info(f"Kicked member {p.id} from group {group_id}.")
                                            await asyncio.sleep(1)
                                        except Exception as e:
                                            LOGGER.error(f"Failed to kick {p.id} from {group_id}: {e}")
                                cleaned_count += 1

                            messages = await owner_client.get_messages(dialog.entity, limit=1)
                            total_messages = messages.total if messages else 0
                            
                            daily_msg_count = self._get_daily_count_for_group(group_id)
                            remaining_daily = self.daily_message_limit - daily_msg_count

                            if total_messages < 20 and remaining_daily > 0:
                                messages_to_send = min(20 - total_messages, remaining_daily)
                                LOGGER.info(f"[Health Check] Group {group_id} has {total_messages} messages. Topping up with {messages_to_send} more.")
                                
                                for _ in range(messages_to_send):
                                    message = random.choice(Config.RANDOM_MESSAGES)
                                    await owner_client.send_message(dialog.entity, message)
                                    self._increment_daily_count_for_group(group_id)
                                    await asyncio.sleep(random.uniform(1,3))
                                topped_up_count += 1
                                
                        except Exception as group_err:
                            LOGGER.error(f"[Health Check] Error processing group {group_id}: {group_err}")

                except Exception as owner_err:
                    LOGGER.error(f"[Health Check] Major error processing owner {owner_key}: {owner_err}")
                finally:
                    if owner_client and owner_client.is_connected():
                        await owner_client.disconnect()
            
            LOGGER.info(f"--- Group Health Check Finished (Trigger: {triggered_by}) ---")
            if triggered_by.startswith("Admin"):
                await self.bot.send_message(
                    ADMIN_USER_ID, 
                    Config.MSG_HEALTH_CHECK_COMPLETE.format(healed_count=healed_count, cleaned_count=cleaned_count, topped_up_count=topped_up_count)
                )
                await self._broadcast_message(Config.MSG_MAINTENANCE_BROADCAST_END)

    async def _periodic_random_messaging_task(self):
        """Every two days, sends one message to 5 random groups."""
        while True:
            await asyncio.sleep(172800)
            LOGGER.info("[Scheduler] Running periodic random messaging task...")

            if len(self.created_groups) < 5:
                LOGGER.info("[Scheduler] Not enough groups (< 5) to run periodic messaging. Skipping.")
                continue

            all_group_ids = list(self.created_groups.keys())
            selected_group_ids = random.sample(all_group_ids, 5)
            LOGGER.info(f"[Scheduler] Selected 5 random groups for messaging: {selected_group_ids}")

            groups_by_owner = {}
            for group_id in selected_group_ids:
                data = self.created_groups.get(group_id)
                if data and data.get("owner_worker_key"):
                    owner_key = data["owner_worker_key"]
                    groups_by_owner.setdefault(owner_key, []).append(int(group_id))

            for owner_key, group_ids in groups_by_owner.items():
                client = None
                try:
                    user_id_str, account_name = owner_key.split(":", 1)
                    user_id = int(user_id_str)
                    session_str = self.session_manager.load_session_string(user_id, account_name)
                    if not session_str:
                        LOGGER.warning(f"[Scheduler] Could not load session for {owner_key}. Skipping {len(group_ids)} groups.")
                        continue

                    client = await self._create_resilient_worker_client(user_id, account_name, session_str)
                    if not client:
                        LOGGER.error(f"[Scheduler] Could not connect as {owner_key}. Skipping {len(group_ids)} groups.")
                        continue

                    for group_id in group_ids:
                        try:
                            message = random.choice(Config.RANDOM_MESSAGES)
                            await client.send_message(PeerChannel(group_id), message)
                            self._increment_daily_count_for_group(group_id)
                            LOGGER.info(f"[Scheduler] Sent a random message to group {group_id} using account {owner_key}.")
                            await asyncio.sleep(random.uniform(5, 10))
                        except Exception as e:
                            LOGGER.error(f"[Scheduler] Failed to send message to group {group_id} with owner {owner_key}: {e}")

                except Exception as e:
                    LOGGER.error(f"[Scheduler] Major error while processing owner {owner_key}: {e}")
                finally:
                    if client and client.is_connected():
                        await client.disconnect()

    async def _message_all_groups_handler(self, event: events.NewMessage.Event):
        if event.sender_id != ADMIN_USER_ID:
            return
        
        await event.reply(Config.MSG_MESSAGE_ALL_GROUPS_STARTED)
        asyncio.create_task(self.run_message_all_groups())

    async def run_message_all_groups(self):
        if self.health_check_lock.locked():
            LOGGER.warning("Message all groups task triggered but another is already in progress. Skipping.")
            await self.bot.send_message(ADMIN_USER_ID, "⚠️ یک عملیات تعمیر و نگهداری دیگر (مانند بررسی سلامت) از قبل در حال اجرا است.")
            return

        async with self.health_check_lock:
            await self._broadcast_message(Config.MSG_MAINTENANCE_BROADCAST_START)
            LOGGER.info("--- Messaging All Groups Task Started ---")
            
            accounts_processed = 0
            total_messages_sent = 0
            
            all_accounts = self.session_manager.get_all_accounts()
            total_accounts = len(all_accounts)

            for owner_key, user_id in all_accounts.items():
                client = None
                try:
                    user_id_str, account_name = owner_key.split(":", 1)
                    
                    session_str = self.session_manager.load_session_string(user_id, account_name)
                    if not session_str:
                        LOGGER.warning(f"[Message All] No session for account {owner_key}, skipping.")
                        continue
                    
                    client = await self._create_resilient_worker_client(user_id, account_name, session_str)
                    if not client:
                        LOGGER.error(f"[Message All] Failed to connect as account {owner_key}, skipping.")
                        continue

                    LOGGER.info(f"[Message All] Processing groups for account {owner_key} ({accounts_processed + 1}/{total_accounts}).")
                    async for dialog in client.iter_dialogs():
                        if dialog.is_group:
                            LOGGER.info(f"[Message All] Found group '{dialog.title}' (ID: {dialog.id}) for account {owner_key}.")
                            try:
                                for i in range(10):
                                    message_text = random.choice(Config.RANDOM_MESSAGES)
                                    await client.send_message(dialog.id, message_text)
                                    total_messages_sent += 1
                                    LOGGER.info(f"[Message All] >> Sent message {i + 1}/10 to group '{dialog.title}' from {owner_key}.")
                                    await asyncio.sleep(1)
                            except (errors.ChatWriteForbiddenError, errors.ChatAdminRequiredError):
                                LOGGER.warning(f"[Message All] Account {owner_key} cannot send messages in group '{dialog.title}'. Skipping.")
                                break
                            except Exception as e:
                                LOGGER.error(f"[Message All] Error sending message to group '{dialog.title}' with account {owner_key}: {e}")
                    
                    accounts_processed += 1
                    if accounts_processed % 5 == 0 and accounts_processed < total_accounts:
                        await self.bot.send_message(
                            ADMIN_USER_ID,
                            f"🔄 **گزارش پیشرفت:**\n\n- **اکانت‌های پردازش شده:** {accounts_processed} از {total_accounts}\n- **مجموع پیام‌های ارسال شده:** {total_messages_sent}"
                        )

                except Exception as e:
                    LOGGER.error(f"[Message All] Major error processing account {owner_key}: {e}")
                finally:
                    if client and client.is_connected():
                        await client.disconnect()

            LOGGER.info("--- Messaging All Groups Task Finished ---")
            await self.bot.send_message(
                ADMIN_USER_ID, 
                Config.MSG_MESSAGE_ALL_GROUPS_COMPLETE.format(
                    accounts_processed=accounts_processed, 
                    total_messages_sent=total_messages_sent
                )
            )
            await self._broadcast_message(Config.MSG_MAINTENANCE_BROADCAST_END)

          
    async def _send_error_explanation(self, user_id: int, e: Exception):
        LOGGER.error(f"An error occurred for user {user_id}", exc_info=True)
        sentry_sdk.capture_exception(e)

        traceback_str = traceback.format_exc()
        
        user_message = "❌ یک خطای پیش‌بینی نشده رخ داد. لطفاً دوباره تلاش کنید."
        if isinstance(e, errors.FloodWaitError):
            user_message = f"⏳ تلگرام از ما خواسته است که {e.seconds} ثانیه صبر کنیم. لطفاً بعد از این مدت دوباره تلاش کنید."
        elif isinstance(e, (errors.UserDeactivatedBanError, errors.PhoneNumberBannedError)):
            user_message = "🚨 متاسفانه این حساب توسط تلگرام مسدود یا حذف شده است و دیگر قابل استفاده نیست."
        elif isinstance(e, asyncio.TimeoutError):
            user_message = "⌛️ اتصال به سرورهای تلگرام بیش از حد طول کشید. لطفاً از اتصال اینترنت خود مطمئن شوید و دوباره تلاش کنید."
        elif isinstance(e, errors.AuthKeyUnregisteredError):
             user_message = "🔑 نشست (Session) این حساب منقضی شده است. لطفاً حساب را حذف کرده و دوباره اضافه کنید."

        try:
            await self.bot.send_message(user_id, user_message)
        except Exception as send_error:
            LOGGER.error(f"Failed to send error explanation message to user {user_id}: {send_error}")

        try:
            admin_error_report = (
                f"**🚨 Error Report for User `{user_id}`**\n\n"
                f"**Simplified Message:**\n{user_message}\n\n"
                f"**Full Traceback:**\n```\n{traceback_str}\n```"
            )
            if len(admin_error_report) > 4096:
                for i in range(0, len(admin_error_report), 4096):
                    await self.bot.send_message(ADMIN_USER_ID, admin_error_report[i:i + 4096])
            else:
                await self.bot.send_message(ADMIN_USER_ID, admin_error_report)
        except Exception as admin_send_error:
            LOGGER.error(f"Failed to send full error traceback to admin: {admin_send_error}")

    async def _test_self_healing_handler(self, event: events.NewMessage.Event):
        await event.reply(
            "💥 **Simulating critical failure!**\n\n"
            "I will now raise an unhandled exception. If you are running this bot with a process manager "
            "(like `systemd` or a Docker restart policy), it should restart automatically within a few moments. "
            "The error will be reported to Sentry if configured."
        )
        await asyncio.sleep(2)
        raise RuntimeError("Simulating a critical failure for self-healing test.")

    def register_handlers(self) -> None:
        self.bot.add_event_handler(self._start_handler, events.NewMessage(pattern='/start'))
        self.bot.add_event_handler(self._message_router, events.NewMessage)
        self.bot.add_event_handler(self._approval_handler, events.CallbackQuery)

    async def run(self) -> None:
        """Main entry point for the bot."""
        await self._initialize_sentry()
        self.register_handlers()
        LOGGER.info("Starting bot...")
        try:
            await self.bot.start(bot_token=BOT_TOKEN)
            LOGGER.info("Bot service started successfully.")

            self.bot.loop.create_task(self._group_maintenance_scheduler_task())
            self.bot.loop.create_task(self._periodic_random_messaging_task())
            
            if self.active_workers_state:
                LOGGER.info(f"Found {len(self.active_workers_state)} workers to resume from previous session.")
                for worker_key, worker_data in list(self.active_workers_state.items()):
                    user_id = worker_data["user_id"]
                    account_name = worker_data["account_name"]
                    LOGGER.info(f"Attempting to resume worker for account '{account_name}' ({worker_key}).")
                    await self._start_worker_task(user_id, account_name)

            if self.known_users:
                await self._broadcast_message("✅ Bot has restarted successfully and is now online.")
            
            await self.bot.run_until_disconnected()

        except Exception as e:
            LOGGER.critical(f"A critical error occurred in the main run loop: {e}", exc_info=True)
            sentry_sdk.capture_exception(e)
            
        finally:
            LOGGER.info("Bot service is shutting down. Disconnecting main bot client.")
            if self.bot.is_connected():
                await self.bot.disconnect()


if __name__ == "__main__":
    bot_instance = GroupCreatorBot(SessionManager)
    try:
        asyncio.run(bot_instance.run())
    except Exception as e:
        LOGGER.critical("Bot crashed at the top level.", exc_info=True)


