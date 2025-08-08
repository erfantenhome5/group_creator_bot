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

import httpx
import sentry_sdk
from cryptography.fernet import Fernet, InvalidToken
from dotenv import load_dotenv
from sentry_sdk.integrations.logging import LoggingIntegration
from sentry_sdk.types import Event, Hint
from telethon import Button, TelegramClient, errors, events, types, sessions
from telethon.extensions import markdown
from telethon.tl.functions.channels import (CreateChannelRequest, GetParticipantRequest,
                                            InviteToChannelRequest, LeaveChannelRequest)
from telethon.tl.functions.messages import (ExportChatInviteRequest,
                                            GetAllStickersRequest,
                                            GetStickerSetRequest,
                                            ImportChatInviteRequest,
                                            SendReactionRequest,
                                            SearchStickerSetsRequest)
from telethon.tl.types import (ChannelParticipantCreator, ChannelParticipantsAdmins,
                               InputStickerSetID, InputStickerSetShortName, Message,
                               PeerChannel, ReactionEmoji)

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
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", OPENROUTER_API_KEY) # Use OpenRouter key as fallback for Gemini


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
        with open(proxy_file_path, 'r', encoding='utf-8') as f:
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

# --- Proxy Manager for Global Rate Limiting ---
class ProxyManager:
    """
    Manages proxy selection and enforces a global rate limit (RPM).
    Note: This manager primarily handles Requests Per Minute (RPM). While it helps
    mitigate other limits like Tokens Per Minute/Day (TPM/TPD) by spacing out
    requests, it does not explicitly track token counts or daily quotas.
    """
    RATE_LIMIT = 480  # requests (kept slightly below 500 for safety)
    TIME_WINDOW = 60  # seconds

    def __init__(self, proxies: List[Dict]):
        self._proxies = proxies
        self._request_timestamps = deque()
        self._lock = asyncio.Lock()

    async def get_proxy(self) -> Optional[Dict]:
        """
        Returns a proxy while respecting the global rate limit.
        Waits if the rate limit has been exceeded.
        """
        if not self._proxies:
            return None

        async with self._lock:
            now = time.monotonic()
            
            # Remove timestamps older than the time window
            while self._request_timestamps and self._request_timestamps[0] <= now - self.TIME_WINDOW:
                self._request_timestamps.popleft()

            # If we've hit the rate limit, wait for the oldest request to expire
            if len(self._request_timestamps) >= self.RATE_LIMIT:
                oldest_request_time = self._request_timestamps[0]
                wait_time = oldest_request_time - (now - self.TIME_WINDOW)
                if wait_time > 0:
                    LOGGER.warning(f"Global proxy rate limit hit. Waiting for {wait_time:.2f} seconds.")
                    await asyncio.sleep(wait_time)
            
            # Add new timestamp and return a random proxy
            self._request_timestamps.append(time.monotonic())
            return random.choice(self._proxies)

# --- Centralized Configuration ---
class Config:
    """Holds all configurable values and UI strings for the bot."""
    # Bot Settings
    MAX_CONCURRENT_WORKERS = 5
    GROUPS_TO_CREATE = 50
    MIN_SLEEP_SECONDS = 144
    MAX_SLEEP_SECONDS = 288
    PROXY_FILE = "proxy.txt"
    PROXY_TIMEOUT = 15
    DAILY_MESSAGE_LIMIT_PER_GROUP = 20
    MESSAGE_SEND_DELAY_MIN = 1
    MESSAGE_SEND_DELAY_MAX = 5
    GROUP_HEALTH_CHECK_INTERVAL_SECONDS = 604800 # 7 days
    AI_REQUEST_TIMEOUT = 30 # [MODIFIED]

    # [NEW] Predefined fallback messages for when AI fails
    PREDEFINED_FALLBACK_MESSAGES = [
        "سلام دوستان!",
        "چه خبر؟",
        "کسی اینجا هست؟",
        "🤔",
        "👍",
        "عالیه!",
        "موافقم.",
        "جالبه.",
        "چه روز خوبی!",
        "امیدوارم همگی خوب باشید."
    ]

    # [NEW] Personas for more human-like conversations
    PERSONAS = [
        "یک فرد بسیار مشتاق و با انگیزه که همیشه در مورد موفقیت و اهداف صحبت می کند.",
        "یک فرد شوخ طبع و بامزه که سعی می کند با جوک و داستان های خنده دار دیگران را بخنداند.",
        "یک فرد کنجکاو و اهل فن که به تکنولوژی و گجت های جدید علاقه دارد.",
        "یک فرد آرام و متفکر که سوالات عمیق می پرسد و به دنبال معنای زندگی است.",
        "یک فرد عملگرا و واقع بین که همیشه به دنبال راه حل های عملی برای مشکلات است.",
        "یک هنرمند خلاق که در مورد هنر، موسیقی و زیبایی صحبت می کند.",
        "یک ورزشکار پرانرژی که در مورد تناسب اندام و سبک زندگی سالم صحبت می کند."
    ]
    
    # [NEW & EXPANDED] User agents for more diverse client representation
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
    BTN_SET_KEYWORDS = "📝 تنظیم کلمات کلیدی AI"
    BTN_SET_STICKERS = "🎨 تنظیم استیکرها"
    BTN_SET_CONVERSATION_ACCOUNTS = "🗣️ تنظیم حساب‌های گفتگو"
    BTN_JOIN_VIA_LINK = "🔗 عضویت با لینک"
    BTN_EXPORT_LINKS = "🔗 صدور لینک‌های گروه"
    BTN_FORCE_CONVERSATION = "💬 شروع مکالمه دستی"
    BTN_STOP_FORCE_CONVERSATION = "⏹️ توقف مکالمه دستی"
    BTN_MANUAL_HEALTH_CHECK = "🩺 بررسی سلامت گروه‌ها" # [NEW] Admin button
    BTN_MESSAGE_ALL_GROUPS = "💬 پیام دار کردن همه گروه ها"

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
        f"**{BTN_JOIN_VIA_LINK}**\n"
        "یکی از حساب‌های خود را با استفاده از لینک دعوت در یک یا چند گروه/کانال عضو کنید.\n\n"
        f"**{BTN_EXPORT_LINKS}**\n"
        "لینک‌های دعوت تمام گروه‌هایی که توسط یک حساب خاص ساخته شده را دریافت کنید.\n\n"
        f"**{BTN_FORCE_CONVERSATION}**\n"
        "مکالمه را به صورت دستی در تمام گروه‌های ساخته شده توسط یک حساب خاص فعال کنید.\n\n"
        f"**{BTN_STOP_FORCE_CONVERSATION}**\n"
        "یک مکالمه دستی در حال اجرا را متوقف کنید.\n\n"
        f"**{BTN_SET_KEYWORDS}**\n"
        "کلمات کلیدی مورد نظر خود را برای تولید محتوای هوش مصنوعی تنظیم کنید.\n\n"
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
    MSG_PROMPT_KEYWORDS = "📝 لطفاً کلمات کلیدی مورد نظر خود را برای تولید محتوای هوش مصنوعی وارد کنید. کلمات را با کاما (,) از هم جدا کنید.\n\nمثال: موفقیت, بازاریابی, ارز دیجیتال, فروش آنلاین"
    MSG_KEYWORDS_SET = "✅ کلمات کلیدی شما با موفقیت ذخیره شد."
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
        self.health_check_lock = asyncio.Lock() # [NEW] Lock for health checks
        self.gemini_api_keys = deque() # ADD THIS LINE
        self.message_all_lock = asyncio.Lock() # ADD THIS LINE
        self.config_file = SESSIONS_DIR / "config.json"
        self.config = self._load_json_file(self.config_file, {})
        self.update_config_from_file()

        self.worker_semaphore = asyncio.Semaphore(self.config.get("MAX_CONCURRENT_WORKERS", 5))
        
        self.counts_file = SESSIONS_DIR / "group_counts.json"
        self.group_counts = self._load_group_counts()
        self.daily_counts_file = SESSIONS_DIR / "daily_counts.json"
        self.daily_counts = self._load_daily_counts()
        self.proxies = load_proxies_from_file(self.config.get("PROXY_FILE", "proxy.txt"))
        if not self.proxies:
            LOGGER.info("No proxies loaded from file. AI requests will attempt to use system proxy settings if available. Telegram connections will be direct.")
        self.proxy_manager = ProxyManager(self.proxies)
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
        self.keywords_file = SESSIONS_DIR / "keywords.json"
        self.user_keywords = self._load_user_keywords()
        self.user_sticker_packs_file = SESSIONS_DIR / "user_sticker_packs.json"
        self.user_sticker_packs = self._load_user_sticker_packs()
        self.conversation_accounts_file = SESSIONS_DIR / "conversation_accounts.json"
        self.conversation_accounts = self._load_conversation_accounts()
        self.user_worker_limits_file = SESSIONS_DIR / "user_worker_limits.json" # [NEW]
        self.user_worker_limits = self._load_user_worker_limits() # [NEW]
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
        self.openrouter_api_key = self.config.get("OPENROUTER_API_KEY", OPENROUTER_API_KEY)
        self.health_check_interval = self.config.get("GROUP_HEALTH_CHECK_INTERVAL_SECONDS", Config.GROUP_HEALTH_CHECK_INTERVAL_SECONDS)
        self.ai_request_timeout = self.config.get("AI_REQUEST_TIMEOUT", Config.AI_REQUEST_TIMEOUT)

        # [FIX] Correctly load and parse the list of Gemini API keys
        gemini_keys_str = self.config.get("GEMINI_API_KEYS", os.getenv("GEMINI_API_KEYS"))
        if gemini_keys_str:
            self.gemini_api_keys = deque([key.strip() for key in gemini_keys_str.split(',') if key.strip()])
        else:
            self.gemini_api_keys = deque()
        


        self.gemini_model_hierarchy = self.config.get("GEMINI_MODEL_HIERARCHY", [
            "gemini-2.5-pro",
            "gemini-2.5-flash",
            "gemini-2.0-flash",
            "gemini-2.0-flash-lite",
            "gemini-1.5-flash"
        ])
        
        self.openrouter_model_hierarchy = self.config.get("OPENROUTER_MODEL_HIERARCHY", [
            "moonshotai/kimi-k2:free",
            "openrouter/auto"
        ])

        self.custom_prompt = self.config.get("CUSTOM_PROMPT", None)

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
            level=logging.INFO,        # Capture INFO level logs from Python's logging
            event_level=logging.ERROR  # Send logs of level ERROR as Sentry events
        )

        sentry_options = {
            "dsn": sentry_dsn,
            "integrations": [sentry_logging],
            "traces_sample_rate": 1.0, # To capture 100% of transactions for tracing
            "_experiments": {
                "enable_logs": True, # To enable the Sentry Logs feature
            },
            "before_send": before_send_hook,
        }
        
        sentry_proxy = await self.proxy_manager.get_proxy()
        if sentry_proxy:
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

    def _load_user_keywords(self) -> Dict[str, List[str]]:
        return self._load_json_file(self.keywords_file, {})

    def _save_user_keywords(self) -> None:
        self._save_json_file(self.user_keywords, self.keywords_file)

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

    def _load_user_worker_limits(self) -> Dict[str, int]: # [NEW]
        return self._load_json_file(self.user_worker_limits_file, {})

    def _save_user_worker_limits(self) -> None: # [NEW]
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


    # PASTE THE NEW FUNCTION HERE
    async def _set_user_limit_handler(self, event: events.NewMessage.Event, user_id: int, limit: int):
        """[NEW] Sets the concurrent worker limit for a specific user."""
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
        device_params = random.choice(Config.USER_AGENTS) # [MODIFIED]

        try:
            proxy_info = f"with proxy {proxy['addr']}:{proxy['port']}" if proxy else "without proxy (direct connection)"
            LOGGER.debug(f"Attempting login connection {proxy_info}")
            client = TelegramClient(session, API_ID, API_HASH, proxy=proxy, timeout=self.proxy_timeout, **device_params)
            client.parse_mode = CustomMarkdown() # Apply custom parser
            await client.connect()
            return client
        except Exception as e:
            LOGGER.error(f"Login connection {proxy_info} failed: {e}")
            return None

    async def _create_worker_client(self, session_string: str, proxy: Optional[Dict]) -> Optional[TelegramClient]:
        session = sessions.StringSession(session_string)
        device_params = random.choice(Config.USER_AGENTS) # [MODIFIED]

        client = TelegramClient(
            session, API_ID, API_HASH, proxy=proxy, timeout=self.proxy_timeout,
            device_model=device_params['device_model'], system_version=device_params['system_version']
        )
        client.parse_mode = CustomMarkdown() # Apply custom parser

        try:
            proxy_info = f"with proxy {proxy['addr']}:{proxy['port']}" if proxy else "without proxy"
            LOGGER.debug(f"Attempting worker connection {proxy_info}")
            await client.connect()
            LOGGER.info(f"Worker connected successfully {proxy_info}")
            return client
        except errors.AuthKeyUnregisteredError:
            # Re-raise this specific error to be handled by the caller
            raise
        except Exception as e:
            LOGGER.error(f"Worker connection {proxy_info} failed: {e}")
            sentry_sdk.capture_exception(e)
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
            [Button.text(Config.BTN_FORCE_CONVERSATION), Button.text(Config.BTN_STOP_FORCE_CONVERSATION)],
            [Button.text(Config.BTN_SET_KEYWORDS), Button.text(Config.BTN_SET_CONVERSATION_ACCOUNTS)],
            [Button.text(Config.BTN_SET_STICKERS)],
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
                # Remove the invalid pack from the user's list to prevent future errors
                if str(user_id) in self.user_sticker_packs and pack_name_to_use in self.user_sticker_packs[str(user_id)]:
                    self.user_sticker_packs[str(user_id)].remove(pack_name_to_use)
                    self._save_user_sticker_packs()
                return None
        
        documents = self.sticker_sets.get(pack_name_to_use)
        return random.choice(documents) if documents else None

    async def _execute_gemini_request(self, api_key: str, model_name: str, prompt: str, proxy_info: Optional[Dict]) -> Optional[List[str]]:
        """A unified function to execute an AI request against the Gemini API."""
        proxy_url = f"http://{proxy_info['addr']}:{proxy_info['port']}" if proxy_info else None
        
        if not api_key: return None
        
        api_model_name = model_name.replace("-latest", "").replace("-preview-0617", "")
        
        api_url = f"https://generativelanguage.googleapis.com/v1beta/models/{api_model_name}:generateContent?key={api_key}"
        payload = {"contents": [{"parts": [{"text": prompt}]}]}
        headers = {'Content-Type': 'application/json'}
        
        async with httpx.AsyncClient(proxy=proxy_url, timeout=self.ai_request_timeout) as client: # [MODIFIED]
            response = await client.post(api_url, json=payload, headers=headers)
            response.raise_for_status()
            res_json = response.json()
            if res_json.get("candidates") and res_json["candidates"][0].get("content", {}).get("parts", [{}])[0].get("text"):
                message = res_json["candidates"][0]["content"]["parts"][0]["text"]
                LOGGER.info(f"Successfully generated message from Gemini model: {model_name}.")
                return [message.strip()]
        
        return None

    async def _generate_persian_messages(self, user_id: int, persona: str, previous_message: Optional[str] = None, ai_is_down: bool = False) -> List[str]:
        """
        Generates a message using a specific persona, optionally replying to a previous message.
        If ai_is_down is True, it will immediately return a predefined message.
        """
        if ai_is_down:
            LOGGER.info("AI is marked as down for this session, using predefined fallback.")
            return [random.choice(Config.PREDEFINED_FALLBACK_MESSAGES)]

        if not self.gemini_api_keys:
            LOGGER.warning("No Gemini API keys are set. Using predefined fallback.")
            return [random.choice(Config.PREDEFINED_FALLBACK_MESSAGES)]

        keywords = self.user_keywords.get(str(user_id), ["موفقیت", "انگیزه", "رشد"])
        
        if previous_message:
            prompt = (
                f"You are a person in a group chat. Your personality is: '{persona}'. "
                f"Someone else just said: '{previous_message}'. "
                f"Write a short, casual, and natural-sounding reply in Persian. "
                f"Keep it to one or two sentences. Use slang and emojis if it fits your personality."
            )
        else:
            prompt = (
                f"You are a person starting a conversation in a group chat. Your personality is: '{persona}'. "
                f"Start a conversation about one of these topics: {', '.join(keywords)}. "
                f"Write a short, casual, and engaging opening message in Persian (one or two sentences). "
                f"Use slang and emojis if it fits your personality."
            )
        
        for model in self.gemini_model_hierarchy:
            LOGGER.info(f"Attempting AI generation with model: {model}")
            
            # Loop through available API keys, giving each one a chance
            for _ in range(len(self.gemini_api_keys)):
                current_key = self.gemini_api_keys[0] # Get the current key at the front
                try:
                    proxy_info = await self.proxy_manager.get_proxy()
                    result = await self._execute_gemini_request(current_key, model, prompt, proxy_info)
                    if result:
                        return result # Success!
                except httpx.HTTPStatusError as e:
                    # If rate limited or key is bad, rotate to the next key and try again
                    if e.response.status_code in [429, 403]: 
                        LOGGER.warning(f"Gemini API key ending in '...{current_key[-4:]}' failed with status {e.response.status_code}. Rotating to next key.")
                        self.gemini_api_keys.rotate(-1) # Move the failed key to the end of the list
                        await asyncio.sleep(1) # Small delay before retrying with the next key
                    else:
                        LOGGER.error(f"HTTP error for {model} with key '...{current_key[-4:]}': {e}", exc_info=True)
                        break # Unrecoverable error for this model, try the next model
                except Exception as e:
                    LOGGER.error(f"Request failed for {model} with key '...{current_key[-4:]}': {e}", exc_info=True)
                    break # Unrecoverable error for this model, try the next model
            
            LOGGER.warning(f"All API keys failed for model {model}. Trying next model in hierarchy.")

        LOGGER.error("All AI models and all API keys in the hierarchy failed. Using a predefined fallback message.")
        return [random.choice(Config.PREDEFINED_FALLBACK_MESSAGES)]

        if not self.gemini_api_key:
            LOGGER.warning("No Gemini API key is set. Using predefined fallback.")
            return [random.choice(Config.PREDEFINED_FALLBACK_MESSAGES)]

        keywords = self.user_keywords.get(str(user_id), ["موفقیت", "انگیزه", "رشد"])
        
        if previous_message:
            prompt = (
                f"You are a person in a group chat. Your personality is: '{persona}'. "
                f"Someone else just said: '{previous_message}'. "
                f"Write a short, casual, and natural-sounding reply in Persian. "
                f"Keep it to one or two sentences. Use slang and emojis if it fits your personality."
            )
        else:
            prompt = (
                f"You are a person starting a conversation in a group chat. Your personality is: '{persona}'. "
                f"Start a conversation about one of these topics: {', '.join(keywords)}. "
                f"Write a short, casual, and engaging opening message in Persian (one or two sentences). "
                f"Use slang and emojis if it fits your personality."
            )
        
        async def make_request_with_backoff(request_func, model_name, prompt_text, max_retries=1, initial_delay=2): # [MODIFIED]
            delay = initial_delay
            for attempt in range(max_retries):
                try:
                    proxy_info = await self.proxy_manager.get_proxy()
                    result = await request_func(model_name, prompt_text, proxy_info)
                    if result:
                        return result
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 429:
                        LOGGER.warning(f"Rate limit hit for {model_name} on attempt {attempt + 1}. Retrying in {delay} seconds with a new proxy.")
                        await asyncio.sleep(delay)
                        delay *= 2
                    else:
                        LOGGER.error(f"HTTP error for {model_name}: {e}", exc_info=True)
                        return None
                except Exception as e:
                    LOGGER.error(f"Request failed for {model_name}: {e}", exc_info=True)
                    return None
            LOGGER.error(f"AI request for {model_name} failed after {max_retries} retries.")
            return None

        # Iterate through the model hierarchy
        for model in self.gemini_model_hierarchy:
            LOGGER.info(f"Attempting AI generation with model: {model}")
            result = await make_request_with_backoff(self._execute_gemini_request, model, prompt)
            if result:
                return result
            LOGGER.warning(f"Model {model} failed. Trying next model in hierarchy.")

        LOGGER.error("All AI models in the hierarchy failed. Using a predefined fallback message.")
        return [random.choice(Config.PREDEFINED_FALLBACK_MESSAGES)]

    async def _ensure_entity_cached(self, client: TelegramClient, group_id: int, account_name: str, retries: int = 5, delay: int = 1) -> bool:
        """[FIXED] Ensures the client has cached the group entity and is a participant."""
        for attempt in range(retries):
            try:
                # Check connection before making calls
                if not client.is_connected():
                    await client.connect()

                # Step 1: Resolve the entity. This is a high-level call.
                group_entity = await client.get_entity(PeerChannel(group_id))
                
                # Step 2: Verify participation.
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
                # This can happen if the entity isn't in the dialogs list yet.
                LOGGER.warning(f"Attempt {attempt + 1}/{retries}: Account '{account_name}' could not find entity for group {group_id}. Retrying in {delay}s. Error: {e}")
                if attempt < retries - 1:
                    await client.get_dialogs(limit=1) # Force update dialogs
                    await asyncio.sleep(delay)
                else:
                     LOGGER.error(f"Account '{account_name}' failed to cache entity for group {group_id} after {retries} retries.")
                     return False
            except Exception as e:
                LOGGER.error(f"Unexpected error while ensuring entity cached for '{account_name}' in group {group_id}: {e}", exc_info=True)
                sentry_sdk.capture_exception(e)
                return False
        return False

    async def _run_interactive_conversation(self, user_id: int, group_id: int, clients_with_meta: List[Dict], num_messages: int, owner_id: int, use_predefined_messages: bool = False):
        if len(clients_with_meta) < 2:
            LOGGER.warning(f"Not enough clients to simulate interactive conversation in group {group_id}.")
            return

        active_clients_meta = list(clients_with_meta)
        ai_failed_for_this_group = False # [NEW] Flag to track AI failure per group
        
        # [FIX] Use random.choices to allow reusing personas if there are more participants than personas
        personas = random.choices(Config.PERSONAS, k=len(active_clients_meta))
        for i, meta in enumerate(active_clients_meta):
            meta['persona'] = personas[i]
            LOGGER.info(f"Assigned persona '{personas[i]}' to account '{meta['account_name']}' for conversation in group {group_id}.")

        try:
            # 1. Kick-off message
            if self._get_daily_count_for_group(group_id) >= self.daily_message_limit:
                LOGGER.info(f"Daily message limit for group {group_id} reached. Skipping conversation.")
                return

            starter_info = random.choice(active_clients_meta)
            starter_client = starter_info['client']
            starter_name = starter_info['account_name']
            starter_persona = starter_info['persona']

            if use_predefined_messages:
                initial_messages = [random.choice(Config.PREDEFINED_FALLBACK_MESSAGES)]
            else:
                initial_messages = await self._generate_persian_messages(user_id, persona=starter_persona)
                if not initial_messages or initial_messages[0] in Config.PREDEFINED_FALLBACK_MESSAGES:
                    ai_failed_for_this_group = True
                    LOGGER.warning(f"Initial AI generation failed for group {group_id}. Switching to predefined messages.")
                    if not initial_messages: # Ensure we have a message if AI returned empty
                        initial_messages = [random.choice(Config.PREDEFINED_FALLBACK_MESSAGES)]

            initial_message_text = self._prepare_spoiler_text(initial_messages[0])
            
            last_message = await starter_client.send_message(PeerChannel(group_id), initial_message_text)
            self._increment_daily_count_for_group(group_id)
            LOGGER.info(f"Account '{starter_name}' (Persona: {starter_persona}) started conversation in group {group_id}.")
            
            messages_sent_this_session = 1
            await asyncio.sleep(random.uniform(Config.MESSAGE_SEND_DELAY_MIN, Config.MESSAGE_SEND_DELAY_MAX))

            # 2. Main reply loop
            while self._get_daily_count_for_group(group_id) < self.daily_message_limit and messages_sent_this_session < num_messages:
                last_sender_id = last_message.sender_id

                possible_repliers = [m for m in active_clients_meta if m.get('account_id') != last_sender_id]
                if not possible_repliers:
                    LOGGER.info("No other bot available to reply. Ending conversation.")
                    break

                replier_info = random.choice(possible_repliers)
                replier_client = replier_info['client']
                replier_name = replier_info['account_name']
                replier_user_id = replier_info['user_id']
                replier_persona = replier_info['persona']

                # Decide whether to send a sticker or text
                if random.random() < 0.15: # 15% chance to send a sticker
                    sticker = await self._get_random_sticker(replier_client, replier_user_id)
                    if sticker:
                        last_message = await replier_client.send_file(PeerChannel(group_id), sticker, reply_to=last_message.id)
                        self._increment_daily_count_for_group(group_id)
                        messages_sent_this_session += 1
                        LOGGER.info(f"Account '{replier_name}' sent a sticker in group {group_id}.")
                        await asyncio.sleep(random.uniform(Config.MESSAGE_SEND_DELAY_MIN, Config.MESSAGE_SEND_DELAY_MAX))
                        continue
                
                if use_predefined_messages:
                    reply_messages = [random.choice(Config.PREDEFINED_FALLBACK_MESSAGES)]
                else:
                    prompt_text = last_message.raw_text or "یک پاسخ جالب بده"
                    reply_messages = await self._generate_persian_messages(user_id, persona=replier_persona, previous_message=prompt_text, ai_is_down=ai_failed_for_this_group)
                
                if not reply_messages:
                    LOGGER.warning(f"Could not generate any reply for '{replier_name}'.")
                    continue
                
                # If AI failed this time, set the flag for future messages in this group
                if not use_predefined_messages and reply_messages[0] in Config.PREDEFINED_FALLBACK_MESSAGES:
                    if not ai_failed_for_this_group:
                        LOGGER.warning(f"AI generation failed mid-conversation for group {group_id}. Switching to predefined messages for the rest of this session.")
                        ai_failed_for_this_group = True

                reply_text = self._prepare_spoiler_text(reply_messages[0])
                
                last_message = await replier_client.send_message(PeerChannel(group_id), reply_text, reply_to=last_message.id)
                self._increment_daily_count_for_group(group_id)
                messages_sent_this_session += 1
                LOGGER.info(f"Account '{replier_name}' (Persona: {replier_persona}) replied in group {group_id}.")
                await asyncio.sleep(random.uniform(Config.MESSAGE_SEND_DELAY_MIN, Config.MESSAGE_SEND_DELAY_MAX))

            self.created_groups[str(group_id)]["last_simulated"] = datetime.utcnow().timestamp()
            self._save_created_groups()
            LOGGER.info(f"Updated 'last_simulated' timestamp for group {group_id}.")

        except (ValueError, errors.rpcerrorlist.ChannelInvalidError) as e:
            LOGGER.error(f"Conversation failed in group {group_id} due to an entity/channel error: {e}")
        except asyncio.CancelledError:
            LOGGER.info(f"Interactive conversation for group {group_id} was cancelled.")
            raise
        except Exception as e:
            LOGGER.error(f"Unexpected error during interactive conversation for group {group_id}: {e}", exc_info=True)
        finally:
            # [MODIFIED] All participants except the owner leave the group
            LOGGER.info(f"Conversation in group {group_id} finished. Participants are now leaving.")
            for meta in active_clients_meta:
                if meta['account_id'] != owner_id:
                    try:
                        await meta['client'](LeaveChannelRequest(PeerChannel(group_id)))
                        LOGGER.info(f"Account '{meta['account_name']}' left group {group_id}.")
                    except Exception as e:
                        LOGGER.error(f"Error making account '{meta['account_name']}' leave group {group_id}: {e}")


    async def run_group_creation_worker(self, user_id: int, account_name: str, user_client: TelegramClient) -> None:
        worker_key = f"{user_id}:{account_name}"
        temp_clients = []
        progress_message = None
        try:
            async with self.worker_semaphore:
                LOGGER.info(f"Worker for {worker_key} started.")
                
                # [NEW] Calculate and send initial time estimate
                avg_sleep_per_group = (self.min_sleep_seconds + self.max_sleep_seconds) / 2
                buffer_for_api_calls = 20  # Estimated seconds for API calls, invites, and conversation per group
                total_estimated_seconds = (avg_sleep_per_group + buffer_for_api_calls) * self.groups_to_create
                eta_str = self._format_time_delta(total_estimated_seconds)
                
                initial_message = (
                    f"🚀 شروع عملیات ساخت گروه برای حساب `{account_name}`...\n\n"
                    f"⏳ **زمان تخمینی برای ساخت {self.groups_to_create} گروه:** حدودا **{eta_str}**."
                )
                progress_message = await self.bot.send_message(user_id, initial_message)
                
                start_time = datetime.now()
                me = await user_client.get_me()

                me = await user_client.get_me()
                owner_id = me.id # [NEW] Store the owner's ID
                u_account_name = me.first_name or me.username or f"ID:{owner_id}"

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
                        p_me = await client.get_me()
                        p_account_name = p_me.first_name or p_me.username or f"ID:{p_me.id}"
                        participant_clients_meta.append({'client': client, 'user_id': user_id, 'account_id': p_me.id, 'account_name': p_account_name})

                all_clients_meta = [{'client': user_client, 'user_id': user_id, 'account_id': owner_id, 'account_name': u_account_name}] + participant_clients_meta

                for i in range(self.groups_to_create):
                    try:
                        current_semester = self._get_group_count(worker_key) + 1
                        group_title = f"collage Semester {current_semester}"
                        create_result = await self._send_request_with_reconnect(
                            user_client, CreateChannelRequest(title=group_title, about="Official group.", megagroup=True), account_name
                        )
                        new_supergroup = create_result.chats[0]
                        LOGGER.info(f"Successfully created supergroup '{new_supergroup.title}' (ID: {new_supergroup.id}).")
                        
                        # [MODIFIED] Store owner ID and worker key
                        self.created_groups[str(new_supergroup.id)] = {
                            "owner_worker_key": worker_key, 
                            "owner_id": owner_id,
                            "last_simulated": 0
                        }
                        self._save_created_groups()

                        invite_link = None
                        try:
                            link_result = await user_client(ExportChatInviteRequest(new_supergroup.id))
                            invite_link = link_result.link
                            LOGGER.info(f"Successfully exported invite link for new group {new_supergroup.id}: {invite_link}")
                        except Exception as e:
                            LOGGER.error(f"Could not export invite link for new group {new_supergroup.id}: {e}")
                            continue

                        if invite_link:
                            match = re.search(r'(?:t\.me/joinchat/|\+)([a-zA-Z0-9_-]+)', invite_link)
                            if match:
                                invite_hash = match.group(1)
                                for p_meta in participant_clients_meta:
                                    p_client = p_meta['client']
                                    p_name = p_meta['account_name']
                                    try:
                                        await p_client(ImportChatInviteRequest(invite_hash))
                                        LOGGER.info(f"Account '{p_name}' successfully joined group {new_supergroup.id} via link.")
                                        await asyncio.sleep(random.uniform(45, 120)) # [MODIFIED] Longer delay to avoid flood waits
                                    except Exception as e:
                                        LOGGER.warning(f"Account '{p_name}' failed to join group {new_supergroup.id} via link: {e}")
                            else:
                                LOGGER.error(f"Could not extract hash from invite link: {invite_link}")
                                continue

                        successful_clients_meta = []
                        ensure_tasks = [self._ensure_entity_cached(meta['client'], new_supergroup.id, meta['account_name']) for meta in all_clients_meta]
                        results = await asyncio.gather(*ensure_tasks)

                        for idx, meta in enumerate(all_clients_meta):
                            if results[idx]:
                                successful_clients_meta.append(meta)
                            else:
                                LOGGER.warning(f"Account '{meta['account_name']}' failed to cache group entity and will not participate.")
                                if meta['client'] in temp_clients and meta['client'].is_connected():
                                    await meta['client'].disconnect()

                        if len(successful_clients_meta) < 2:
                             LOGGER.warning(f"Not enough clients ({len(successful_clients_meta)}) could cache the group. Aborting conversation for group {new_supergroup.id}.")
                        else:
                            await self._run_interactive_conversation(user_id, new_supergroup.id, successful_clients_meta, num_messages=self.daily_message_limit, owner_id=owner_id)

                        self._set_group_count(worker_key, current_semester)
                        
                        # Calculate and update progress
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
                                pass # Ignore if we can't edit the message
                        break
                else: # This block runs if the for loop completes without a break
                    if progress_message: await progress_message.edit(f"✅ [{account_name}] Finished creating {self.groups_to_create} groups.")

        except asyncio.CancelledError:
            LOGGER.info(f"Task for {worker_key} was cancelled.")
            if progress_message: await progress_message.edit(f"⏹️ Operation for `{account_name}` stopped.")
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

    async def _run_conversation_task(self, user_id: int, group_id: int, num_messages: Optional[int] = None):
        clients_with_meta = []
        clients_to_disconnect = []
        try:
            group_data = self.created_groups.get(str(group_id))
            if not group_data or "owner_id" not in group_data:
                LOGGER.error(f"[Conversation Task] Cannot run for group {group_id}, owner_id is missing.")
                return
            owner_id = group_data["owner_id"]

            participant_names = self.conversation_accounts.get(str(user_id), [])
            if len(participant_names) < 2:
                LOGGER.warning(f"[Conversation Task] Not enough accounts for user {user_id} to simulate.")
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
                    clients_with_meta.append({'client': client, 'user_id': user_id, 'account_id': me.id, 'account_name': p_account_name})

            ensure_tasks = [self._ensure_entity_cached(meta['client'], group_id, meta['account_name']) for meta in clients_with_meta]
            results = await asyncio.gather(*ensure_tasks)
            
            successful_clients_meta = [meta for i, meta in enumerate(clients_with_meta) if results[i]]

            if len(successful_clients_meta) >= 2:
                await self._run_interactive_conversation(user_id, group_id, successful_clients_meta, num_messages=num_messages, owner_id=owner_id)
            else:
                LOGGER.warning(f"[Conversation Task] Not enough clients could connect and cache the entity for group {group_id}.")

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

    async def _set_keywords_handler(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        self.user_sessions[user_id]['state'] = 'awaiting_keywords'
        await event.reply(Config.MSG_PROMPT_KEYWORDS, buttons=[[Button.text(Config.BTN_BACK)]])

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
            [Button.text(Config.BTN_MANUAL_HEALTH_CHECK)], # [NEW]
            [Button.text(Config.BTN_MESSAGE_ALL_GROUPS)], # ADD THIS LINE
            [Button.text("Set AI Model Hierarchy")],
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
        set_user_limit_match = re.match(r"/set_user_limit (\d+) (\d+)", text) # [NEW]
        broadcast_match = re.match(r"/broadcast (.+)", text, re.DOTALL) # [NEW]
        send_random_links_match = re.match(r"/send_random_links (\d+)", text) # [NEW]

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
        elif text == "/force_refine":
            await self._force_refine_handler(event)
        elif text == "/test_self_healing":
            await self._test_self_healing_handler(event)
        elif text == "/test_ai_generation":
            await self._test_ai_generation_handler(event)
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
        
        # Handle list input for AI_MODEL_HIERARCHY
        if key == "AI_MODEL_HIERARCHY":
            value = [model.strip() for model in value.split(',')]
        else:
            # Try to convert to number if possible for other keys
            try:
                if '.' in value:
                    value = float(value)
                else:
                    value = int(value)
            except ValueError:
                pass # Keep as string
        
        self.config[key] = value
        self._save_json_file(self.config, self.config_file)
        self.update_config_from_file() # Reload config into memory
        
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
                pass # Expected
            await event.reply(f"✅ Worker `{worker_key}` has been terminated.")
        else:
            await event.reply(f"❌ No active worker found with key `{worker_key}`.")

    async def _restart_worker_handler(self, event: events.NewMessage.Event, worker_key: str):
        if worker_key in self.active_workers:
            await event.reply(f"🔄 Restarting worker `{worker_key}`...")
            await self._terminate_worker_handler(event, worker_key)
            await asyncio.sleep(2) # Give it a moment to fully stop
            
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
        """[MODIFIED] Sends a specified number of random group links to the admin."""
        if event.sender_id != ADMIN_USER_ID:
            return

        if not self.known_users:
            await event.reply("❌ No users available to source links from.")
            return

        # 1. Select a random user to source the links from
        source_user_id = random.choice(self.known_users)

        # 2. Get all accounts for that user
        user_accounts = self.session_manager.get_user_accounts(source_user_id)
        if not user_accounts:
            await event.reply(f"❌ Randomly selected user `{source_user_id}` has no accounts. Please try the command again.")
            return

        # 3. Select a random account from that user
        source_account_name = random.choice(user_accounts)
        owner_key = f"{source_user_id}:{source_account_name}"

        # 4. Find all groups created by that account
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

        # 5. Randomly select groups
        selected_group_ids = random.sample(owned_group_ids, num_links)

        await event.reply(f"⏳ Preparing to send you {num_links} random links from account `{owner_key}`...")

        # 6. Generate invite links
        links = []
        client = None
        try:
            session_str = self.session_manager.load_session_string(source_user_id, source_account_name)
            proxy = self.account_proxies.get(owner_key)
            client = await self._create_worker_client(session_str, proxy)

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
                await asyncio.sleep(1) # Rate limiting

        except Exception as e:
            await self._send_error_explanation(ADMIN_USER_ID, e)
            await event.reply(f"❌ An error occurred while generating links for `{owner_key}`.")
            return
        finally:
            if client and client.is_connected():
                await client.disconnect()

        # 7. Send the links to the ADMIN
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
        """[NEW] Exports all invite links for all groups created by all users."""
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

                proxy = self.account_proxies.get(owner_key)
                client = await self._create_worker_client(session_str, proxy)
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
                    await asyncio.sleep(1) # Small delay to avoid flood waits

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
                device_params = random.choice(Config.USER_AGENTS) # [MODIFIED]
                LOGGER.debug(f"Testing proxy: {proxy['addr']} with device: {device_params}")
                client = TelegramClient(sessions.StringSession(), API_ID, API_HASH, proxy=proxy, timeout=self.proxy_timeout, **device_params)
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
            device_params = random.choice(Config.USER_AGENTS) # [MODIFIED]
            LOGGER.debug(f"Testing direct connection with device: {device_params}")
            client = TelegramClient(sessions.StringSession(), API_ID, API_HASH, timeout=self.proxy_timeout, **device_params)
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
        self.user_worker_limits.clear() # [NEW]
        self._save_user_keywords()
        self._save_pending_users()
        self._save_created_groups()
        self._save_conversation_accounts()
        self._save_user_worker_limits() # [NEW]
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
        
        try:
            text = event.message.text
            if user_id in self.banned_users:
                await event.reply("❌ You are banned from using this bot.")
                return

            if text.startswith('/'):
                if user_id == ADMIN_USER_ID:
                    await self._admin_command_handler(event)
                # Allow non-admins to use /start
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

            # --- State Handling ---
            state_handlers = {
                'awaiting_keywords': self._handle_keywords_input,
                'awaiting_sticker_packs': self._handle_sticker_packs_input,
                'awaiting_conv_accounts': self._handle_conv_accounts_input,
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
                if state in ['awaiting_phone', 'awaiting_code', 'awaiting_password', 'awaiting_account_name']:
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

            # --- Authenticated Text/Button Handling ---
            button_handlers = {
                Config.BTN_MANAGE_ACCOUNTS: self._manage_accounts_handler,
                Config.BTN_HELP: self._help_handler,
                Config.BTN_BACK: self._start_handler,
                Config.BTN_SETTINGS: self._settings_handler,
                Config.BTN_ADD_ACCOUNT: self._initiate_login_flow,
                Config.BTN_ADD_ACCOUNT_SELENIUM: self._initiate_selenium_login_flow,
                Config.BTN_SERVER_STATUS: self._server_status_handler,
                Config.BTN_SET_KEYWORDS: self._set_keywords_handler,
                Config.BTN_SET_STICKERS: self._set_stickers_handler,
                Config.BTN_SET_CONVERSATION_ACCOUNTS: self._set_conv_accs_handler,
                Config.BTN_JOIN_VIA_LINK: self._join_via_link_handler,
                Config.BTN_EXPORT_LINKS: self._export_links_handler,
                Config.BTN_FORCE_CONVERSATION: self._force_conversation_handler,
                Config.BTN_STOP_FORCE_CONVERSATION: self._stop_force_conversation_handler,
                Config.BTN_MANUAL_HEALTH_CHECK: self._manual_health_check_handler, # [NEW]
                Config.BTN_MESSAGE_ALL_GROUPS: self._message_all_groups_handler, # ADD THIS LINE

            }
            
            # Admin settings buttons
            if user_id == ADMIN_USER_ID:
                admin_settings_map = {
                    "Set AI Model Hierarchy": "AI_MODEL_HIERARCHY",
                    "Set Worker Limit": "MAX_CONCURRENT_WORKERS",
                    "Set Group Count": "GROUPS_TO_CREATE", 
                    "Set Sleep Times": "MIN_SLEEP_SECONDS,MAX_SLEEP_SECONDS",
                    "Set Daily Msg Limit": "DAILY_MESSAGE_LIMIT_PER_GROUP", 
                    "Set Proxy Timeout": "PROXY_TIMEOUT",
                    "Set Master Password": "MASTER_PASSWORD_HASH",
                    "View Config": "VIEW_CONFIG" # Special case
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
            # [FIX] This is not a real error. Re-raise it so Telethon can handle it.
            raise
        except Exception as e:
            # Global error handler for the message router
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
            assigned_proxy = self.account_proxies.get(worker_key)
            user_client = await self._create_worker_client(session_str, assigned_proxy)
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
                return user_client # Return client so the caller knows not to disconnect it
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

        # [NEW] Check user-specific worker limit
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

    async def _handle_sticker_packs_input(self, event: events.NewMessage.Event) -> None:
        user_id = str(event.sender_id)
        packs_text = event.message.text.strip()
        if packs_text:
            packs = [pack.strip() for pack in packs_text.split(',')]
            self.user_sticker_packs[user_id] = packs
            self._save_user_sticker_packs()
            await event.reply(Config.MSG_STICKERS_SET, buttons=self._build_main_menu())
        else:
            # Allow clearing the list
            self.user_sticker_packs[user_id] = []
            self._save_user_sticker_packs()
            await event.reply("✅ لیست استیکرهای شما پاک شد.", buttons=self._build_main_menu())
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
            await self._send_error_explanation(user_id, e)
        finally:
            if client and client.is_connected():
                await client.disconnect()
            self.user_sessions[user_id]['state'] = 'authenticated'

    async def _process_export_link_request(self, event: events.NewMessage.Event) -> None:
        """
        Handles the logic for exporting group invite links for a selected account.
        This method was renamed from _handle_export_account_selection for clarity.
        """
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

            # Ensure dialogs are updated to cache group entities
            await client.get_dialogs(limit=100)  # Fetch dialogs to populate entity cache

            for group_id in owned_group_ids:
                try:
                    # Resolve the group entity
                    group_entity = await client.get_entity(PeerChannel(group_id))
                    # Export the invite link using the resolved entity
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
                os.remove(file_path)  # Clean up the file after sending
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
                # Clean up the task from the dictionary once it's done or cancelled
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
        selected_proxy = self._get_available_proxy()
        if selected_proxy:
            LOGGER.info(f"Using proxy {selected_proxy['addr']}:{selected_proxy['port']} for login.")
        else:
            LOGGER.info("No available proxy from file. Attempting direct connection for login.")

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
            await self._send_error_explanation(user_id, e)
            self.user_sessions[user_id]['state'] = 'awaiting_phone'
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
                await self._send_error_explanation(user_id, e)
                self.user_sessions[user_id]['state'] = 'awaiting_phone'
        except Exception as e:
            await self._send_error_explanation(user_id, e)
            self.user_sessions[user_id]['state'] = 'awaiting_phone'

    async def _handle_password_input(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        try:
            await self.user_sessions[user_id]['client'].sign_in(password=event.message.text.strip())
            self.user_sessions[user_id]['state'] = 'awaiting_account_name'
            await event.reply('✅ Login successful! Please enter a nickname for this account (e.g., `Main Account` or `Second Number`).', buttons=[[Button.text(Config.BTN_BACK)]])
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
        """Handles clicks on the admin settings buttons."""
        user_id = event.sender_id

        if config_key == "VIEW_CONFIG":
            await self._view_config_handler(event)
            return
        
        if config_key == "AI_MODEL_HIERARCHY":
            current_hierarchy = ", ".join(self.gemini_model_hierarchy)
            prompt_message = f"Please enter the new AI model hierarchy, separated by commas.\n**Current:**\n`{current_hierarchy}`"
        elif config_key == "MIN_SLEEP_SECONDS,MAX_SLEEP_SECONDS":
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
        """Processes the value entered by the admin for a config setting."""
        user_id = event.sender_id
        session = self.user_sessions.get(user_id, {})
        key = session.get('config_key_to_set')
        value_str = event.message.text.strip()

        if not key:
            await event.reply("An internal error occurred. Please try again.", buttons=self._build_main_menu())
            session['state'] = 'authenticated'
            return

        if key == "AI_MODEL_HIERARCHY":
            value = [model.strip() for model in value_str.split(',')]
            self.config[key] = value
            await event.reply(f"✅ AI Model Hierarchy has been updated.")
        elif key == "MIN_SLEEP_SECONDS,MAX_SLEEP_SECONDS":
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
            # Try to convert to number if possible
            try:
                if '.' in value_str:
                    value = float(value_str)
                else:
                    value = int(value_str)
            except ValueError:
                value = value_str # Keep as string
            
            self.config[key] = value
            await event.reply(f"✅ Config key `{key}` has been set to `{value}`.")

        self._save_json_file(self.config, self.config_file)
        self.update_config_from_file()
        if key == "MAX_CONCURRENT_WORKERS":
            self.worker_semaphore = asyncio.Semaphore(self.max_workers)

        session['state'] = 'authenticated'
        session.pop('config_key_to_set', None)
        await self._settings_handler(event)

    # --- [FIXED] DM Chat Handlers ---
    async def _start_dm_chat_handler(self, event: events.NewMessage.Event):
        """Starts the process of setting up a DM chat simulation."""
        user_id = event.sender_id
        if user_id != ADMIN_USER_ID:
            return
        self.user_sessions[user_id]['state'] = 'awaiting_dm_target_id'
        await event.reply("👤 Please enter the **User ID** or **username** of the target you want to start a DM chat with.", buttons=[[Button.text(Config.BTN_BACK)]])

    async def _handle_dm_target_id(self, event: events.NewMessage.Event):
        """Handles receiving the target user ID for the DM chat."""
        user_id = event.sender_id
        target_id = event.text.strip()
        
        # Simple validation: if it's a number, it's a user ID. Otherwise, it's a username.
        try:
            target_entity = int(target_id)
        except ValueError:
            target_entity = target_id.lstrip('@') # Allow with or without @

        self.user_sessions[user_id]['dm_target'] = target_entity
        self.user_sessions[user_id]['state'] = 'awaiting_dm_account_selection'
        
        # [MODIFIED] Allow admin to use any account from any user
        all_accounts = self.session_manager.get_all_accounts()
        if not all_accounts:
            await event.reply("❌ No accounts from any user are connected to the bot.")
            self.user_sessions[user_id]['state'] = 'authenticated'
            return

        buttons = [[Button.text(full_account_key)] for full_account_key in all_accounts.keys()]
        buttons.append([Button.text(Config.BTN_BACK)])
        await event.reply("🤖 Please select the account that will initiate the DM chat (format is `UserID:AccountName`).", buttons=buttons)

    async def _handle_dm_account_selection(self, event: events.NewMessage.Event):
        """Handles selecting the account to use for the DM."""
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
        """Handles the initial message and starts the DM task."""
        user_id = event.sender_id
        initial_message = event.text.strip()
        session_data = self.user_sessions.get(user_id, {})
        
        # [MODIFIED] Use the stored dm_user_id and dm_account_name
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
            proxy = self.account_proxies.get(f"{dm_user_id}:{account_name}")
            client = await self._create_worker_client(session_str, proxy)
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
            
            # Clean up DM state
            session_data.pop('dm_target', None)
            session_data.pop('dm_account_name', None)
            session_data.pop('dm_user_id', None)
            session_data['state'] = 'authenticated'

    # These are placeholders for the other DM states that were in the original router but not implemented
    async def _handle_dm_persona(self, event: events.NewMessage.Event):
        await event.reply("This part of the DM feature is not yet implemented.")
        self.user_sessions[event.sender_id]['state'] = 'authenticated'

    async def _handle_dm_sticker_packs(self, event: events.NewMessage.Event):
        await event.reply("This part of the DM feature is not yet implemented.")
        self.user_sessions[event.sender_id]['state'] = 'authenticated'

    async def _stop_dm_chat_handler(self, event: events.NewMessage.Event):
        await event.reply("DM chat stopping functionality is not yet implemented.")

    # --- [NEW] AI-assisted DM message handlers ---
    async def _start_dm_message_handler(self, event: events.NewMessage.Event):
        """Starts the AI-assisted DM message workflow."""
        user_id = event.sender_id
        if user_id != ADMIN_USER_ID:
            return
        
        self.user_sessions[user_id]['state'] = 'awaiting_dm_message_account_selection'
        
        all_accounts = self.session_manager.get_all_accounts()
        if not all_accounts:
            await event.reply("❌ No accounts are connected to the bot.")
            self.user_sessions[user_id]['state'] = 'authenticated'
            return

        buttons = [[Button.text(full_account_key)] for full_account_key in all_accounts.keys()]
        buttons.append([Button.text(Config.BTN_BACK)])
        await event.reply("🤖 Please select the account to send the message from (format is `UserID:AccountName`).", buttons=buttons)

    async def _handle_dm_message_account_selection(self, event: events.NewMessage.Event):
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
        self.user_sessions[user_id]['state'] = 'awaiting_dm_message_target_user'
        await event.reply("👤 Please enter the target username.", buttons=[[Button.text(Config.BTN_BACK)]])

    async def _handle_dm_message_target_user(self, event: events.NewMessage.Event):
        user_id = event.sender_id
        target_user = event.text.strip()
        self.user_sessions[user_id]['dm_target'] = target_user
        self.user_sessions[user_id]['state'] = 'awaiting_dm_message_prompt'
        await event.reply("✍️ Please provide the prompt for the AI.", buttons=[[Button.text(Config.BTN_BACK)]])

    async def _handle_dm_message_prompt(self, event: events.NewMessage.Event):
        user_id = event.sender_id
        prompt = event.text.strip()
        session_data = self.user_sessions.get(user_id, {})
        
        account_name = session_data.get('dm_account_name')
        dm_user_id = session_data.get('dm_user_id')
        target_user = session_data.get('dm_target')

        if not all([account_name, dm_user_id, target_user, prompt]):
            await event.reply("❌ An internal error occurred. Please start over.")
            self.user_sessions[user_id]['state'] = 'authenticated'
            return

        await event.reply(f"⏳ Reading message history with {target_user}...")
        LOGGER.info(f"Starting AI-assisted DM for account '{account_name}' to target '{target_user}' with prompt: '{prompt}'")
        
        client = None
        try:
            session_str = self.session_manager.load_session_string(dm_user_id, account_name)
            proxy = self.account_proxies.get(f"{dm_user_id}:{account_name}")
            client = await self._create_worker_client(session_str, proxy)
            if not client:
                LOGGER.error(f"Failed to create client for account '{account_name}'")
                await event.reply("❌ Failed to connect with the selected account.")
                return
            LOGGER.info(f"Client created successfully for '{account_name}'.")

            LOGGER.info(f"Fetching last 50 messages from '{target_user}'.")
            history = await client.get_messages(target_user, limit=50)
            if not history:
                LOGGER.warning(f"No message history found with '{target_user}'.")
                history_text = "(No previous messages found)"
            else:
                LOGGER.info(f"Found {len(history)} messages in history.")
                history_text = "\n".join([f"{(msg.sender.username if msg.sender else 'Unknown')}: {msg.text}" for msg in reversed(history) if msg.text])
                LOGGER.debug(f"Formatted history:\n{history_text}")
            
            ai_prompt = (
                f"This is a conversation history with {target_user}:\n\n{history_text}\n\n"
                f"Based on this conversation, what is it about? After summarizing, follow this instruction: {prompt}"
            )
            
            # Using OpenRouter for this task
            model_name = self.openrouter_model_hierarchy[0]
            headers = {"Authorization": f"Bearer {self.openrouter_api_key}", "Content-Type": "application/json"}
            data = {"model": model_name, "messages": [{"role": "user", "content": ai_prompt}]}
            api_url = "https://openrouter.ai/api/v1/chat/completions"

            LOGGER.info(f"Sending prompt to OpenRouter model '{model_name}'.")
            async with httpx.AsyncClient(timeout=120.0) as http_client:
                response = await http_client.post(api_url, json=data, headers=headers)
                response.raise_for_status()
                res_json = response.json()
                ai_message = res_json.get("choices", [{}])[0].get("message", {}).get("content", "")
            LOGGER.info(f"Received AI-generated message: '{ai_message}'")

            LOGGER.info(f"Sending AI message from '{account_name}' to '{target_user}'.")
            await client.send_message(target_user, ai_message)
            await event.reply("✅ Message sent. Waiting for reply...")
            LOGGER.info("Message sent. Now waiting for a reply.")
            
            # Wait for a reply
            @client.on(events.NewMessage(from_users=target_user))
            async def reply_handler(reply_event):
                if not reply_event.is_private:
                    return
                
                LOGGER.info(f"Received a reply from '{target_user}': '{reply_event.text}'")
                reply_text = reply_event.text
                
                # Feed reply back to AI
                follow_up_prompt = f"The user replied: {reply_text}. What should be the response?"
                data["messages"].append({"role": "assistant", "content": ai_message})
                data["messages"].append({"role": "user", "content": follow_up_prompt})

                async with httpx.AsyncClient(timeout=120.0) as http_client:
                    response = await http_client.post(api_url, json=data, headers=headers)
                    response.raise_for_status()
                    res_json = response.json()
                    follow_up_message = res_json.get("choices", [{}])[0].get("message", {}).get("content", "")
                
                LOGGER.info(f"Generated follow-up message: '{follow_up_message}'")
                await client.send_message(target_user, follow_up_message)
                LOGGER.info("Follow-up message sent.")
                client.remove_event_handler(reply_handler)
                LOGGER.info(f"Removed reply handler for target '{target_user}'.")

        except Exception as e:
            await self._send_error_explanation(user_id, e)
        finally:
            if client and client.is_connected():
                await client.disconnect()
            session_data['state'] = 'authenticated'

    async def _approval_handler(self, event: events.CallbackQuery.Event):
        user_id = event.sender_id
        data = event.data.decode('utf-8')

        if user_id != ADMIN_USER_ID:
            await event.answer("You are not authorized to perform this action.")
            return

        # --- AI Patching Logic ---
        if data == "patch_feature":
            if hasattr(self, 'suggested_code') and self.suggested_code:
                try:
                    # Backup the current script before patching, with a timestamp
                    backup_path = f"{__file__}.{datetime.now().strftime('%Y%m%d%H%M%S')}.bak"
                    shutil.copyfile(__file__, backup_path)
                    LOGGER.info(f"Created backup of the script at {backup_path}")
                    
                    with open(__file__, "w", encoding='utf-8') as f:
                        f.write(self.suggested_code)
                    
                    await event.edit("✅ Code patched successfully. Restarting bot...")
                    self.suggested_code = None # Clear after use
                    await self.bot.disconnect()
                    os.execv(sys.executable, ['python'] + sys.argv)
                except Exception as e:
                    LOGGER.error(f"Failed to patch the code: {e}", exc_info=True)
                    await event.edit(f"❌ Failed to patch the code: {e}")
            else:
                await event.edit("❌ No suggested code found to apply.")
            return
        
        if data == "ignore_feature":
            self.suggested_code = None # Clear the suggestion
            await event.edit("👍 Suggestion ignored.")
            return

        # --- User Approval Logic ---
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

    # --- [NEW & REFACTORED] Group Health Maintenance ---
    async def _group_maintenance_scheduler_task(self):
        """Background task that periodically runs the group health check."""
        while True:
            await asyncio.sleep(self.health_check_interval)
            LOGGER.info("[Scheduler] Running periodic group health check...")
            await self.run_group_health_check(triggered_by="Scheduler")

    async def _manual_health_check_handler(self, event: events.NewMessage.Event):
        """Handles the admin's manual request to run a health check."""
        if event.sender_id != ADMIN_USER_ID:
            return
        
        await event.reply(Config.MSG_HEALTH_CHECK_STARTED)
        # Run the check in the background to not block the bot
        asyncio.create_task(self.run_group_health_check(triggered_by=f"Admin ({event.sender_id})"))

    async def run_group_health_check(self, triggered_by: str):
        """
        The core logic for the group health check. Can be called by the scheduler or manually.
        It checks all groups for member count and message count, and takes action if needed.
        """
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
                    
                    proxy = self.account_proxies.get(owner_key)
                    owner_client = await self._create_worker_client(session_str, proxy)
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

                        # --- Group Discovery & Healing Logic ---
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

                        # --- Standard Health Check Logic ---
                        try:
                            # 1. Member Cleanup Check
                            participants = await owner_client.get_participants(dialog.entity, limit=200)
                            if len(participants) > 1:
                                LOGGER.info(f"[Health Check] Group {group_id} has {len(participants)} members. Cleaning up...")
                                for p in participants:
                                    if p.id != owner_id:
                                        try:
                                            await owner_client.kick_participant(dialog.entity, p)
                                            LOGGER.info(f"Kicked member {p.id} from group {group_id}.")
                                            await asyncio.sleep(1) # Rate limit
                                        except Exception as e:
                                            LOGGER.error(f"Failed to kick {p.id} from {group_id}: {e}")
                                cleaned_count += 1

                            # 2. Message Top-Up Check
                            messages = await owner_client.get_messages(dialog.entity, limit=1)
                            total_messages = messages.total if messages else 0
                            
                            daily_msg_count = self._get_daily_count_for_group(group_id)
                            remaining_daily = self.daily_message_limit - daily_msg_count

                            if total_messages < 20 and remaining_daily > 0:
                                messages_to_send = min(20 - total_messages, remaining_daily)
                                LOGGER.info(f"[Health Check] Group {group_id} has {total_messages} messages. Topping up with {messages_to_send} more.")
                                
                                conv_clients_meta = []
                                temp_clients = []
                                try:
                                    participant_names = self.conversation_accounts.get(str(user_id), [])
                                    if len(participant_names) < 2:
                                        LOGGER.warning(f"Not enough conv accounts for user {user_id} to top up group {group_id}.")
                                        continue

                                    invite_link_res = await owner_client(ExportChatInviteRequest(dialog.entity))
                                    invite_hash = re.search(r'(?:t\.me/joinchat/|\+)([a-zA-Z0-9_-]+)', invite_link_res.link).group(1)

                                    for p_name in participant_names:
                                        if p_name == account_name: continue
                                        p_session = self.session_manager.load_session_string(user_id, p_name)
                                        p_proxy = self.account_proxies.get(f"{user_id}:{p_name}")
                                        p_client = await self._create_worker_client(p_session, p_proxy)
                                        if p_client:
                                            temp_clients.append(p_client)
                                            await p_client(ImportChatInviteRequest(invite_hash))
                                            p_me = await p_client.get_me()
                                            conv_clients_meta.append({'client': p_client, 'user_id': user_id, 'account_id': p_me.id, 'account_name': p_name})
                                    
                                    all_clients_meta = [{'client': owner_client, 'user_id': user_id, 'account_id': owner_id, 'account_name': account_name}] + conv_clients_meta

                                    if len(all_clients_meta) >= 2:
                                        await self._run_interactive_conversation(user_id, group_id, all_clients_meta, num_messages=messages_to_send, owner_id=owner_id, use_predefined_messages=True)
                                        topped_up_count += 1
                                    else:
                                        LOGGER.warning(f"Could not gather enough clients to top up group {group_id}.")

                                finally:
                                    for tc in temp_clients:
                                        if tc.is_connected(): await tc.disconnect()
                                
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
    async def _message_all_groups_handler(self, event: events.NewMessage.Event):
        """Handles the admin's request to message all groups."""
        if event.sender_id != ADMIN_USER_ID:
            return
        
        await event.reply(Config.MSG_MESSAGE_ALL_GROUPS_STARTED)
        # Run the task in the background to not block the bot
        asyncio.create_task(self.run_message_all_groups())

    async def run_message_all_groups(self):
        """The core logic for sending messages to all groups for all accounts."""
        if self.message_all_lock.locked():
            LOGGER.warning("Message all groups task triggered but another is already in progress. Skipping.")
            await self.bot.send_message(ADMIN_USER_ID, "⚠️ عملیات ارسال پیام همگانی از قبل در حال اجرا است.")
            return

        async with self.message_all_lock:
            LOGGER.info("--- Messaging All Groups Task Started ---")
            
            accounts_processed = 0
            total_messages_sent = 0
            
            all_accounts = self.session_manager.get_all_accounts()

            for owner_key, user_id in all_accounts.items():
                client = None
                try:
                    user_id_str, account_name = owner_key.split(":", 1)
                    
                    session_str = self.session_manager.load_session_string(user_id, account_name)
                    if not session_str:
                        LOGGER.warning(f"[Message All] No session for account {owner_key}, skipping.")
                        continue
                    
                    proxy = self.account_proxies.get(owner_key)
                    client = await self._create_worker_client(session_str, proxy)
                    if not client:
                        LOGGER.error(f"[Message All] Failed to connect as account {owner_key}, skipping.")
                        continue

                    LOGGER.info(f"[Message All] Processing groups for account {owner_key}.")
                    async for dialog in client.iter_dialogs():
                        if dialog.is_group:
                            try:
                                for _ in range(10):
                                    message_text = random.choice(Config.PREDEFINED_FALLBACK_MESSAGES)
                                    await client.send_message(dialog.id, message_text)
                                    total_messages_sent += 1
                                    # Use a longer, more random delay between messages to appear human
                                    await asyncio.sleep(random.uniform(10, 25))
                            except (errors.ChatWriteForbiddenError, errors.ChatAdminRequiredError):
                                LOGGER.warning(f"[Message All] Account {owner_key} cannot send messages in group {dialog.id}. Skipping.")
                                break # Move to the next group
                            except Exception as e:
                                LOGGER.error(f"[Message All] Error sending message to group {dialog.id} with account {owner_key}: {e}")
                    
                    accounts_processed += 1

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
    async def _send_error_explanation(self, user_id: int, e: Exception):
        """Logs an error and sends a simplified explanation to the user and a detailed one to the admin."""
        LOGGER.error(f"An error occurred for user {user_id}", exc_info=True)
        sentry_sdk.capture_exception(e)

        traceback_str = traceback.format_exc()
        
        # [NEW] Simplified error mapping for users
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

        # [NEW] Send full traceback to admin
        try:
            admin_error_report = (
                f"**🚨 Error Report for User `{user_id}`**\n\n"
                f"**Simplified Message:**\n{user_message}\n\n"
                f"**Full Traceback:**\n```\n{traceback_str}\n```"
            )
            # Split the message if it's too long for Telegram
            if len(admin_error_report) > 4096:
                for i in range(0, len(admin_error_report), 4096):
                    await self.bot.send_message(ADMIN_USER_ID, admin_error_report[i:i + 4096])
            else:
                await self.bot.send_message(ADMIN_USER_ID, admin_error_report)
        except Exception as admin_send_error:
            LOGGER.error(f"Failed to send full error traceback to admin: {admin_send_error}")

    async def _generate_ai_code_suggestion(self, prompt: str, current_code: str) -> Optional[Dict]:
        """Calls the OpenRouter API to get a code suggestion."""
        if not self.openrouter_api_key:
            LOGGER.error("OPENROUTER_API_KEY not set. Cannot generate AI code suggestion.")
            await self.bot.send_message(ADMIN_USER_ID, "❌ `OPENROUTER_API_KEY` is not set. Cannot generate AI code suggestion.")
            return None

        full_prompt = (
            f"{prompt}\n\n"
            "The response must be a valid JSON object with two string keys: 'suggestion' and 'code'. "
            "'suggestion' should be a brief, one-line explanation of the change. "
            "'code' must contain the complete, modified, and runnable Python source code for the bot.\n\n"
            f"**Current Source Code:**\n```python\n{current_code}\n```"
        )

        headers = {"Authorization": f"Bearer {self.openrouter_api_key}", "Content-Type": "application/json"}
        api_url = "https://openrouter.ai/api/v1/chat/completions"
        
        for model_name in self.openrouter_model_hierarchy:
            LOGGER.info(f"Attempting AI code suggestion with model: {model_name}")
            data = {"model": model_name, "messages": [{"role": "user", "content": full_prompt}]}
            try:
                async with httpx.AsyncClient(timeout=120.0) as client:
                    response = await client.post(api_url, json=data, headers=headers)
                    response.raise_for_status()
                    res_json = response.json()
                    
                    message_content = res_json.get("choices", [{}])[0].get("message", {}).get("content", "")
                    
                    json_str = message_content
                    start_index = json_str.find('{')
                    end_index = json_str.rfind('}')
                    
                    if start_index == -1 or end_index == -1:
                        LOGGER.warning(f"AI response from model {model_name} did not contain a JSON object. Response: {message_content}")
                        continue

                    json_str = json_str[start_index:end_index+1]
                    
                    try:
                        json_str_cleaned = json_str.replace('\n', '\\n')
                        parsed_json = json.loads(json_str_cleaned)

                        if "suggestion" in parsed_json and "code" in parsed_json:
                            return parsed_json
                        else:
                            LOGGER.warning(f"AI JSON response from model {model_name} was missing 'suggestion' or 'code' keys. Response: {parsed_json}")
                            continue
                    except json.JSONDecodeError as e:
                        LOGGER.warning(f"Failed to decode JSON from AI response from model {model_name}: {e}. Raw text: {json_str}")
                        continue

            except Exception as e:
                LOGGER.warning(f"An error occurred during AI code suggestion with model {model_name}: {e}")
        
        LOGGER.error("All AI models in the hierarchy failed for code suggestion.")
        await self.bot.send_message(ADMIN_USER_ID, "❌ All AI models failed to generate a valid code suggestion.")
        return None

    async def _trigger_ai_suggestion(self, test_mode=False):
        """Contains the core logic for generating and proposing an AI code suggestion."""
        try:
            with open(__file__, 'r', encoding='utf-8') as f:
                current_code = f.read()
            
            if test_mode:
                prompt = "This is a test. Please confirm you can read the code by suggesting a harmless change, like adding a comment. The full code must be returned."
            else:
                prompt = (
                    "Analyze the following Python code for a Telegram bot. "
                    "Suggest one new feature or a refinement to an existing one."
                )

            ai_response = await self._generate_ai_code_suggestion(prompt, current_code)

            if ai_response:
                self.suggested_code = ai_response['code']
                suggestion_text = ai_response['suggestion']
                
                message = (
                    f"**💡 AI Feature Suggestion**\n\n"
                    f"{suggestion_text}\n\n"
                    f"Do you want to apply this change? The bot will restart if you approve."
                )
                await self.bot.send_message(
                    ADMIN_USER_ID, 
                    message, 
                    buttons=[
                        [Button.inline("✅ Apply & Restart", data="patch_feature")],
                        [Button.inline("❌ Ignore", data="ignore_feature")]
                    ]
                )
                LOGGER.info("Sent AI feature suggestion to admin for approval.")
            else:
                LOGGER.error("Failed to get a valid AI suggestion.")
                await self.bot.send_message(ADMIN_USER_ID, "❌ Failed to get a valid response from the AI.")

        except Exception as e:
            LOGGER.error(f"Failed to get AI feature suggestion: {e}", exc_info=True)
            await self.bot.send_message(ADMIN_USER_ID, f"❌ An error occurred during AI analysis: {e}")

    async def _force_refine_handler(self, event: events.NewMessage.Event):
        """Manually triggers the AI feature suggestion process."""
        if event.sender_id != ADMIN_USER_ID:
            return
        await event.reply("🤖 Triggering AI code analysis... This may take a moment.")
        LOGGER.info(f"Admin {event.sender_id} manually triggered the AI feature suggestion.")
        await self._trigger_ai_suggestion()

    async def _test_self_healing_handler(self, event: events.NewMessage.Event):
        """Simulates a critical error to test the self-healing (external restart) mechanism."""
        await event.reply(
            "💥 **Simulating critical failure!**\n\n"
            "I will now raise an unhandled exception. If you are running this bot with a process manager "
            "(like `systemd` or a Docker restart policy), it should restart automatically within a few moments. "
            "The error will be reported to Sentry if configured."
        )
        await asyncio.sleep(2)
        # This will crash the bot. The OS/process manager is responsible for restarting it.
        raise RuntimeError("Simulating a critical failure for self-healing test.")

    async def _test_ai_generation_handler(self, event: events.NewMessage.Event):
        """Allows the admin to test the AI message generation directly."""
        await event.reply("🧪 Testing AI message generation...")
        test_prompt = "Generate a short, friendly test message in English to confirm the AI is working."
        messages = await self._generate_persian_messages(event.sender_id, persona="a helpful assistant", previous_message=test_prompt)
        if messages:
            await event.reply(f"✅ **AI Response:**\n\n{messages[0]}")
        else:
            await event.reply("❌ **Failed to generate AI message.**\n\nCheck the logs for errors. This could be due to an invalid API key, network issues, or a problem with the OpenRouter service.")

    async def _daily_feature_suggestion(self):
        """[ENABLED] This background task runs daily to suggest AI-powered code refinements."""
        while True:
            await asyncio.sleep(86400) # Run once a day
            LOGGER.info("[Scheduler] Running daily AI feature suggestion task...")
            await self._trigger_ai_suggestion()

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

            # [NEW] Start the background scheduler for group health maintenance.
            self.bot.loop.create_task(self._group_maintenance_scheduler_task())
            
            # Start the background scheduler for AI feature suggestions.
            self.bot.loop.create_task(self._daily_feature_suggestion())
            
            # Automatically resume workers that were active before a restart.
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
