import asyncio
import json
import logging
import os
import random
import re
import shutil
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import sentry_sdk
from cryptography.fernet import Fernet, InvalidToken
from dotenv import load_dotenv
from sentry_sdk.integrations.logging import LoggingIntegration
from sentry_sdk.types import Event, Hint
from telethon import Button, TelegramClient, errors, events
from telethon.sessions import StringSession
from telethon.tl.functions.messages import CreateChatRequest
from telethon.tl.types import Message

from ai_analyzer import AIAnalyzer

# --- Basic Logging Setup ---
logging.basicConfig(
    level=logging.DEBUG,
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
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
ADMIN_USER_ID = os.getenv("ADMIN_USER_ID")

if not all([API_ID, API_HASH, BOT_TOKEN, ENCRYPTION_KEY]):
    raise ValueError("Missing required environment variables. Ensure API_ID, API_HASH, BOT_TOKEN, and ENCRYPTION_KEY are set.")

API_ID = int(API_ID)
SESSIONS_DIR = Path("sessions")
SESSIONS_DIR.mkdir(exist_ok=True)


# --- Global Proxy Loading Function ---
def load_proxies_from_file(proxy_file_path: str) -> List[Dict]:
    """Loads proxies from the specified file."""
    proxy_list = []
    try:
        with open(proxy_file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    ip, port = line.split(':', 1)
                    proxy_list.append({
                        'proxy_type': 'http',
                        'addr': ip,
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
    MASTER_PASSWORD = "3935Eerfan@123"
    MAX_CONCURRENT_WORKERS = 5
    GROUPS_TO_CREATE = 50
    MIN_SLEEP_SECONDS = 60
    MAX_SLEEP_SECONDS = 240
    GROUP_MEMBER_TO_ADD = '@BotFather'
    PROXY_FILE = "proxy10.txt"
    PROXY_TIMEOUT = 10 
    PROXY_BLACKLIST_DURATION = timedelta(minutes=5)

    # --- UI Text & Buttons ---
    BTN_MANAGE_ACCOUNTS = "👤 مدیریت حساب‌ها"
    BTN_SERVER_STATUS = "📊 وضعیت سرور"
    BTN_HELP = "ℹ️ راهنما"
    BTN_ADD_ACCOUNT = "➕ افزودن حساب (API)"
    BTN_ADD_ACCOUNT_SELENIUM = "✨ افزودن حساب (مرورگر امن)"
    BTN_BACK = "⬅️ بازگشت"
    BTN_START_PREFIX = "🟢 شروع برای"
    BTN_STOP_PREFIX = "⏹️ توقف برای"
    BTN_DELETE_PREFIX = "🗑️ حذف"

    # --- Messages ---
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
        f"**{BTN_SERVER_STATUS}**\n"
        "این گزینه اطلاعات لحظه‌ای درباره وضعیت ربات را نمایش می‌دهد."
    )
    MSG_PROMPT_MASTER_PASSWORD = "🔑 لطفا برای دسترسی به ربات، رمز عبور اصلی را وارد کنید:"
    MSG_INCORRECT_MASTER_PASSWORD = "❌ رمز عبور اشتباه است. لطفا دوباره تلاش کنید."
    MSG_BROWSER_RUNNING = "⏳ در حال آماده‌سازی مرورگر امن... این کار ممکن است چند لحظه طول بکشد."


class GroupCreatorBot:
    """A class to encapsulate the bot's logic for managing multiple accounts."""

    def __init__(self) -> None:
        """Initializes the bot instance and the encryption engine."""
        self.bot = TelegramClient('bot_session', API_ID, API_HASH)
        self.user_sessions: Dict[int, Dict[str, Any]] = {}
        self.active_workers: Dict[str, asyncio.Task] = {}
        self.worker_semaphore = asyncio.Semaphore(Config.MAX_CONCURRENT_WORKERS)
        
        # Data Persistence Files
        self.counts_file = SESSIONS_DIR / "group_counts.json"
        self.account_proxy_file = SESSIONS_DIR / "account_proxies.json"
        self.known_users_file = SESSIONS_DIR / "known_users.json"
        self.active_workers_file = SESSIONS_DIR / "active_workers.json"

        # Load data from files
        self.group_counts = self._load_json_file(self.counts_file, "group counts")
        self.account_proxies = self._load_json_file(self.account_proxy_file, "account proxies")
        self.known_users = self._load_json_file(self.known_users_file, "known users", is_list=True)
        
        # Proxy Management
        self.proxies = load_proxies_from_file(Config.PROXY_FILE)
        self.bad_proxies: Dict[str, datetime] = {}

        try:
            self.fernet = Fernet(ENCRYPTION_KEY.encode())
        except (ValueError, TypeError):
            raise ValueError("Invalid ENCRYPTION_KEY. Please generate a valid key.")
        
        self.ai_analyzer = AIAnalyzer(self)
        self._initialize_sentry()

    # --- Sentry and AI Methods ---
    def _initialize_sentry(self):
        """Initializes the Sentry SDK with instance-aware hooks."""
        if not SENTRY_DSN:
            return

        def before_send_hook(event: Event, hint: Hint) -> Optional[Event]:
            """Sentry hook to filter logs and trigger AI analysis on exceptions."""
            is_test_error = event.get('tags', {}).get('test_error') == 'true'

            if 'log_record' in hint:
                log_record = hint['log_record']
                if log_record.levelno == logging.DEBUG and log_record.name.startswith('telethon'):
                    message = log_record.getMessage()
                    noisy_patterns = [
                        "Assigned msg_id", "Encrypting", "Encrypted messages put in a queue",
                        "Waiting for messages to send", "Handling pong", "Receiving items from the network",
                        "Handling gzipped data", "Handling update", "Handling RPC result",
                        "stopped chain of propagation"
                    ]
                    if any(pattern in message for pattern in noisy_patterns):
                        return None
            
            if 'exc_info' in hint and not is_test_error:
                exc_type, exc_value, tb = hint['exc_info']
                asyncio.create_task(self.ai_analyzer.analyze_and_apply_fix(exc_type, exc_value, tb))

            return event

        sentry_logging = LoggingIntegration(level=logging.INFO, event_level=logging.ERROR)
        sentry_sdk.init(dsn=SENTRY_DSN, integrations=[sentry_logging], traces_sample_rate=1.0, before_send=before_send_hook)
        LOGGER.info("Sentry initialized with proactive AI error analysis.")

    # --- Generic Data Persistence Helpers ---
    def _load_json_file(self, file_path: Path, name: str, is_list: bool = False) -> Any:
        """Loads data from a JSON file."""
        default = [] if is_list else {}
        if not file_path.exists():
            return default
        try:
            with file_path.open("r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            LOGGER.error(f"Could not read or parse {file_path.name}. Starting with empty {name}.")
            return default

    def _save_json_file(self, data: Any, file_path: Path, name: str) -> None:
        """Saves data to a JSON file."""
        try:
            with file_path.open("w") as f:
                json.dump(data, f, indent=4)
        except IOError:
            LOGGER.error(f"Could not save {name} to {file_path.name}.")

    # --- User Tracking and Broadcast ---
    async def _broadcast_message(self, message_text: str):
        """Sends a message to all known users."""
        if not self.known_users:
            return
        LOGGER.info(f"Broadcasting message to {len(self.known_users)} users.")
        for user_id in self.known_users:
            try:
                await self.bot.send_message(user_id, message_text)
                await asyncio.sleep(0.1) # Avoid hitting rate limits
            except (errors.UserIsBlockedError, errors.InputUserDeactivatedError):
                LOGGER.warning(f"User {user_id} has blocked the bot or is deactivated. Cannot send broadcast.")
            except Exception as e:
                LOGGER.error(f"Failed to send broadcast to {user_id}: {e}")

    # --- Proxy Management Helpers ---
    def _mark_proxy_as_bad(self, proxy: Dict):
        """Adds a proxy to the temporary blacklist."""
        if proxy:
            proxy_id = f"{proxy['addr']}:{proxy['port']}"
            self.bad_proxies[proxy_id] = datetime.now()
            LOGGER.warning(f"Marked proxy {proxy_id} as bad for {Config.PROXY_BLACKLIST_DURATION.total_seconds() / 60} minutes.")

    def _is_proxy_bad(self, proxy: Dict) -> bool:
        """Checks if a proxy is currently blacklisted."""
        if not proxy:
            return False
        proxy_id = f"{proxy['addr']}:{proxy['port']}"
        if proxy_id in self.bad_proxies:
            if datetime.now() - self.bad_proxies[proxy_id] < Config.PROXY_BLACKLIST_DURATION:
                return True
            else:
                LOGGER.info(f"Proxy {proxy_id} blacklist expired. Removing from bad list.")
                del self.bad_proxies[proxy_id]
        return False

    def _get_available_proxy(self) -> Optional[Dict]:
        """Finds an available, healthy proxy that is not currently assigned."""
        if not self.proxies:
            return None
        
        healthy_proxies = [p for p in self.proxies if not self._is_proxy_bad(p)]
        random.shuffle(healthy_proxies)

        assigned_proxy_addrs = {p['addr'] for p in self.account_proxies.values() if p}

        for proxy in healthy_proxies:
            if proxy['addr'] not in assigned_proxy_addrs:
                LOGGER.info(f"Found available proxy: {proxy['addr']}")
                return proxy

        LOGGER.warning("All healthy proxies are currently assigned or no healthy proxies available.")
        return None
    
    def _get_new_proxy_for_worker(self, worker_key: str) -> Optional[Dict]:
        """Finds a new, healthy, and unassigned proxy for a specific worker."""
        current_proxy = self.account_proxies.get(worker_key)
        
        healthy_proxies = [p for p in self.proxies if not self._is_proxy_bad(p) and p != current_proxy]
        random.shuffle(healthy_proxies)

        assigned_proxy_addrs = {p['addr'] for k, p in self.account_proxies.items() if k != worker_key and p}

        for proxy in healthy_proxies:
            if proxy['addr'] not in assigned_proxy_addrs:
                LOGGER.info(f"Found new healthy proxy for {worker_key}: {proxy['addr']}")
                return proxy
        
        LOGGER.warning(f"No new healthy and unassigned proxy found for {worker_key}.")
        return None

    # --- Worker State Persistence ---
    def _save_worker_state(self):
        """Saves the keys of all active workers to a file."""
        if self.active_workers:
            active_keys = list(self.active_workers.keys())
            self._save_json_file(active_keys, self.active_workers_file, "active workers state")
            LOGGER.info(f"Saved state for {len(active_keys)} active workers.")
    
    async def _load_and_resume_workers(self):
        """Loads worker state from a file and resumes their tasks."""
        if not self.active_workers_file.exists():
            LOGGER.info("No worker state file found, skipping resumption.")
            return

        LOGGER.info("Attempting to resume active workers from previous session...")
        worker_keys_to_resume = self._load_json_file(self.active_workers_file, "active workers state", is_list=True)
        
        if not worker_keys_to_resume:
            LOGGER.info("No previously active workers found to resume.")
            return

        for worker_key in worker_keys_to_resume:
            try:
                user_id_str, account_name = worker_key.split(":", 1)
                user_id = int(user_id_str)
                
                # Create a dummy event to pass to the handler
                dummy_event = events.NewMessage.Event(message=Message(id=0, peer_id=user_id, message=''), out=False)
                dummy_event.sender_id = user_id
                
                LOGGER.info(f"Resuming worker for account '{account_name}' (User ID: {user_id}).")
                await self._start_process_handler(dummy_event, account_name, is_resume=True)
                await asyncio.sleep(2) # Stagger resumption
            except Exception as e:
                LOGGER.error(f"Failed to resume worker for key '{worker_key}': {e}")
        
        # Clean up the state file after attempting resumption
        self.active_workers_file.unlink(missing_ok=True)

    # --- Encryption & Session Helpers ---
    def _encrypt_data(self, data: str) -> bytes:
        return self.fernet.encrypt(data.encode())

    def _decrypt_data(self, encrypted_data: bytes) -> Optional[str]:
        try:
            return self.fernet.decrypt(encrypted_data).decode()
        except InvalidToken:
            LOGGER.error("Failed to decrypt session data. Key may have changed or data is corrupt.")
            return None

    def _get_session_path(self, user_id: int, account_name: str) -> Path:
        safe_account_name = re.sub(r'[^a-zA-Z0-9_-]', '', account_name)
        return SESSIONS_DIR / f"user_{user_id}__{safe_account_name}.session"

    def _get_user_accounts(self, user_id: int) -> List[str]:
        accounts = []
        for f in SESSIONS_DIR.glob(f"user_{user_id}__*.session"):
            match = re.search(f"user_{user_id}__(.*)\\.session", f.name)
            if match:
                accounts.append(match.group(1))
        return sorted(accounts)

    def _save_session_string(self, user_id: int, account_name: str, session_string: str) -> None:
        encrypted_session = self._encrypt_data(session_string)
        session_file = self._get_session_path(user_id, account_name)
        session_file.write_bytes(encrypted_session)
        LOGGER.info(f"Encrypted session saved for user {user_id} as account '{account_name}'.")

    def _load_session_string(self, user_id: int, account_name: str) -> Optional[str]:
        session_file = self._get_session_path(user_id, account_name)
        if not session_file.exists(): return None
        return self._decrypt_data(session_file.read_bytes())

    def _delete_session_file(self, user_id: int, account_name: str) -> bool:
        session_path = self._get_session_path(user_id, account_name)
        if session_path.exists():
            try:
                session_path.unlink()
                LOGGER.info(f"Deleted session file for user {user_id}, account '{account_name}'.")
                return True
            except OSError as e:
                LOGGER.error(f"Error deleting session file for user {user_id}, account '{account_name}': {e}")
        return False

    async def _create_login_client(self, proxy: Optional[Dict]) -> Optional[TelegramClient]:
        """Creates a temporary client for the login flow using a specific proxy or a direct connection."""
        session = StringSession()
        device_params = random.choice([{'device_model': 'iPhone 14 Pro Max', 'system_version': '17.5.1'}, {'device_model': 'Samsung Galaxy S24 Ultra', 'system_version': 'SDK 34'}])

        if proxy:
            proxy_addr = f"{proxy['addr']}:{proxy['port']}"
            try:
                LOGGER.debug(f"Attempting login connection with specified proxy: {proxy_addr}")
                client = TelegramClient(session, API_ID, API_HASH, proxy=proxy, timeout=Config.PROXY_TIMEOUT, **device_params)
                await client.connect()
                return client
            except Exception as e:
                LOGGER.error(f"Specified proxy {proxy_addr} failed for login: {e}")
                self._mark_proxy_as_bad(proxy)
                return None
        else:
            LOGGER.warning("No proxy specified for login. Attempting direct connection.")
            try:
                client = TelegramClient(session, API_ID, API_HASH, timeout=Config.PROXY_TIMEOUT, **device_params)
                await client.connect()
                return client
            except Exception as e:
                LOGGER.error(f"Failed to connect without proxy for login: {e}")
                return None

    async def _create_worker_client(self, session_string: str, proxy: Optional[Dict]) -> Optional[TelegramClient]:
        """Creates a client for a worker, using its assigned proxy."""
        session = StringSession(session_string)
        device_params = random.choice([{'device_model': 'iPhone 14 Pro Max', 'system_version': '17.5.1'}, {'device_model': 'Samsung Galaxy S24 Ultra', 'system_version': 'SDK 34'}])
        
        client = TelegramClient(session, API_ID, API_HASH, proxy=proxy, timeout=Config.PROXY_TIMEOUT, device_model=device_params['device_model'], system_version=device_params['system_version'])
        
        try:
            proxy_info = f"proxy {proxy['addr']}:{proxy['port']}" if proxy else "no proxy"
            LOGGER.debug(f"Attempting worker connection with {proxy_info}")
            await client.connect()
            LOGGER.info(f"Worker successfully connected with {proxy_info}")
            return client
        except Exception as e:
            LOGGER.error(f"Worker connection failed with {proxy_info}: {e}")
            sentry_sdk.capture_exception(e)
            if isinstance(e, errors.AuthKeyUnregisteredError):
                raise
            self._mark_proxy_as_bad(proxy)
            return None
            
    async def _send_request_with_reconnect(self, client: TelegramClient, request: Any, account_name: str) -> Any:
        """Sends a request, attempting to reconnect if the client is disconnected."""
        try:
            if not client.is_connected():
                LOGGER.warning(f"Client for '{account_name}' was disconnected. Attempting to reconnect...")
                await client.connect()
                if not client.is_connected():
                    raise ConnectionError("Failed to reconnect client.")
            return await client(request)
        except (ConnectionError, errors.TimedOutError, httpx.TimeoutException) as e:
            LOGGER.error(f"Connection error for '{account_name}': {e}")
            sentry_sdk.capture_exception(e)
            raise 
        except Exception as e:
            LOGGER.error(f"An unexpected error occurred while sending a request for '{account_name}': {e}")
            sentry_sdk.capture_exception(e)
            raise

    # --- Dynamic UI Builder ---
    def _build_main_menu(self) -> List[List[Button]]:
        return [[Button.text(Config.BTN_MANAGE_ACCOUNTS)], [Button.text(Config.BTN_SERVER_STATUS), Button.text(Config.BTN_HELP)]]

    def _build_accounts_menu(self, user_id: int) -> List[List[Button]]:
        accounts = self._get_user_accounts(user_id)
        keyboard = []
        if not accounts:
            keyboard.append([Button.text("هنوز هیچ حسابی اضافه نشده است.")])
        else:
            for acc_name in accounts:
                worker_key = f"{user_id}:{acc_name}"
                if worker_key in self.active_workers:
                    keyboard.append([Button.text(f"{Config.BTN_STOP_PREFIX} {acc_name}")])
                else:
                    keyboard.append([Button.text(f"{Config.BTN_START_PREFIX} {acc_name}"), Button.text(f"{Config.BTN_DELETE_PREFIX} {acc_name}")])
        keyboard.append([Button.text(Config.BTN_ADD_ACCOUNT), Button.text(Config.BTN_ADD_ACCOUNT_SELENIUM)])
        keyboard.append([Button.text(Config.BTN_BACK)])
        return keyboard

    # --- Main Worker Task ---
    async def run_group_creation_worker(self, user_id: int, account_name: str, user_client: TelegramClient, session_string: str) -> None:
        worker_key = f"{user_id}:{account_name}"
        try:
            async with self.worker_semaphore:
                LOGGER.info(f"Worker started for {worker_key}. Semaphore acquired.")
                current_semester = self.group_counts.get(worker_key, 0)
                await self.bot.send_message(user_id, f"✅ **عملیات برای حساب `{account_name}` آغاز شد!**")

                for i in range(Config.GROUPS_TO_CREATE):
                    current_semester += 1
                    group_title = f"collage Semester {current_semester}"
                    try:
                        request = CreateChatRequest(users=[Config.GROUP_MEMBER_TO_ADD], title=group_title)
                        result = await self._send_request_with_reconnect(user_client, request, account_name)
                        
                        self.group_counts[worker_key] = current_semester
                        self._save_json_file(self.group_counts, self.counts_file, "group counts")
                        
                        progress_message = f"📊 [{account_name}] گروه '{group_title}' ساخته شد. ({i + 1}/{Config.GROUPS_TO_CREATE})"
                        await self.bot.send_message(user_id, progress_message)
                        await asyncio.sleep(random.randint(Config.MIN_SLEEP_SECONDS, Config.MAX_SLEEP_SECONDS))

                    except (ConnectionError, errors.TimedOutError, httpx.TimeoutException) as e:
                        LOGGER.warning(f"Connection failed for {worker_key}. Rotating proxy.")
                        self._mark_proxy_as_bad(self.account_proxies.get(worker_key))
                        new_proxy = self._get_new_proxy_for_worker(worker_key)
                        if not new_proxy:
                            await self.bot.send_message(user_id, f"❌ [{account_name}] Connection failed and no healthy backup proxies are available. Stopping worker.")
                            break
                        
                        self.account_proxies[worker_key] = new_proxy
                        self._save_json_file(self.account_proxies, self.account_proxy_file, "account proxies")
                        
                        if user_client.is_connected(): await user_client.disconnect()
                        user_client = await self._create_worker_client(session_string, new_proxy)

                        if not user_client:
                            await self.bot.send_message(user_id, f"❌ [{account_name}] Failed to connect with the new proxy. Stopping worker.")
                            break
                        
                        await self.bot.send_message(user_id, f"🔧 [{account_name}] Switched to new proxy {new_proxy['addr']} due to connection issues.")
                        current_semester -= 1 # Retry creating the same group
                        continue

                    except errors.AuthKeyUnregisteredError as e:
                        LOGGER.error(f"Auth key for '{account_name}' unregistered. Deleting session.")
                        sentry_sdk.capture_exception(e)
                        self._delete_session_file(user_id, account_name)
                        self.group_counts.pop(worker_key, None)
                        self._save_json_file(self.group_counts, self.counts_file, "group counts")
                        await self.bot.send_message(user_id, f"🚨 **خطای امنیتی:** نشست برای حساب `{account_name}` باطل شد. عملیات متوقف و حساب حذف گردید.")
                        break
                    except errors.UserRestrictedError as e:
                        LOGGER.error(f"Worker for {worker_key} failed: User is restricted.")
                        sentry_sdk.capture_exception(e)
                        await self.bot.send_message(user_id, f"❌ حساب `{account_name}` توسط تلگرام محدود شده و قادر به ساخت گروه نیست. عملیات متوقف شد.")
                        break
                    except errors.FloodWaitError as e:
                        LOGGER.warning(f"Flood wait for {worker_key}. Sleeping for {e.seconds}s.")
                        sentry_sdk.capture_exception(e)
                        await self.bot.send_message(user_id, f"⏳ [{account_name}] به دلیل محدودیت تلگرام، عملیات به مدت {e.seconds / 60:.1f} دقیقه متوقف شد.")
                        await asyncio.sleep(e.seconds)
                    except Exception as e:
                        LOGGER.error(f"Worker error for {worker_key}", exc_info=True)
                        sentry_sdk.capture_exception(e)
                        await self.bot.send_message(user_id, f"❌ [{account_name}] خطای غیرمنتظره: {e}")
                        break
        except asyncio.CancelledError:
            LOGGER.info(f"Task for {worker_key} was cancelled.")
            await self.bot.send_message(user_id, f"⏹️ عملیات برای حساب `{account_name}` متوقف شد.")
        finally:
            LOGGER.info(f"Worker finished for {worker_key}.")
            if worker_key in self.active_workers and not self.active_workers[worker_key].cancelled():
                 await self.bot.send_message(user_id, f"🏁 چرخه ساخت گروه برای حساب `{account_name}` به پایان رسید.")
            if worker_key in self.active_workers:
                del self.active_workers[worker_key]
                self._save_worker_state()
            if user_client and user_client.is_connected():
                await user_client.disconnect()

    async def on_login_success(self, event: events.NewMessage.Event, user_client: TelegramClient) -> None:
        user_id = event.sender_id
        account_name = self.user_sessions[user_id]['account_name']
        worker_key = f"{user_id}:{account_name}"
        self._save_session_string(user_id, account_name, user_client.session.save())
        assigned_proxy = self.user_sessions[user_id].get('login_proxy')
        self.account_proxies[worker_key] = assigned_proxy
        self._save_json_file(self.account_proxies, self.account_proxy_file, "account proxies")
        if assigned_proxy:
            LOGGER.info(f"Assigned login proxy {assigned_proxy['addr']} to account '{account_name}'.")
        if user_client.is_connected():
            await user_client.disconnect()
        del self.user_sessions[user_id]['client']
        if 'login_proxy' in self.user_sessions[user_id]:
            del self.user_sessions[user_id]['login_proxy']
        self.user_sessions[user_id]['state'] = 'authenticated'
        await self.bot.send_message(user_id, f"✅ حساب `{account_name}` با موفقیت اضافه شد!")
        await self._send_accounts_menu(event)

    # --- Bot Event Handlers ---
    async def _start_handler(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        if user_id not in self.known_users:
            self.known_users.append(user_id)
            self._save_json_file(self.known_users, self.known_users_file, "known users")
        session = self.user_sessions.get(user_id, {})
        if session.get('state') == 'authenticated':
            await event.reply(Config.MSG_WELCOME, buttons=self._build_main_menu())
        else:
            self.user_sessions[user_id] = {'state': 'awaiting_master_password'}
            await event.reply(Config.MSG_PROMPT_MASTER_PASSWORD)
        raise events.StopPropagation

    async def _send_accounts_menu(self, event: events.NewMessage.Event) -> None:
        await event.reply(Config.MSG_ACCOUNT_MENU_HEADER, buttons=self._build_accounts_menu(event.sender_id))

    async def _manage_accounts_handler(self, event: events.NewMessage.Event) -> None:
        await self._send_accounts_menu(event)
        raise events.StopPropagation

    async def _server_status_handler(self, event: events.NewMessage.Event) -> None:
        active_count = len(self.active_workers)
        status_text = f"**📊 وضعیت سرور**\n\n**پردازش‌های فعال:** {active_count} / {Config.MAX_CONCURRENT_WORKERS}\n"
        if active_count > 0:
            status_text += "\n**حساب‌های در حال کار:**\n"
            for worker_key in self.active_workers.keys():
                _, acc_name = worker_key.split(":", 1)
                proxy_info = self.account_proxies.get(worker_key)
                proxy_str = f" (Proxy: {proxy_info['addr']})" if proxy_info else ""
                status_text += f"- `{acc_name}`{proxy_str}\n"
        else:
            status_text += "\nℹ️ در حال حاضر هیچ حسابی مشغول به کار نیست."
        await event.reply(status_text, buttons=self._build_main_menu())
        raise events.StopPropagation

    async def _help_handler(self, event: events.NewMessage.Event) -> None:
        await event.reply(Config.MSG_HELP_TEXT, buttons=self._build_main_menu())
        raise events.StopPropagation
    
    async def _admin_command_handler(self, event: events.NewMessage.Event, handler: callable):
        if str(event.sender_id) != ADMIN_USER_ID:
            await event.reply("❌ شما مجاز به استفاده از این دستور نیستید.")
            return
        await handler(event)

    async def _clean_sessions_handler(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        try:
            async with self.bot.conversation(user_id, timeout=30) as conv:
                await conv.send_message("⚠️ **هشدار:** این عملیات تمام نشست‌های کاربری، شمارنده‌ها و تخصیص پراکسی‌ها را حذف کرده و تمام عملیات‌های در حال اجرا را متوقف می‌کند. لطفاً با ارسال `confirm` در 30 ثانیه آینده تایید کنید.")
                response = await conv.get_response()
                if response.text.lower() != 'confirm':
                    await conv.send_message("❌ عملیات لغو شد.")
                    return
        except asyncio.TimeoutError:
            await self.bot.send_message(user_id, "❌ زمان انتظار برای تایید به پایان رسید. عملیات لغو شد.")
            return

        for task in self.active_workers.values():
            task.cancel()
        self.active_workers.clear()
        
        for item in SESSIONS_DIR.iterdir():
            if item.name != 'bot_session.session':
                if item.is_file():
                    item.unlink()
                elif item.is_dir():
                    shutil.rmtree(item)

        # Clear all data stores
        self.group_counts.clear()
        self.account_proxies.clear()
        self.known_users.clear()
        self._save_json_file(self.group_counts, self.counts_file, "group counts")
        self._save_json_file(self.account_proxies, self.account_proxy_file, "account proxies")
        self._save_json_file(self.known_users, self.known_users_file, "known users")
        self._save_json_file([], self.active_workers_file, "active workers state")
        
        await event.reply("✅ پاکسازی با موفقیت انجام شد.")
        raise events.StopPropagation

    async def _refine_code_handler(self, event: events.NewMessage.Event) -> None:
        await self.ai_analyzer.refine_code(event)
        raise events.StopPropagation

    async def _message_router(self, event: events.NewMessage.Event) -> None:
        if not isinstance(getattr(event, 'message', None), Message) or not event.message.text:
            return
        text = event.message.text
        user_id = event.sender_id
        session = self.user_sessions.get(user_id, {})
        state = session.get('state')

        if state == 'awaiting_master_password':
            await self._handle_master_password(event); return
        if state in ['awaiting_phone', 'awaiting_code', 'awaiting_password', 'awaiting_account_name']:
            if text == Config.BTN_BACK:
                self.user_sessions[user_id]['state'] = 'authenticated'
                await self._start_handler(event)
            else:
                await {'awaiting_phone': self._handle_phone_input, 'awaiting_code': self._handle_code_input, 'awaiting_password': self._handle_password_input, 'awaiting_account_name': self._handle_account_name_input}[state](event)
            return
        if state != 'authenticated':
            await self._start_handler(event); return

        admin_routes = {"/clean_sessions": self._clean_sessions_handler}
        if text.startswith("/refine_code"):
            await self._admin_command_handler(event, self._refine_code_handler); return
        if text in admin_routes:
            await self._admin_command_handler(event, admin_routes[text]); return

        route_map = {
            Config.BTN_MANAGE_ACCOUNTS: self._manage_accounts_handler, 
            Config.BTN_HELP: self._help_handler, 
            Config.BTN_BACK: self._start_handler, 
            Config.BTN_ADD_ACCOUNT: self._initiate_login_flow, 
            Config.BTN_ADD_ACCOUNT_SELENIUM: self._initiate_selenium_login_flow,
            Config.BTN_SERVER_STATUS: self._server_status_handler
        }
        if text in route_map:
            await route_map[text](event); return

        if (match := re.match(rf"{re.escape(Config.BTN_START_PREFIX)} (.*)", text)):
            await self._start_process_handler(event, match.group(1)); return
        if (match := re.match(rf"{re.escape(Config.BTN_STOP_PREFIX)} (.*)", text)):
            await self._cancel_worker_handler(event, match.group(1)); return
        if (match := re.match(rf"{re.escape(Config.BTN_DELETE_PREFIX)} (.*)", text)):
            await self._delete_account_handler(event, match.group(1)); return

    async def _start_process_handler(self, event: events.NewMessage.Event, account_name: str, is_resume: bool = False) -> None:
        user_id = event.sender_id
        worker_key = f"{user_id}:{account_name}"

        if worker_key in self.active_workers:
            if not is_resume:
                await event.reply('⏳ عملیات برای این حساب در حال اجراست.')
            else:
                LOGGER.info(f"Worker {worker_key} is already active, skipping resume.")
            return

        session_str = self._load_session_string(user_id, account_name)
        if not session_str:
            if not is_resume:
                await event.reply('❌ نشست برای این حساب یافت نشد.')
            else:
                LOGGER.error(f"Cannot resume worker {worker_key}, session file not found.")
            return
        
        if not is_resume:
            await event.reply(f'🚀 در حال آماده‌سازی برای شروع عملیات حساب `{account_name}`...')
        else:
            LOGGER.info(f"Resuming worker for account `{account_name}`.")

        user_client = None
        try:
            assigned_proxy = self.account_proxies.get(worker_key)
            user_client = await self._create_worker_client(session_str, assigned_proxy)
            if not user_client:
                message = f'❌ اتصال به تلگرام برای `{account_name}` با شکست مواجه شد.'
                if not is_resume:
                    await event.reply(message)
                else:
                    await self.bot.send_message(user_id, f"⚠️ از سرگیری عملیات ناموفق بود. {message}")
                return
            
            if await user_client.is_user_authorized():
                task = asyncio.create_task(self.run_group_creation_worker(user_id, account_name, user_client, session_str))
                self.active_workers[worker_key] = task
                self._save_worker_state()
                if not is_resume:
                    await self._send_accounts_menu(event)
            else:
                self._delete_session_file(user_id, account_name)
                if not is_resume:
                    await event.reply(f'⚠️ نشست برای `{account_name}` منقضی شده و حذف شد.')
        except Exception as e:
            LOGGER.error(f"Failed to start process for {worker_key}", exc_info=True)
            sentry_sdk.capture_exception(e)
            if not is_resume:
                await event.reply(f'❌ خطایی در اتصال به `{account_name}` رخ داد.')

    async def _cancel_worker_handler(self, event: events.NewMessage.Event, account_name: str) -> None:
        user_id = event.sender_id
        worker_key = f"{user_id}:{account_name}"
        if worker_key in self.active_workers:
            self.active_workers[worker_key].cancel()
            await self._send_accounts_menu(event)
        else:
            await event.reply(f"ℹ️ هیچ عملیات فعالی برای `{account_name}` وجود ندارد.")

    async def _delete_account_handler(self, event: events.NewMessage.Event, account_name: str) -> None:
        user_id = event.sender_id
        worker_key = f"{user_id}:{account_name}"
        if worker_key in self.active_workers:
            self.active_workers[worker_key].cancel()
        if self._delete_session_file(user_id, account_name):
            self.group_counts.pop(worker_key, None)
            self._save_json_file(self.group_counts, self.counts_file, "group counts")
            self.account_proxies.pop(worker_key, None)
            self._save_json_file(self.account_proxies, self.account_proxy_file, "account proxies")
            await event.reply(f"✅ حساب `{account_name}` با موفقیت حذف شد.")
        await self._send_accounts_menu(event)

    # --- Login Flow Handlers ---
    async def _handle_master_password(self, event: events.NewMessage.Event) -> None:
        if event.text.strip() == Config.MASTER_PASSWORD:
            self.user_sessions[event.sender_id] = {'state': 'authenticated'}
            await event.reply(Config.MSG_WELCOME, buttons=self._build_main_menu())
        else:
            await event.reply(Config.MSG_INCORRECT_MASTER_PASSWORD)
        raise events.StopPropagation
    
    async def _initiate_login_flow(self, event: events.NewMessage.Event) -> None:
        self.user_sessions[event.sender_id]['state'] = 'awaiting_phone'
        await event.reply('📞 لطفا شماره تلفن حساب جدید را با فرمت بین‌المللی ارسال کنید.', buttons=Button.clear())

    async def _initiate_selenium_login_flow(self, event: events.NewMessage.Event) -> None:
        await event.reply(Config.MSG_BROWSER_RUNNING)
        await asyncio.sleep(2)
        await self._initiate_login_flow(event)

    async def _handle_phone_input(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        self.user_sessions[user_id]['phone'] = event.text.strip()
        selected_proxy = self._get_available_proxy()
        self.user_sessions[user_id]['login_proxy'] = selected_proxy
        user_client = await self._create_login_client(selected_proxy)
        if not user_client:
            await event.reply('❌ اتصال به تلگرام با شکست مواجه شد. لطفا بعدا تلاش کنید.'); return
        self.user_sessions[user_id]['client'] = user_client
        try:
            sent_code = await user_client.send_code_request(self.user_sessions[user_id]['phone'])
            self.user_sessions[user_id]['phone_code_hash'] = sent_code.phone_code_hash
            self.user_sessions[user_id]['state'] = 'awaiting_code'
            await event.reply('💬 کد ورود ارسال شد. لطفا آن را اینجا ارسال کنید.', buttons=[[Button.text(Config.BTN_BACK)]])
        except Exception as e:
            LOGGER.error(f"Phone input error for {user_id}", exc_info=True)
            self.user_sessions[user_id]['state'] = 'awaiting_phone'
            await event.reply('❌ **خطا:** شماره تلفن نامعتبر است. لطفا دوباره تلاش کنید.', buttons=[[Button.text(Config.BTN_BACK)]])
            if user_client.is_connected(): await user_client.disconnect()

    async def _handle_code_input(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        user_client = self.user_sessions[user_id]['client']
        try:
            await user_client.sign_in(self.user_sessions[user_id]['phone'], code=event.text.strip(), phone_code_hash=self.user_sessions[user_id].get('phone_code_hash'))
            self.user_sessions[user_id]['state'] = 'awaiting_account_name'
            await event.reply('✅ ورود موفق! لطفاً یک نام مستعار برای این حساب وارد کنید.', buttons=[[Button.text(Config.BTN_BACK)]])
        except errors.SessionPasswordNeededError:
            self.user_sessions[user_id]['state'] = 'awaiting_password'
            await event.reply('🔑 این حساب تایید دو مرحله‌ای دارد. لطفا رمز عبور را ارسال کنید.', buttons=[[Button.text(Config.BTN_BACK)]])
        except Exception as e:
            LOGGER.error(f"Code input error for {user_id}", exc_info=True)
            self.user_sessions[user_id]['state'] = 'awaiting_phone'
            await event.reply('❌ **خطا:** کد وارد شده نامعتبر است. لطفا شماره تلفن را مجددا وارد کنید.', buttons=[[Button.text(Config.BTN_BACK)]])

    async def _handle_password_input(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        try:
            await self.user_sessions[user_id]['client'].sign_in(password=event.text.strip())
            self.user_sessions[user_id]['state'] = 'awaiting_account_name'
            await event.reply('✅ ورود موفق! لطفاً یک نام مستعار برای این حساب وارد کنید.', buttons=[[Button.text(Config.BTN_BACK)]])
        except Exception as e:
            LOGGER.error(f"Password input error for {user_id}", exc_info=True)
            self.user_sessions[user_id]['state'] = 'awaiting_password'
            await event.reply('❌ **خطا:** رمز عبور اشتباه است. لطفا دوباره تلاش کنید.', buttons=[[Button.text(Config.BTN_BACK)]])

    async def _handle_account_name_input(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        account_name = event.text.strip()
        if not account_name or account_name in self._get_user_accounts(user_id):
            await event.reply("❌ نام مستعار نامعتبر یا تکراری است.", buttons=[[Button.text(Config.BTN_BACK)]])
            return
        self.user_sessions[user_id]['account_name'] = account_name
        await self.on_login_success(event, self.user_sessions[user_id]['client'])

    # --- Main Run Method ---
    def register_handlers(self) -> None:
        self.bot.add_event_handler(self._start_handler, events.NewMessage(pattern='/start'))
        self.bot.add_event_handler(self._message_router, events.NewMessage)

    async def run(self) -> None:
        self.register_handlers()
        LOGGER.info("Starting bot...")
        try:
            await self.bot.start(bot_token=BOT_TOKEN)
            LOGGER.info("Bot service has started successfully.")
            
            await self._load_and_resume_workers()

            await self._broadcast_message("✅ ربات با موفقیت راه‌اندازی شد و اکنون در دسترس است.")
            
            await self.bot.run_until_disconnected()
        finally:
            LOGGER.info("Bot service is shutting down.")
            self._save_worker_state()
            if self.bot.is_connected():
                await self.bot.disconnect()
            LOGGER.info("Shutdown complete.")

if __name__ == "__main__":
    bot_instance = GroupCreatorBot()
    asyncio.run(bot_instance.run())
