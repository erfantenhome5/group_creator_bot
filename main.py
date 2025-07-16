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
from telethon import Button, TelegramClient, errors, events
from telethon.sessions import StringSession
from telethon.tl import functions
from telethon.tl.functions.messages import CreateChatRequest, ExportChatInviteRequest
from telethon.tl.types import Message

from session_manager import SessionManager

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
ADMIN_USER_ID = os.getenv("ADMIN_USER_ID")
MASTER_PASSWORD_HASH = os.getenv("MASTER_PASSWORD_HASH")

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
    MAX_CONCURRENT_WORKERS = 5
    GROUPS_TO_CREATE = 50
    MIN_SLEEP_SECONDS = 60
    MAX_SLEEP_SECONDS = 240
    GROUP_MEMBER_TO_ADD = '@BotFather'
    PROXY_FILE = "proxy10.txt"
    PROXY_TIMEOUT = 5 

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
        f"**{BTN_SERVER_STATUS}**\n"
        "این گزینه اطلاعات لحظه‌ای درباره وضعیت ربات را نمایش می‌دهد."
    )
    MSG_PROMPT_MASTER_PASSWORD = "🔑 لطفاً برای دسترسی به ربات، رمز عبور اصلی را وارد کنید:"
    MSG_INCORRECT_MASTER_PASSWORD = "❌ رمز عبور اشتباه است. لطفاً دوباره تلاش کنید."
    MSG_BROWSER_RUNNING = "⏳ در حال آماده‌سازی مرورگر امن... این کار ممکن است چند لحظه طول بکشد."

class GroupCreatorBot:
    """A class to encapsulate the bot's logic for managing multiple accounts."""

    def __init__(self) -> None:
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
        self.active_workers_file = SESSIONS_DIR / "active_workers.json"
        self.active_workers_state = self._load_active_workers_state()
        try:
            fernet = Fernet(ENCRYPTION_KEY.encode())
            self.session_manager = SessionManager(fernet, SESSIONS_DIR)
        except (ValueError, TypeError):
            raise ValueError("Invalid ENCRYPTION_KEY. Please generate a valid key.")
        
        self._initialize_sentry()

    # --- Sentry Methods ---
    def _initialize_sentry(self):
        """Initializes the Sentry SDK with instance-aware hooks."""
        if not SENTRY_DSN:
            return

        def before_send_hook(event: Event, hint: Hint) -> Optional[Event]:
            """Sentry hook to filter logs and handle exceptions."""
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
                    for pattern in noisy_patterns:
                        if pattern in message:
                            return None
            return event

        sentry_logging = LoggingIntegration(
            level=logging.INFO,
            event_level=logging.ERROR,
            sentry_logs_level=logging.DEBUG
        )
        
        sentry_options = {
            "dsn": SENTRY_DSN,
            "integrations": [sentry_logging],
            "traces_sample_rate": 1.0,
            "_experiments": {
                "enable_logs": True,
            },
            "before_send": before_send_hook,
        }

        proxies_for_sentry = load_proxies_from_file("proxy10.txt")
        if proxies_for_sentry:
            sentry_proxy = random.choice(proxies_for_sentry)
            proxy_url = f"http://{sentry_proxy['addr']}:{sentry_proxy['port']}"
            sentry_options["http_proxy"] = proxy_url
            sentry_options["https_proxy"] = proxy_url
            LOGGER.info(f"Sentry will use proxy: {proxy_url}")
        else:
            LOGGER.info("Sentry will not use a proxy (none found).")

        sentry_sdk.init(**sentry_options)
        LOGGER.info("Sentry initialized for error reporting.")

    # --- Proxy Helpers ---
    def _load_account_proxies(self) -> Dict[str, Dict]:
        """Loads the account-to-proxy assignments from a JSON file."""
        if not self.account_proxy_file.exists():
            return {}
        try:
            with self.account_proxy_file.open("r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            LOGGER.error("Could not read or parse account_proxies.json. Starting with empty assignments.")
            return {}

    def _save_account_proxies(self) -> None:
        """Saves the current account-to-proxy assignments to a JSON file."""
        try:
            with self.account_proxy_file.open("w") as f:
                json.dump(self.account_proxies, f, indent=4)
        except IOError:
            LOGGER.error("Could not save account_proxies.json.")

    def _get_available_proxy(self) -> Optional[Dict]:
        """Finds the first available proxy that is not currently assigned to any account."""
        if not self.proxies:
            return None
        assigned_proxy_addrs = {proxy_data['addr'] for proxy_data in self.account_proxies.values() if proxy_data}
        for proxy in self.proxies:
            if proxy['addr'] not in assigned_proxy_addrs:
                LOGGER.info(f"Found available proxy: {proxy['addr']}")
                return proxy
        LOGGER.warning("All proxies are currently assigned. No available proxy found.")
        return None

    # --- Group Count Helpers ---
    def _load_group_counts(self) -> Dict[str, int]:
        """Loads the group creation counts from a JSON file."""
        if not self.counts_file.exists():
            return {}
        try:
            with self.counts_file.open("r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            LOGGER.error("Could not read or parse group_counts.json. Starting with empty counts.")
            return {}

    def _save_group_counts(self) -> None:
        """Saves the current group creation counts to a JSON file."""
        try:
            with self.counts_file.open("w") as f:
                json.dump(self.group_counts, f, indent=4)
        except IOError:
            LOGGER.error("Could not save group_counts.json.")

    def _get_group_count(self, worker_key: str) -> int:
        """Gets the current group count for a specific worker."""
        return self.group_counts.get(worker_key, 0)

    def _set_group_count(self, worker_key: str, count: int) -> None:
        """Sets the group count for a worker and saves it to the file."""
        self.group_counts[worker_key] = count
        self._save_group_counts()

    def _remove_group_count(self, worker_key: str) -> None:
        """Removes a worker's group count, typically on account deletion."""
        if worker_key in self.group_counts:
            del self.group_counts[worker_key]
            self._save_group_counts()

    # --- User and Worker State Management ---
    def _load_known_users(self) -> List[int]:
        """Loads the list of known user IDs from a file."""
        if not self.known_users_file.exists():
            return []
        try:
            with self.known_users_file.open("r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            LOGGER.error("Could not read known_users.json.")
            return []

    def _save_known_users(self) -> None:
        """Saves the list of known user IDs to a file."""
        try:
            with self.known_users_file.open("w") as f:
                json.dump(self.known_users, f)
        except IOError:
            LOGGER.error("Could not save known_users.json.")

    def _load_active_workers_state(self) -> Dict[str, Dict]:
        """Loads the state of active workers from a file."""
        if not self.active_workers_file.exists():
            return {}
        try:
            with self.active_workers_file.open("r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            LOGGER.error("Could not read active_workers.json.")
            return {}

    def _save_active_workers_state(self) -> None:
        """Saves the current state of active workers to a file."""
        try:
            with self.active_workers_file.open("w") as f:
                json.dump(self.active_workers_state, f, indent=4)
        except IOError:
            LOGGER.error("Could not save active_workers.json.")

    async def _broadcast_message(self, message_text: str):
        """Sends a message to all known users."""
        LOGGER.info(f"ارسال پیام به {len(self.known_users)} کاربر.")
        for user_id in self.known_users:
            try:
                await self.bot.send_message(user_id, message_text)
                await asyncio.sleep(0.1) 
            except (errors.UserIsBlockedError, errors.InputUserDeactivatedError):
                LOGGER.warning(f"کاربر {user_id} ربات را بلاک کرده یا غیرفعال است. امکان ارسال پیام وجود ندارد.")
            except Exception as e:
                LOGGER.error(f"خطا در ارسال پیام به {user_id}: {e}")

    async def _create_login_client(self, proxy: Optional[Dict]) -> Optional[TelegramClient]:
        """Creates a temporary client for the login flow, using the specified proxy."""
        session = StringSession()
        device_params = random.choice([{'device_model': 'iPhone 14 Pro Max', 'system_version': '17.5.1'}, {'device_model': 'Samsung Galaxy S24 Ultra', 'system_version': 'SDK 34'}])

        try:
            proxy_info = f"پروکسی {proxy['addr']}:{proxy['port']}" if proxy else "بدون پروکسی"
            LOGGER.debug(f"تلاش برای اتصال ورود با {proxy_info}")
            client = TelegramClient(session, API_ID, API_HASH, proxy=proxy, timeout=Config.PROXY_TIMEOUT, **device_params)
            await client.connect()
            return client
        except Exception as e:
            LOGGER.error(f"اتصال ورود با {proxy_info} ناموفق بود: {e}")
            return None

    async def _create_worker_client(self, session_string: str, proxy: Optional[Dict]) -> Optional[TelegramClient]:
        """Creates a client for a worker, using its assigned proxy."""
        session = StringSession(session_string)
        device_params = random.choice([{'device_model': 'iPhone 14 Pro Max', 'system_version': '17.5.1'}, {'device_model': 'Samsung Galaxy S24 Ultra', 'system_version': 'SDK 34'}])
        
        client = TelegramClient(
            session,
            API_ID,
            API_HASH,
            proxy=proxy,
            timeout=Config.PROXY_TIMEOUT,
            device_model=device_params['device_model'],
            system_version=device_params['system_version']
        )
        
        try:
            proxy_info = f"پروکسی {proxy['addr']}:{proxy['port']}" if proxy else "بدون پروکسی"
            LOGGER.debug(f"تلاش برای اتصال کارگر با {proxy_info}")
            await client.connect()
            LOGGER.info(f"کارگر با موفقیت با {proxy_info} متصل شد")
            return client
        except Exception as e:
            LOGGER.error(f"اتصال کارگر با {proxy_info} ناموفق بود: {e}")
            sentry_sdk.capture_exception(e)
            if isinstance(e, errors.AuthKeyUnregisteredError):
                raise
            return None
            
    async def _send_request_with_reconnect(self, client: TelegramClient, request: Any, account_name: str) -> Any:
        """
        Sends a request, attempting to reconnect if the client is disconnected.
        Raises the original error if reconnection or the request fails.
        """
        try:
            if not client.is_connected():
                LOGGER.warning(f"مشتری برای '{account_name}' قطع شده است. تلاش برای اتصال مجدد...")
                await client.connect()
                if client.is_connected():
                    LOGGER.info(f"اتصال مجدد مشتری برای '{account_name}' با موفقیت انجام شد.")
                else:
                    LOGGER.error(f"اتصال مجدد مشتری برای '{account_name}' ناموفق بود.")
                    raise ConnectionError("اتصال مجدد مشتری ناموفق بود.")
            
            return await client(request)
        except ConnectionError as e:
            LOGGER.error(f"خطای اتصال برای '{account_name}' حتی پس از بررسی: {e}")
            sentry_sdk.capture_exception(e)
            raise 
        except Exception as e:
            LOGGER.error(f"خطای غیرمنتظره در ارسال درخواست برای '{account_name}': {e}")
            sentry_sdk.capture_exception(e)
            raise

    # --- Dynamic UI Builder ---
    def _build_main_menu(self) -> List[List[Button]]:
        return [
            [Button.text(Config.BTN_MANAGE_ACCOUNTS)],
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

    # --- Main Worker Task ---
    async def run_group_creation_worker(self, user_id: int, account_name: str, user_client: TelegramClient) -> None:
        worker_key = f"{user_id}:{account_name}"
        try:
            async with self.worker_semaphore:
                LOGGER.info(f"کارگر برای {worker_key} شروع شد. قفل سماforر گرفته شد.")

                avg_sleep = (Config.MIN_SLEEP_SECONDS + Config.MAX_SLEEP_SECONDS) / 2
                estimated_total_minutes = (Config.GROUPS_TO_CREATE * avg_sleep) / 60
                
                current_semester = self._get_group_count(worker_key)

                await self.bot.send_message(user_id, f"✅ **عملیات برای حساب `{account_name}` آغاز شد!**\n\n⏳ تخمین زمان کل عملیات: حدود {estimated_total_minutes:.0f} دقیقه.")

                for i in range(Config.GROUPS_TO_CREATE):
                    current_semester += 1
                    # MODIFIED: Changed group title to English as requested
                    group_title = f"collage Semester {current_semester}"

                    try:
                        request = CreateChatRequest(users=[Config.GROUP_MEMBER_TO_ADD], title=group_title)
                        result = await self._send_request_with_reconnect(user_client, request, account_name)

                        chat = None
                        if hasattr(result, 'chats') and result.chats:
                            chat = result.chats[0]
                        elif hasattr(result, 'updates') and hasattr(result.updates, 'chats') and result.updates.chats:
                            chat = result.updates.chats[0]
                        else:
                            LOGGER.error(f"چت در نتیجه نوع {type(result)} برای حساب {account_name} یافت نشد")
                            await self.bot.send_message(user_id, f"❌ [{account_name}] خطای غیرمنتظره: اطلاعات گروه یافت نشد.")
                            current_semester -= 1 
                            continue
                        
                        # MODIFIED: Replaced old history toggle with supergroup upgrade logic
                        # --- Upgrade to Supergroup and set History Visibility ---
                        try:
                            # Telethon needs an InputChannel for these operations, which we get from the chat entity
                            input_channel = await user_client.get_input_entity(chat.id)

                            # 1. Upgrade to a supergroup by making it public with a random username.
                            # This is a reliable method. The username must start with a letter.
                            random_username = 'a' + uuid.uuid4().hex 
                            LOGGER.info(f"Attempting to upgrade group {chat.id} to a supergroup by setting username: {random_username}")
                            
                            await user_client(functions.channels.UpdateUsernameRequest(
                                channel=input_channel,
                                username=random_username
                            ))
                            LOGGER.info(f"Group {chat.id} successfully upgraded to a supergroup.")

                            # 2. Now that it's a supergroup, make chat history visible to new members.
                            await user_client(functions.channels.TogglePreHistoryHiddenRequest(
                                channel=input_channel,
                                enabled=False
                            ))
                            LOGGER.info(f"Chat history for new members in group {chat.id} is now visible.")
                            
                            # 3. (Optional but good practice) Make the group private again to not leave public groups around.
                            await user_client(functions.channels.UpdateUsernameRequest(
                                channel=input_channel,
                                username=""  # Setting username to empty string makes it private
                            ))
                            LOGGER.info(f"Group {chat.id} has been made private again.")

                        except errors.UsernameOccupiedError as e:
                            # This is unlikely with a UUID but handled just in case.
                            LOGGER.error(f"Could not upgrade group {chat.id} because the random username was occupied. Skipping history toggle. Error: {e}")
                        except Exception as e:
                            # This will catch other potential errors during the upgrade/toggle process.
                            LOGGER.warning(f"Could not make chat history visible for group {chat.id}. Error: {e}\n{traceback.format_exc()}")


                        self._set_group_count(worker_key, current_semester)
                        
                        groups_made = i + 1
                        groups_remaining = Config.GROUPS_TO_CREATE - groups_made
                        time_remaining_minutes = (groups_remaining * avg_sleep) / 60

                        progress_message = (
                            f"📊 [{account_name}] گروه '{group_title}' ساخته شد. ({groups_made}/{Config.GROUPS_TO_CREATE})\n"
                            f"⏳ زمان تقریبی باقی‌مانده: {time_remaining_minutes:.0f} دقیقه."
                        )
                        await self.bot.send_message(user_id, progress_message)

                        sleep_time = random.randint(Config.MIN_SLEEP_SECONDS, Config.MAX_SLEEP_SECONDS)
                        await asyncio.sleep(sleep_time)

                    except errors.AuthKeyUnregisteredError as e:
                        LOGGER.error(f"کلید احراز هویت برای حساب '{account_name}' ثبت نشده است. حذف نشست.")
                        sentry_sdk.capture_exception(e)
                        self.session_manager.delete_session_file(user_id, account_name)
                        self._remove_group_count(worker_key)
                        await self.bot.send_message(user_id, f"🚨 **خطای امنیتی:** نشست برای حساب `{account_name}` به دلیل استفاده همزمان از چند نقطه، توسط تلگرام باطل شد. عملیات متوقف و حساب حذف گردید. لطفاً آن را دوباره اضافه کنید.")
                        break 
                    except errors.UserRestrictedError as e:
                        LOGGER.error(f"کارگر برای {worker_key} شکست خورد: کاربر محدود شده است.")
                        sentry_sdk.capture_exception(e)
                        await self.bot.send_message(user_id, f"❌ حساب `{account_name}` توسط تلگرام محدود شده و قادر به ساخت گروه نیست. عملیات متوقف شد.")
                        break
                    except errors.FloodWaitError as e:
                        LOGGER.warning(f"خطای انتظار سیل برای {worker_key}. خواب برای {e.seconds} ثانیه.")
                        sentry_sdk.capture_exception(e)
                        resume_time = datetime.now() + timedelta(seconds=e.seconds)
                        await self.bot.send_message(user_id, f"⏳ [{account_name}] به دلیل محدودیت تلگرام، عملیات به مدت {e.seconds / 60:.1f} دقیقه تا ساعت {resume_time:%H:%M:%S} متوقف شد.")
                        await asyncio.sleep(e.seconds)
                    except Exception as e:
                        LOGGER.error(f"خطای کارگر برای {worker_key}", exc_info=True)
                        sentry_sdk.capture_exception(e)
                        await self.bot.send_message(user_id, "❌ **خطای غیرمنتظره:** مشکلی در انجام عملیات رخ داد. لطفاً دوباره تلاش کنید.")
                        break
        except asyncio.CancelledError:
            LOGGER.info(f"وظیفه برای {worker_key} توسط کاربر لغو شد.")
            await self.bot.send_message(user_id, f"⏹️ عملیات برای حساب `{account_name}` توسط شما متوقف شد.")
        finally:
            LOGGER.info(f"کارگر برای {worker_key} به پایان رسید.")
            if worker_key in self.active_workers:
                del self.active_workers[worker_key]
                self.active_workers_state.pop(worker_key, None)
                self._save_active_workers_state()
            if user_client and user_client.is_connected():
                await user_client.disconnect()

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
            LOGGER.info(f"پروکسی ورود {proxy_addr} به حساب '{account_name}' اختصاص یافت.")
        else:
            LOGGER.info(f"حساب '{account_name}' مستقیماً وارد شد و بدون پروکسی اجرا خواهد شد.")

        if user_client and user_client.is_connected():
            await user_client.disconnect()
            LOGGER.info(f"مشتری ورود برای کاربر {user_id} ('{account_name}') با موفقیت قطع شد.")

        if 'client' in self.user_sessions[user_id]:
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
            self._save_known_users()
            LOGGER.info(f"کاربر جدید ربات را شروع کرد: {user_id}")
            
        session = self.user_sessions.get(user_id, {})
        if session.get('state') == 'authenticated':
            await event.reply(Config.MSG_WELCOME, buttons=self._build_main_menu())
        else:
            self.user_sessions[user_id] = {'state': 'awaiting_master_password'}
            await event.reply(Config.MSG_PROMPT_MASTER_PASSWORD, buttons=Button.clear())
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

        status_text = f"**📊 وضعیت سرور**\n\n"
        status_text += f"**کارگران فعال:** {active_count} / {max_workers}\n"

        if active_count > 0:
            status_text += "\n**حساب‌های در حال عملیات:**\n"
            for worker_key in self.active_workers.keys():
                _, acc_name = worker_key.split(":", 1)
                proxy_info = self.account_proxies.get(worker_key)
                proxy_str = f" (پروکسی: {proxy_info['addr']})" if proxy_info else ""
                status_text += f"- `{acc_name}`{proxy_str}\n"
        else:
            status_text += "\nℹ️ در حال حاضر هیچ حسابی در حال عملیات نیست."

        await event.reply(status_text, buttons=self._build_main_menu())
        raise events.StopPropagation

    async def _help_handler(self, event: events.NewMessage.Event) -> None:
        await event.reply(Config.MSG_HELP_TEXT, buttons=self._build_main_menu())
        raise events.StopPropagation

    async def _admin_command_handler(self, event: events.NewMessage.Event, handler: callable):
        """Wrapper to check for admin privileges before running a command."""
        if str(event.sender_id) != ADMIN_USER_ID:
            await event.reply("❌ شما مجوز استفاده از این دستور را ندارید.")
            return
        await handler(event)

    async def _debug_test_proxies_handler(self, event: events.NewMessage.Event) -> None:
        LOGGER.info(f"ادمین {event.sender_id} تست بی‌صدا پروکسی‌ها را آغاز کرد.")
        
        if not self.proxies:
            LOGGER.debug("تست پروکسی: هیچ پروکسی‌ای در فایل یافت نشد.")
            await self.bot.send_message(event.sender_id, "⚠️ هیچ پروکسی‌ای در فایل برای تست یافت نشد.")
            return
        
        await self.bot.send_message(event.sender_id, "🧪 شروع تست بی‌صدا پروکسی‌ها... نتایج در لاگ‌های سیستمی خواهد بود.")

        LOGGER.debug("--- شروع تست پروکسی ---")
        for proxy in self.proxies:
            proxy_addr = f"{proxy['addr']}:{proxy['port']}"
            client = None
            try:
                device_params = random.choice([{'device_model': 'iPhone 14 Pro Max', 'system_version': '17.5.1'}, {'device_model': 'Samsung Galaxy S24 Ultra', 'system_version': 'SDK 34'}])
                
                LOGGER.debug(f"تست پروکسی: {proxy} با دستگاه: {device_params}")
                
                client = TelegramClient(StringSession(), API_ID, API_HASH, proxy=proxy, timeout=Config.PROXY_TIMEOUT, **device_params)
                await client.connect()
                if client.is_connected():
                    LOGGER.info(f"  ✅ موفقیت: {proxy_addr}")
            except Exception as e:
                LOGGER.warning(f"  ❌ شکست ({type(e).__name__}): {proxy_addr} - {e}")
            finally:
                if client and client.is_connected():
                    await client.disconnect()

        LOGGER.debug("--- تست اتصال مستقیم ---")
        client = None
        try:
            device_params = random.choice([{'device_model': 'iPhone 14 Pro Max', 'system_version': '17.5.1'}, {'device_model': 'Samsung Galaxy S24 Ultra', 'system_version': 'SDK 34'}])
            LOGGER.debug(f"تست اتصال مستقیم با دستگاه: {device_params}")
            client = TelegramClient(StringSession(), API_ID, API_HASH, timeout=Config.PROXY_TIMEOUT, **device_params)
            await client.connect()
            if client.is_connected():
                LOGGER.info("  ✅ موفقیت: اتصال مستقیم")
        except Exception as e:
            LOGGER.warning(f"  ❌ شکست ({type(e).__name__}): اتصال مستقیم - {e}")
        finally:
            if client and client.is_connected():
                await client.disconnect()
        
        LOGGER.info("تست بی‌صدا پروکسی‌ها به پایان رسید.")
        await self.bot.send_message(event.sender_id, "🏁 تست بی‌صدا پروکسی‌ها به پایان رسید. نتایج را در لاگ‌های سیستمی بررسی کنید.")
        raise events.StopPropagation

    async def _clean_sessions_handler(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        LOGGER.info(f"ادمین {user_id} پاکسازی نشست‌ها را آغاز کرد.")

        try:
            async with self.bot.conversation(user_id, timeout=30) as conv:
                await conv.send_message("⚠️ **هشدار:** این عملیات تمام نشست‌های کاربر، شمارنده‌ها و تخصیص‌های پروکسی را حذف کرده و تمام عملیات در حال اجرا را متوقف می‌کند. لطفاً با ارسال `تأیید` در عرض 30 ثانیه تأیید کنید.")
                response = await conv.get_response()
                if response.text.lower() != 'تأیید':
                    await conv.send_message("❌ عملیات لغو شد.")
                    return
        except asyncio.TimeoutError:
            await self.bot.send_message(user_id, "❌ زمان تأیید تمام شد. عملیات لغو شد.")
            return

        msg = await self.bot.send_message(user_id, "🧹 پاکسازی نشست‌ها و توقف عملیات...")
        
        stopped_workers = []
        if self.active_workers:
            LOGGER.info("توقف تمام کارگران فعال قبل از پاکسازی نشست‌ها.")
            for worker_key, task in list(self.active_workers.items()):
                task.cancel()
                stopped_workers.append(worker_key.split(":", 1)[1])
            self.active_workers.clear()
            await asyncio.sleep(1) 

        report = ["**📝 گزارش پاکسازی:**\n"]
        if stopped_workers:
            report.append(f"⏹️ **عملیات متوقف شده:** {', '.join(f'`{name}`' for name in stopped_workers)}\n")

        deleted_files_count = 0
        
        if SESSIONS_DIR.exists():
            for item in SESSIONS_DIR.iterdir():
                if item.name != 'bot_session.session':
                    try:
                        if item.is_file():
                            item.unlink()
                            deleted_files_count += 1
                            LOGGER.debug(f"فایل حذف شد: {item.name}")
                    except OSError as e:
                        LOGGER.error(f"ناتوانی در حذف فایل {item}: {e}")
        
        self.group_counts.clear()
        self.account_proxies.clear()
        self.known_users.clear()

        report.append(f"🗑️ **فایل‌های داده حذف شده:** {deleted_files_count} فایل\n")
        LOGGER.info(f"{deleted_files_count} فایل داده از {SESSIONS_DIR} حذف شد.")

        folders_to_clean = ["selenium_sessions", "api_sessions", "telethon_sessions"]
        for folder_name in folders_to_clean:
            folder_path = Path(folder_name)
            if folder_path.exists() and folder_path.is_dir():
                try:
                    shutil.rmtree(folder_path)
                    report.append(f"📁 **پوشه حذف شده:** `{folder_name}`\n")
                    LOGGER.info(f"پوشه حذف شد: {folder_name}")
                except OSError as e:
                    LOGGER.error(f"ناتوانی در حذف پوشه {folder_path}: {e}")
            
        report.append("\n✅ پاکسازی با موفقیت تکمیل شد.")
        
        await msg.edit(''.join(report))
        raise events.StopPropagation

    async def _test_sentry_handler(self, event: events.NewMessage.Event) -> None:
        LOGGER.info(f"ادمین {event.sender_id} تست Sentry را آغاز کرد.")
        await event.reply("🧪 ارسال یک خطای آزمایشی به Sentry. لطفاً داشبورد Sentry خود را بررسی کنید.")
        try:
            division_by_zero = 1 / 0
        except Exception as e:
            sentry_sdk.capture_exception(e)
            await event.reply("✅ خطای آزمایشی به Sentry ارسال شد!")

    async def _initiate_login_flow(self, event: events.NewMessage.Event) -> None:
        self.user_sessions[event.sender_id]['state'] = 'awaiting_phone'
        await event.reply('📞 لطفاً شماره تلفن حساب جدید را با فرمت بین‌المللی (مثلاً `+989123456789`) ارسال کنید.', buttons=Button.clear())

    async def _initiate_selenium_login_flow(self, event: events.NewMessage.Event) -> None:
        await event.reply(Config.MSG_BROWSER_RUNNING)
        await asyncio.sleep(2)
        await self._initiate_login_flow(event)

    async def _message_router(self, event: events.NewMessage.Event) -> None:
        if not isinstance(getattr(event, 'message', None), Message) or not event.message.text:
            return

        text = event.message.text
        user_id = event.sender_id
        session = self.user_sessions.get(user_id, {})
        state = session.get('state')

        # Global cancel command
        if text == '/cancel':
            if 'state' in self.user_sessions.get(user_id, {}):
                del self.user_sessions[user_id]['state']
            await event.reply("✅ عملیات جاری لغو شد.", buttons=self._build_main_menu())
            return

        if state == 'awaiting_master_password':
            await self._handle_master_password(event)
            return

        login_flow_states = ['awaiting_phone', 'awaiting_code', 'awaiting_password', 'awaiting_account_name']
        if state in login_flow_states:
            if text == Config.BTN_BACK:
                self.user_sessions[user_id]['state'] = 'authenticated'
                await self._start_handler(event)
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
            await event.reply('⏳ عملیات برای این حساب در حال انجام است.')
            return

        session_str = self.session_manager.load_session_string(user_id, account_name)
        if not session_str:
            await event.reply('❌ هیچ نشستی برای این حساب یافت نشد. لطفاً آن را حذف و دوباره اضافه کنید.')
            return

        await event.reply(f'🚀 آماده‌سازی برای شروع عملیات برای حساب `{account_name}`...')
        
        user_client = None
        try:
            assigned_proxy = self.account_proxies.get(worker_key)
            user_client = await self._create_worker_client(session_str, assigned_proxy)
            
            if not user_client:
                await event.reply(f'❌ اتصال به تلگرام برای حساب `{account_name}` با استفاده از پروکسی اختصاصی ناموفق بود.')
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
                await event.reply(f'⚠️ نشست برای حساب `{account_name}` منقضی شده و حذف شده است. لطفاً آن را دوباره اضافه کنید.')
        except errors.AuthKeyUnregisteredError as e:
            LOGGER.error(f"کلید احراز هویت برای حساب '{account_name}' ثبت نشده است. حذف نشست.")
            sentry_sdk.capture_exception(e)
            self.session_manager.delete_session_file(user_id, account_name)
            self._remove_group_count(worker_key)
            await event.reply(f"🚨 **هشدار امنیتی:** نشست برای حساب `{account_name}` به دلیل استفاده همزمان از چند مکان توسط تلگرام باطل شد. حساب حذف شده است. لطفاً دوباره آن را اضافه کنید.")
            await self._send_accounts_menu(event)
        except Exception as e:
            LOGGER.error(f"خطا در شروع فرآیند برای {worker_key}", exc_info=True)
            sentry_sdk.capture_exception(e)
            await event.reply(f'❌ خطایی در اتصال به حساب `{account_name}` رخ داد.')
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
            LOGGER.info(f"کاربر لغو عملیات را برای کارگر {worker_key} آغاز کرد.")
            try:
                await task
            except asyncio.CancelledError:
                LOGGER.info(f"وظیفه کارگر {worker_key} با موفقیت لغو و پاکسازی شد.")
            
            await self._send_accounts_menu(event)
        else:
            await event.reply(f"ℹ️ هیچ عملیات فعالی برای توقف برای حساب `{account_name}` وجود ندارد.")

    async def _delete_account_handler(self, event: events.NewMessage.Event, account_name: str) -> None:
        user_id = event.sender_id
        worker_key = f"{user_id}:{account_name}"

        if worker_key in self.active_workers:
            self.active_workers[worker_key].cancel()
            LOGGER.info(f"کارگر برای {worker_key} به دلیل حذف حساب لغو شد.")

        if self.session_manager.delete_session_file(user_id, account_name):
            self._remove_group_count(worker_key)
            if worker_key in self.account_proxies:
                del self.account_proxies[worker_key]
                self._save_account_proxies()
            await event.reply(f"✅ حساب `{account_name}` با موفقیت حذف شد و تمام عملیات مرتبط متوقف شد.")
        else:
            await event.reply(f"✅ عملیات برای حساب `{account_name}` متوقف شد (نشست وجود نداشت).")

        await self._send_accounts_menu(event)

    # --- Login Flow Handlers ---
    async def _handle_master_password(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        if MASTER_PASSWORD_HASH:
            hashed_input = hashlib.sha256(event.message.text.strip().encode()).hexdigest()
            if hashed_input == MASTER_PASSWORD_HASH:
                self.user_sessions[user_id] = {'state': 'authenticated'}
                await event.reply(Config.MSG_WELCOME, buttons=self._build_main_menu())
            else:
                await event.reply(Config.MSG_INCORRECT_MASTER_PASSWORD)
        else: # Fallback to plain text if hash is not set
            if event.message.text.strip() == "3935Eerfan@123":
                self.user_sessions[user_id] = {'state': 'authenticated'}
                await event.reply(Config.MSG_WELCOME, buttons=self._build_main_menu())
            else:
                await event.reply(Config.MSG_INCORRECT_MASTER_PASSWORD)
        raise events.StopPropagation

    async def _handle_phone_input(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        phone_number = event.message.text.strip()

        if not re.match(r'^\+\d{10,}$', phone_number):
            await event.reply(
                '❌ **فرمت شماره تلفن نامعتبر است.**\n'
                'لطفاً شماره را با فرمت بین‌المللی کامل (مثلاً `+989123456789`) وارد کنید.',
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
                proxy_msg = f" با پروکسی {selected_proxy['addr']}:{selected_proxy['port']}" if selected_proxy else " مستقیماً"
                await event.reply(f'❌ اتصال به تلگرام{proxy_msg} ناموفق بود. لطفاً بعداً دوباره تلاش کنید.')
                return
                
            self.user_sessions[user_id]['client'] = user_client
            sent_code = await user_client.send_code_request(self.user_sessions[user_id]['phone'])
            self.user_sessions[user_id]['phone_code_hash'] = sent_code.phone_code_hash
            self.user_sessions[user_id]['state'] = 'awaiting_code'
            await event.reply('💬 یک کد ورود ارسال شده است. لطفاً آن را اینجا بفرستید.', buttons=[[Button.text(Config.BTN_BACK)]])
        except Exception as e:
            LOGGER.error(f"خطای ورودی تلفن برای {user_id}", exc_info=True)
            sentry_sdk.capture_exception(e)
            self.user_sessions[user_id]['state'] = 'awaiting_phone' 
            await event.reply(
                '❌ **خطا:** شماره تلفن نامعتبر یا مشکلی در ارسال کد. لطفاً با فرمت بین‌المللی (+کد کشور) دوباره تلاش کنید یا عملیات را لغو کنید.',
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
            await event.reply('✅ ورود با موفقیت انجام شد! لطفاً یک نام مستعار برای این حساب وارد کنید (مثلاً `حساب اصلی` یا `شماره دوم`).', buttons=[[Button.text(Config.BTN_BACK)]])
        except errors.SessionPasswordNeededError:
            self.user_sessions[user_id]['state'] = 'awaiting_password'
            await event.reply('🔑 این حساب احراز هویت دو مرحله‌ای دارد. لطفاً رمز عبور را ارسال کنید.', buttons=[[Button.text(Config.BTN_BACK)]])
        except errors.PhoneCodeExpiredError:
            try:
                LOGGER.warning(f"کد تلفن برای {user_id} منقضی شده است. درخواست کد جدید.")
                sent_code = await user_client.send_code_request(self.user_sessions[user_id]['phone'])
                self.user_sessions[user_id]['phone_code_hash'] = sent_code.phone_code_hash
                self.user_sessions[user_id]['state'] = 'awaiting_code'
                await event.reply('⚠️ کد منقضی شده (احتمالاً به دلیل تغییر سرور). یک کد جدید ارسال شده است. لطفاً کد جدید را وارد کنید.', buttons=[[Button.text(Config.BTN_BACK)]])
            except Exception as e:
                LOGGER.error(f"ناتوانی در ارسال مجدد کد برای {user_id} پس از انقضا: {e}", exc_info=True)
                sentry_sdk.capture_exception(e)
                self.user_sessions[user_id]['state'] = 'awaiting_phone'
                await event.reply('❌ **خطا:** کد قبلی منقضی شده و ارسال مجدد ناموفق بود. لطفاً شماره تلفن را دوباره وارد کنید.', buttons=[[Button.text(Config.BTN_BACK)]])
        except Exception as e:
            LOGGER.error(f"خطای ورودی کد برای {user_id}", exc_info=True)
            sentry_sdk.capture_exception(e)
            self.user_sessions[user_id]['state'] = 'awaiting_phone'
            await event.reply('❌ **خطا:** کد نامعتبر است. لطفاً شماره تلفن را دوباره وارد کنید.', buttons=[[Button.text(Config.BTN_BACK)]])

    async def _handle_password_input(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        try:
            await self.user_sessions[user_id]['client'].sign_in(password=event.message.text.strip())
            self.user_sessions[user_id]['state'] = 'awaiting_account_name'
            await event.reply('✅ ورود با موفقیت انجام شد! لطفاً یک نام مستعار برای این حساب وارد کنید (مثلاً `حساب اصلی` یا `شماره دوم`).', buttons=[[Button.text(Config.BTN_BACK)]])
        except Exception as e:
            LOGGER.error(f"خطای ورودی رمز عبور برای {user_id}", exc_info=True)
            sentry_sdk.capture_exception(e)
            self.user_sessions[user_id]['state'] = 'awaiting_password'
            await event.reply('❌ **خطا:** رمز عبور اشتباه است. لطفاً دوباره تلاش کنید.', buttons=[[Button.text(Config.BTN_BACK)]])

    async def _handle_account_name_input(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        account_name = event.message.text.strip()
        if not account_name:
            await event.reply("❌ نام مستعار نمی‌تواند خالی باشد. لطفاً یک نام وارد کنید.", buttons=[[Button.text(Config.BTN_BACK)]])
            return

        if account_name in self.session_manager.get_user_accounts(user_id):
            await event.reply(f"❌ شما قبلاً حساب با نام مستعار `{account_name}` دارید. لطفاً نام دیگری انتخاب کنید.", buttons=[[Button.text(Config.BTN_BACK)]])
            return

        self.user_sessions[user_id]['account_name'] = account_name
        user_client = self.user_sessions[user_id]['client']
        await self.on_login_success(event, user_client)

    # --- Main Run Method ---
    def register_handlers(self) -> None:
        self.bot.add_event_handler(self._start_handler, events.NewMessage(pattern='/start'))
        self.bot.add_event_handler(self._message_router, events.NewMessage)

    async def run(self) -> None:
        self.register_handlers()
        LOGGER.info("شروع ربات...")
        try:
            await self.bot.start(bot_token=BOT_TOKEN)
            LOGGER.info("سرویس ربات با موفقیت شروع شد.")
            # Resume any workers that were active before a restart
            for worker_key, worker_data in self.active_workers_state.items():
                user_id = worker_data["user_id"]
                account_name = worker_data["account_name"]
                LOGGER.info(f"از سرگیری کارگر برای حساب '{account_name}' پس از راه‌اندازی مجدد.")
                dummy_event = events.NewMessage.Event(self.bot.build_in_message(user_id))
                await self._start_process_handler(dummy_event, account_name)

            if self.known_users:
                await self._broadcast_message("✅ ربات با موفقیت شروع شد و اکنون در دسترس است.")
            await self.bot.run_until_disconnected()
        finally:
            LOGGER.info("سرویس ربات در حال تعطیل شدن است. قطع اتصال مشتری اصلی ربات.")
            if self.bot.is_connected():
                await self.bot.disconnect()

if __name__ == "__main__":
    bot_instance = GroupCreatorBot()
    asyncio.run(bot_instance.run())
