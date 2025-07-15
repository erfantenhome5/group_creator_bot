import asyncio
import json
import logging
import os
import random
import re
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from cryptography.fernet import Fernet, InvalidToken
from dotenv import load_dotenv
from telethon import Button, TelegramClient, errors, events
from telethon.sessions import StringSession
from telethon.tl.functions.messages import CreateChatRequest, ExportChatInviteRequest
from telethon.tl.types import Message

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

# --- Centralized Configuration ---
class Config:
    """Holds all configurable values and UI strings for the bot."""
    # Bot Settings
    MASTER_PASSWORD = "3935Eerfan@123"
    MAX_CONCURRENT_WORKERS = 5
    GROUPS_TO_CREATE = 50
    MIN_SLEEP_SECONDS = 60   # 1 minute
    MAX_SLEEP_SECONDS = 240  # 4 minutes
    GROUP_MEMBER_TO_ADD = '@BotFather'
    PROXY_FILE = "proxy10.txt"
    PROXY_TIMEOUT = 5 # Increased timeout for better reliability

    # --- UI Text & Buttons ---
    # Main Menu
    BTN_MANAGE_ACCOUNTS = "👤 مدیریت حساب‌ها"
    BTN_SERVER_STATUS = "📊 وضعیت سرور"
    BTN_HELP = "ℹ️ راهنما"

    # Account Management Menu
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
        "این گزینه اطلاعات لحظه‌ای درباره وضعیت ربات را نمایش می‌دهد:\n"
        "  - تعداد کل پردازش‌های فعال.\n"
        "  - لیست حساب‌هایی که در حال حاضر مشغول به کار هستند."
    )
    MSG_PROMPT_MASTER_PASSWORD = "🔑 لطفا برای دسترسی به ربات، رمز عبور اصلی را وارد کنید:"
    MSG_INCORRECT_MASTER_PASSWORD = "❌ رمز عبور اشتباه است. لطفا دوباره تلاش کنید."
    MSG_BROWSER_RUNNING = "⏳ در حال آماده‌سازی مرورگر امن... این کار ممکن است چند لحظه طول بکشد."


# --- Environment Loading ---
load_dotenv()
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")

if not all([API_ID, API_HASH, BOT_TOKEN, ENCRYPTION_KEY]):
    raise ValueError("Missing required environment variables. Ensure API_ID, API_HASH, BOT_TOKEN, and ENCRYPTION_KEY are set.")

API_ID = int(API_ID)
SESSIONS_DIR = Path("sessions")
SESSIONS_DIR.mkdir(exist_ok=True)


class GroupCreatorBot:
    """A class to encapsulate the bot's logic for managing multiple accounts."""

    def __init__(self) -> None:
        """Initializes the bot instance and the encryption engine."""
        self.bot = TelegramClient('bot_session', API_ID, API_HASH)
        self.user_sessions: Dict[int, Dict[str, Any]] = {} # Combined session state
        self.active_workers: Dict[str, asyncio.Task] = {}  # Key is "user_id:account_name"
        self.worker_semaphore = asyncio.Semaphore(Config.MAX_CONCURRENT_WORKERS)
        self.counts_file = SESSIONS_DIR / "group_counts.json"
        self.group_counts = self._load_group_counts()
        self.proxies = self._load_proxies()
        try:
            self.fernet = Fernet(ENCRYPTION_KEY.encode())
        except (ValueError, TypeError):
            raise ValueError("Invalid ENCRYPTION_KEY. Please generate a valid key.")

    # --- Proxy Helpers ---
    def _load_proxies(self) -> List[Dict]:
        """Loads proxies from the specified file."""
        proxy_list = []
        try:
            with open(Config.PROXY_FILE, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        # Handle proxies with format ip:port for HTTP proxies without auth
                        ip, port = line.split(':', 1)
                        proxy_list.append({
                            'proxy_type': 'http',
                            'addr': ip,
                            'port': int(port)
                        })
                    except ValueError:
                        LOGGER.warning(f"Skipping malformed proxy line: {line}. Expected format is IP:PORT.")
            LOGGER.info(f"Loaded {len(proxy_list)} proxies from {Config.PROXY_FILE}.")
        except FileNotFoundError:
            LOGGER.warning(f"Proxy file '{Config.PROXY_FILE}' not found. Continuing without proxies.")
        return proxy_list

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

    async def _create_new_user_client(self, session_string: Optional[str] = None) -> Optional[TelegramClient]:
        """Tries to connect with a proxy, falls back to no proxy."""
        session = StringSession(session_string) if session_string else StringSession()
        device_params = random.choice([{'device_model': 'iPhone 14 Pro Max', 'system_version': '17.5.1'}, {'device_model': 'Samsung Galaxy S24 Ultra', 'system_version': 'SDK 34'}])

        # Shuffle proxies to not always try in the same order
        shuffled_proxies = self.proxies.copy()
        random.shuffle(shuffled_proxies)

        for proxy in shuffled_proxies:
            proxy_addr = f"{proxy['addr']}:{proxy['port']}"
            try:
                LOGGER.debug(f"Attempting to connect with proxy: {proxy_addr}")
                client = TelegramClient(
                    session,
                    API_ID,
                    API_HASH,
                    proxy=proxy,
                    timeout=Config.PROXY_TIMEOUT,
                    device_model=device_params['device_model'],
                    system_version=device_params['system_version']
                )
                await client.connect()
                LOGGER.info(f"Successfully connected using proxy: {proxy_addr}")
                return client
            except Exception as e:
                LOGGER.warning(f"Failed to connect with proxy {proxy_addr}: {e}")
                if isinstance(e, errors.AuthKeyUnregisteredError):
                    raise  # Re-raise the specific error to be handled by the caller
                continue

        LOGGER.warning("All proxies failed. Attempting to connect without a proxy...")
        try:
            LOGGER.debug("Attempting to connect without a proxy.")
            client = TelegramClient(
                session,
                API_ID,
                API_HASH,
                timeout=Config.PROXY_TIMEOUT,
                device_model=device_params['device_model'],
                system_version=device_params['system_version']
            )
            await client.connect()
            LOGGER.info("Successfully connected without a proxy.")
            return client
        except Exception as e:
            LOGGER.error(f"Failed to connect without a proxy: {e}")
            if isinstance(e, errors.AuthKeyUnregisteredError):
                raise  # Re-raise the specific error to be handled by the caller
            return None


    # --- Dynamic UI Builder ---
    def _build_main_menu(self) -> List[List[Button]]:
        return [
            [Button.text(Config.BTN_MANAGE_ACCOUNTS)],
            [Button.text(Config.BTN_SERVER_STATUS), Button.text(Config.BTN_HELP)],
        ]

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
                LOGGER.info(f"Worker started for {worker_key}. Semaphore acquired.")

                avg_sleep = (Config.MIN_SLEEP_SECONDS + Config.MAX_SLEEP_SECONDS) / 2
                estimated_total_minutes = (Config.GROUPS_TO_CREATE * avg_sleep) / 60
                
                current_semester = self._get_group_count(worker_key)

                await self.bot.send_message(user_id, f"✅ **عملیات برای حساب `{account_name}` آغاز شد!**\n\n⏳ تخمین زمان کل عملیات: حدود {estimated_total_minutes:.0f} دقیقه.")

                for i in range(Config.GROUPS_TO_CREATE):
                    current_semester += 1
                    group_title = f"collage Semester {current_semester}"
                    try:
                        result = await user_client(CreateChatRequest(users=[Config.GROUP_MEMBER_TO_ADD], title=group_title))

                        chat = None
                        if hasattr(result, 'chats') and result.chats:
                            chat = result.chats[0]
                        elif hasattr(result, 'updates') and hasattr(result.updates, 'chats') and result.updates.chats:
                            chat = result.updates.chats[0]
                        else:
                            LOGGER.error(f"Could not find chat in result of type {type(result)} for account {account_name}")
                            await self.bot.send_message(user_id, f"❌ [{account_name}] خطای غیرمنتظره: اطلاعات گروه یافت نشد.")
                            current_semester -= 1 
                            continue

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

                    except errors.AuthKeyUnregisteredError:
                        LOGGER.error(f"Authentication key for account '{account_name}' is unregistered during worker execution. Deleting session.")
                        self._delete_session_file(user_id, account_name)
                        self._remove_group_count(worker_key)
                        await self.bot.send_message(user_id, f"🚨 **خطای امنیتی:** نشست برای حساب `{account_name}` به دلیل استفاده همزمان از چند نقطه، توسط تلگرام باطل شد. عملیات متوقف و حساب حذف گردید. لطفاً آن را دوباره اضافه کنید.")
                        break 
                    except errors.UserRestrictedError:
                        LOGGER.error(f"Worker for {worker_key} failed: User is restricted.")
                        await self.bot.send_message(user_id, f"❌ حساب `{account_name}` توسط تلگرام محدود شده و قادر به ساخت گروه نیست. عملیات متوقف شد.")
                        break
                    except errors.FloodWaitError as fwe:
                        resume_time = datetime.now() + timedelta(seconds=fwe.seconds)
                        await self.bot.send_message(user_id, f"⏳ [{account_name}] به دلیل محدودیت تلگرام، عملیات به مدت {fwe.seconds / 60:.1f} دقیقه تا ساعت {resume_time:%H:%M:%S} متوقف شد.")
                        await asyncio.sleep(fwe.seconds)
                    except Exception as e:
                        LOGGER.error(f"Worker error for {worker_key}", exc_info=e)
                        await self.bot.send_message(user_id, f"❌ [{account_name}] خطای غیرمنتظره در ساخت گروه رخ داد.")
                        break
        except asyncio.CancelledError:
            LOGGER.info(f"Task for {worker_key} was cancelled by user.")
            await self.bot.send_message(user_id, f"⏹️ عملیات برای حساب `{account_name}` توسط شما متوقف شد.")
        finally:
            LOGGER.info(f"Worker finished for {worker_key}.")
            if worker_key in self.active_workers and not self.active_workers[worker_key].cancelled():
                 await self.bot.send_message(user_id, f"🏁 چرخه ساخت گروه برای حساب `{account_name}` به پایان رسید.")

            if worker_key in self.active_workers:
                del self.active_workers[worker_key]
            # FIXED: Ensure client is always disconnected gracefully
            if user_client and user_client.is_connected():
                await user_client.disconnect()


    async def on_login_success(self, event: events.NewMessage.Event, user_client: TelegramClient) -> None:
        user_id = event.sender_id
        account_name = self.user_sessions[user_id]['account_name']
        self._save_session_string(user_id, account_name, user_client.session.save())

        if 'client' in self.user_sessions[user_id]:
            del self.user_sessions[user_id]['client']
        self.user_sessions[user_id]['state'] = 'authenticated' 

        await self.bot.send_message(user_id, f"✅ حساب `{account_name}` با موفقیت اضافه شد!")
        await self._send_accounts_menu(event)

    # --- Bot Event Handlers ---
    async def _start_handler(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        session = self.user_sessions.get(user_id, {})
        if session.get('state') == 'authenticated':
            await event.reply(Config.MSG_WELCOME, buttons=self._build_main_menu())
        else:
            self.user_sessions[user_id] = {'state': 'awaiting_master_password'}
            await event.reply(Config.MSG_PROMPT_MASTER_PASSWORD)
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
        status_text += f"**پردازش‌های فعال:** {active_count} / {max_workers}\n"

        if active_count > 0:
            status_text += "\n**حساب‌های در حال کار:**\n"
            for worker_key in self.active_workers.keys():
                _, acc_name = worker_key.split(":", 1)
                status_text += f"- `{acc_name}`\n"
        else:
            status_text += "\nℹ️ در حال حاضر هیچ حسابی مشغول به کار نیست."

        await event.reply(status_text, buttons=self._build_main_menu())
        raise events.StopPropagation

    async def _help_handler(self, event: events.NewMessage.Event) -> None:
        await event.reply(Config.MSG_HELP_TEXT, buttons=self._build_main_menu())
        raise events.StopPropagation

    async def _debug_test_proxies_handler(self, event: events.NewMessage.Event) -> None:
        """Runs a silent proxy test, logging results to debug."""
        LOGGER.info(f"User {event.sender_id} initiated a silent proxy test.")
        
        if not self.proxies:
            LOGGER.debug("Proxy test: No proxies found in the file.")
            await self.bot.send_message(event.sender_id, "⚠️ No proxies found in file to test.")
            return
        
        await self.bot.send_message(event.sender_id, "🧪 Starting silent proxy test... Results will be in the system logs.")

        LOGGER.debug("--- Starting Proxy Test ---")
        for proxy in self.proxies:
            proxy_addr = f"{proxy['addr']}:{proxy['port']}"
            client = None
            try:
                device_params = random.choice([{'device_model': 'iPhone 14 Pro Max', 'system_version': '17.5.1'}, {'device_model': 'Samsung Galaxy S24 Ultra', 'system_version': 'SDK 34'}])
                
                LOGGER.debug(f"Testing proxy: {proxy} with device: {device_params}")
                
                client = TelegramClient(
                    StringSession(), 
                    API_ID, 
                    API_HASH, 
                    proxy=proxy, 
                    timeout=Config.PROXY_TIMEOUT,
                    device_model=device_params['device_model'],
                    system_version=device_params['system_version']
                )
                await client.connect()
                if client.is_connected():
                    LOGGER.info(f"  ✅ SUCCESS: {proxy_addr}")
            except Exception as e:
                LOGGER.warning(f"  ❌ FAILED ({type(e).__name__}): {proxy_addr} - {e}")
            finally:
                # FIXED: Ensure client is always disconnected gracefully
                if client and client.is_connected():
                    await client.disconnect()

        LOGGER.debug("--- Testing Direct Connection ---")
        client = None
        try:
            device_params = random.choice([{'device_model': 'iPhone 14 Pro Max', 'system_version': '17.5.1'}, {'device_model': 'Samsung Galaxy S24 Ultra', 'system_version': 'SDK 34'}])
            LOGGER.debug(f"Testing direct connection with device: {device_params}")
            client = TelegramClient(
                StringSession(), 
                API_ID, 
                API_HASH, 
                timeout=Config.PROXY_TIMEOUT,
                device_model=device_params['device_model'],
                system_version=device_params['system_version']
            )
            await client.connect()
            if client.is_connected():
                LOGGER.info("  ✅ SUCCESS: Direct Connection")
        except Exception as e:
            LOGGER.warning(f"  ❌ FAILED ({type(e).__name__}): Direct Connection - {e}")
        finally:
            # FIXED: Ensure client is always disconnected gracefully
            if client and client.is_connected():
                await client.disconnect()
        
        LOGGER.info("Silent proxy test finished.")
        await self.bot.send_message(event.sender_id, "🏁 Silent proxy test finished. Check system logs for results.")
        raise events.StopPropagation

    async def _clean_sessions_handler(self, event: events.NewMessage.Event) -> None:
        """Stops all workers and cleans up all user session files and folders."""
        user_id = event.sender_id
        LOGGER.info(f"User {user_id} initiated session cleaning.")

        try:
            async with self.bot.conversation(user_id, timeout=30) as conv:
                await conv.send_message("⚠️ **هشدار:** این عملیات تمام نشست‌های کاربری را حذف کرده و تمام عملیات‌های در حال اجرا را متوقف می‌کند. لطفاً با ارسال `confirm` در 30 ثانیه آینده تایید کنید.")
                response = await conv.get_response()
                if response.text.lower() != 'confirm':
                    await conv.send_message("❌ عملیات لغو شد.")
                    return
        except asyncio.TimeoutError:
            await self.bot.send_message(user_id, "❌ زمان انتظار برای تایید به پایان رسید. عملیات لغو شد.")
            return

        msg = await self.bot.send_message(user_id, "🧹 در حال پاکسازی نشست‌ها و توقف عملیات‌ها...")
        
        stopped_workers = []
        if self.active_workers:
            LOGGER.info("Stopping all active workers before cleaning sessions.")
            for worker_key, task in list(self.active_workers.items()):
                task.cancel()
                stopped_workers.append(worker_key.split(":", 1)[1])
            self.active_workers.clear()
            await asyncio.sleep(1) 

        report = ["**📝 گزارش پاکسازی:**\n"]
        if stopped_workers:
            report.append(f"⏹️ **عملیات‌های متوقف شده:** {', '.join(f'`{name}`' for name in stopped_workers)}\n")

        deleted_files_count = 0
        deleted_folders = []
        
        if SESSIONS_DIR.exists():
            for item in SESSIONS_DIR.iterdir():
                if item.is_file() and item.name.endswith(".session") and item.name != 'bot_session.session':
                    try:
                        item.unlink()
                        deleted_files_count += 1
                        LOGGER.debug(f"Deleted session file: {item.name}")
                    except OSError as e:
                        LOGGER.error(f"Failed to delete file {item}: {e}")

        report.append(f"🗑️ **فایل‌های نشست حذف شده:** {deleted_files_count} عدد\n")
        LOGGER.info(f"Deleted {deleted_files_count} user session files.")

        folders_to_clean = ["selenium_sessions", "api_sessions", "telethon_sessions"]
        for folder_name in folders_to_clean:
            folder_path = Path(folder_name)
            if folder_path.exists() and folder_path.is_dir():
                try:
                    shutil.rmtree(folder_path)
                    deleted_folders.append(folder_name)
                    LOGGER.info(f"Deleted folder: {folder_name}")
                except OSError as e:
                    LOGGER.error(f"Failed to delete folder {folder_path}: {e}")
        
        if deleted_folders:
            report.append(f"📁 **پوشه‌های حذف شده:** {', '.join(f'`{name}`' for name in deleted_folders)}\n")
            
        report.append("\n✅ پاکسازی با موفقیت انجام شد.")
        
        await msg.edit(''.join(report))
        raise events.StopPropagation

    async def _initiate_login_flow(self, event: events.NewMessage.Event) -> None:
        self.user_sessions[event.sender_id]['state'] = 'awaiting_phone'
        await event.reply('📞 لطفا شماره تلفن حساب جدید را با فرمت بین‌المللی ارسال کنید (مثال: `+989123456789`).', buttons=Button.clear())

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

        route_map = {
            Config.BTN_MANAGE_ACCOUNTS: self._manage_accounts_handler,
            Config.BTN_HELP: self._help_handler,
            Config.BTN_BACK: self._start_handler,
            Config.BTN_ADD_ACCOUNT: self._initiate_login_flow,
            Config.BTN_ADD_ACCOUNT_SELENIUM: self._initiate_selenium_login_flow,
            Config.BTN_SERVER_STATUS: self._server_status_handler,
            "/debug_proxies": self._debug_test_proxies_handler,
            "/clean_sessions": self._clean_sessions_handler,
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
            await event.reply('⏳ عملیات برای این حساب در حال اجراست.')
            return

        session_str = self._load_session_string(user_id, account_name)
        if not session_str:
            await event.reply('❌ نشست برای این حساب یافت نشد. لطفا آن را حذف و دوباره اضافه کنید.')
            return

        await event.reply(f'🚀 در حال آماده‌سازی برای شروع عملیات حساب `{account_name}`...')
        
        user_client = None
        try:
            user_client = await self._create_new_user_client(session_str)
            
            if not user_client:
                await event.reply(f'❌ اتصال به تلگرام برای حساب `{account_name}` با استفاده از پراکسی و بدون پراکسی با شکست مواجه شد.')
                return
                
            if await user_client.is_user_authorized():
                task = asyncio.create_task(self.run_group_creation_worker(user_id, account_name, user_client))
                self.active_workers[worker_key] = task
                await self._send_accounts_menu(event)
            else:
                # This case might happen if the session is valid but the user has been logged out.
                self._delete_session_file(user_id, account_name)
                self._remove_group_count(worker_key)
                await event.reply(f'⚠️ نشست برای حساب `{account_name}` منقضی شده و حذف شد. لطفا دوباره آن را اضافه کنید.')
        except errors.AuthKeyUnregisteredError:
            LOGGER.error(f"Authentication key for account '{account_name}' is unregistered. Deleting session.")
            self._delete_session_file(user_id, account_name)
            self._remove_group_count(worker_key)
            await event.reply(f"🚨 **خطای امنیتی:** نشست برای حساب `{account_name}` به دلیل استفاده همزمان از چند نقطه، توسط تلگرام باطل شد. این حساب حذف گردید. لطفاً آن را دوباره اضافه کنید.")
            await self._send_accounts_menu(event)
        except Exception as e:
            LOGGER.error(f"Failed to start process for {worker_key}", exc_info=e)
            await event.reply(f'❌ خطایی در اتصال به حساب `{account_name}` رخ داد.')
        finally:
            # FIXED: Ensure client is always disconnected gracefully if it was created but not passed to a worker
            if user_client and not self.active_workers.get(worker_key):
                if user_client.is_connected():
                    await user_client.disconnect()

    async def _cancel_worker_handler(self, event: events.NewMessage.Event, account_name: str) -> None:
        user_id = event.sender_id
        worker_key = f"{user_id}:{account_name}"

        if worker_key in self.active_workers:
            self.active_workers[worker_key].cancel()
            LOGGER.info(f"User initiated cancellation for worker {worker_key}.")
            await self._send_accounts_menu(event)
        else:
            await event.reply(f"ℹ️ هیچ عملیات فعالی برای حساب `{account_name}` جهت توقف وجود ندارد.")

    async def _delete_account_handler(self, event: events.NewMessage.Event, account_name: str) -> None:
        user_id = event.sender_id
        worker_key = f"{user_id}:{account_name}"

        if worker_key in self.active_workers:
            self.active_workers[worker_key].cancel()
            LOGGER.info(f"Worker cancelled for {worker_key} due to account deletion.")

        if self._delete_session_file(user_id, account_name):
            self._remove_group_count(worker_key)
            await event.reply(f"✅ حساب `{account_name}` با موفقیت حذف شد و عملیات مرتبط متوقف گردید.")
        else:
            await event.reply(f"✅ عملیات برای حساب `{account_name}` متوقف شد (نشست از قبل وجود نداشت).")

        await self._send_accounts_menu(event)

    # --- Login Flow Handlers ---
    async def _handle_master_password(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        if event.text.strip() == Config.MASTER_PASSWORD:
            self.user_sessions[user_id] = {'state': 'authenticated'}
            await event.reply(Config.MSG_WELCOME, buttons=self._build_main_menu())
        else:
            await event.reply(Config.MSG_INCORRECT_MASTER_PASSWORD)
        raise events.StopPropagation

    async def _handle_phone_input(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        self.user_sessions[user_id]['phone'] = event.text.strip()
        
        user_client = None
        try:
            user_client = await self._create_new_user_client()
            if not user_client:
                await event.reply('❌ اتصال به تلگرام با استفاده از پراکسی و بدون پراکسی با شکست مواجه شد. لطفا بعدا تلاش کنید.')
                return
                
            self.user_sessions[user_id]['client'] = user_client
            sent_code = await user_client.send_code_request(self.user_sessions[user_id]['phone'])
            self.user_sessions[user_id]['phone_code_hash'] = sent_code.phone_code_hash
            self.user_sessions[user_id]['state'] = 'awaiting_code'
            await event.reply('💬 کد ورود ارسال شد. لطفا آن را اینجا ارسال کنید.', buttons=[[Button.text(Config.BTN_BACK)]])
        except Exception as e:
            LOGGER.error(f"Phone input error for {user_id}", exc_info=e)
            self.user_sessions[user_id]['state'] = 'awaiting_phone' 
            await event.reply(
                '❌ **خطا:** شماره تلفن نامعتبر است یا مشکلی در ارسال کد رخ داد. لطفا دوباره با فرمت بین‌المللی (+کد کشور) تلاش کنید یا عملیات را لغو کنید.',
                buttons=[[Button.text(Config.BTN_BACK)]]
            )
        finally:
            # FIXED: Ensure client is always disconnected gracefully if not passed to the next stage
            if user_client and self.user_sessions.get(user_id, {}).get('state') != 'awaiting_code':
                 if user_client.is_connected():
                    await user_client.disconnect()

    async def _handle_code_input(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        user_client = self.user_sessions[user_id]['client']
        try:
            await user_client.sign_in(self.user_sessions[user_id]['phone'], code=event.text.strip(), phone_code_hash=self.user_sessions[user_id].get('phone_code_hash'))
            self.user_sessions[user_id]['state'] = 'awaiting_account_name'
            await event.reply('✅ ورود موفق! لطفاً یک نام مستعار برای این حساب وارد کنید (مثلا: `حساب اصلی` یا `شماره دوم`).', buttons=[[Button.text(Config.BTN_BACK)]])
        except errors.SessionPasswordNeededError:
            self.user_sessions[user_id]['state'] = 'awaiting_password'
            await event.reply('🔑 این حساب تایید دو مرحله‌ای دارد. لطفا رمز عبور را ارسال کنید.', buttons=[[Button.text(Config.BTN_BACK)]])
        except Exception as e:
            LOGGER.error(f"Code input error for {user_id}", exc_info=e)
            self.user_sessions[user_id]['state'] = 'awaiting_phone'
            await event.reply('❌ **خطا:** کد وارد شده نامعتبر است. لطفا شماره تلفن را مجددا وارد کنید.', buttons=[[Button.text(Config.BTN_BACK)]])

    async def _handle_password_input(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        try:
            await self.user_sessions[user_id]['client'].sign_in(password=event.text.strip())
            self.user_sessions[user_id]['state'] = 'awaiting_account_name'
            await event.reply('✅ ورود موفق! لطفاً یک نام مستعار برای این حساب وارد کنید (مثلا: `حساب اصلی` یا `شماره دوم`).', buttons=[[Button.text(Config.BTN_BACK)]])
        except Exception as e:
            LOGGER.error(f"Password input error for {user_id}", exc_info=e)
            self.user_sessions[user_id]['state'] = 'awaiting_password'
            await event.reply('❌ **خطا:** رمز عبور اشتباه است. لطفا دوباره تلاش کنید.', buttons=[[Button.text(Config.BTN_BACK)]])

    async def _handle_account_name_input(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        account_name = event.text.strip()
        if not account_name:
            await event.reply("❌ نام مستعار نمی‌تواند خالی باشد. لطفا یک نام وارد کنید.", buttons=[[Button.text(Config.BTN_BACK)]])
            return

        if account_name in self._get_user_accounts(user_id):
            await event.reply(f"❌ شما قبلا حسابی با نام `{account_name}` اضافه کرده‌اید. لطفا یک نام دیگر انتخاب کنید.", buttons=[[Button.text(Config.BTN_BACK)]])
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
        LOGGER.info("Starting bot...")
        try:
            await self.bot.start(bot_token=BOT_TOKEN)
            LOGGER.info("Bot service has started successfully.")
            await self.bot.run_until_disconnected()
        finally:
            LOGGER.info("Bot service is shutting down. Disconnecting main bot client.")
            if self.bot.is_connected():
                await self.bot.disconnect()

if __name__ == "__main__":
    bot_instance = GroupCreatorBot()
    asyncio.run(bot_instance.run())
