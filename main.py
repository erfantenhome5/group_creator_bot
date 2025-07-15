import asyncio
import json
import logging
import os
import random
import re
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
    level=logging.INFO,
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
    PROXY_FILE = "proxy10"
    PROXY_TIMEOUT = 2

    # --- UI Text & Buttons ---
    # Main Menu
    BTN_MANAGE_ACCOUNTS = "👤 مدیریت حساب‌ها"
    BTN_SERVER_STATUS = "📊 وضعیت سرور"
    BTN_HELP = "ℹ️ راهنما"
    BTN_TEST_PROXIES = "🧪 تست پراکسی‌ها"

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
                        ip, port, user, pw = line.split(':')
                        proxy_list.append({
                            'proxy_type': 'http',
                            'addr': ip,
                            'port': int(port),
                            'username': user,
                            'password': pw
                        })
                    except ValueError:
                        LOGGER.warning(f"Skipping malformed proxy line: {line}")
            LOGGER.info(f"Loaded {len(proxy_list)} proxies.")
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

    async def _create_new_user_client(self, session_string: Optional[str] = None, is_test: bool = False) -> Optional[TelegramClient]:
        """Tries to connect with a proxy, falls back to no proxy."""
        session = StringSession(session_string) if session_string else StringSession()
        device_params = random.choice([{'device_model': 'iPhone 14 Pro Max', 'system_version': '17.5.1'}, {'device_model': 'Samsung Galaxy S24 Ultra', 'system_version': 'SDK 34'}])

        # Shuffle proxies to not always try in the same order
        shuffled_proxies = self.proxies.copy()
        random.shuffle(shuffled_proxies)

        for proxy in shuffled_proxies:
            try:
                if not is_test: LOGGER.info(f"Attempting to connect with proxy: {proxy['addr']}")
                client = TelegramClient(session, API_ID, API_HASH, proxy=proxy, timeout=Config.PROXY_TIMEOUT, **device_params)
                await client.connect()
                if not is_test: LOGGER.info(f"Successfully connected using proxy: {proxy['addr']}")
                return client
            except Exception as e:
                if not is_test: LOGGER.warning(f"Failed to connect with proxy {proxy['addr']}: {e}")
                continue

        if not is_test: LOGGER.warning("All proxies failed. Attempting to connect without a proxy...")
        try:
            client = TelegramClient(session, API_ID, API_HASH, timeout=Config.PROXY_TIMEOUT, **device_params)
            await client.connect()
            if not is_test: LOGGER.info("Successfully connected without a proxy.")
            return client
        except Exception as e:
            if not is_test: LOGGER.error(f"Failed to connect without a proxy: {e}")
            return None


    # --- Dynamic UI Builder ---
    def _build_main_menu(self) -> List[List[Button]]:
        return [
            [Button.text(Config.BTN_MANAGE_ACCOUNTS)],
            [Button.text(Config.BTN_SERVER_STATUS), Button.text(Config.BTN_HELP)],
            [Button.text(Config.BTN_TEST_PROXIES)],
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

                # Calculate and inform the user about the estimated time
                avg_sleep = (Config.MIN_SLEEP_SECONDS + Config.MAX_SLEEP_SECONDS) / 2
                estimated_total_minutes = (Config.GROUPS_TO_CREATE * avg_sleep) / 60
                
                current_semester = self._get_group_count(worker_key)

                await self.bot.send_message(user_id, f"✅ **عملیات برای حساب `{account_name}` آغاز شد!**\n\n⏳ تخمین زمان کل عملیات: حدود {estimated_total_minutes:.0f} دقیقه.")

                for i in range(Config.GROUPS_TO_CREATE):
                    # Create a group, then wait
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
                            current_semester -= 1 # Roll back count since it failed
                            continue

                        # If successful, save the new count
                        self._set_group_count(worker_key, current_semester)
                        
                        groups_made = i + 1
                        groups_remaining = Config.GROUPS_TO_CREATE - groups_made
                        time_remaining_minutes = (groups_remaining * avg_sleep) / 60

                        # Updated progress message without the invite link
                        progress_message = (
                            f"📊 [{account_name}] گروه '{group_title}' ساخته شد. ({groups_made}/{Config.GROUPS_TO_CREATE})\n"
                            f"⏳ زمان تقریبی باقی‌مانده: {time_remaining_minutes:.0f} دقیقه."
                        )
                        await self.bot.send_message(user_id, progress_message)

                        # Wait for a random time between 1 and 4 minutes AFTER creating the group
                        sleep_time = random.randint(Config.MIN_SLEEP_SECONDS, Config.MAX_SLEEP_SECONDS)
                        await asyncio.sleep(sleep_time)

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
            if user_client.is_connected():
                await user_client.disconnect()


    async def on_login_success(self, event: events.NewMessage.Event, user_client: TelegramClient) -> None:
        user_id = event.sender_id
        account_name = self.user_sessions[user_id]['account_name']
        self._save_session_string(user_id, account_name, user_client.session.save())

        if 'client' in self.user_sessions[user_id]:
            del self.user_sessions[user_id]['client']
        self.user_sessions[user_id]['state'] = 'authenticated' # Set state to authenticated

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

    async def _test_proxies_handler(self, event: events.NewMessage.Event) -> None:
        """Handles the proxy test command."""
        msg = await event.reply("🧪 **شروع تست پراکسی‌ها...**\n\nلطفا صبر کنید، این عملیات ممکن است کمی طول بکشد.")
        
        results = "📝 **نتایج تست پراکسی:**\n\n"
        
        if not self.proxies:
            results += "⚠️ هیچ پراکسی‌ای در فایل `proxy10` یافت نشد.\n"

        for proxy in self.proxies:
            proxy_addr = f"{proxy['addr']}:{proxy['port']}"
            client = None
            try:
                # Use a temporary in-memory session for testing
                client = TelegramClient(StringSession(), API_ID, API_HASH, proxy=proxy, timeout=Config.PROXY_TIMEOUT)
                await client.connect()
                if await client.is_connected():
                    results += f"✅ `{proxy_addr}`: **موفق**\n"
                else:
                    results += f"❌ `{proxy_addr}`: **ناموفق (خطای اتصال)**\n"
            except Exception as e:
                error_type = type(e).__name__
                results += f"❌ `{proxy_addr}`: **ناموفق** ({error_type})\n"
            finally:
                if client and client.is_connected():
                    await client.disconnect()

        # Test direct connection
        results += "\n---\n**تست اتصال مستقیم (بدون پراکسی):**\n"
        client = None
        try:
            client = TelegramClient(StringSession(), API_ID, API_HASH, timeout=Config.PROXY_TIMEOUT)
            await client.connect()
            if await client.is_connected():
                results += "✅ **موفق**\n"
            else:
                results += "❌ **ناموفق (خطای اتصال)**\n"
        except Exception as e:
            error_type = type(e).__name__
            results += f"❌ **ناموفق** ({error_type})\n"
        finally:
            if client and client.is_connected():
                await client.disconnect()
                
        await msg.edit(results)
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
            Config.BTN_TEST_PROXIES: self._test_proxies_handler,
            "/test_proxies": self._test_proxies_handler,
        }
        
        # Match commands and buttons
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
        user_client = await self._create_new_user_client(session_str)
        
        if not user_client:
            await event.reply(f'❌ اتصال به تلگرام برای حساب `{account_name}` با استفاده از پراکسی و بدون پراکسی با شکست مواجه شد.')
            return
            
        try:
            if await user_client.is_user_authorized():
                task = asyncio.create_task(self.run_group_creation_worker(user_id, account_name, user_client))
                self.active_workers[worker_key] = task
                await self._send_accounts_menu(event)
            else:
                self._delete_session_file(user_id, account_name)
                self._remove_group_count(worker_key)
                await event.reply(f'⚠️ نشست برای حساب `{account_name}` منقضی شده و حذف شد. لطفا دوباره آن را اضافه کنید.')
        except Exception as e:
            LOGGER.error(f"Failed to start process for {worker_key}", exc_info=e)
            await event.reply(f'❌ خطایی در اتصال به حساب `{account_name}` رخ داد.')

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
        
        user_client = await self._create_new_user_client()
        if not user_client:
            await event.reply('❌ اتصال به تلگرام با استفاده از پراکسی و بدون پراکسی با شکست مواجه شد. لطفا بعدا تلاش کنید.')
            return
            
        self.user_sessions[user_id]['client'] = user_client
        try:
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
        await self.bot.start(bot_token=BOT_TOKEN)
        LOGGER.info("Bot service has started successfully.")
        await self.bot.run_until_disconnected()

if __name__ == "__main__":
    bot_instance = GroupCreatorBot()
    asyncio.run(bot_instance.run())
