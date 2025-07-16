import asyncio
import hashlib
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

import sentry_sdk
from cryptography.fernet import Fernet, InvalidToken
from dotenv import load_dotenv
from pyrogram import Client, filters, idle
from pyrogram.errors import (AuthKeyUnregistered, FloodWait, InputUserDeactivated,
                             PhoneCodeExpired, SessionPasswordNeeded, UserIsBlocked)
from pyrogram.handlers import MessageHandler
from pyrogram.raw import functions
from pyrogram.types import (KeyboardButton, Message, ReplyKeyboardMarkup,
                            ReplyKeyboardRemove)
from sentry_sdk.integrations.logging import LoggingIntegration
from sentry_sdk.types import Event, Hint

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
MASTER_PASSWORD_HASH = os.getenv("MASTER_PASSWORD_HASH")

if not all([API_ID, API_HASH, BOT_TOKEN, ENCRYPTION_KEY]):
    raise ValueError("Missing required environment variables.")

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
                        "scheme": "http",
                        "hostname": ip,
                        "port": int(port)
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
    MAX_CONCURRENT_WORKERS = 5
    GROUPS_TO_CREATE = 50
    MIN_SLEEP_SECONDS = 60
    MAX_SLEEP_SECONDS = 240
    GROUP_MEMBER_TO_ADD = 'BotFather'
    PROXY_FILE = "proxy10.txt"
    # --- UI Text & Buttons ---
    BTN_MANAGE_ACCOUNTS = "👤 مدیریت حساب‌ها"
    BTN_SERVER_STATUS = "📊 وضعیت سرور"
    BTN_HELP = "ℹ️ راهنما"
    BTN_ADD_ACCOUNT = "➕ افزودن حساب"
    BTN_BACK = "⬅️ بازگشت"
    BTN_START_PREFIX = "🟢 شروع برای"
    BTN_STOP_PREFIX = "⏹️ توقف برای"
    BTN_DELETE_PREFIX = "🗑️ حذف"
    # --- Messages ---
    MSG_WELCOME = "**🤖 به ربات سازنده گروه خوش آمدید!**"
    MSG_ACCOUNT_MENU_HEADER = "👤 **مدیریت حساب‌ها**"
    MSG_HELP_TEXT = "برای راهنمایی در مورد نحوه استفاده از ربات، با ادمین تماس بگیرید."
    MSG_PROMPT_MASTER_PASSWORD = "🔑 لطفا برای دسترسی به ربات، رمز عبور اصلی را وارد کنید:"
    MSG_INCORRECT_MASTER_PASSWORD = "❌ رمز عبور اشتباه است."

class GroupCreatorBot:
    """A class to encapsulate the bot's logic for managing multiple accounts."""

    def __init__(self) -> None:
        """Initializes the bot instance and the encryption engine."""
        self.bot = Client("bot_session", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, workdir=str(SESSIONS_DIR))
        self.user_sessions: Dict[int, Dict[str, Any]] = {} 
        self.active_workers: Dict[str, asyncio.Task] = {}  
        self.worker_semaphore = asyncio.Semaphore(Config.MAX_CONCURRENT_WORKERS)
        self.counts_file = SESSIONS_DIR / "group_counts.json"
        self.group_counts = self._load_json_file(self.counts_file)
        self.proxies = load_proxies_from_file(Config.PROXY_FILE)
        self.account_proxy_file = SESSIONS_DIR / "account_proxies.json"
        self.account_proxies = self._load_json_file(self.account_proxy_file)
        self.known_users_file = SESSIONS_DIR / "known_users.json"
        self.known_users = self._load_json_file(self.known_users_file, default=[])
        self.active_workers_file = SESSIONS_DIR / "active_workers.json"
        self.active_workers_state = self._load_json_file(self.active_workers_file)
        
        self.ai_analyzer = AIAnalyzer(self)
        self._initialize_sentry()

    def _initialize_sentry(self):
        if not SENTRY_DSN: return

        def before_send_hook(event: Event, hint: Hint) -> Optional[Event]:
            is_test_error = event.get('tags', {}).get('test_error') == 'true'
            if 'exc_info' in hint and not is_test_error:
                exc_type, exc_value, tb = hint['exc_info']
                asyncio.create_task(self.ai_analyzer.analyze_and_apply_fix(exc_type, exc_value, tb))
            return event

        sentry_logging = LoggingIntegration(level=logging.INFO, event_level=logging.ERROR)
        sentry_sdk.init(dsn=SENTRY_DSN, integrations=[sentry_logging], before_send=before_send_hook)
        LOGGER.info("Sentry initialized.")

    def _load_json_file(self, path: Path, default: Any = {}) -> Any:
        if not path.exists(): return default
        try:
            with path.open("r") as f:
                data = json.load(f)
                if isinstance(default, list) and not isinstance(data, list): return []
                if isinstance(default, dict) and not isinstance(data, dict): return {}
                return data
        except (json.JSONDecodeError, IOError):
            LOGGER.error(f"Could not read or parse {path.name}. Starting with empty data.")
            return default

    def _save_json_file(self, path: Path, data: Any) -> None:
        try:
            with path.open("w") as f: json.dump(data, f, indent=4)
        except IOError: LOGGER.error(f"Could not save {path.name}.")

    def _get_available_proxy(self) -> Optional[Dict]:
        if not self.proxies: return None
        assigned_proxy_hosts = {p['hostname'] for p in self.account_proxies.values() if p}
        for proxy in self.proxies:
            if proxy['hostname'] not in assigned_proxy_hosts:
                return proxy
        return None

    def get_user_accounts(self, user_id: int) -> List[str]:
        accounts = []
        for f in SESSIONS_DIR.glob(f"user_{user_id}_*.session"):
            match = re.search(f"user_{user_id}__(.*)\\.session", f.name)
            if match: accounts.append(match.group(1))
        return sorted(accounts)

    def delete_session_file(self, user_id: int, account_name: str) -> bool:
        session_name = self._get_session_name(user_id, account_name)
        session_path = SESSIONS_DIR / f"{session_name}.session"
        if session_path.exists():
            try:
                session_path.unlink()
                LOGGER.info(f"Deleted session file for {account_name}.")
                return True
            except OSError as e:
                LOGGER.error(f"Error deleting session file for {account_name}: {e}")
        return False

    def _get_session_name(self, user_id: int, account_name: str) -> str:
        safe_name = re.sub(r'[^a-zA-Z0-9_-]', '', account_name)
        return f"user_{user_id}__{safe_name}"

    async def _create_user_client(self, session_name: str, proxy: Optional[Dict]) -> Optional[Client]:
        try:
            client = Client(session_name, api_id=API_ID, api_hash=API_HASH, workdir=str(SESSIONS_DIR), proxy=proxy)
            await client.start()
            return client
        except Exception as e:
            LOGGER.error(f"Failed to create client for {session_name}: {e}")
            sentry_sdk.capture_exception(e)
            return None

    async def run_group_creation_worker(self, user_id: int, account_name: str, user_client: Client) -> None:
        worker_key = f"{user_id}:{account_name}"
        try:
            async with self.worker_semaphore:
                LOGGER.info(f"Worker started for {worker_key}.")
                current_semester = self.group_counts.get(worker_key, 0)
                for i in range(Config.GROUPS_TO_CREATE):
                    current_semester += 1
                    group_title = f"collage Semester {current_semester}"
                    try:
                        new_chat = await user_client.create_supergroup(group_title)
                        await user_client.invoke(functions.channels.TogglePreHistoryHidden(channel=await user_client.resolve_peer(new_chat.id), enabled=False))
                        self.group_counts[worker_key] = current_semester
                        self._save_json_file(self.counts_file, self.group_counts)
                        await self.bot.send_message(user_id, f"📊 [{account_name}] گروه '{group_title}' ساخته شد.")
                        await asyncio.sleep(random.randint(Config.MIN_SLEEP_SECONDS, Config.MAX_SLEEP_SECONDS))
                    except FloodWait as e:
                        await self.bot.send_message(user_id, f"⏳ [{account_name}] محدودیت تلگرام. {e.value} ثانیه صبر می‌کنیم.")
                        await asyncio.sleep(e.value)
                    except Exception as e:
                        LOGGER.error(f"Worker error for {worker_key}", exc_info=True)
                        sentry_sdk.capture_exception(e)
                        await self.bot.send_message(user_id, f"❌ [{account_name}] خطای غیرمنتظره: {type(e).__name__}")
                        break
        except asyncio.CancelledError:
            LOGGER.info(f"Task for {worker_key} was cancelled.")
        finally:
            if worker_key in self.active_workers:
                del self.active_workers[worker_key]
                self.active_workers_state.pop(worker_key, None)
                self._save_json_file(self.active_workers_file, self.active_workers_state)
            if user_client.is_connected:
                await user_client.stop()
            LOGGER.info(f"Worker finished for {worker_key}.")

    def _build_main_menu(self):
        return ReplyKeyboardMarkup([[KeyboardButton(Config.BTN_MANAGE_ACCOUNTS)], [KeyboardButton(Config.BTN_SERVER_STATUS), KeyboardButton(Config.BTN_HELP)]], resize_keyboard=True)

    def _build_accounts_menu(self, user_id: int):
        keyboard = []
        accounts = self.get_user_accounts(user_id)
        for acc_name in accounts:
            worker_key = f"{user_id}:{acc_name}"
            if worker_key in self.active_workers:
                keyboard.append([KeyboardButton(f"{Config.BTN_STOP_PREFIX} {acc_name}")])
            else:
                keyboard.append([KeyboardButton(f"{Config.BTN_START_PREFIX} {acc_name}"), KeyboardButton(f"{Config.BTN_DELETE_PREFIX} {acc_name}")])
        keyboard.append([KeyboardButton(Config.BTN_ADD_ACCOUNT)])
        keyboard.append([KeyboardButton(Config.BTN_BACK)])
        return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

    async def register_handlers(self):
        self.bot.add_handler(MessageHandler(self._message_router))

    async def _message_router(self, client, message):
        user_id = message.from_user.id
        text = message.text

        if user_id not in self.known_users:
            self.known_users.append(user_id)
            self._save_json_file(self.known_users_file, self.known_users)

        session = self.user_sessions.get(user_id, {})
        state = session.get('state')

        if state == 'awaiting_master_password':
            await self._handle_master_password(client, message)
            return

        if state != 'authenticated':
            self.user_sessions[user_id] = {'state': 'awaiting_master_password'}
            await message.reply(Config.MSG_PROMPT_MASTER_PASSWORD, reply_markup=ReplyKeyboardRemove())
            return
        
        # ... Other state handlers for login flow ...
        
        if text == Config.BTN_MANAGE_ACCOUNTS:
            await message.reply(Config.MSG_ACCOUNT_MENU_HEADER, reply_markup=self._build_accounts_menu(user_id))
        elif text == Config.BTN_ADD_ACCOUNT:
            self.user_sessions[user_id]['state'] = 'awaiting_phone'
            await message.reply("📞 لطفا شماره تلفن را وارد کنید:", reply_markup=ReplyKeyboardRemove())
        elif text == Config.BTN_BACK:
            await message.reply("منوی اصلی:", reply_markup=self._build_main_menu())
        elif text.startswith(Config.BTN_START_PREFIX):
            account_name = text.replace(f"{Config.BTN_START_PREFIX} ", "")
            await self._start_process_handler(client, message, account_name)
        # ... other button handlers ...

    async def _handle_master_password(self, client, message):
        user_id = message.from_user.id
        if hashlib.sha256(message.text.strip().encode()).hexdigest() == MASTER_PASSWORD_HASH:
            self.user_sessions[user_id] = {'state': 'authenticated'}
            await message.reply(Config.MSG_WELCOME, reply_markup=self._build_main_menu())
        else:
            await message.reply(Config.MSG_INCORRECT_MASTER_PASSWORD)

    async def _start_process_handler(self, client, message, account_name, is_resume=False):
        user_id = message.from_user.id if message else self.active_workers_state[f"{ADMIN_USER_ID}:{account_name}"]["user_id"]
        worker_key = f"{user_id}:{account_name}"

        if worker_key in self.active_workers:
            if not is_resume: await message.reply('⏳ عملیات برای این حساب در حال اجراست.')
            return

        session_name = self._get_session_name(user_id, account_name)
        if not (SESSIONS_DIR / f"{session_name}.session").exists():
            if not is_resume: await message.reply('❌ نشست برای این حساب یافت نشد.')
            return
        
        if not is_resume:
            await message.reply(f'🚀 در حال آماده‌سازی برای شروع عملیات حساب `{account_name}`...')
        
        assigned_proxy = self.account_proxies.get(worker_key)
        user_client = await self._create_user_client(session_name, assigned_proxy)
        
        if user_client:
            task = asyncio.create_task(self.run_group_creation_worker(user_id, account_name, user_client))
            self.active_workers[worker_key] = task
            self.active_workers_state[worker_key] = {"user_id": user_id, "account_name": account_name}
            self._save_json_file(self.active_workers_file, self.active_workers_state)
            if not is_resume: await message.reply("✅ عملیات شروع شد.", reply_markup=self._build_accounts_menu(user_id))
        else:
            if not is_resume: await message.reply(f'❌ اتصال به حساب `{account_name}` ناموفق بود.')

    async def run(self) -> None:
        await self.register_handlers()
        LOGGER.info("Starting bot...")
        await self.bot.start()
        LOGGER.info("Bot service has started successfully.")
        
        # Resume workers
        for worker_key, data in list(self.active_workers_state.items()):
            user_id = data['user_id']
            account_name = data['account_name']
            LOGGER.info(f"Resuming worker for {account_name}")
            await self._start_process_handler(None, account_name, is_resume=True)
        
        if self.known_users:
            await self._broadcast_message("✅ ربات با موفقیت راه‌اندازی شد و اکنون در دسترس است.")
        
        await idle()
        
        LOGGER.info("Bot service is shutting down.")
        self._save_json_file(self.active_workers_file, self.active_workers_state)
        await self.bot.stop()

if __name__ == "__main__":
    bot_instance = GroupCreatorBot()
    asyncio.run(bot_instance.run())
