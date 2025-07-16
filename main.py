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
from sentry_sdk.integrations.logging import LoggingIntegration
from sentry_sdk.types import Event, Hint
from telethon import Button, TelegramClient, errors, events
from telethon.sessions import StringSession
from telethon.tl import functions
from telethon.tl.functions.messages import CreateChatRequest
from telethon.tl.types import Message

from ai_analyzer import AIAnalyzer
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
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
ADMIN_USER_ID = os.getenv("ADMIN_USER_ID")

if not all([API_ID, API_HASH, BOT_TOKEN, ENCRYPTION_KEY]):
    raise ValueError("Missing required environment variables.")

API_ID = int(API_ID)
SESSIONS_DIR = Path("sessions")
SESSIONS_DIR.mkdir(exist_ok=True)


# --- Global Proxy Loading Function ---
def load_proxies_from_file(proxy_file_path: str) -> List[Dict]:
    proxy_list = []
    try:
        with open(proxy_file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line: continue
                try:
                    ip, port = line.split(':', 1)
                    proxy_list.append({'proxy_type': 'http', 'addr': ip, 'port': int(port)})
                except ValueError:
                    LOGGER.warning(f"Skipping malformed proxy line: {line}.")
        LOGGER.info(f"Loaded {len(proxy_list)} proxies from {proxy_file_path}.")
    except FileNotFoundError:
        LOGGER.warning(f"Proxy file '{proxy_file_path}' not found.")
    return proxy_list

# --- Centralized Configuration ---
class Config:
    MASTER_PASSWORD = "3935Eerfan@123"
    MAX_CONCURRENT_WORKERS = 5
    GROUPS_TO_CREATE = 50
    MIN_SLEEP_SECONDS = 60
    MAX_SLEEP_SECONDS = 240
    GROUP_MEMBER_TO_ADD = '@BotFather'
    PROXY_FILE = "proxy10.txt"
    PROXY_TIMEOUT = 5 
    BTN_MANAGE_ACCOUNTS = "👤 مدیریت حساب‌ها"
    BTN_SERVER_STATUS = "📊 وضعیت سرور"
    BTN_HELP = "ℹ️ راهنما"
    BTN_ADD_ACCOUNT = "➕ افزودن حساب (API)"
    BTN_ADD_ACCOUNT_SELENIUM = "✨ افزودن حساب (مرورگر امن)"
    BTN_BACK = "⬅️ بازگشت"
    BTN_START_PREFIX = "🟢 شروع برای"
    BTN_STOP_PREFIX = "⏹️ توقف برای"
    BTN_DELETE_PREFIX = "🗑️ حذف"
    MSG_WELCOME = "**🤖 به ربات سازنده گروه خوش آمدید!**"
    MSG_ACCOUNT_MENU_HEADER = "👤 **مدیریت حساب‌ها**"
    MSG_HELP_TEXT = "برای راهنمایی در مورد نحوه استفاده از ربات، با ادمین تماس بگیرید."
    MSG_PROMPT_MASTER_PASSWORD = "🔑 لطفا برای دسترسی به ربات، رمز عبور اصلی را وارد کنید:"
    MSG_INCORRECT_MASTER_PASSWORD = "❌ رمز عبور اشتباه است."
    MSG_BROWSER_RUNNING = "⏳ در حال آماده‌سازی مرورگر امن... این کار ممکن است چند لحظه طول بکشد."

class GroupCreatorBot:
    def __init__(self) -> None:
        self.bot = TelegramClient('bot_session', API_ID, API_HASH)
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
        try:
            fernet = Fernet(ENCRYPTION_KEY.encode())
            self.session_manager = SessionManager(fernet, SESSIONS_DIR)
        except (ValueError, TypeError):
            raise ValueError("Invalid ENCRYPTION_KEY. Please generate a valid key.")
        
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

        sentry_sdk.init(dsn=SENTRY_DSN, before_send=before_send_hook)
        LOGGER.info("Sentry initialized with proactive AI error analysis.")

    def _load_json_file(self, path: Path, default: Any = {}) -> Any:
        if not path.exists(): return default
        try:
            with path.open("r") as f:
                data = json.load(f)
                if isinstance(default, list) and not isinstance(data, list): return []
                if isinstance(default, dict) and not isinstance(data, dict): return {}
                return data
        except (json.JSONDecodeError, IOError):
            return default

    def _save_json_file(self, path: Path, data: Any) -> None:
        try:
            with path.open("w") as f: json.dump(data, f, indent=4)
        except IOError: LOGGER.error(f"Could not save {path.name}.")

    def _get_available_proxy(self) -> Optional[Dict]:
        if not self.proxies: return None
        assigned_proxy_hosts = {p['addr'] for p in self.account_proxies.values() if p}
        for proxy in self.proxies:
            if proxy['addr'] not in assigned_proxy_hosts:
                return proxy
        return None

    async def _create_user_client(self, proxy: Optional[Dict]) -> Optional[TelegramClient]:
        try:
            client = TelegramClient(StringSession(), API_ID, API_HASH, proxy=proxy)
            await client.connect()
            return client
        except Exception as e:
            LOGGER.error(f"Failed to create client: {e}")
            sentry_sdk.capture_exception(e)
            return None

    async def run_group_creation_worker(self, user_id: int, account_name: str, user_client: TelegramClient) -> None:
        worker_key = f"{user_id}:{account_name}"
        try:
            async with self.worker_semaphore:
                LOGGER.info(f"Worker started for {worker_key}.")
                current_semester = self.group_counts.get(worker_key, 0)
                for i in range(Config.GROUPS_TO_CREATE):
                    current_semester += 1
                    group_title = f"collage Semester {current_semester}"
                    try:
                        result = await user_client(CreateChatRequest(users=[Config.GROUP_MEMBER_TO_ADD], title=group_title))
                        chat = result.chats[0]
                        await user_client(functions.channels.TogglePreHistoryHiddenRequest(channel=chat.id, enabled=False))
                        self.group_counts[worker_key] = current_semester
                        self._save_json_file(self.counts_file, self.group_counts)
                        await self.bot.send_message(user_id, f"📊 [{account_name}] گروه '{group_title}' ساخته شد.")
                        await asyncio.sleep(random.randint(Config.MIN_SLEEP_SECONDS, Config.MAX_SLEEP_SECONDS))
                    except errors.FloodWaitError as e:
                        await self.bot.send_message(user_id, f"⏳ [{account_name}] محدودیت تلگرام. {e.seconds} ثانیه صبر می‌کنیم.")
                        await asyncio.sleep(e.seconds)
                    except Exception as e:
                        LOGGER.error(f"Worker error for {worker_key}", exc_info=True)
                        sentry_sdk.capture_exception(e)
                        user_error_message = await self.ai_analyzer.explain_error_for_user(e)
                        await self.bot.send_message(user_id, user_error_message)
                        break
        except asyncio.CancelledError:
            LOGGER.info(f"Task for {worker_key} was cancelled.")
        finally:
            if worker_key in self.active_workers:
                del self.active_workers[worker_key]
                self.active_workers_state.pop(worker_key, None)
                self._save_json_file(self.active_workers_file, self.active_workers_state)
            if user_client.is_connected():
                await user_client.disconnect()
            LOGGER.info(f"Worker finished for {worker_key}.")

    def _build_main_menu(self):
        return [
            [Button.text(Config.BTN_MANAGE_ACCOUNTS)],
            [Button.text(Config.BTN_SERVER_STATUS), Button.text(Config.BTN_HELP)]
        ]

    def _build_accounts_menu(self, user_id: int):
        keyboard = []
        accounts = self.session_manager.get_user_accounts(user_id)
        for acc_name in accounts:
            worker_key = f"{user_id}:{acc_name}"
            if worker_key in self.active_workers:
                keyboard.append([Button.text(f"{Config.BTN_STOP_PREFIX} {acc_name}")])
            else:
                keyboard.append([Button.text(f"{Config.BTN_START_PREFIX} {acc_name}"), Button.text(f"{Config.BTN_DELETE_PREFIX} {acc_name}")])
        keyboard.append([Button.text(Config.BTN_ADD_ACCOUNT), Button.text(Config.BTN_ADD_ACCOUNT_SELENIUM)])
        keyboard.append([Button.text(Config.BTN_BACK)])
        return keyboard

    async def register_handlers(self):
        self.bot.add_event_handler(self._message_router)

    async def _message_router(self, event: events.NewMessage.Event):
        user_id = event.sender_id
        text = event.raw_text

        if user_id not in self.known_users:
            self.known_users.append(user_id)
            self._save_json_file(self.known_users_file, self.known_users)

        session = self.user_sessions.get(user_id, {})
        state = session.get('state')

        if text == '/cancel':
            if 'state' in self.user_sessions.get(user_id, {}):
                del self.user_sessions[user_id]['state']
            await event.reply("✅ عملیات فعلی لغو شد.", buttons=self._build_main_menu())
            return

        if state == 'awaiting_master_password':
            await self._handle_master_password(event)
            return
            
        if state == 'awaiting_refine_prompt':
            await self._handle_refine_prompt(event)
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
            "/refine_code": self._refine_code_handler,
            "/test_self_heal": self._test_self_heal_handler,
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

        if text.startswith(Config.BTN_START_PREFIX):
            account_name = text.replace(f"{Config.BTN_START_PREFIX} ", "")
            await self._start_process_handler(event, account_name)
        elif text.startswith(Config.BTN_STOP_PREFIX):
            account_name = text.replace(f"{Config.BTN_STOP_PREFIX} ", "")
            await self._cancel_worker_handler(event, account_name)
        elif text.startswith(Config.BTN_DELETE_PREFIX):
            account_name = text.replace(f"{Config.BTN_DELETE_PREFIX} ", "")
            await self._delete_account_handler(event, account_name)

    async def _start_handler(self, event):
        user_id = event.sender_id
        if user_id not in self.known_users:
            self.known_users.append(user_id)
            self._save_json_file(self.known_users_file, self.known_users)
        
        session = self.user_sessions.get(user_id, {})
        if session.get('state') == 'authenticated':
            await event.reply(Config.MSG_WELCOME, buttons=self._build_main_menu())
        else:
            self.user_sessions[user_id] = {'state': 'awaiting_master_password'}
            await event.reply(Config.MSG_PROMPT_MASTER_PASSWORD, buttons=ReplyKeyboardRemove())

    async def run(self) -> None:
        await self.register_handlers()
        LOGGER.info("Starting bot...")
        await self.bot.start(bot_token=BOT_TOKEN)
        LOGGER.info("Bot service has started successfully.")
        
        for worker_key, data in list(self.active_workers_state.items()):
            user_id = data['user_id']
            account_name = data['account_name']
            LOGGER.info(f"Resuming worker for {account_name}")
            dummy_event = events.NewMessage.Event(self.bot.build_in_message(user_id))
            await self._start_process_handler(dummy_event, account_name, is_resume=True)
        
        if self.known_users:
            await self._broadcast_message("✅ ربات با موفقیت راه‌اندازی شد و اکنون در دسترس است.")
        
        await idle()
        
        LOGGER.info("Bot service is shutting down.")
        self._save_json_file(self.active_workers_file, self.active_workers_state)
        await self.bot.stop()

    async def _handle_master_password(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        if event.raw_text.strip() == Config.MASTER_PASSWORD:
            self.user_sessions[user_id] = {'state': 'authenticated'}
            await event.reply(Config.MSG_WELCOME, buttons=self._build_main_menu())
        else:
            await event.reply(Config.MSG_INCORRECT_MASTER_PASSWORD)
        raise events.StopPropagation
    
    # ... Other handlers ...

if __name__ == "__main__":
    bot_instance = GroupCreatorBot()
    asyncio.run(bot_instance.run())
