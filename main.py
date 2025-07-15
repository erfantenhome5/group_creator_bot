import asyncio
import logging
import os
import re
import shutil
import random
from pathlib import Path
from typing import Any, Dict, List, Optional

from cryptography.fernet import Fernet, InvalidToken
from dotenv import load_dotenv
from telethon import Button, TelegramClient, errors, events
from telethon.sessions import StringSession
from telethon.tl.functions.messages import CreateChatRequest, ExportChatInviteRequest
from telethon.tl.types import Message

print("--- main.py script starting ---")

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
    MASTER_PASSWORD = "3935Eerfan@123"
    MAX_CONCURRENT_WORKERS = 5
    GROUPS_TO_CREATE = 50
    
    MIN_SLEEP_SECONDS = 288
    MAX_SLEEP_SECONDS = 360

    GROUP_NAME_BASE = "collage Semester"
    GROUP_MEMBER_TO_ADD = '@BotFather'
    
    # --- Device Simulation (User Agent for Telethon) ---
    # This simulates different devices to make API usage look more natural.
    DEVICE_PROFILES = [
        {'device_model': 'iPhone 14 Pro Max', 'system_version': '17.5.1', 'app_version': '10.9.3'},
        {'device_model': 'Samsung Galaxy S24 Ultra', 'system_version': 'SDK 34', 'app_version': '10.9.3'},
        {'device_model': 'Google Pixel 8 Pro', 'system_version': 'SDK 34', 'app_version': '10.9.3'},
        {'device_model': 'Desktop', 'system_version': 'Windows 11', 'app_version': '4.16.8'},
        {'device_model': 'MacBook Pro', 'system_version': 'macOS 14.5', 'app_version': '10.9.3'},
    ]
    
    BTN_MANAGE_ACCOUNTS = "👤 مدیریت حساب‌ها"
    BTN_ADD_ACCOUNT = "➕ افزودن حساب جدید"
    BTN_BACK = "⬅️ بازگشت"
    BTN_START_PREFIX = "🟢 شروع برای"
    BTN_STOP_PREFIX = "⏹️ توقف برای"
    BTN_DELETE_PREFIX = "🗑️ حذف"
    
    METHOD_API = "🚀 API (سریع و سبک)"
    METHOD_SELENIUM = "🛡️ Selenium (ظاهری)" # Label changed to indicate it's for appearance
    
    MSG_WELCOME = "**🤖 به ربات سازنده گروه خوش آمدید!**"
    MSG_ACCOUNT_MENU_HEADER = "👤 **مدیریت حساب‌ها**\n\nاز این منو می‌توانید حساب‌های خود را مدیریت کرده و عملیات ساخت گروه را برای هرکدام آغاز یا متوقف کنید."
    MSG_PROMPT_MASTER_PASSWORD = "🔑 لطفا برای دسترسی به ربات، رمز عبور اصلی را وارد کنید:"
    MSG_INCORRECT_MASTER_PASSWORD = "❌ رمز عبور اشتباه است. لطفا دوباره تلاش کنید."

# --- Environment & Paths ---
load_dotenv()
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")

if not all([API_ID, API_HASH, BOT_TOKEN, ENCRYPTION_KEY]):
    raise ValueError("Missing required environment variables.")

API_ID = int(API_ID)
SESSIONS_DIR = Path("sessions") # Simplified to one session directory
SESSIONS_DIR.mkdir(exist_ok=True)

class GroupCreatorBot:
    def __init__(self) -> None:
        self.bot = TelegramClient('bot_session', API_ID, API_HASH)
        self.user_sessions: Dict[int, Dict[str, Any]] = {}
        self.active_workers: Dict[str, asyncio.Task] = {}
        self.api_semaphore = asyncio.Semaphore(Config.MAX_CONCURRENT_WORKERS)
        self.sessions_lock = asyncio.Lock()
        self.workers_lock = asyncio.Lock()

        try:
            self.fernet = Fernet(ENCRYPTION_KEY.encode())
        except (ValueError, TypeError):
            raise ValueError("Invalid ENCRYPTION_KEY.")

    # --- Account & Session Management ---
    def get_all_accounts(self) -> List[str]:
        """Lists accounts by looking for session directories."""
        if not SESSIONS_DIR.is_dir():
            return []
        return sorted([d.name for d in SESSIONS_DIR.iterdir() if d.is_dir()])

    def _get_session_path(self, account_name: str) -> Path:
        return SESSIONS_DIR / account_name / f"{account_name}.session"

    def _get_counter_path(self, account_name: str) -> Path:
        return SESSIONS_DIR / account_name / "group.counter"

    def _read_counter(self, account_name: str) -> int:
        counter_file = self._get_counter_path(account_name)
        if not counter_file.exists(): return 0
        try: return int(counter_file.read_text())
        except (ValueError, OSError): return 0

    def _write_counter(self, account_name: str, value: int):
        counter_file = self._get_counter_path(account_name)
        try: counter_file.write_text(str(value))
        except OSError as e: LOGGER.error(f"Error writing counter for {account_name}: {e}")
    
    def _delete_account(self, account_name: str) -> bool:
        dir_to_delete = SESSIONS_DIR / account_name
        if not dir_to_delete.exists(): return False
        try:
            shutil.rmtree(dir_to_delete)
            LOGGER.info(f"Deleted directory for account '{account_name}'.")
            return True
        except OSError as e:
            LOGGER.error(f"Error deleting directory for '{account_name}': {e}")
            return False

    # --- UI Builder ---
    def _build_main_menu(self) -> List[List[Button]]:
        return [[Button.text(Config.BTN_MANAGE_ACCOUNTS)], [Button.text(Config.BTN_BACK)]]

    def _build_accounts_menu(self, user_id: int) -> List[List[Button]]:
        accounts = self.get_all_accounts()
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

        keyboard.append([Button.text(Config.BTN_ADD_ACCOUNT)])
        keyboard.append([Button.text(Config.BTN_BACK)])
        return keyboard

    # --- Worker Task ---
    async def run_group_creation_worker(self, user_id: int, account_name: str):
        worker_key = f"{user_id}:{account_name}"
        session_path = self._get_session_path(account_name)
        
        # --- Select a random device profile for this worker ---
        selected_device = random.choice(Config.DEVICE_PROFILES)
        user_client = TelegramClient(
            str(session_path), 
            API_ID, 
            API_HASH,
            device_model=selected_device.get('device_model'),
            system_version=selected_device.get('system_version'),
            app_version=selected_device.get('app_version')
        )
        
        try:
            async with self.api_semaphore:
                await user_client.connect()
                if not await user_client.is_user_authorized():
                    await self.bot.send_message(user_id, f"⚠️ نشست برای حساب `{account_name}` منقضی شده. لطفا حذف و دوباره اضافه کنید.")
                    return

                await self.bot.send_message(user_id, f"✅ عملیات برای `{account_name}` آغاز شد.")
                current_counter = self._read_counter(account_name)
                
                for i in range(Config.GROUPS_TO_CREATE):
                    current_counter += 1
                    group_title = f"{Config.GROUP_NAME_BASE} {current_counter}"
                    try:
                        await user_client(CreateChatRequest(users=[Config.GROUP_MEMBER_TO_ADD], title=group_title))
                        self._write_counter(account_name, current_counter)
                        wait_time = random.randint(Config.MIN_SLEEP_SECONDS, Config.MAX_SLEEP_SECONDS)
                        await self.bot.send_message(user_id, f"✅ [{account_name}] گروه '{group_title}' ساخته شد. در حال انتظار برای {wait_time // 60} دقیقه و {wait_time % 60} ثانیه...")
                        await asyncio.sleep(wait_time)
                    except Exception as e:
                        await self.bot.send_message(user_id, f"❌ [{account_name}] خطایی در ساخت گروه رخ داد: {e}")
                        break
        except asyncio.CancelledError:
            await self.bot.send_message(user_id, f"⏹️ عملیات برای `{account_name}` متوقف شد.")
        finally:
            if user_client.is_connected(): await user_client.disconnect()
            async with self.workers_lock:
                if worker_key in self.active_workers: del self.active_workers[worker_key]
            LOGGER.info(f"Worker finished for {worker_key}.")

    # --- Bot Handlers & State Machine ---
    async def _start_handler(self, event):
        async with self.sessions_lock:
            self.user_sessions[event.sender_id] = {'state': 'awaiting_master_password'}
        await event.reply(Config.MSG_PROMPT_MASTER_PASSWORD)
        raise events.StopPropagation

    async def _send_main_menu(self, event):
        async with self.sessions_lock:
            self.user_sessions[event.sender_id] = {'state': 'authenticated'}
        await event.reply(Config.MSG_WELCOME, buttons=self._build_main_menu())

    async def _send_accounts_menu(self, event):
        async with self.sessions_lock:
            self.user_sessions[event.sender_id] = {'state': 'manage_accounts'}
        await event.reply(Config.MSG_ACCOUNT_MENU_HEADER, buttons=self._build_accounts_menu(event.sender_id))

    async def _initiate_add_account_flow(self, event):
        buttons = [
            [Button.text(Config.METHOD_API)],
            [Button.text(Config.METHOD_SELENIUM)],
            [Button.text(Config.BTN_BACK)]
        ]
        async with self.sessions_lock:
            self.user_sessions[event.sender_id]['state'] = 'awaiting_add_method'
        await event.reply("لطفا روش افزودن حساب را انتخاب کنید:", buttons=buttons)

    async def _handle_login_flow(self, event, text):
        user_id = event.sender_id
        async with self.sessions_lock:
            state = self.user_sessions[user_id].get('sub_state')

        if state == 'awaiting_name':
            account_name = text.strip()
            if not re.match("^[a-zA-Z0-9_-]+$", account_name) or (SESSIONS_DIR / account_name).exists():
                await event.reply("❌ نام نامعتبر است یا از قبل وجود دارد. لطفا نام دیگری انتخاب کنید.")
                return
            (SESSIONS_DIR / account_name).mkdir(exist_ok=True)
            async with self.sessions_lock:
                self.user_sessions[user_id]['account_name'] = account_name
                self.user_sessions[user_id]['sub_state'] = 'awaiting_phone'
            await event.reply("لطفا شماره تلفن را با فرمت بین‌المللی ارسال کنید:")
        
        elif state == 'awaiting_phone':
            phone = text.strip()
            client = TelegramClient(StringSession(), API_ID, API_HASH)
            async with self.sessions_lock:
                self.user_sessions[user_id]['phone'] = phone
                self.user_sessions[user_id]['client'] = client
            try:
                await client.connect()
                sent_code = await client.send_code_request(phone)
                async with self.sessions_lock:
                    self.user_sessions[user_id]['phone_code_hash'] = sent_code.phone_code_hash
                    self.user_sessions[user_id]['sub_state'] = 'awaiting_code'
                await event.reply("کد ارسال شده را وارد کنید:")
            except Exception as e:
                await event.reply(f"❌ خطایی در ارسال کد رخ داد: {e}. لطفا دوباره تلاش کنید.")
                async with self.sessions_lock:
                    self.user_sessions[user_id]['sub_state'] = 'awaiting_phone'

        elif state == 'awaiting_code':
            async with self.sessions_lock:
                client = self.user_sessions[user_id]['client']
                phone = self.user_sessions[user_id]['phone']
                phone_code_hash = self.user_sessions[user_id]['phone_code_hash']
            try:
                await client.sign_in(phone, text.strip(), phone_code_hash=phone_code_hash)
                await self._finalize_login(event)
            except errors.SessionPasswordNeededError:
                async with self.sessions_lock:
                    self.user_sessions[user_id]['sub_state'] = 'awaiting_password'
                await event.reply("این حساب تایید دو مرحله‌ای دارد. لطفا رمز عبور را ارسال کنید:")
            except Exception as e:
                await event.reply(f"❌ کد نامعتبر است: {e}. لطفا دوباره تلاش کنید.")

        elif state == 'awaiting_password':
            async with self.sessions_lock:
                client = self.user_sessions[user_id]['client']
            try:
                await client.sign_in(password=text.strip())
                await self._finalize_login(event)
            except Exception as e:
                await event.reply(f"❌ رمز عبور اشتباه است: {e}. لطفا دوباره تلاش کنید.")

    async def _finalize_login(self, event):
        user_id = event.sender_id
        async with self.sessions_lock:
            client = self.user_sessions[user_id]['client']
            account_name = self.user_sessions[user_id]['account_name']
        
        session_path = self._get_session_path(account_name)
        with open(session_path, "w") as f:
            f.write(client.session.save())
        await client.disconnect()
        await event.reply(f"✅ حساب `{account_name}` با موفقیت اضافه شد.")
        await self._send_accounts_menu(event)

    async def _message_router(self, event: events.NewMessage.Event):
        user_id = event.sender_id
        text = event.text.strip()
        
        async with self.sessions_lock:
            session = self.user_sessions.get(user_id, {})
            state = session.get('state')
        
        if state not in ['authenticated', 'manage_accounts', 'awaiting_add_method', 'adding_account']:
            if text == Config.MASTER_PASSWORD:
                await self._send_main_menu(event)
            else:
                if state == 'awaiting_master_password':
                    await event.reply(Config.MSG_INCORRECT_MASTER_PASSWORD)
                await self._start_handler(event)
            return

        if text == Config.BTN_BACK:
            await self._send_main_menu(event)
            return

        if state == 'authenticated':
            if text == Config.BTN_MANAGE_ACCOUNTS:
                await self._send_accounts_menu(event)
            return

        if state == 'manage_accounts':
            if text == Config.BTN_ADD_ACCOUNT:
                await self._initiate_add_account_flow(event)
            elif text.startswith(Config.BTN_START_PREFIX):
                acc_name = text.split(' ')[2]
                worker_key = f"{user_id}:{acc_name}"
                task = asyncio.create_task(self.run_group_creation_worker(user_id, acc_name))
                async with self.workers_lock:
                    self.active_workers[worker_key] = task
                await event.reply(f"🚀 عملیات برای `{acc_name}` آغاز شد.")
                await self._send_accounts_menu(event)
            elif text.startswith(Config.BTN_STOP_PREFIX):
                acc_name = text.split(' ')[2]
                worker_key = f"{user_id}:{acc_name}"
                async with self.workers_lock:
                    if worker_key in self.active_workers:
                        self.active_workers[worker_key].cancel()
                        await event.reply(f"⏹️ در حال توقف عملیات برای `{acc_name}`.")
                await self._send_accounts_menu(event)
            elif text.startswith(Config.BTN_DELETE_PREFIX):
                acc_name = text.split(' ')[2]
                if self._delete_account(acc_name):
                    await event.reply(f"🗑️ حساب `{acc_name}` حذف شد.")
                await self._send_accounts_menu(event)
            return

        if state == 'awaiting_add_method':
            if text in [Config.METHOD_API, Config.METHOD_SELENIUM]:
                async with self.sessions_lock:
                    self.user_sessions[user_id] = {'state': 'adding_account', 'sub_state': 'awaiting_name'}
                await event.reply("یک نام مستعار برای حساب وارد کنید (فقط حروف انگلیسی و اعداد):")
            return

        if state == 'adding_account':
            await self._handle_login_flow(event, text)
            return

    async def run(self):
        try:
            await self.bot.start(bot_token=BOT_TOKEN)
            self.bot.add_event_handler(self._start_handler, events.NewMessage(pattern='/start'))
            self.bot.add_event_handler(self._message_router, events.NewMessage)
            LOGGER.info("Bot service has started successfully and is now running.")
            await self.bot.run_until_disconnected()
        except Exception as e:
            LOGGER.critical(f"FATAL ERROR in bot.run(): {e}", exc_info=True)
            raise

if __name__ == "__main__":
    try:
        bot = GroupCreatorBot()
        asyncio.run(bot.run())
    except Exception as e:
        LOGGER.critical(f"FATAL ERROR in main block: {e}", exc_info=True)
