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

# Import Selenium client only if the file exists
try:
    from selenium_client import SeleniumClient
    SELENIUM_ENABLED = True
except ImportError:
    SELENIUM_ENABLED = False
    print("WARNING: selenium_client.py not found. Selenium method will be disabled.")


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

# --- Helper function to load from text files ---
def load_from_file(filename: str) -> List[str]:
    """Loads lines from a text file, stripping whitespace and ignoring empty lines."""
    try:
        with open(filename, 'r') as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        LOGGER.warning(f"File not found: {filename}. Proceeding with an empty list.")
        return []

# --- Centralized Configuration ---
class Config:
    MASTER_PASSWORD = "3935Eerfan@123"
    MAX_CONCURRENT_API_WORKERS = 5
    MAX_CONCURRENT_SELENIUM_WORKERS = 1
    GROUPS_TO_CREATE = 50
    
    MIN_SLEEP_SECONDS = 288
    MAX_SLEEP_SECONDS = 360

    GROUP_NAME_BASE = "collage Semester"
    GROUP_MEMBER_TO_ADD = '@BotFather'
    
    PROXIES = load_from_file("proxies.txt")
    USER_AGENTS = load_from_file("user_agents.txt")

    BTN_MANAGE_ACCOUNTS = "👤 مدیریت حساب‌ها"
    BTN_ADD_ACCOUNT = "➕ افزودن حساب جدید"
    BTN_BACK = "⬅️ بازگشت"
    BTN_START_PREFIX = "🟢 شروع برای"
    BTN_STOP_PREFIX = "⏹️ توقف برای"
    BTN_DELETE_PREFIX = "🗑️ حذف"
    
    METHOD_API = "🚀 API (سریع و سبک)"
    METHOD_SELENIUM = "🛡️ Selenium (امن و کند)"
    
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

print("--- Checking Environment Variables ---")
print(f"API_ID: {'Loaded' if API_ID else 'MISSING'}")
print(f"API_HASH: {'Loaded' if API_HASH else 'MISSING'}")
print(f"BOT_TOKEN: {'Loaded' if BOT_TOKEN else 'MISSING'}")
print(f"ENCRYPTION_KEY: {'Loaded' if ENCRYPTION_KEY else 'MISSING'}")
print("------------------------------------")

if not all([API_ID, API_HASH, BOT_TOKEN, ENCRYPTION_KEY]):
    print("FATAL ERROR: One or more environment variables are missing. Please check your .env file.")
    raise ValueError("Missing required environment variables.")

API_ID = int(API_ID)
TELETHON_SESSIONS_DIR = Path("telethon_sessions")
SELENIUM_SESSIONS_DIR = Path("selenium_sessions")
TELETHON_SESSIONS_DIR.mkdir(exist_ok=True)
SELENIUM_SESSIONS_DIR.mkdir(exist_ok=True)

class GroupCreatorBot:
    def __init__(self) -> None:
        self.bot = TelegramClient('bot_session', API_ID, API_HASH)
        self.user_sessions: Dict[int, Dict[str, Any]] = {}
        self.active_workers: Dict[str, asyncio.Task] = {}
        self.api_semaphore = asyncio.Semaphore(Config.MAX_CONCURRENT_API_WORKERS)
        self.selenium_semaphore = asyncio.Semaphore(Config.MAX_CONCURRENT_SELENIUM_WORKERS)
        
        self.sessions_lock = asyncio.Lock()
        self.workers_lock = asyncio.Lock()

        try:
            self.fernet = Fernet(ENCRYPTION_KEY.encode())
        except (ValueError, TypeError):
            raise ValueError("Invalid ENCRYPTION_KEY.")
        
        self._cleanup_stale_plugins()

    def _cleanup_stale_plugins(self):
        LOGGER.info("Cleaning up stale proxy plugin files...")
        count = 0
        for f in Path.cwd().glob("proxy_plugin_*.zip"):
            try:
                f.unlink()
                count += 1
            except OSError as e:
                LOGGER.error(f"Error deleting stale plugin {f.name}: {e}")
        if count > 0:
            LOGGER.info(f"Removed {count} stale plugin files.")

    def get_random_proxy(self) -> Optional[str]:
        return random.choice(Config.PROXIES) if Config.PROXIES else None

    def get_random_user_agent(self) -> Optional[str]:
        if Config.USER_AGENTS:
            return random.choice(Config.USER_AGENTS)
        return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"

    def get_all_accounts(self) -> Dict[str, List[str]]:
        accounts = {'api': [], 'selenium': []}
        for d in TELETHON_SESSIONS_DIR.iterdir():
            if d.is_dir(): accounts['api'].append(d.name)
        for d in SELENIUM_SESSIONS_DIR.iterdir():
            if d.is_dir(): accounts['selenium'].append(d.name)
        accounts['api'].sort()
        accounts['selenium'].sort()
        return accounts

    def get_account_type(self, account_name: str) -> Optional[str]:
        if (TELETHON_SESSIONS_DIR / account_name).exists(): return 'api'
        if (SELENIUM_SESSIONS_DIR / account_name).exists(): return 'selenium'
        return None

    def _get_api_session_path(self, account_name: str) -> Path:
        return TELETHON_SESSIONS_DIR / account_name / f"{account_name}.session"

    def _get_counter_path(self, account_name: str, acc_type: str) -> Path:
        base_dir = TELETHON_SESSIONS_DIR if acc_type == 'api' else SELENIUM_SESSIONS_DIR
        return base_dir / account_name / "group.counter"

    def _read_counter(self, account_name: str, acc_type: str) -> int:
        counter_file = self._get_counter_path(account_name, acc_type)
        if not counter_file.exists(): return 0
        try: return int(counter_file.read_text())
        except (ValueError, OSError): return 0

    def _write_counter(self, account_name: str, acc_type: str, value: int):
        counter_file = self._get_counter_path(account_name, acc_type)
        try: counter_file.write_text(str(value))
        except OSError as e: LOGGER.error(f"Error writing counter for {account_name}: {e}")
    
    def _delete_account(self, account_name: str) -> bool:
        acc_type = self.get_account_type(account_name)
        if not acc_type: return False
        
        dir_to_delete = TELETHON_SESSIONS_DIR / account_name if acc_type == 'api' else SELENIUM_SESSIONS_DIR / account_name
        try:
            shutil.rmtree(dir_to_delete)
            LOGGER.info(f"Deleted directory for account '{account_name}' ({acc_type}).")
            return True
        except OSError as e:
            LOGGER.error(f"Error deleting directory for '{account_name}': {e}")
            return False

    def _build_main_menu(self) -> List[List[Button]]:
        return [[Button.text(Config.BTN_MANAGE_ACCOUNTS)], [Button.text(Config.BTN_BACK)]]

    def _build_accounts_menu(self, user_id: int) -> List[List[Button]]:
        accounts = self.get_all_accounts()
        keyboard = []
        
        all_accounts = [(name, 'api') for name in accounts['api']] + [(name, 'selenium') for name in accounts['selenium']]
        all_accounts.sort()

        if not all_accounts:
            keyboard.append([Button.text("هنوز هیچ حسابی اضافه نشده است.")])
        else:
            for acc_name, acc_type in all_accounts:
                worker_key = f"{user_id}:{acc_name}"
                type_label = "(API)" if acc_type == 'api' else "(Selenium)"
                if worker_key in self.active_workers:
                    keyboard.append([Button.text(f"{Config.BTN_STOP_PREFIX} {acc_name} {type_label}")])
                else:
                    keyboard.append([
                        Button.text(f"{Config.BTN_START_PREFIX} {acc_name} {type_label}"),
                        Button.text(f"{Config.BTN_DELETE_PREFIX} {acc_name}")
                    ])

        keyboard.append([Button.text(Config.BTN_ADD_ACCOUNT)])
        keyboard.append([Button.text(Config.BTN_BACK)])
        return keyboard

    async def run_group_creation_worker_api(self, user_id: int, account_name: str):
        worker_key = f"{user_id}:{account_name}"
        session_path = self._get_api_session_path(account_name)
        user_client = TelegramClient(str(session_path), API_ID, API_HASH)
        
        try:
            async with self.api_semaphore:
                await user_client.connect()
                if not await user_client.is_user_authorized():
                    await self.bot.send_message(user_id, f"⚠️ نشست برای حساب API `{account_name}` منقضی شده. لطفا حذف و دوباره اضافه کنید.")
                    return

                await self.bot.send_message(user_id, f"✅ عملیات API برای `{account_name}` آغاز شد.")
                current_counter = self._read_counter(account_name, 'api')
                
                for i in range(Config.GROUPS_TO_CREATE):
                    current_counter += 1
                    group_title = f"{Config.GROUP_NAME_BASE} {current_counter}"
                    try:
                        await user_client(CreateChatRequest(users=[Config.GROUP_MEMBER_TO_ADD], title=group_title))
                        self._write_counter(account_name, 'api', current_counter)
                        wait_time = random.randint(Config.MIN_SLEEP_SECONDS, Config.MAX_SLEEP_SECONDS)
                        await self.bot.send_message(user_id, f"✅ [API:{account_name}] گروه '{group_title}' ساخته شد. در حال انتظار برای {wait_time // 60} دقیقه و {wait_time % 60} ثانیه...")
                        await asyncio.sleep(wait_time)
                    except Exception as e:
                        await self.bot.send_message(user_id, f"❌ [API:{account_name}] خطایی در ساخت گروه رخ داد: {e}")
                        break
        except asyncio.CancelledError:
            await self.bot.send_message(user_id, f"⏹️ عملیات API برای `{account_name}` متوقف شد.")
        finally:
            if user_client.is_connected(): await user_client.disconnect()
            async with self.workers_lock:
                if worker_key in self.active_workers: del self.active_workers[worker_key]
            LOGGER.info(f"API Worker finished for {worker_key}.")

    async def run_group_creation_worker_selenium(self, user_id: int, account_name: str):
        worker_key = f"{user_id}:{account_name}"
        selenium_client = None
        loop = asyncio.get_running_loop()
        try:
            async with self.selenium_semaphore:
                proxy = self.get_random_proxy()
                user_agent = self.get_random_user_agent()
                
                await self.bot.send_message(user_id, f"🚀 در حال اجرای مرورگر برای `{account_name}`...")
                selenium_client = await loop.run_in_executor(None, SeleniumClient, account_name, proxy, user_agent)
                
                is_logged_in = await loop.run_in_executor(None, selenium_client.is_logged_in)
                if not is_logged_in:
                    await self.bot.send_message(user_id, f"⚠️ حساب Selenium `{account_name}` وارد نشده. لطفا حذف و دوباره اضافه کنید.")
                    return

                await self.bot.send_message(user_id, f"✅ عملیات Selenium برای `{account_name}` آغاز شد.")
                current_counter = self._read_counter(account_name, 'selenium')

                for i in range(Config.GROUPS_TO_CREATE):
                    current_counter += 1
                    group_title = f"{Config.GROUP_NAME_BASE} {current_counter}"
                    success = await loop.run_in_executor(None, selenium_client.create_group, group_title, Config.GROUP_MEMBER_TO_ADD)

                    if success:
                        self._write_counter(account_name, 'selenium', current_counter)
                        wait_time = random.randint(Config.MIN_SLEEP_SECONDS, Config.MAX_SLEEP_SECONDS)
                        await self.bot.send_message(user_id, f"✅ [Selenium:{account_name}] گروه '{group_title}' ساخته شد. در حال انتظار برای {wait_time // 60} دقیقه و {wait_time % 60} ثانیه...")
                        await asyncio.sleep(wait_time)
                    else:
                        await self.bot.send_message(user_id, f"❌ [Selenium:{account_name}] خطایی در ساخت گروه رخ داد.")
                        break
        except asyncio.CancelledError:
            await self.bot.send_message(user_id, f"⏹️ عملیات Selenium برای `{account_name}` متوقف شد.")
        finally:
            if selenium_client: await loop.run_in_executor(None, selenium_client.close)
            async with self.workers_lock:
                if worker_key in self.active_workers: del self.active_workers[worker_key]
            LOGGER.info(f"Selenium Worker finished for {worker_key}.")

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
        buttons = [[Button.text(Config.METHOD_API)]]
        if SELENIUM_ENABLED:
            buttons.append([Button.text(Config.METHOD_SELENIUM)])
        buttons.append([Button.text(Config.BTN_BACK)])
        
        async with self.sessions_lock:
            self.user_sessions[event.sender_id]['state'] = 'awaiting_add_method'
        await event.reply("لطفا روش افزودن حساب را انتخاب کنید:", buttons=buttons)

    async def _handle_api_login(self, event, text):
        user_id = event.sender_id
        async with self.sessions_lock:
            state = self.user_sessions[user_id].get('sub_state')

        if state == 'awaiting_name':
            account_name = text.strip()
            if not re.match("^[a-zA-Z0-9_-]+$", account_name) or self.get_account_type(account_name):
                await event.reply("❌ نام نامعتبر است یا از قبل وجود دارد. لطفا نام دیگری انتخاب کنید.")
                return
            (TELETHON_SESSIONS_DIR / account_name).mkdir(exist_ok=True)
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
                await self._finalize_api_login(event)
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
                await self._finalize_api_login(event)
            except Exception as e:
                await event.reply(f"❌ رمز عبور اشتباه است: {e}. لطفا دوباره تلاش کنید.")

    async def _finalize_api_login(self, event):
        user_id = event.sender_id
        async with self.sessions_lock:
            client = self.user_sessions[user_id]['client']
            account_name = self.user_sessions[user_id]['account_name']
        
        session_path = self._get_api_session_path(account_name)
        with open(session_path, "w") as f:
            f.write(client.session.save())
        await client.disconnect()
        await event.reply(f"✅ حساب API `{account_name}` با موفقیت اضافه شد.")
        await self._send_accounts_menu(event)

    async def _message_router(self, event: events.NewMessage.Event):
        user_id = event.sender_id
        text = event.text.strip()
        
        async with self.sessions_lock:
            session = self.user_sessions.get(user_id, {})
            state = session.get('state')
        
        print(f"DEBUG: user_id={user_id} state='{state}' text='{text}'")

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
                parts = text.split(' ')
                acc_name = parts[2]
                acc_type = 'api' if parts[3] == '(API)' else 'selenium'
                worker_key = f"{user_id}:{acc_name}"
                task = asyncio.create_task(
                    self.run_group_creation_worker_api(user_id, acc_name) if acc_type == 'api' 
                    else self.run_group_creation_worker_selenium(user_id, acc_name)
                )
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
            if text == Config.METHOD_API:
                async with self.sessions_lock:
                    self.user_sessions[user_id] = {'state': 'adding_account', 'sub_state': 'awaiting_name', 'method': 'api'}
                await event.reply("یک نام مستعار برای حساب API وارد کنید (فقط حروف انگلیسی و اعداد):")
            elif text == Config.METHOD_SELENIUM and SELENIUM_ENABLED:
                async with self.sessions_lock:
                    self.user_sessions[user_id]['state'] = 'adding_account'
                try:
                    async with self.bot.conversation(user_id, timeout=300) as conv:
                        await conv.send_message("برای افزودن حساب با روش سلنیوم، لطفا یک نام مستعار وارد کنید:")
                        acc_name_msg = await conv.get_response()
                        account_name = acc_name_msg.text.strip()

                        if not re.match("^[a-zA-Z0-9_-]+$", account_name) or self.get_account_type(account_name):
                            await conv.send_message("❌ نام نامعتبر است یا از قبل وجود دارد. عملیات لغو شد.")
                            await self._send_accounts_menu(event)
                            return
                        
                        (SELENIUM_SESSIONS_DIR / account_name).mkdir(exist_ok=True)
                        
                        await conv.send_message("بسیار خب. اکنون شماره تلفن را با فرمت بین‌المللی ارسال کنید:")
                        phone_msg = await conv.get_response()
                        phone = phone_msg.text.strip()

                        await conv.send_message(f"🚀 در حال اجرای مرورگر برای `{account_name}`. این ممکن است کمی طول بکشد.")
                        
                        loop = asyncio.get_running_loop()
                        proxy = self.get_random_proxy()
                        user_agent = self.get_random_user_agent()
                        selenium_client = await loop.run_in_executor(None, SeleniumClient, account_name, proxy, user_agent)

                        async def get_code():
                            await conv.send_message("لطفا کد ورودی که دریافت کردید را وارد کنید:")
                            return (await conv.get_response()).text.strip()

                        async def get_password():
                            await conv.send_message("این حساب نیاز به رمز تایید دو مرحله‌ای دارد. لطفا آن را وارد کنید:")
                            return (await conv.get_response()).text.strip()

                        success = await loop.run_in_executor(None, selenium_client.login, phone, get_code, get_password)
                        
                        if success:
                            await conv.send_message(f"✅ نشست با موفقیت برای حساب `{account_name}` ذخیره شد.")
                        else:
                            await conv.send_message(f"❌ ورود ناموفق بود برای حساب `{account_name}`.")
                        
                        await loop.run_in_executor(None, selenium_client.close)
                        await self._send_accounts_menu(event)

                except asyncio.TimeoutError:
                    await event.reply("زمان به پایان رسید. عملیات لغو شد.")
                    await self._send_accounts_menu(event)
            return

        if session.get('state') == 'adding_account' and session.get('method') == 'api':
            await self._handle_api_login(event, text)
            return

    async def run(self):
        try:
            print("--- Attempting to start bot client ---")
            await self.bot.start(bot_token=BOT_TOKEN)
            print("--- Bot client started successfully ---")
            
            self.bot.add_event_handler(self._start_handler, events.NewMessage(pattern='/start'))
            self.bot.add_event_handler(self._message_router, events.NewMessage)
            
            LOGGER.info("Bot service has started successfully and is now running.")
            print("--- Bot is now running and waiting for messages ---")
            
            await self.bot.run_until_disconnected()
        except Exception as e:
            print(f"FATAL ERROR in bot.run(): {e}")
            LOGGER.critical(f"FATAL ERROR in bot.run(): {e}", exc_info=True)
            raise

if __name__ == "__main__":
    print("--- Running main block ---")
    try:
        bot = GroupCreatorBot()
        print("--- GroupCreatorBot instance created ---")
        asyncio.run(bot.run())
    except Exception as e:
        print(f"FATAL ERROR in main block: {e}")
        LOGGER.critical(f"FATAL ERROR in main block: {e}", exc_info=True)
