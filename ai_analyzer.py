import asyncio
import json
import logging
import os
import random
import re
import traceback
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List

import httpx
import sentry_sdk
from telethon import errors, events

# Use the same logger as the main script
LOGGER = logging.getLogger(__name__)

class AIAnalyzer:
    """
    Handles all interactions with the Gemini AI model for code analysis, error diagnosis, and automated deployment.
    """

    def __init__(self, bot_instance):
        self.bot = bot_instance
        self.gemini_api_key = os.getenv("GEMINI_API_KEY")
        self.admin_user_id = os.getenv("ADMIN_USER_ID")
        self.github_username = os.getenv("GITHUB_USERNAME")
        self.github_token = os.getenv("GITHUB_TOKEN")
        self.proxies = self.bot.proxies

    async def _run_git_command(self, command: str) -> bool:
        """Executes a shell command and returns True on success."""
        process = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        if process.returncode == 0:
            LOGGER.info(f"Git command successful: {command}")
            return True
        else:
            LOGGER.error(f"Git command failed: {command}\nError: {stderr.decode().strip()}")
            return False

    async def _git_push_changes(self, commit_message: str) -> bool:
        """Commits and pushes changes to the GitHub repository."""
        if not all([self.github_username, self.github_token]):
            LOGGER.warning("GitHub username or token not set. Skipping push.")
            await self.bot.bot.send_message(int(self.admin_user_id), "⚠️ **هشدار:** اطلاعات گیت‌هاب تنظیم نشده است. از پوش کردن کد صرف نظر شد.")
            return False

        repo_url = f"https://{self.github_username}:{self.github_token}@github.com/erfantenhome5/group_creator_bot.git"
        
        try:
            LOGGER.info("Configuring local git user identity as a fallback...")
            # Set local config instead of global to avoid permission issues
            if not await self._run_git_command('git config user.name "AI Bot"'): return False
            if not await self._run_git_command('git config user.email "ai-bot@example.com"'): return False

            LOGGER.info("Attempting to push changes to GitHub...")
            if not await self._run_git_command('git add main.py ai_analyzer.py'): return False
            if not await self._run_git_command(f'git commit -m "{commit_message}"'): return False
            if not await self._run_git_command(f'git push {repo_url} HEAD:main'): return False

            LOGGER.info("Successfully pushed changes to GitHub.")
            await self.bot.bot.send_message(int(self.admin_user_id), "✅ **موفقیت:** تغییرات با موفقیت به گیت‌هاب پوش شد.")
            return True
        except Exception as e:
            LOGGER.error(f"Failed to push to GitHub: {e}")
            await self.bot.bot.send_message(int(self.admin_user_id), f"❌ **خطا در پوش به گیت‌هاب:**\n`{e}`")
            return False

    async def analyze_and_apply_fix(self, exc_type, exc_value, tb):
        """Analyzes an error, generates a fix, applies it, pushes to Git, and restarts."""
        if not self.gemini_api_key or not self.admin_user_id:
            LOGGER.warning("Cannot run AI error analysis: GEMINI_API_KEY or ADMIN_USER_ID is not set.")
            return

        try:
            LOGGER.info(f"AI is analyzing an error: {exc_type.__name__}")
            main_py_path = Path(__file__).parent.joinpath("main.py")
            source_code = main_py_path.read_text()
            traceback_str = "".join(traceback.format_exception(exc_type, exc_value, tb))
            prompt = self._construct_error_analysis_prompt(source_code, traceback_str)
            
            suggestions, used_model = await self._call_gemini_with_fallback(prompt, ["gemini-1.5-flash", "gemini-pro"])
            if not suggestions:
                LOGGER.error("AI error analysis returned no suggestions."); return

            corrected_function = self._extract_python_code_from_response(suggestions)
            response_message = f"🚨 **گزارش خودکار از هوش مصنوعی ({used_model}):**\n\n`{exc_type.__name__}`\n\n{suggestions}"
            await self.bot.bot.send_message(int(self.admin_user_id), response_message[:4096])

            if corrected_function:
                if self._apply_code_fix(main_py_path, corrected_function):
                    LOGGER.info("Code fix applied successfully. Proceeding with deployment.")
                    await self.bot.bot.send_message(int(self.admin_user_id), "✅ **اصلاحیه با موفقیت اعمال شد. در حال آماده‌سازی برای استقرار...")
                    
                    commit_message = f"AI Fix: Resolve {exc_type.__name__}"
                    await self._git_push_changes(commit_message)
                    
                    self.bot._save_worker_state()
                    await self.bot._broadcast_message("⚙️ ربات برای اعمال یک بروزرسانی مهم در حال راه‌اندازی مجدد است. لطفاً چند لحظه صبر کنید.")
                    await asyncio.sleep(2)
                    
                    LOGGER.info("Restarting bot service...")
                    process = await asyncio.create_subprocess_shell('sudo systemctl restart telegram_bot.service')
                    await process.wait()
                else:
                    LOGGER.error("Failed to apply the AI-generated code fix.")
                    await self.bot.bot.send_message(int(self.admin_user_id), "❌ **خطا در اعمال خودکار اصلاحیه.**")
            else:
                LOGGER.warning("AI analysis was generated, but no code block was found to apply.")
        except Exception as e:
            LOGGER.error(f"The AI self-healing process itself failed: {e}", exc_info=True)

    async def refine_code(self, event: events.NewMessage.Event):
        """Performs a general or prompted code review using Gemini AI."""
        if not self.gemini_api_key or not self.admin_user_id:
            return

        try:
            command_parts = event.text.split(' ', 1)
            admin_prompt = command_parts[1] if len(command_parts) > 1 else None

            main_py_path = Path(__file__).parent.joinpath("main.py")
            source_code = main_py_path.read_text()
            
            log_path = Path("bot_activity.log")
            recent_logs = "".join(log_path.read_text().splitlines()[-100:]) if log_path.exists() else ""

            if admin_prompt:
                prompt = self._construct_prompted_code_refinement_prompt(source_code, recent_logs, admin_prompt)
                await self.bot.bot.send_message(int(self.admin_user_id), f"🤖 **درخواست شما دریافت شد:** `{admin_prompt}`")
            else:
                prompt = self._construct_code_refinement_prompt(source_code, recent_logs)
                await self.bot.bot.send_message(int(self.admin_user_id), "🤖 **درخواست شما دریافت شد.**\nهوش مصنوعی در حال انجام یک بررسی کلی روی کد است...")

            suggestions, used_model = await self._call_gemini_with_fallback(prompt, ["gemini-1.5-flash", "gemini-pro"])

            if not suggestions:
                await self.bot.bot.send_message(int(self.admin_user_id), "❌ هوش مصنوعی نتوانست پیشنهادی ارائه دهد."); return
            
            response_message = f"📝 **گزارش بهبود کد از هوش مصنوعی ({used_model}):**\n\n{suggestions}"
            await self.bot.bot.send_message(int(self.admin_user_id), response_message[:4096])

        except Exception as e:
            LOGGER.error(f"Code refinement process failed: {e}", exc_info=True)
            sentry_sdk.capture_exception(e)
            await self.bot.bot.send_message(int(self.admin_user_id), f"❌ **خطا در فرآیند بهبود کد:**\n`{e}`")

    def _extract_python_code_from_response(self, response: str) -> Optional[str]:
        match = re.search(r"```python\n(.*?)```", response, re.DOTALL)
        return match.group(1).strip() if match else None

    def _apply_code_fix(self, file_path: Path, new_function_code: str) -> bool:
        try:
            match = re.search(r"def\s+(\w+)\s*\(", new_function_code)
            if not match:
                LOGGER.error("AI fix did not contain a valid function definition."); return False
            func_name = match.group(1)
            lines = file_path.read_text().splitlines()
            start_line_idx, func_indentation = -1, -1

            for i, line in enumerate(lines):
                if re.search(rf"^\s*(async\s+)?def\s+{func_name}\s*\(", line):
                    start_line_idx = i
                    func_indentation = len(line) - len(line.lstrip(' '))
                    break
            
            if start_line_idx == -1:
                LOGGER.error(f"Could not find function '{func_name}' in source code."); return False

            end_line_idx = -1
            for i in range(start_line_idx + 1, len(lines)):
                line = lines[i]
                if line.strip() and (len(line) - len(line.lstrip(' '))) <= func_indentation:
                    end_line_idx = i
                    break
            if end_line_idx == -1: end_line_idx = len(lines)

            modified_code = "\n".join(lines[:start_line_idx] + new_function_code.splitlines() + lines[end_line_idx:])
            file_path.write_text(modified_code)
            LOGGER.info(f"Successfully applied fix for function '{func_name}'.")
            return True
        except Exception as e:
            LOGGER.error(f"Failed to apply code fix: {e}", exc_info=True)
            return False

    def _construct_error_analysis_prompt(self, source_code: str, traceback_str: str) -> str:
        return (
            "You are an expert Python developer specializing in Telethon. Your task is to perform a root cause analysis of an error and generate a fix.\n\n"
            "**Context:** A multi-account Telegram bot that creates groups.\n\n"
            "**Your Task:**\n"
            "1.  **Analyze:** Examine the traceback and source code to find the root cause.\n"
            "2.  **Generate Corrected Function:** Rewrite the *entire Python function* where the error occurred, with the fix applied.\n"
            "3.  **Explain:** Briefly explain the root cause and the fix.\n\n"
            f"### Traceback:\n```\n{traceback_str}\n```\n\n"
            f"### Full Source Code:\n```python\n{source_code}\n```\n\n"
            "**Output Format:** Provide your analysis in Persian. First, the explanation, then the complete, corrected Python function in a `python` markdown block."
        )

    def _construct_prompted_code_refinement_prompt(self, source_code: str, recent_logs: str, admin_prompt: str) -> str:
        return (
            "You are an expert Python developer specializing in Telethon. Your task is to perform a targeted code modification based on an admin's request.\n\n"
            f"**Admin's Request:**\n```\n{admin_prompt}\n```\n\n"
            "**Your Task:**\n"
            "1.  **Understand & Analyze:** Understand the request and analyze the provided code and logs.\n"
            "2.  **Generate Corrected Function(s):** Rewrite the *entire Python function(s) or Class(es)* that need to be modified. The code must be complete.\n"
            "3.  **Explain:** Briefly explain what you changed.\n\n"
            f"### Recent Log Entries:\n```log\n{recent_logs}\n```\n\n"
            f"### Full Source Code:\n```python\n{source_code}\n```\n\n"
            "**Output Format:** Provide your analysis in Persian. First, the explanation, then the complete, corrected code in a `python` markdown block."
        )
    
    def _construct_user_explanation_prompt(self, error: Exception) -> str:
        """Constructs a prompt to generate a user-friendly explanation for an error."""
        error_type = type(error).__name__
        error_details = str(error)
        return (
            "You are a helpful assistant for a Telegram bot. An error occurred, and your task is to explain it to the user in simple, non-technical Persian.\n\n"
            f"**Technical Error:**\n- **Type:** `{error_type}`\n- **Details:** `{error_details}`\n\n"
            "**Your Task:**\n"
            "1.  Read the technical error type and details.\n"
            "2.  Write a very short, simple, one-sentence explanation in Persian for a non-technical user.\n"
            "3.  If possible, suggest a simple action, like 'لطفاً دوباره تلاش کنید' (please try again).\n\n"
            "**Example:**\n- If the error is `ConnectionError`, you could say: 'مشکلی در اتصال به سرورهای تلگرام پیش آمده است. لطفاً لحظاتی دیگر دوباره تلاش کنید.'\n"
            "- If the error is `UserRestrictedError`, you could say: 'این حساب توسط تلگرام محدود شده و قادر به انجام این کار نیست.'\n\n"
            "**Do not include technical terms like 'Error', 'Exception', 'Traceback', etc. in your final response.** Just provide the simple, helpful sentence."
        )

    async def _call_gemini_with_fallback(self, prompt: str, models: List[str]) -> (Optional[str], Optional[str]):
        if not self.gemini_api_key: return None, None
        for model_name in models:
            api_url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={self.gemini_api_key}"
            headers = {'Content-Type': 'application/json'}
            payload = {"contents": [{"parts": [{"text": prompt}]}]}
            proxy_url = None
            if self.proxies:
                proxy = random.choice(self.proxies)
                proxy_url = f"http://{proxy['addr']}:{proxy['port']}"
            try:
                async with httpx.AsyncClient(proxy=proxy_url, timeout=120) as client:
                    response = await client.post(api_url, json=payload, headers=headers)
                if response.status_code == 429:
                    LOGGER.warning(f"Rate limit for {model_name}. Trying next."); continue
                response.raise_for_status()
                result = response.json()
                return result['candidates'][0]['content']['parts'][0]['text'], model_name
            except Exception as e:
                LOGGER.error(f"Gemini API call failed for model {model_name}: {e}"); continue
        LOGGER.error(f"All Gemini models failed: {models}"); return None, None
