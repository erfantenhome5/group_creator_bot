import asyncio
import json
import logging
import os
import random
import re
import traceback
from pathlib import Path
from typing import Optional, Dict, List

import httpx
import sentry_sdk
from telethon import errors

# Use the same logger as the main script
LOGGER = logging.getLogger(__name__)

class AIAnalyzer:
    """
    Handles all interactions with the Gemini AI model for code analysis and error diagnosis.
    """

    def __init__(self, bot_instance):
        self.bot = bot_instance
        self.gemini_api_key = os.getenv("GEMINI_API_KEY")
        self.admin_user_id = os.getenv("ADMIN_USER_ID")
        self.proxies = self.bot.proxies

    async def analyze_and_apply_fix(self, exc_type, exc_value, tb):
        """
        Analyzes an error, generates a corrected function, and notifies the admin.
        """
        if not self.gemini_api_key or not self.admin_user_id:
            LOGGER.warning("Cannot run AI error analysis: GEMINI_API_KEY or ADMIN_USER_ID is not set.")
            return

        try:
            important_errors = (errors.AuthKeyUnregisteredError, ConnectionError)
            model_priority_list = ["gemini-pro", "gemini-2.0-flash"] if isinstance(exc_value, important_errors) else ["gemini-2.0-flash"]
            
            LOGGER.info(f"AI is analyzing an error: {exc_type.__name__} with model priority: {model_priority_list}")
            
            main_py_path = Path(__file__).parent.joinpath("main.py")
            source_code = main_py_path.read_text()
            traceback_str = "".join(traceback.format_exception(exc_type, exc_value, tb))

            prompt = self._construct_error_analysis_prompt(source_code, traceback_str)
            
            suggestions, used_model = await self._call_gemini_with_fallback(prompt, model_priority_list)
            
            if not suggestions:
                LOGGER.error("AI error analysis returned no suggestions after trying all fallback models.")
                return

            corrected_function = self._extract_python_code_from_response(suggestions)
            
            response_message = (
                f"🚨 **گزارش خودکار از هوش مصنوعی ({used_model}):**\n\n"
                f"یک خطا از نوع `{exc_type.__name__}` شناسایی شد. تحلیل و راه حل زیر تولید شد و در حال اعمال است:\n\n"
                f"{suggestions}"
            )
            
            for i in range(0, len(response_message), 4096):
                await self.bot.bot.send_message(int(self.admin_user_id), response_message[i:i+4096])

            if corrected_function:
                # This is a placeholder for the self-healing logic.
                # In a real-world scenario, this would involve more complex code replacement.
                LOGGER.info("AI generated a potential fix. Manual application is recommended.")
                await self.bot.bot.send_message(int(self.admin_user_id), "✅ **اصلاحیه تولید شد. لطفاً کد پیشنهادی را به صورت دستی بررسی و اعمال کنید.**")

        except Exception as e:
            LOGGER.error(f"The AI self-healing process itself failed: {e}", exc_info=True)

    async def explain_error_for_user(self, error: Exception) -> str:
        """
        Uses Gemini to generate a simple, user-friendly explanation for an error.
        """
        default_message = "❌ **خطای غیرمنتظره:** مشکلی در انجام عملیات رخ داد. لطفاً دوباره تلاش کنید."
        if not self.gemini_api_key:
            return default_message

        try:
            prompt = self._construct_user_explanation_prompt(error)
            explanation, _ = await self._call_gemini_with_fallback(prompt, ["gemini-2.0-flash"])
            
            if explanation:
                return f"❌ **خطا:** {explanation}"
            else:
                return default_message
        except Exception as e:
            LOGGER.error(f"AI user explanation generation failed: {e}")
            return default_message

    def _extract_python_code_from_response(self, response: str) -> Optional[str]:
        """Extracts a Python code block from the AI's markdown response."""
        try:
            match = re.search(r"```python\n(.*?)```", response, re.DOTALL)
            if match:
                return match.group(1).strip()
            return None
        except Exception:
            return None

    async def refine_code(self, event, custom_prompt: str):
        """
        Handles the /refine_code command, analyzing the bot's code and recent logs based on user instructions.
        """
        if not self.gemini_api_key:
            await event.reply("❌ قابلیت بهبود کد فعال نیست: کلید API Gemini تنظیم نشده است.")
            return

        await event.reply("🤖 در حال ارسال کد، لاگ‌ها و دستورالعمل شما برای تحلیل توسط هوش مصنوعی Gemini... این کار ممکن است کمی طول بکشد.")
        
        try:
            main_py_path = Path(__file__).parent.joinpath("main.py")
            source_code = main_py_path.read_text()

            log_file_path = Path("bot_activity.log")
            recent_logs = ""
            if log_file_path.exists():
                with open(log_file_path, 'r') as f:
                    lines = f.readlines()
                    recent_logs = "".join(lines[-50:])
            
            prompt = self._construct_code_refinement_prompt(source_code, recent_logs, custom_prompt)
            suggestions, used_model = await self._call_gemini_with_fallback(prompt, ["gemini-pro", "gemini-2.0-flash"])

            if suggestions:
                response_message = (
                    f"✨ **پیشنهادات بهبود کد از هوش مصنوعی ({used_model}):**\n\n"
                    f"{suggestions}"
                )
                for i in range(0, len(response_message), 4096):
                    await event.reply(response_message[i:i+4096])
            else:
                await event.reply("❌ هوش مصنوعی نتوانست پیشنهادی برای بهبود کد ارائه دهد. لطفاً دستورالعمل‌های دقیق‌تری ارائه دهید.")

        except FileNotFoundError:
            await event.reply("❌ خطا: فایل کد منبع ربات یافت نشد.")
        except Exception as e:
            LOGGER.error(f"AI code refinement failed: {e}", exc_info=True)
            sentry_sdk.capture_exception(e)
            await event.reply(f"❌ خطایی در هنگام ارتباط با سرویس Gemini رخ داد: `{type(e).__name__}`")

    def _construct_error_analysis_prompt(self, source_code: str, traceback_str: str) -> str:
        """Constructs a sophisticated prompt for analyzing a specific error and generating a fix."""
        return (
            "You are an expert Python developer and a specialist in debugging asynchronous applications, "
            "particularly Telegram bots built with the Telethon library. Your task is to perform a root cause analysis of an error that was just captured and generate a fix.\n\n"
            "**Context:** The user is running a multi-account Telegram bot that creates groups. The bot uses proxies and runs multiple operations concurrently.\n\n"
            "**Your Task:**\n"
            "1.  **Analyze the Traceback and Source Code:** Carefully examine the provided traceback and the full source code to identify the exact root cause of the error.\n"
            "2.  **Generate the Corrected Function:** Rewrite the *entire Python function* where the error occurred, with the fix applied. The function should be complete and syntactically correct.\n"
            "3.  **Explain the Fix:** In a separate section, explain the root cause of the error and how your corrected function resolves it.\n\n"
            "---"
            "### Traceback:\n"
            "```\n"
            f"{traceback_str}\n"
            "```\n\n"
            "### Full Source Code:\n"
            "```python\n"
            f"{source_code}\n"
            "```\n\n"
            "---"
            "**Output Format:** Provide your analysis in Persian. First, provide the explanation under a 'تحلیل و راه حل' heading. Then, provide the complete, corrected Python function inside a `python` markdown block."
        )

    def _construct_code_refinement_prompt(self, source_code: str, recent_logs: str, custom_prompt: str) -> str:
        """Constructs a sophisticated prompt for a general code review, including user instructions."""
        return (
            "You are an expert Python developer and a specialist in optimizing asynchronous applications, "
            "particularly Telegram bots built with the Telethon library. Your task is to conduct a code review and suggest improvements based on the user's specific request.\n\n"
            "**User's Instructions:**\n"
            f"```\n{custom_prompt}\n```\n\n"
            "**Your Task:**\n"
            "1.  **Prioritize the User's Request:** Your primary goal is to address the user's instructions.\n"
            "2.  **Analyze the Source Code & Logs:** Review the full source code and recent logs to understand the context of the request.\n"
            "3.  **Provide Actionable Suggestions:** Offer a clear, concise, and bulleted list of suggestions that fulfill the user's request. Also include any other critical improvements you notice for robustness, efficiency, or clarity.\n"
            "4.  **Provide Code Snippets:** Where appropriate, include corrected code snippets to illustrate your recommendations.\n\n"
            "---"
            "### Recent Log Entries:\n"
            "```log\n"
            f"{recent_logs}\n"
            "```\n\n"
            "### Full Source Code:\n"
            "```python\n"
            f"{source_code}\n"
            "```\n\n"
            "---"
            "**Output Format:** Please provide your analysis in Persian, using clear headings for each suggestion."
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
        """
        Tries to call the Gemini API with a list of models, falling back to the next on failure.
        Returns the suggestion and the name of the model that succeeded.
        """
        if not self.gemini_api_key:
            return None, None

        for model_name in models:
            api_url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={self.gemini_api_key}"
            headers = {'Content-Type': 'application/json'}
            payload = {"contents": [{"parts": [{"text": prompt}]}]}

            proxy_url = None
            if self.proxies:
                proxy = random.choice(self.proxies)
                proxy_url = f"http://{proxy['addr']}:{proxy['port']}"

            try:
                LOGGER.info(f"Attempting to call Gemini API with model: {model_name}")
                async with httpx.AsyncClient(proxy=proxy_url) as client:
                    response = await client.post(api_url, json=payload, headers=headers, timeout=120)
                
                if response.status_code == 429:
                    LOGGER.warning(f"Rate limit hit for model {model_name}. Trying next model.")
                    continue 

                response.raise_for_status()
                
                result = response.json()
                LOGGER.info(f"Successfully received response from model: {model_name}")
                return result['candidates'][0]['content']['parts'][0]['text'], model_name
            
            except httpx.HTTPStatusError as e:
                LOGGER.error(f"HTTP error calling Gemini API with model {model_name}: {e}")
                continue
            except Exception as e:
                LOGGER.error(f"A non-HTTP error occurred while calling Gemini API with model {model_name}: {e}")
                break
        
        LOGGER.error(f"All specified Gemini models failed: {models}")
        return None, None
