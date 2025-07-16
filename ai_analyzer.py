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
        Analyzes an error, generates a patch, notifies the admin, and applies the fix.
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

            patch_content = self._extract_patch_from_response(suggestions)
            
            response_message = (
                f"🚨 **گزارش خودکار و اقدام اصلاحی از هوش مصنوعی ({used_model}):**\n\n"
                f"یک خطا از نوع `{exc_type.__name__}` شناسایی شد. تحلیل و راه حل زیر تولید و در حال اعمال است:\n\n"
                f"{suggestions}"
            )
            
            for i in range(0, len(response_message), 4096):
                await self.bot.bot.send_message(int(self.admin_user_id), response_message[i:i+4096])

            if patch_content:
                patch_file = Path("fix.patch")
                patch_file.write_text(patch_content)
                
                apply_script_content = (
                    "#!/bin/bash\n"
                    "echo 'Applying patch...'\n"
                    "patch -p1 main.py < fix.patch\n"
                    "if [ $? -eq 0 ]; then\n"
                    "    echo 'Patch applied successfully. Restarting bot service...'\n"
                    "    sudo systemctl restart telegram_bot.service\n"
                    "    echo 'Service restarted.'\n"
                    "else\n"
                    "    echo 'Error applying patch. Please check the patch file and apply it manually.'\n"
                    "fi\n"
                )
                apply_script_file = Path("apply_and_restart.sh")
                apply_script_file.write_text(apply_script_content)
                os.chmod(apply_script_file, 0o755) # Make the script executable

                # Execute the script to apply the patch and restart
                LOGGER.info("Executing self-healing script: apply_and_restart.sh")
                process = await asyncio.create_subprocess_shell(f'./{apply_script_file}')
                await process.wait()
                
                patch_file.unlink()
                apply_script_file.unlink()
            else:
                LOGGER.warning("AI analysis was generated, but no patch was found to apply.")

        except Exception as e:
            LOGGER.error(f"The AI self-healing process itself failed: {e}", exc_info=True)

    def _extract_patch_from_response(self, response: str) -> Optional[str]:
        """Extracts a diff/patch block from the AI's markdown response."""
        try:
            match = re.search(r"```diff\n(.*?)```", response, re.DOTALL)
            if match:
                return match.group(1).strip()
            return None
        except Exception:
            return None

    async def refine_code(self, event):
        """
        Handles the /refine_code command, analyzing the bot's code and recent logs.
        """
        if not self.gemini_api_key:
            await event.reply("❌ قابلیت تحلیل کد با هوش مصنوعی غیرفعال است. لطفاً `GEMINI_API_KEY` را در فایل `.env` تنظیم کنید.")
            return

        await event.reply("🤖 در حال ارسال کد و لاگ‌ها برای تحلیل توسط هوش مصنوعی Gemini... این کار ممکن است کمی طول بکشد.")
        
        try:
            source_code = Path(__file__).parent.joinpath("main.py").read_text()
            
            try:
                with open("bot_activity.log", "r") as f:
                    log_lines = f.readlines()
                    recent_logs = "".join(log_lines[-50:])
            except FileNotFoundError:
                recent_logs = "Log file not found."

            prompt = self._construct_code_refinement_prompt(source_code, recent_logs)
            
            suggestions, _ = await self._call_gemini_with_fallback(prompt, ["gemini-2.0-flash"])

            if suggestions:
                response_message = f"**💡 پیشنهادات Gemini برای بهبود کد (بر اساس کد و لاگ‌ها):**\n\n{suggestions}"
                for i in range(0, len(response_message), 4096):
                    await event.reply(response_message[i:i+4096])
            else:
                await event.reply("❌ هوش مصنوعی نتوانست پیشنهادی ارائه دهد. لطفاً دوباره تلاش کنید.")

        except FileNotFoundError:
            await event.reply("❌ خطا: فایل کد منبع ربات یافت نشد.")
        except Exception as e:
            LOGGER.error(f"AI code refinement failed: {e}", exc_info=True)
            sentry_sdk.capture_exception(e)
            await event.reply(f"❌ خطایی در هنگام ارتباط با سرویس Gemini رخ داد: `{type(e).__name__}`")

    def _construct_error_analysis_prompt(self, source_code: str, traceback_str: str) -> str:
        """Constructs a sophisticated prompt for analyzing a specific error and generating a patch."""
        return (
            "You are an expert Python developer and a specialist in debugging asynchronous applications, "
            "particularly Telegram bots built with the Telethon library. Your task is to perform a root cause analysis of an error that was just captured and generate a fix.\n\n"
            "**Context:** The user is running a multi-account Telegram bot that creates groups. The bot uses proxies and runs multiple operations concurrently.\n\n"
            "**Your Task:**\n"
            "1.  **Analyze the Traceback and Source Code:** Carefully examine the provided traceback and the full source code to identify the exact root cause of the error.\n"
            "2.  **Generate a Patch:** Create a code patch in the `diff` format that fixes the bug. The patch should be clean, targeted, and follow best practices. Enclose the patch in a markdown block like this: ```diff ... ```.\n"
            "3.  **Explain the Fix:** In a separate section, explain the root cause of the error and how your patch resolves it.\n\n"
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
            "**Output Format:** Provide your analysis in Persian. First, provide the explanation under a 'تحلیل و راه حل' heading. Then, provide the patch inside a `diff` markdown block."
        )

    def _construct_code_refinement_prompt(self, source_code: str, recent_logs: str) -> str:
        """Constructs a sophisticated prompt for a general code review."""
        return (
            "You are an expert Python developer and a specialist in optimizing asynchronous applications, "
            "particularly Telegram bots built with the Telethon library. Your task is to conduct a code review and suggest improvements.\n\n"
            "**Context:** The user is running a multi-account Telegram bot that creates groups. The bot uses proxies and runs multiple operations concurrently. The provided logs show its recent activity.\n\n"
            "**Your Task:**\n"
            "1.  **Analyze the Source Code:** Review the full source code for potential issues related to performance, stability, and error handling.\n"
            "2.  **Analyze the Logs:** Examine the recent logs for any warnings, errors, or unusual patterns that might indicate underlying problems.\n"
            "3.  **Provide Actionable Suggestions:** Offer a clear, concise, and bulleted list of suggestions to improve the code. Focus on:\n"
            "    - **Robustness:** How can the bot better handle unexpected situations or network failures?\n"
            "    - **Efficiency:** Are there any performance bottlenecks or opportunities to optimize resource usage?\n"
            "    - **Clarity & Maintainability:** Can the code be made easier to read and maintain?\n"
            "4.  **Provide Code Snippets:** Where appropriate, include small, corrected code snippets to illustrate your recommendations.\n\n"
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
