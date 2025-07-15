import time
import asyncio
from pathlib import Path
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

class SeleniumClient:
    """
    Manages a Selenium browser instance to interact with Telegram Web.
    Uses undetected-chromedriver to avoid bot detection.
    """
    def __init__(self, account_name: str, proxy: str = None):
        self.account_name = account_name
        self.proxy = proxy
        self.driver = None

        options = uc.ChromeOptions()
        user_data_dir = Path.cwd() / "selenium_sessions" / self.account_name
        options.add_argument(f"--user-data-dir={user_data_dir}")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--log-level=3")
        
        if self.proxy:
            options.add_argument(f'--proxy-server={self.proxy}')

        # Use undetected_chromedriver
        self.driver = uc.Chrome(options=options, use_subprocess=True)

    def is_logged_in(self) -> bool:
        """Checks if the user is logged in."""
        try:
            self.driver.get("https://web.telegram.org/a/")
            WebDriverWait(self.driver, 25).until(
                EC.presence_of_element_located((By.ID, "telegram-search-input"))
            )
            return True
        except TimeoutException:
            return False

    async def login(self, phone: str, get_code_callback, get_password_callback):
        """Guides the user through the login process."""
        self.driver.get("https://web.telegram.org/a/")
        try:
            # 1. Enter Phone Number
            phone_input = WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'input-field-input') and @inputmode='tel']"))
            )
            await asyncio.sleep(1)
            phone_input.send_keys(phone)
            
            next_button = self.driver.find_element(By.XPATH, "//button[contains(., 'Next')]")
            next_button.click()

            # 2. Enter Code
            code = await get_code_callback()
            code_input = WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.XPATH, "//input[@inputmode='numeric']"))
            )
            code_input.send_keys(code)
            
            # 3. Check for Password
            try:
                password_input = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, "//input[@type='password']"))
                )
                password = await get_password_callback()
                password_input.send_keys(password)
            except TimeoutException:
                pass # No password needed

            WebDriverWait(self.driver, 30).until(
                EC.presence_of_element_located((By.ID, "telegram-search-input"))
            )
            return True
        except Exception as e:
            print(f"An unexpected error occurred during login for {self.account_name}: {e}")
            return False

    def create_group(self, group_name: str, member_username: str) -> bool:
        """Creates a new group with a given name and initial member."""
        try:
            # 1. Click New Message FAB
            new_chat_button = WebDriverWait(self.driver, 20).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button.floating-button"))
            )
            new_chat_button.click()

            # 2. Click "New Group"
            new_group_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//div[contains(@class, 'ListItem-title') and text()='New Group']"))
            )
            new_group_button.click()

            # 3. Add member
            search_input = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//input[@placeholder='Add Members']"))
            )
            search_input.send_keys(member_username)
            
            member_xpath = f"//div[contains(@class, 'ListItem-title')]/span[text()='{member_username.replace('@','')}']"
            member_result = WebDriverWait(self.driver, 15).until(
                EC.element_to_be_clickable((By.XPATH, member_xpath))
            )
            member_result.click()

            # 4. Click next arrow
            next_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button.Button.floating-button.is-primary"))
            )
            next_button.click()

            # 5. Enter group name
            group_name_input = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//div[@role='textbox' and @aria-label='Group Name']"))
            )
            group_name_input.send_keys(group_name)

            # 6. Click create button
            create_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button.Button.floating-button.is-primary"))
            )
            create_button.click()
            
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.XPATH, f"//h3[contains(@class, 'peer-title') and text()='{group_name}']"))
            )
            return True
        except Exception as e:
            print(f"An unexpected error occurred during group creation for {self.account_name}: {e}")
            return False

    def close(self):
        if self.driver:
            self.driver.quit()
