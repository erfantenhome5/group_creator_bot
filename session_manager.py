import logging
import re
from pathlib import Path
from typing import Optional, List

from cryptography.fernet import Fernet, InvalidToken

LOGGER = logging.getLogger(__name__)

class SessionManager:
    """
    Manages all session-related operations, including encryption, file paths, and cleanup.
    """
    def __init__(self, fernet: Fernet, sessions_dir: Path):
        self.fernet = fernet
        self.sessions_dir = sessions_dir

    def _encrypt_data(self, data: str) -> bytes:
        """Encrypts data using the provided Fernet key."""
        return self.fernet.encrypt(data.encode())

    def _decrypt_data(self, encrypted_data: bytes) -> Optional[str]:
        """Decrypts data; returns None if decryption fails."""
        try:
            return self.fernet.decrypt(encrypted_data).decode()
        except InvalidToken:
            LOGGER.error("Failed to decrypt session data. Key may have changed or data is corrupt.")
            return None

    def _get_session_path(self, user_id: int, account_name: str) -> Path:
        """Constructs a safe, standardized path for a session file."""
        safe_account_name = re.sub(r'[^a-zA-Z0-9_-]', '', account_name)
        return self.sessions_dir / f"user_{user_id}__{safe_account_name}.session"

    def get_user_accounts(self, user_id: int) -> List[str]:
        """Gets a list of all account names for a given user ID."""
        accounts = []
        for f in self.sessions_dir.glob(f"user_{user_id}__*.session"):
            match = re.search(f"user_{user_id}__(.*)\\.session", f.name)
            if match:
                accounts.append(match.group(1))
        return sorted(accounts)

    def save_session_string(self, user_id: int, account_name: str, session_string: str) -> None:
        """Encrypts and saves a session string to its designated file."""
        encrypted_session = self._encrypt_data(session_string)
        session_file = self._get_session_path(user_id, account_name)
        session_file.write_bytes(encrypted_session)
        LOGGER.info(f"Encrypted session saved for user {user_id} as account '{account_name}'.")

    def load_session_string(self, user_id: int, account_name: str) -> Optional[str]:
        """Loads and decrypts a session string from its file."""
        session_file = self._get_session_path(user_id, account_name)
        if not session_file.exists():
            return None
        return self._decrypt_data(session_file.read_bytes())

    def delete_session_file(self, user_id: int, account_name: str) -> bool:
        """Deletes a specific session file."""
        session_path = self._get_session_path(user_id, account_name)
        if session_path.exists():
            try:
                session_path.unlink()
                LOGGER.info(f"Deleted session file for user {user_id}, account '{account_name}'.")
                return True
            except OSError as e:
                LOGGER.error(f"Error deleting session file for user {user_id}, account '{account_name}': {e}")
        return False
