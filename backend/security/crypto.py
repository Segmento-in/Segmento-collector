import os
import logging
from cryptography.fernet import Fernet

logger = logging.getLogger(__name__)

# LOAD KEY FROM ENV ONLY
FERNET_KEY = os.getenv("FERNET_KEY")

if not FERNET_KEY:
    raise Exception("FERNET_KEY not set in environment")

try:
    fernet = Fernet(FERNET_KEY.encode())
except Exception as e:
    raise Exception(f"Invalid FERNET_KEY: {str(e)}")


# ENCRYPT
def encrypt_value(value):
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    return fernet.encrypt(value.encode()).decode()


# DECRYPT
def decrypt_value(value):

    if not value:
        return value

    try:
        return fernet.decrypt(value.encode()).decode()

    except Exception:
        logger.warning("Decryption failed - possible key mismatch")
        return None