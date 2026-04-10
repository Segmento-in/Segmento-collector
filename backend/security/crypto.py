import os
import logging
from cryptography.fernet import Fernet

# MASTER KEY
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
KEY_PATH = os.path.join(BASE_DIR, "secret.key")

def load_key():
    # 1. Try Environment Variable (Priority)
    env_key = os.getenv("FERNET_KEY")
    if env_key:
        try:
            Fernet(env_key.encode())  # Validate format
            return env_key.encode()
        except Exception as e:
            logging.error(f"Invalid FERNET_KEY environment variable provided: {str(e)}")

    # 2. Check for Production (Fail-fast)
    is_prod = os.getenv("RENDER") or os.getenv("FLASK_ENV") == "production"
    if is_prod:
        raise RuntimeError(
            "CRITICAL: FERNET_KEY environment variable is missing in production! "
            "Decryption/Encryption cannot proceed safely. Set this variable immediately."
        )

    # 3. Fallback to File (Local only)
    if not os.path.exists(KEY_PATH):
        logging.warning("No secret key found. Generating a new one (Local development only).")
        key = Fernet.generate_key()
        with open(KEY_PATH, "wb") as f:
            f.write(key)
        return key

    try:
        key = open(KEY_PATH, "rb").read()
        Fernet(key)  # Validate key format
        return key
    except Exception as e:
        raise ValueError(f"Invalid or corrupted key file: {str(e)}. Regenerate or restore.")

fernet = Fernet(load_key())
logger = logging.getLogger(__name__)

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
        decrypted = fernet.decrypt(value.encode()).decode()
        return decrypted

    except Exception:
        logger.warning("Decryption failed - possible key mismatch")
        return None