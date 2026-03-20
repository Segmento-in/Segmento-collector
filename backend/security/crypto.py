import os
from cryptography.fernet import Fernet, InvalidToken

# MASTER KEY
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
KEY_PATH = os.path.join(BASE_DIR, "secret.key")

def load_key():
    if not os.path.exists(KEY_PATH):
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
        print("TRY DECRYPT:", value[:20], flush=True)

        decrypted = fernet.decrypt(value.encode()).decode()

        print("DECRYPTED OK", flush=True)

        return decrypted

    except Exception as e:
        print("DECRYPT FAILED:", e, flush=True)
        return value