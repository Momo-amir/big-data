import os
import base64
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.fernet import Fernet

# The key is stored in the AES_KEY env-var as a 32-byte (256-bit) hex string.
# Generate once with: python -c "import os; print(os.urandom(32).hex())"
def _get_key() -> bytes:
    raw = os.getenv("AES_KEY")
    if raw:
        return bytes.fromhex(raw)
    # Fallback: dev key
    return b'\x00' * 32


#  Method 1: AES-GCM: built-in authentication tag detects tampering,
#  where CBC offers no integrity check
def encrypt_gcm(plaintext: str) -> str:
    key = _get_key()
    nonce = os.urandom(12)
    ct = AESGCM(key).encrypt(nonce, plaintext.encode(), None)
    return base64.b64encode(nonce + ct).decode()


def decrypt_gcm(token: str) -> str:
    key = _get_key()
    raw = base64.b64decode(token)
    nonce, ct = raw[:12], raw[12:]
    return AESGCM(key).decrypt(nonce, ct, None).decode()


# Method 2: AES-CBC (manual) 
def encrypt_cbc(plaintext: str) -> str:
    key = _get_key()
    iv = os.urandom(16)
    data = plaintext.encode()
    pad = 16 - len(data) % 16 # padding for block size
    
    data += bytes([pad] * pad)
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    ct = cipher.encryptor().update(data)
    return base64.b64encode(iv + ct).decode()


def decrypt_cbc(token: str) -> str:
    key = _get_key()
    raw = base64.b64decode(token)
    iv, ct = raw[:16], raw[16:]
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    data = cipher.decryptor().update(ct)
    pad = data[-1]
    return data[:-pad].decode()


# Method 3: AES-CBC via Fernet 
def _fernet() -> Fernet:
    # Fernet requires a 32-byte URL-safe base64 key derived from AES_KEY
    return Fernet(base64.urlsafe_b64encode(_get_key()))


def encrypt_fernet(plaintext: str) -> str:
    return _fernet().encrypt(plaintext.encode()).decode()


def decrypt_fernet(token: str) -> str:
    return _fernet().decrypt(token.encode()).decode()



encrypt = encrypt_gcm
decrypt = decrypt_gcm
