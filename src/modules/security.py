"""
Encryption module — AES-256-CBC chosen over AES-256-GCM for this assignment.

Why CBC and not GCM?
  The assignment explicitly requires AES with CBC as the operation mode (Krav 2 af 3).
  CBC is implemented here with PKCS#7 padding and a random 16-byte IV prepended
  to the ciphertext so each encryption of the same plaintext produces a different
  ciphertext (IND-CPA secure).

Trade-off acknowledged:
  GCM adds an authentication tag that detects tampering (AEAD). CBC has no built-in
  integrity check. For a production system GCM would be preferred; here CBC is used
  to satisfy the curriculum requirement.
"""

import os
import base64
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend


def _get_key() -> bytes:
    """
    Load the 32-byte AES-256 key from the AES_KEY environment variable
    (stored as a 64-char hex string).
    Generate with: python -c "import os; print(os.urandom(32).hex())"
    Falls back to a zero-byte dev key if the variable is not set.
    """
    raw = os.getenv("AES_KEY")
    if raw:
        return bytes.fromhex(raw)
    return b"\x00" * 32


def encrypt(plaintext: str) -> str:
    """
    AES-256-CBC encryption with PKCS#7 padding.
    Returns base64(IV || ciphertext).
    """
    key = _get_key()
    iv = os.urandom(16)
    data = plaintext.encode("utf-8")

    # PKCS#7 padding to AES block size (16 bytes)
    pad_len = 16 - len(data) % 16
    data += bytes([pad_len] * pad_len)

    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    ct = cipher.encryptor().update(data) + cipher.encryptor().finalize()

    # Prepend IV so decrypt can recover it without a separate channel
    return base64.b64encode(iv + ct).decode("utf-8")


def decrypt(token: str) -> str:
    """
    AES-256-CBC decryption. Expects base64(IV || ciphertext).
    """
    key = _get_key()
    raw = base64.b64decode(token)
    iv, ct = raw[:16], raw[16:]

    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    data = cipher.decryptor().update(ct) + cipher.decryptor().finalize()

    # Remove PKCS#7 padding
    pad_len = data[-1]
    return data[:-pad_len].decode("utf-8")
