"""
Encryption module — AES-256-GCM (AEAD).

GCM provides both confidentiality and integrity: a 16-byte authentication tag
is produced during encryption and verified during decryption, making ciphertext
tampering detectable. A random 12-byte nonce is prepended to the ciphertext so
each encryption of the same plaintext produces a different output (IND-CPA secure).

Wire format: base64(nonce[12] || tag[16] || ciphertext)
"""

import os
import base64
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


def _get_key() -> bytes:
    raw = os.getenv("AES_KEY")
    if raw:
        return bytes.fromhex(raw)
    return b"\x00" * 32


def encrypt(plaintext: str) -> str:
    key = _get_key()
    nonce = os.urandom(12)
    ct_with_tag = AESGCM(key).encrypt(nonce, plaintext.encode("utf-8"), None)
    # ct_with_tag is ciphertext || tag (tag appended by the library)
    return base64.b64encode(nonce + ct_with_tag).decode("utf-8")


def decrypt(token: str) -> str:
    key = _get_key()
    raw = base64.b64decode(token)
    nonce, ct_with_tag = raw[:12], raw[12:]
    plaintext = AESGCM(key).decrypt(nonce, ct_with_tag, None)
    return plaintext.decode("utf-8")
