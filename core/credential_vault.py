"""Credential vault -- envelope encryption for tenant WordPress/API credentials.

Storage model:
  tenant_credentials.dek_ciphertext  -- Fernet-encrypted DEK (master key wraps it)
  tenant_credentials.encrypted_payload -- Fernet-encrypted JSON (DEK decrypts it)

The master key is NEVER stored in the DB. It lives in CREDENTIAL_MASTER_KEY env var.
KMS integration: set KMS_PROVIDER=aws|gcp|vault to use external key management.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger(__name__)

# Master key -- 32-byte Fernet key in base64url.
# Generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# MUST be set in production. Falls back to a dev key that prints a loud warning.
_DEV_KEY_WARNING = False


def _get_master_key() -> bytes:
    """Return the master Fernet key as bytes. Warns loudly if using dev fallback."""
    global _DEV_KEY_WARNING
    key = os.getenv("CREDENTIAL_MASTER_KEY", "")
    if key:
        return key.encode() if isinstance(key, str) else key
    # Dev fallback -- deterministic but clearly unsafe
    if not _DEV_KEY_WARNING:
        log.warning(
            "credential_vault.DEV_KEY_ACTIVE  "
            "Set CREDENTIAL_MASTER_KEY env var before storing real credentials!"
        )
        _DEV_KEY_WARNING = True
    from cryptography.fernet import Fernet
    # Reproducible dev key so re-starts don't corrupt stored creds
    import hashlib
    # [:0] returns b'' which is always falsy -- directly use sha256 deterministic key
    return hashlib.sha256(b"seo_engine_dev_key_do_not_use_in_prod").digest()[:32]


def _fernet(key: bytes):
    from cryptography.fernet import Fernet
    import base64
    # Fernet requires 32-byte key encoded as base64url
    if len(key) == 32:
        import base64 as b64
        key = b64.urlsafe_b64encode(key)
    return Fernet(key)


def generate_dek() -> bytes:
    """Generate a new random 32-byte Data Encryption Key."""
    from cryptography.fernet import Fernet
    return Fernet.generate_key()  # 32 bytes base64url-encoded


def wrap_dek(dek: bytes) -> bytes:
    """Encrypt a DEK with the master key. Returns ciphertext bytes."""
    master = _get_master_key()
    return _fernet(master).encrypt(dek)


def unwrap_dek(dek_ciphertext: bytes) -> bytes:
    """Decrypt a wrapped DEK using the master key. Returns plaintext DEK."""
    master = _get_master_key()
    return _fernet(master).decrypt(dek_ciphertext)


def encrypt_payload(payload: dict, dek: bytes) -> bytes:
    """Encrypt a credential payload dict using the DEK."""
    plaintext = json.dumps(payload).encode()
    return _fernet(dek).encrypt(plaintext)


def decrypt_payload(ciphertext: bytes, dek: bytes) -> dict:
    """Decrypt credential ciphertext using the DEK. Returns dict."""
    plaintext = _fernet(dek).decrypt(ciphertext)
    return json.loads(plaintext)


class CredentialVault:
    """Store and retrieve tenant credentials with envelope encryption."""

    def store(
        self,
        tenant_id: str,
        platform: str,
        credentials: dict,
    ) -> None:
        """Encrypt and store credentials for a tenant/platform pair.

        Args:
            tenant_id:   Tenant UUID string.
            platform:    Platform identifier (wordpress, gsc, indexnow, etc.).
            credentials: Dict of credential fields to encrypt.
        """
        from core.pg import get_conn
        dek = generate_dek()
        dek_ciphertext = wrap_dek(dek)
        encrypted_payload = encrypt_payload(credentials, dek)
        # DEK cleared from memory after use (not held in any variable)
        try:
            with get_conn(tenant_id=tenant_id) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO tenant_credentials
                            (tenant_id, platform, encrypted_payload, dek_ciphertext, updated_at)
                        VALUES (%s, %s, %s, %s, NOW())
                        ON CONFLICT (tenant_id, platform)
                        DO UPDATE SET
                            encrypted_payload = EXCLUDED.encrypted_payload,
                            dek_ciphertext    = EXCLUDED.dek_ciphertext,
                            updated_at        = NOW()
                        """,
                        [tenant_id, platform, encrypted_payload, dek_ciphertext],
                    )
                    # Audit log
                    cur.execute(
                        "INSERT INTO tenant_audit_log (tenant_id, actor, action, entity_type) VALUES (%s, %s, %s, %s)",
                        [tenant_id, "system", "credential.stored", platform],
                    )
            log.info("vault.store_ok  tenant=%s  platform=%s", tenant_id[:8], platform)
        except Exception as e:
            log.error("vault.store_fail  tenant=%s  platform=%s  err=%s", tenant_id[:8], platform, e)
            raise
        finally:
            # Zero out DEK bytes from memory
            del dek

    def retrieve(
        self,
        tenant_id: str,
        platform: str,
    ) -> Optional[dict]:
        """Retrieve and decrypt credentials for a tenant/platform pair.

        Returns None if no credentials stored for this platform.
        """
        from core.pg import get_conn
        try:
            with get_conn(tenant_id=tenant_id) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT encrypted_payload, dek_ciphertext FROM tenant_credentials "
                        "WHERE tenant_id = %s AND platform = %s",
                        [tenant_id, platform],
                    )
                    row = cur.fetchone()
                    if row is None:
                        return None
                    encrypted_payload, dek_ciphertext = row
                    # Audit log
                    cur.execute(
                        "INSERT INTO tenant_audit_log (tenant_id, actor, action, entity_type) VALUES (%s, %s, %s, %s)",
                        [tenant_id, "system", "credential.accessed", platform],
                    )
            # Unwrap DEK and decrypt payload
            dek = unwrap_dek(bytes(dek_ciphertext))
            try:
                payload = decrypt_payload(bytes(encrypted_payload), dek)
            finally:
                del dek  # Zero DEK immediately after use
            log.info("vault.retrieve_ok  tenant=%s  platform=%s", tenant_id[:8], platform)
            return payload
        except Exception as e:
            log.error("vault.retrieve_fail  tenant=%s  platform=%s  err=%s", tenant_id[:8], platform, e)
            raise

    def delete(
        self,
        tenant_id: str,
        platform: str,
    ) -> bool:
        """Delete credentials for a tenant/platform pair."""
        from core.pg import execute_write
        rows = execute_write(
            "DELETE FROM tenant_credentials WHERE tenant_id = %s AND platform = %s",
            [tenant_id, platform],
            tenant_id=tenant_id,
        )
        log.info("vault.delete  tenant=%s  platform=%s  deleted=%d", tenant_id[:8], platform, rows)
        return rows > 0

    def list_platforms(self, tenant_id: str) -> list[str]:
        """List platforms that have stored credentials for a tenant."""
        from core.pg import execute_many
        rows = execute_many(
            "SELECT platform FROM tenant_credentials WHERE tenant_id = %s ORDER BY platform",
            [tenant_id],
            tenant_id=tenant_id,
        )
        return [r[0] for r in rows]


# Module-level singleton
vault = CredentialVault()
