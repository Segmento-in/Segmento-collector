import sqlite3
from datetime import datetime, timezone

import requests
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

from backend.security.secure_db import decrypt_payload
from backend.security.secure_db import encrypt_payload


DB = "identity.db"

SOURCE_ALIASES = {
    "search_console": "search-console"
}


def get_db():
    con = sqlite3.connect(
        DB,
        timeout=60,
        isolation_level=None,
        check_same_thread=False
    )

    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    con.execute("PRAGMA synchronous=NORMAL;")

    return con


def _normalize_source(source):
    return SOURCE_ALIASES.get(source, source)


def persist_google_tokens(uid, source, access_token, refresh_token):
    if not uid or not source:
        raise ValueError("uid and source are required")

    if not access_token:
        raise ValueError("access_token is required")

    source = _normalize_source(source)

    secured = encrypt_payload({
        "access_token": access_token,
        "refresh_token": refresh_token
    })

    con = get_db()

    try:
        cur = con.cursor()

        cur.execute(
            """
            UPDATE google_accounts
            SET access_token=?, refresh_token=?
            WHERE uid=? AND source=?
            """,
            (
                secured["access_token"],
                secured["refresh_token"],
                uid,
                source
            )
        )

        if cur.rowcount == 0:
            raise ValueError(
                f"No Google account found for uid={uid} source={source}"
            )

        con.commit()

    finally:
        con.close()


def ensure_valid_google_token(creds, uid, source):
    if creds is None:
        raise ValueError("creds is required")

    if not isinstance(creds, Credentials):
        raise TypeError("creds must be a google.oauth2.credentials.Credentials")

    if not uid or not source:
        raise ValueError("uid and source are required")

    source = _normalize_source(source)

    should_refresh = (
        not creds.token
        or creds.expired
        or creds.expiry is None
    )

    if should_refresh:
        if not creds.refresh_token:
            raise ValueError(
                f"Cannot refresh Google token for source={source}: missing refresh_token"
            )

        print(f"[TOKEN] Refreshing {source} for {uid}", flush=True)

        try:
            decrypted = decrypt_payload({
                "client_id": creds.client_id,
                "client_secret": creds.client_secret
            })

            new_creds = Credentials(
                token=creds.token,
                refresh_token=creds.refresh_token,
                token_uri=creds.token_uri,
                client_id=decrypted["client_id"],
                client_secret=decrypted["client_secret"],
                scopes=creds.scopes
            )

            new_creds.refresh(Request())
        except Exception as e:
            print(f"[TOKEN] Refresh failed for {source}: {e}", flush=True)
            raise

        persist_google_tokens(uid, source, new_creds.token, new_creds.refresh_token)

        return new_creds

    return creds


def refresh_google_oauth_token(creds, uid, source):
    return ensure_valid_google_token(creds, uid, source)


def _parse_pinterest_expiry(expires_at):
    if not expires_at:
        return None

    if isinstance(expires_at, (int, float)):
        return datetime.fromtimestamp(expires_at, tz=timezone.utc)

    text = str(expires_at).strip()

    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            return datetime.fromtimestamp(float(text), tz=timezone.utc)
        except ValueError:
            return None


def _get_pinterest_refresh_payload(uid):
    con = get_db()

    try:
        cur = con.cursor()

        cur.execute(
            """
            SELECT access_token, refresh_token, expires_at
            FROM pinterest_tokens
            WHERE uid=?
            LIMIT 1
            """,
            (uid,)
        )
        token_row = cur.fetchone()

        if not token_row:
            raise ValueError(f"Pinterest tokens not found for uid={uid}")

        cur.execute(
            """
            SELECT config_json
            FROM connector_configs
            WHERE uid=? AND connector='pinterest'
            LIMIT 1
            """,
            (uid,)
        )
        cfg_row = cur.fetchone()

        if not cfg_row or not cfg_row[0]:
            raise ValueError(f"Pinterest config not found for uid={uid}")

    finally:
        con.close()

    access_token, refresh_token, expires_at = token_row
    config = decrypt_payload({
        "config_json": cfg_row[0]
    })
    config_json = config.get("config_json")

    if not config_json:
        raise ValueError(f"Pinterest config payload missing for uid={uid}")

    import json

    creds = decrypt_payload(json.loads(config_json))

    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "expires_at": expires_at,
        "client_id": creds.get("client_id"),
        "client_secret": creds.get("client_secret")
    }


def refresh_pinterest_token(uid):
    payload = _get_pinterest_refresh_payload(uid)

    if not payload.get("refresh_token"):
        raise ValueError(f"Pinterest refresh token missing for uid={uid}")

    if not payload.get("client_id") or not payload.get("client_secret"):
        raise ValueError(f"Pinterest client credentials missing for uid={uid}")

    body = {
        "grant_type": "refresh_token",
        "refresh_token": payload["refresh_token"],
        "client_id": payload["client_id"],
        "client_secret": payload["client_secret"]
    }

    response = requests.post(
        "https://api.pinterest.com/v5/oauth/token",
        data=body,
        timeout=20
    )

    try:
        response.raise_for_status()
    except Exception as e:
        raise RuntimeError(
            f"Pinterest token refresh failed for uid={uid}: {response.text}"
        ) from e

    token_data = response.json()
    access_token = token_data.get("access_token")
    refresh_token = token_data.get("refresh_token") or payload["refresh_token"]
    expires_in = token_data.get("expires_in")

    if not access_token:
        raise ValueError("Pinterest refresh response missing access_token")

    expires_at = None
    if expires_in is not None:
        expires_at = datetime.now(timezone.utc).timestamp() + int(expires_in)

    con = get_db()

    try:
        cur = con.cursor()
        cur.execute(
            """
            UPDATE pinterest_tokens
            SET access_token=?, refresh_token=?, expires_at=?
            WHERE uid=?
            """,
            (access_token, refresh_token, expires_at, uid)
        )

        if cur.rowcount == 0:
            raise ValueError(f"Pinterest tokens not found for uid={uid}")

        con.commit()

    finally:
        con.close()

    return access_token


def ensure_valid_pinterest_token(uid):
    payload = _get_pinterest_refresh_payload(uid)
    expires_at = _parse_pinterest_expiry(payload.get("expires_at"))

    if expires_at and expires_at <= datetime.now(timezone.utc):
        return refresh_pinterest_token(uid)

    return payload.get("access_token")
