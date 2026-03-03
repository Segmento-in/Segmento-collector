import datetime
import json
import sqlite3
import time
from urllib.parse import urlencode

import requests

from destinations.destination_router import push_to_destination
from security.crypto import encrypt_value
from security.secure_fetch import fetchone_secure


DB = "identity.db"
SOURCE = "instagram"
GRAPH_BASE = "https://graph.facebook.com/v19.0"
SCOPES = [
    "instagram_basic",
    "pages_show_list",
    "instagram_manage_insights",
    "pages_read_engagement",
]


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _safe_graph_get(url, params=None, timeout=30, retries=3):
    for attempt in range(retries):
        r = requests.get(url, params=params, timeout=timeout)

        if r.status_code == 429:
            time.sleep(min(2 * (attempt + 1), 8))
            continue

        return r

    return r


def get_instagram_auth_url(uid):
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT client_id, scopes
        FROM connector_configs
        WHERE uid=? AND connector=?
        LIMIT 1
        """,
        (uid, SOURCE),
    )
    row = fetchone_secure(cur)
    con.close()

    if not row:
        raise Exception("Instagram app not configured")

    params = {
        "client_id": row["client_id"],
        "redirect_uri": row["scopes"],
        "scope": ",".join(SCOPES),
        "response_type": "code",
    }
    return "https://www.facebook.com/v19.0/dialog/oauth?" + urlencode(params)


def exchange_instagram_code(uid, code):
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT client_id, client_secret, scopes
        FROM connector_configs
        WHERE uid=? AND connector=?
        LIMIT 1
        """,
        (uid, SOURCE),
    )
    row = fetchone_secure(cur)
    con.close()

    if not row:
        raise Exception("Instagram app not configured")

    res = _safe_graph_get(
        f"{GRAPH_BASE}/oauth/access_token",
        params={
            "client_id": row["client_id"],
            "client_secret": row["client_secret"],
            "redirect_uri": row["scopes"],
            "code": code,
        },
    )
    return res.json()


def _exchange_long_lived_token(uid, short_lived_token):
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT client_id, client_secret
        FROM connector_configs
        WHERE uid=? AND connector=?
        LIMIT 1
        """,
        (uid, SOURCE),
    )
    row = fetchone_secure(cur)
    con.close()

    if not row:
        return {"access_token": short_lived_token}

    res = _safe_graph_get(
        f"{GRAPH_BASE}/oauth/access_token",
        params={
            "grant_type": "fb_exchange_token",
            "client_id": row["client_id"],
            "client_secret": row["client_secret"],
            "fb_exchange_token": short_lived_token,
        },
    )

    data = res.json()
    if data.get("access_token"):
        return data

    # Graceful fallback to short-lived token if exchange fails.
    return {"access_token": short_lived_token}


def _fetch_connected_ig_account(access_token):
    pages_res = _safe_graph_get(
        f"{GRAPH_BASE}/me/accounts",
        params={"access_token": access_token, "limit": 100},
    )
    pages_data = pages_res.json()
    pages = pages_data.get("data", [])

    if not pages:
        return None, None

    for page in pages:
        page_id = page.get("id")
        page_token = page.get("access_token")
        if not page_id:
            continue

        detail_res = _safe_graph_get(
            f"{GRAPH_BASE}/{page_id}",
            params={
                "access_token": page_token or access_token,
                "fields": "instagram_business_account{id}",
            },
        )
        detail_data = detail_res.json()
        ig_data = detail_data.get("instagram_business_account") or {}
        ig_id = ig_data.get("id")
        if ig_id:
            return ig_id, (page_token or access_token)

    return None, None


def _save_ig_connection(uid, ig_account_id, access_token):
    con = get_db()
    cur = con.cursor()

    enc_token = encrypt_value(access_token)

    cur.execute(
        """
        INSERT OR REPLACE INTO instagram_connections
        (uid, ig_account_id, access_token, connected_at)
        VALUES (?, ?, ?, ?)
        """,
        (
            uid,
            ig_account_id,
            enc_token,
            datetime.datetime.now(datetime.UTC).isoformat(),
        ),
    )

    cur.execute(
        """
        INSERT OR REPLACE INTO google_connections
        (uid, source, enabled)
        VALUES (?, ?, 1)
        """,
        (uid, SOURCE),
    )

    # Persist current OAuth access token on connector config as well.
    cur.execute(
        """
        UPDATE connector_configs
        SET access_token=?, status='connected'
        WHERE uid=? AND connector=?
        """,
        (enc_token, uid, SOURCE),
    )

    con.commit()
    con.close()


def get_token_and_account(uid):
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT ig_account_id, access_token
        FROM instagram_connections
        WHERE uid=?
        LIMIT 1
        """,
        (uid,),
    )
    row = fetchone_secure(cur)
    con.close()
    if not row:
        return None, None
    return row["ig_account_id"], row["access_token"]


def get_state(uid):
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT state_json
        FROM connector_state
        WHERE uid=? AND source=?
        LIMIT 1
        """,
        (uid, SOURCE),
    )
    row = fetchone_secure(cur)
    con.close()

    if not row:
        return {"last_timestamp": "1970-01-01T00:00:00+0000"}

    try:
        return json.loads(row["state_json"])
    except Exception:
        return {"last_timestamp": "1970-01-01T00:00:00+0000"}


def save_state(uid, state):
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO connector_state
        (uid, source, state_json, updated_at)
        VALUES (?, ?, ?, ?)
        """,
        (
            uid,
            SOURCE,
            json.dumps(state),
            datetime.datetime.now(datetime.UTC).isoformat(),
        ),
    )
    con.commit()
    con.close()


def get_active_destination(uid):
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT dest_type, host, port, username, password, database_name
        FROM destination_configs
        WHERE uid=? AND source=? AND is_active=1
        LIMIT 1
        """,
        (uid, SOURCE),
    )
    row = fetchone_secure(cur)
    con.close()
    if not row:
        return None
    return {
        "type": row["dest_type"],
        "host": row["host"],
        "port": row["port"],
        "username": row["username"],
        "password": row["password"],
        "database_name": row["database_name"],
    }


def sync_instagram(uid, sync_type="historical"):
    try:
        ig_account_id, access_token = get_token_and_account(uid)

        if not ig_account_id or not access_token:
            return {"status": "error", "message": "Instagram not connected"}

        state = get_state(uid)
        last_timestamp = state.get("last_timestamp", "1970-01-01T00:00:00+0000")
        newest_timestamp = last_timestamp

        params = {
            "access_token": access_token,
            "limit": 50,
            "fields": (
                "id,caption,media_type,media_url,permalink,timestamp,"
                "like_count,comments_count"
            ),
        }

        if sync_type == "incremental" and last_timestamp:
            params["since"] = last_timestamp

        url = f"{GRAPH_BASE}/{ig_account_id}/media"
        rows = []

        while url:
            res = _safe_graph_get(url, params=params)
            data = res.json()

            if res.status_code != 200:
                err = (data.get("error") or {}).get("message", "Instagram API error")
                code = (data.get("error") or {}).get("code")
                if code == 190:
                    return {
                        "status": "error",
                        "message": "Instagram token expired. Please reconnect.",
                        "error_code": code,
                    }
                return {
                    "status": "error",
                    "message": f"Instagram API error ({res.status_code}): {err}",
                }

            for item in data.get("data", []):
                ts = item.get("timestamp")
                rows.append(
                    {
                        "uid": uid,
                        "ig_account_id": ig_account_id,
                        "id": item.get("id"),
                        "caption": item.get("caption"),
                        "media_type": item.get("media_type"),
                        "media_url": item.get("media_url"),
                        "permalink": item.get("permalink"),
                        "timestamp": ts,
                        "like_count": item.get("like_count"),
                        "comments_count": item.get("comments_count"),
                    }
                )
                if ts and ts > newest_timestamp:
                    newest_timestamp = ts

            paging = data.get("paging", {})
            url = paging.get("next")
            params = None

        if newest_timestamp != last_timestamp:
            save_state(uid, {"last_timestamp": newest_timestamp})

        dest_cfg = get_active_destination(uid)
        if not dest_cfg:
            return {
                "status": "success",
                "rows_found": len(rows),
                "rows_pushed": 0,
                "sync_type": sync_type,
                "message": "No active destination",
            }

        pushed = push_to_destination(dest_cfg, SOURCE, rows) if rows else 0
        return {
            "status": "success",
            "rows_found": len(rows),
            "rows_pushed": pushed,
            "sync_type": sync_type,
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}


def handle_oauth_callback(uid, code):
    token_data = exchange_instagram_code(uid, code)
    short_lived_token = token_data.get("access_token")

    if not short_lived_token:
        return {
            "status": "error",
            "message": "Token exchange failed",
            "details": token_data,
        }

    long_token_data = _exchange_long_lived_token(uid, short_lived_token)
    access_token = long_token_data.get("access_token", short_lived_token)

    ig_account_id, token_for_ig = _fetch_connected_ig_account(access_token)
    if not ig_account_id:
        return {
            "status": "error",
            "message": "No Instagram Business Account found on managed pages",
        }

    _save_ig_connection(uid, ig_account_id, token_for_ig or access_token)
    return {"status": "success", "ig_account_id": ig_account_id}


def disconnect_instagram(uid):
    con = get_db()
    cur = con.cursor()

    cur.execute(
        """
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source=?
        """,
        (uid, SOURCE),
    )

    cur.execute("DELETE FROM instagram_connections WHERE uid=?", (uid,))

    cur.execute(
        """
        UPDATE connector_configs
        SET status='disconnected'
        WHERE uid=? AND connector=?
        """,
        (uid, SOURCE),
    )

    con.commit()
    con.close()
