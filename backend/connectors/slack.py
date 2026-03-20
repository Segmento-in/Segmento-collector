import datetime
import json
import sqlite3
import time

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure

DB = "identity.db"
SOURCE = "slack"
API_BASE = "https://slack.com/api"


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(message: str):
    print(f"[SLACK] {message}", flush=True)


def _mask_token(token: str | None) -> str | None:
    if not token:
        return None
    if len(token) <= 8:
        return "*" * len(token)
    return f"{token[:4]}{'*' * (len(token) - 8)}{token[-4:]}"


def _get_config(uid: str) -> dict | None:
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT config_json
        FROM connector_configs
        WHERE uid=? AND connector=?
        LIMIT 1
        """,
        (uid, SOURCE),
    )
    row = fetchone_secure(cur)
    con.close()

    if not row or not row.get("config_json"):
        return None

    try:
        return json.loads(row["config_json"])
    except Exception:
        return None


def _update_status(uid: str, status: str):
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        UPDATE connector_configs
        SET status=?
        WHERE uid=? AND connector=?
        """,
        (status, uid, SOURCE),
    )
    con.commit()
    con.close()


def _set_connection_enabled(uid: str, enabled: bool):
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        UPDATE google_connections
        SET enabled=?
        WHERE uid=? AND source=?
        """,
        (1 if enabled else 0, uid, SOURCE),
    )

    if cur.rowcount == 0:
        cur.execute(
            """
            INSERT INTO google_connections (uid, source, enabled)
            VALUES (?, ?, ?)
            """,
            (uid, SOURCE, 1 if enabled else 0),
        )

    con.commit()
    con.close()


def save_config(uid: str, bot_token: str):
    config = {
        "bot_token": bot_token.strip(),
    }

    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, config_json, status, created_at)
        VALUES (?, ?, ?, 'pending', ?)
        """,
        (
            uid,
            SOURCE,
            encrypt_value(json.dumps(config)),
            datetime.datetime.now(datetime.UTC).isoformat(),
        ),
    )
    con.commit()
    con.close()
    _log(f"Config saved for uid={uid}")


def _request_with_retry(method: str, url: str, headers: dict, params: dict = None, retries: int = 5, timeout: int = 40):
    for attempt in range(retries):
        try:
            res = requests.request(method, url, headers=headers, params=params, timeout=timeout)
        except requests.RequestException as e:
            if attempt == retries - 1:
                raise
            wait_s = min(2 ** attempt, 30)
            _log(f"network error (attempt {attempt + 1}): {e}; sleeping {wait_s}s")
            time.sleep(wait_s)
            continue

        if res.status_code == 429:
            retry_after = res.headers.get("Retry-After")
            try:
                retry_wait = int(retry_after) if retry_after else 0
            except Exception:
                retry_wait = 0
            exp_wait = min(2 ** attempt, 60)
            wait_s = max(retry_wait, exp_wait)
            if attempt == retries - 1:
                return res
            _log(f"rate limited; sleeping {wait_s}s")
            time.sleep(wait_s)
            continue

        if res.status_code in (500, 502, 503, 504):
            if attempt == retries - 1:
                return res
            wait_s = min(2 ** attempt, 30)
            _log(f"server error {res.status_code}; sleeping {wait_s}s")
            time.sleep(wait_s)
            continue

        if res.status_code != 200:
             _log(f"HTTP error {res.status_code}: {res.text[:200]}")
             if attempt == retries - 1:
                 return res
             time.sleep(min(2 ** attempt, 10))
             continue

        return res
    return res


def connect_slack(uid: str) -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Slack not configured for this user"}

    bot_token = cfg.get("bot_token")
    if not bot_token:
        return {"status": "error", "message": "Missing Slack Bot Token"}

    headers = {
        "Authorization": f"Bearer {bot_token}",
        "Content-Type": "application/json"
    }

    try:
        res = _request_with_retry("POST", f"{API_BASE}/auth.test", headers=headers)
        data = res.json()
        if not data.get("ok"):
             _log(f"Connection failed for uid={uid}: {data.get('error')}")
             _update_status(uid, "error")
             _set_connection_enabled(uid, False)
             return {"status": "error", "message": f"Slack API Error: {data.get('error')}"}
        
        team_name = data.get("team")
        team_id = data.get("team_id")
    except Exception as exc:
        _log(f"Connection failed for uid={uid}: {exc}")
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    _log(f"Connected uid={uid} team={team_name} ({team_id})")
    return {
        "status": "success",
        "team": team_name,
        "team_id": team_id,
        "bot_token": _mask_token(bot_token)
    }

def disconnect_slack(uid: str) -> dict:
    _set_connection_enabled(uid, False)
    _update_status(uid, "disconnected")
    _log(f"Disconnected uid={uid}")
    return {"status": "disconnected"}


def _get_active_destination(uid: str) -> dict | None:
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
    row = cur.fetchone()
    con.close()

    if not row:
        return None

    return {
        "type": row[0],
        "host": row[1],
        "port": row[2],
        "username": row[3],
        "password": row[4],
        "database_name": row[5],
    }

def _push_rows(dest_cfg: dict | None, route_source: str, label: str, rows: list[dict]) -> int:
    if not dest_cfg:
        _log(f"No active destination configured. Skipping push for {label}")
        return 0

    if not rows:
        _log(f"No rows generated for {label}. Skipping push")
        return 0

    _log(
        f"Pushing {len(rows)} rows to destination "
        f"(route_source={route_source}, label={label}, dest_type={dest_cfg.get('type')})"
    )

    pushed = push_to_destination(dest_cfg, route_source, rows)

    _log(
        f"Destination push completed "
        f"(route_source={route_source}, label={label}, rows_pushed={pushed})"
    )

    return pushed


def _fetch_paginated_slack(endpoint: str, headers: dict, params: dict = None, data_key: str = None) -> list[dict]:
    if params is None:
        params = {}
    params.setdefault("limit", 200)
    
    all_results = []
    cursor = None
    
    while True:
        if cursor:
            params["cursor"] = cursor
            
        res = _request_with_retry("GET", f"{API_BASE}/{endpoint}", headers=headers, params=params)
        data = res.json()
        
        if not data.get("ok"):
            _log(f"Error fetching {endpoint}: {data.get('error')}")
            break
            
        items = data.get(data_key, [])
        all_results.extend(items)
        
        cursor = data.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break
            
    return all_results

def sync_slack(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Slack not configured"}

    bot_token = cfg.get("bot_token")
    if not bot_token:
        return {"status": "error", "message": "Missing Slack Bot Token"}

    headers = {
        "Authorization": f"Bearer {bot_token}",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    try:
         # Verify connection again briefly
         res = _request_with_retry("POST", f"{API_BASE}/auth.test", headers={"Authorization": f"Bearer {bot_token}", "Content-Type": "application/json"})
         if not res.json().get("ok"):
             raise Exception(f"Auth test failed: {res.json().get('error')}")
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    dest_cfg = _get_active_destination(uid)
    now = datetime.datetime.now(datetime.UTC).isoformat() + "Z"

    if dest_cfg:
        _log(f"Active destination found for uid={uid}: type={dest_cfg.get('type')}")
    else:
        _log(f"No active destination found for uid={uid} source={SOURCE}")

    total_rows_pushed = 0

    # 1. Fetch Users
    _log(f"uid={uid} fetching users...")
    users = _fetch_paginated_slack("users.list", headers, data_key="members")
    
    user_rows = []
    for user in users:
        user_rows.append({
            "uid": uid,
            "source": "slack_users",
            "user_id": user.get("id"),
            "name": user.get("name"),
            "real_name": user.get("real_name"),
            "is_bot": user.get("is_bot"),
            "deleted": user.get("deleted"),
            "raw_json": json.dumps(user, default=str),
            "fetched_at": now
        })
        
    pushed = _push_rows(dest_cfg, SOURCE, "slack_users", user_rows)
    total_rows_pushed += pushed

    # 2. Fetch Channels
    _log(f"uid={uid} fetching channels...")
    channels = _fetch_paginated_slack("conversations.list", headers, params={"types": "public_channel,private_channel"}, data_key="channels")
    
    channel_rows = []
    channel_ids = []
    for channel in channels:
        ch_id = channel.get("id")
        channel_ids.append(ch_id)
        channel_rows.append({
            "uid": uid,
            "source": "slack_channels",
            "channel_id": ch_id,
            "name": channel.get("name"),
            "is_private": channel.get("is_private"),
            "is_archived": channel.get("is_archived"),
            "member_count": channel.get("num_members"),
            "raw_json": json.dumps(channel, default=str),
            "fetched_at": now
        })
        
    pushed = _push_rows(dest_cfg, SOURCE, "slack_channels", channel_rows)
    total_rows_pushed += pushed

    # 3. Fetch Messages for each channel
    _log(f"uid={uid} discovered {len(channel_ids)} channels.")
    messages_found = 0
    
    for ch_id in channel_ids:
        _log(f"uid={uid} fetching messages for channel {ch_id}...")
        
        # Slack conversations.history needs 'channel' param
        messages = _fetch_paginated_slack("conversations.history", headers, params={"channel": ch_id}, data_key="messages")
        messages_found += len(messages)
        
        message_rows = []
        for msg in messages:
            message_rows.append({
                "uid": uid,
                "source": "slack_messages",
                "message_id": msg.get("client_msg_id") or msg.get("ts"), # Fallback to ts if no client_msg_id
                "channel_id": ch_id,
                "user_id": msg.get("user") or msg.get("bot_id"),
                "ts": msg.get("ts"),
                "text": msg.get("text"),
                "type": msg.get("type"),
                "subtype": msg.get("subtype"),
                "raw_json": json.dumps(msg, default=str),
                "fetched_at": now
            })
            
        pushed = _push_rows(dest_cfg, SOURCE, f"slack_messages:{ch_id}", message_rows)
        total_rows_pushed += pushed

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")

    result = {
        "status": "success",
        "channels_processed": len(channel_ids),
        "users_found": len(users),
        "messages_found": messages_found,
        "rows_pushed": total_rows_pushed,
        "sync_type": sync_type,
    }
    if not dest_cfg:
        result["message"] = "No active destination configured"
    return result
