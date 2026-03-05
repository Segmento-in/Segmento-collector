import requests
import sqlite3
import datetime
import json
import time

from security.crypto import encrypt_value, decrypt_value
from security.secure_fetch import fetchone_secure
from destinations.destination_router import push_to_destination

DB = "identity.db"
SOURCE = "socialinsider"
BASE_URL = "https://api.socialinsider.io"

def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con

# ---------------- CREDENTIALS ----------------

def get_credentials(uid):
    con = get_db()
    cur = con.cursor()
    cur.execute("""
        SELECT api_key, scopes, access_token
        FROM connector_configs
        WHERE uid=? AND connector='socialinsider'
        LIMIT 1
    """, (uid,))
    row = fetchone_secure(cur)
    con.close()

    if not row:
        raise Exception("Social Insider not configured")

    api_key  = decrypt_value(row["api_key"])
    platform = decrypt_value(row["scopes"])        # stored in scopes column
    handle   = decrypt_value(row["access_token"])  # stored in access_token column

    return api_key, platform, handle

def save_credentials(uid, api_key, platform, handle):
    con = get_db()
    cur = con.cursor()

    enc_key      = encrypt_value(api_key)
    enc_platform = encrypt_value(platform)
    enc_handle   = encrypt_value(handle)

    cur.execute("""
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, api_key, scopes, access_token, status, created_at)
        VALUES (?, 'socialinsider', ?, ?, ?, 'configured', datetime('now'))
    """, (uid, enc_key, enc_platform, enc_handle))

    con.commit()
    con.close()

# ---------------- STATE ----------------

def get_state(uid):
    con = get_db()
    cur = con.cursor()
    cur.execute("""
        SELECT state_json
        FROM connector_state
        WHERE uid=? AND source='socialinsider'
        LIMIT 1
    """, (uid,))
    row = cur.fetchone()
    con.close()

    if not row:
        return {"last_sync_date": None}

    try:
        return json.loads(row[0])
    except Exception:
        return {"last_sync_date": None}

def save_state(uid, state):
    con = get_db()
    cur = con.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO connector_state
        (uid, source, state_json, updated_at)
        VALUES (?, 'socialinsider', ?, ?)
    """, (
        uid,
        json.dumps(state),
        datetime.datetime.utcnow().isoformat()
    ))
    con.commit()
    con.close()

# ---------------- DESTINATION ----------------

def get_active_destination(uid):
    con = get_db()
    cur = con.cursor()
    cur.execute("""
        SELECT dest_type, host, port, username, password, database_name
        FROM destination_configs
        WHERE uid=? AND source=? AND is_active=1
        LIMIT 1
    """, (uid, SOURCE))
    row = fetchone_secure(cur)
    con.close()

    if not row:
        return None

    return {
        "type":          row["dest_type"],
        "host":          row["host"],
        "port":          row["port"],
        "username":      row["username"],
        "password":      row["password"],
        "database_name": row["database_name"],
    }

# ---------------- HTTP HELPER ----------------

def _si_request(api_key, method, path, payload=None, params=None, retries=3):
    url = BASE_URL + path
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    for attempt in range(retries):
        try:
            if method == "GET":
                r = requests.get(url, headers=headers, params=params, timeout=30)
            else:
                r = requests.post(url, headers=headers, json=payload, params=params, timeout=30)

            if r.status_code == 429:
                wait = 2 ** attempt
                time.sleep(wait)
                continue

            if r.status_code == 401:
                raise Exception("Unauthorized: Invalid Social Insider API Key")
            
            if r.status_code == 403:
                raise Exception("Forbidden: Unauthorized account access")

            if r.status_code >= 500:
                wait = 2 ** attempt
                time.sleep(wait)
                continue

            r.raise_for_status()
            return r.json()

        except requests.exceptions.RequestException as e:
            if attempt == retries - 1:
                raise
            time.sleep(2 ** attempt)

    raise Exception(f"Social Insider {method} request failed after retries")

# ---------------- INSERT HELPERS ----------------

def _insert_posts(uid, platform, handle, posts):
    if not posts:
        return
    con = get_db()
    cur = con.cursor()
    now = datetime.datetime.utcnow().isoformat()
    for post in posts:
        cur.execute("""
            INSERT OR REPLACE INTO socialinsider_posts
            (uid, platform, handle, post_id, publish_date, content_type,
             engagement, reach, impressions, saves, video_views, raw_json, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            uid, platform, handle,
            post.get("post_id"),
            post.get("publish_date"),
            post.get("content_type"),
            post.get("engagement"),
            post.get("reach"),
            post.get("impressions"),
            post.get("saves"),
            post.get("video_views"),
            json.dumps(post),
            now
        ))
    con.commit()
    con.close()

def _insert_profile_insights(uid, platform, handle, insights):
    if not insights:
        return
    con = get_db()
    cur = con.cursor()
    now = datetime.datetime.utcnow().isoformat()
    for entry in insights:
        cur.execute("""
            INSERT OR REPLACE INTO socialinsider_profile_insights
            (uid, platform, handle, follower_count, follower_growth,
             gender_distribution, age_distribution, geo_distribution, industry, raw_json, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            uid, platform, handle,
            entry.get("follower_count"),
            entry.get("follower_growth"),
            json.dumps(entry.get("gender_distribution")),
            json.dumps(entry.get("age_distribution")),
            json.dumps(entry.get("geo_distribution")),
            entry.get("industry"),
            json.dumps(entry),
            now
        ))
    con.commit()
    con.close()

# ---------------- CORE FUNCTIONS ----------------

def connect_socialinsider(uid):
    try:
        api_key, platform, handle = get_credentials(uid)
    except Exception as e:
        return {"status": "error", "message": str(e)}

    try:
        # Validate by fetching a small sample or calling a test endpoint
        # Social Insider doesn't have a specific ping endpoint in the request, 
        _si_request(api_key, "GET", "/api/v1/account")

    except Exception as e:
        return {"status": "error", "message": f"Invalid Social Insider credentials: {e}"}

    con = get_db()
    cur = con.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO google_connections
        (uid, source, enabled)
        VALUES (?, 'socialinsider', 1)
    """, (uid,))
    cur.execute("""
        INSERT OR REPLACE INTO socialinsider_connections
        (uid, api_key, platform, handle, created_at)
        VALUES (?, ?, ?, ?, ?)
    """, (uid, encrypt_value(api_key), platform, handle, datetime.datetime.utcnow().isoformat()))
    cur.execute("""
        UPDATE connector_configs
        SET status='connected'
        WHERE uid=? AND connector='socialinsider'
    """, (uid,))
    con.commit()
    con.close()

    return {"status": "success"}

def disconnect_socialinsider(uid):
    con = get_db()
    cur = con.cursor()
    cur.execute("UPDATE google_connections SET enabled=0 WHERE uid=? AND source='socialinsider'", (uid,))
    cur.execute("DELETE FROM socialinsider_connections WHERE uid=?", (uid,))
    cur.execute("UPDATE connector_configs SET status='disconnected' WHERE uid=? AND connector='socialinsider'", (uid,))
    con.commit()
    con.close()
    return {"status": "success"}

def sync_socialinsider(uid, sync_type="historical"):
    try:
        api_key, platform, handle = get_credentials(uid)
    except Exception as e:
        return {"status": "error", "message": str(e)}

    state = get_state(uid)
    now = datetime.datetime.utcnow()
    
    if sync_type == "historical":
        start_date = (now - datetime.timedelta(days=365)).strftime("%Y-%m-%d")
    else:
        start_date = state.get("last_sync_date") or (now - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    
    end_date = now.strftime("%Y-%m-%d")
    
    all_data = []
    
    # 1. Fetch Posts
    posts = []
    offset = 0
    limit = 100
    while True:
        res = _si_request(api_key, "POST", "/api/v1/posts", payload={
            "platform": platform,
            "profile_name": handle,
            "date_start": start_date,
            "date_end": end_date,
            "offset": offset,
            "limit": limit
        })
        batch = res.get("data", [])
        posts.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
    
    if posts:
        _insert_posts(uid, platform, handle, posts)
        all_data.extend(posts)

    # 2. Fetch Profile Insights
    # Note: Profile insights might not need date range in the same way, but usually it does.
    insights_res = _si_request(api_key, "GET", "/api/v1/account")
    insights = insights_res.get("data", [])
    if isinstance(insights, dict): # Handle case where it's a single object
        insights = [insights]
    
    if insights:
        _insert_profile_insights(uid, platform, handle, insights)
        all_data.extend(insights)

    # Push to Destination
    dest_cfg = get_active_destination(uid)
    rows_pushed = 0
    if dest_cfg and all_data:
        rows_pushed = push_to_destination(dest_cfg, SOURCE, all_data)

    # Update State
    state["last_sync_date"] = end_date
    save_state(uid, state)

    return {
        "status": "success",
        "rows_found": len(all_data),
        "rows_pushed": rows_pushed,
        "posts": len(posts),
        "insights": len(insights),
        "sync_type": sync_type
    }
