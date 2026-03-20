import requests
import datetime
import json
import time
import sqlite3
from urllib.parse import urlencode

from backend.security.crypto import decrypt_value
from backend.security.secure_fetch import fetchone_secure
from backend.destinations.destination_router import push_to_destination

DB = "identity.db"
SOURCE = "whatsapp"
GRAPH_VERSION = "v25.0"
GRAPH_BASE = f"https://graph.facebook.com/{GRAPH_VERSION}"

def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con

def get_whatsapp_connection(uid):
    con = get_db()
    cur = con.cursor()
    cur.execute("""
        SELECT waba_id, phone_number_id, access_token_encrypted
        FROM whatsapp_connections
        WHERE uid=?
        LIMIT 1
    """, (uid,))
    row = fetchone_secure(cur)
    con.close()
    
    if not row:
        raise Exception("WhatsApp not connected")
    
    # Decrypt token
    access_token = decrypt_value(row["access_token_encrypted"])
    return row["waba_id"], row["phone_number_id"], access_token

def get_state(uid):
    con = get_db()
    cur = con.cursor()
    cur.execute("""
        SELECT state_json
        FROM connector_state
        WHERE uid=? AND source='whatsapp'
        LIMIT 1
    """, (uid,))
    row = cur.fetchone()
    con.close()
    
    if not row:
        return {"last_sync_date": None}
    
    return json.loads(row[0])

def save_state(uid, state):
    con = get_db()
    cur = con.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO connector_state
        (uid, source, state_json, updated_at)
        VALUES (?, 'whatsapp', ?, ?)
    """, (
        uid,
        json.dumps(state),
        datetime.datetime.utcnow().isoformat()
    ))
    con.commit()
    con.close()

def _safe_get(url, params):
    while True:
        r = requests.get(url, params=params, timeout=60)
        
        # Rate limit
        if r.status_code == 429:
            time.sleep(5)
            continue
            
        data = r.json()
        
        # Token expired
        if isinstance(data, dict) and data.get("error", {}).get("code") == 190:
            raise Exception("WhatsApp token expired")
            
        if r.status_code != 200:
            error_msg = data.get("error", {}).get("message", "Unknown Meta API error")
            raise Exception(f"Meta API Error: {error_msg}")
            
        return data

def sync_whatsapp(uid=None, sync_type="historical"):
    # If uid is not passed (internal call), try to get from global g
    print(f"[WHATSAPP] Sync started for UID {uid}", flush=True)

    if uid is None:
        from flask import g
        uid = getattr(g, "user_id", None)
        
    if not uid:
        raise Exception("UID required for WhatsApp sync")

    waba_id, phone_number_id, access_token = get_whatsapp_connection(uid)
    state = get_state(uid)

    today = datetime.date.today()
    today_str = today.strftime("%Y-%m-%d")

    if sync_type == "incremental" and state.get("last_sync_date"):
        since_date = state.get("last_sync_date")
    else:
        # Default historical window: 30 days
        since_date = (today - datetime.timedelta(days=30)).strftime("%Y-%m-%d")

    con = get_db()
    cur = con.cursor()
    now = datetime.datetime.utcnow().isoformat()
    
    rows_pushed = 0
    all_rows = []

    # 1. WABA Account Info
    url = f"{GRAPH_BASE}/{waba_id}"
    params = {
        "access_token": access_token,
        "fields": "id,name,currency,timezone_id,verification_status,messaging_limit,account_review_status"
    }
    waba_data = _safe_get(url, params)
    
    cur.execute("""
        INSERT OR REPLACE INTO whatsapp_business_accounts
        (uid, waba_id, name, currency, timezone_id, messaging_limit, verification_status, raw_json, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        uid,
        waba_data.get("id"),
        waba_data.get("name"),
        waba_data.get("currency"),
        waba_data.get("timezone_id"),
        json.dumps(waba_data.get("messaging_limit")),
        waba_data.get("verification_status"),
        json.dumps(waba_data),
        now
    ))
    all_rows.append({"table": "whatsapp_business_accounts", "data": waba_data})

    # 2. Phone Numbers
    url = f"{GRAPH_BASE}/{waba_id}/phone_numbers"
    params = {
        "access_token": access_token,
        "fields": "id,display_phone_number,verified_name,quality_rating,status,platform_type",
        "limit": 100
    }
    while url:
        res = _safe_get(url, params)
        for phone in res.get("data", []):
            cur.execute("""
                INSERT OR REPLACE INTO whatsapp_phone_numbers
                (uid, waba_id, phone_number_id, display_phone_number, verified_name, quality_rating, status, platform_type, raw_json, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                uid,
                waba_id,
                phone.get("id"),
                phone.get("display_phone_number"),
                phone.get("verified_name"),
                phone.get("quality_rating"),
                phone.get("status"),
                phone.get("platform_type"),
                json.dumps(phone),
                now
            ))
            all_rows.append({"table": "whatsapp_phone_numbers", "data": phone})
        
        url = res.get("paging", {}).get("next")
        params = None

    # 3. Message Templates
    url = f"{GRAPH_BASE}/{waba_id}/message_templates"
    params = {
        "access_token": access_token,
        "fields": "name,namespace,category,language,status,components",
        "limit": 100
    }
    while url:
        res = _safe_get(url, params)
        for temp in res.get("data", []):
            cur.execute("""
                INSERT OR REPLACE INTO whatsapp_message_templates
                (uid, waba_id, template_name, namespace, category, language, status, raw_json, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                uid,
                waba_id,
                temp.get("name"),
                temp.get("namespace"),
                temp.get("category"),
                temp.get("language"),
                temp.get("status"),
                json.dumps(temp),
                now
            ))
            all_rows.append({"table": "whatsapp_message_templates", "data": temp})
        
        url = res.get("paging", {}).get("next")
        params = None

    # 4. Conversation Analytics
    # Note: Using PHONE-NUMBER-ID as per requirement
    try:
        url = f"{GRAPH_BASE}/{phone_number_id}/conversations"
    except Exception as e:
        print("[WHATSAPP] Conversation analytics unavailable:", e, flush=True)

    params = {
        "access_token": access_token,
        "fields": "conversation_id,start_time,end_time,origin_type,category,messages_sent,messages_received",
        "limit": 100
    }
    # For filtering, we might need a time range if supported, otherwise fetch all and filter or fetch all for 30 days.
    # Cursor pagination handles the rest.
    while url:
        res = _safe_get(url, params)
        for conv in res.get("data", []):
            start_time = conv.get("start_time")
            if not start_time: continue
            
            # Simple check for since_date if API doesn't filter (fallback)
            if start_time < since_date: continue

            conv_date = start_time[:10]
            
            cur.execute("""
                INSERT OR REPLACE INTO whatsapp_conversation_analytics
                (uid, phone_number_id, conversation_id, category, origin_type, start_time, end_time, messages_sent, messages_received, date, raw_json, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                uid,
                phone_number_id,
                conv.get("conversation_id"),
                conv.get("category"),
                conv.get("origin_type"),
                start_time,
                conv.get("end_time"),
                conv.get("messages_sent"),
                conv.get("messages_received"),
                conv_date,
                json.dumps(conv),
                now
            ))
            all_rows.append({"table": "whatsapp_conversation_analytics", "data": conv})
            
        url = res.get("paging", {}).get("next")
        params = None

    # 5. Messaging Insights
    url = f"{GRAPH_BASE}/{phone_number_id}/insights"
    params = {
        "access_token": access_token,
        "fields": "sent,delivered,read,failed",
        "limit": 100
    }
    while url:
        res = _safe_get(url, params)
        for insight in res.get("data", []):
            # The insights API might return a list of day-wise data
            # If so, it usually has a 'start_time' or 'end_time' or 'date'
            insight_date = insight.get("date") or insight.get("start_time", "")[:10]
            if not insight_date: insight_date = today_str
            
            if insight_date < since_date: continue

            cur.execute("""
                INSERT OR REPLACE INTO whatsapp_message_insights
                (uid, phone_number_id, sent, delivered, read, failed, date, raw_json, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                uid,
                phone_number_id,
                insight.get("sent"),
                insight.get("delivered"),
                insight.get("read"),
                insight.get("failed"),
                insight_date,
                json.dumps(insight),
                now
            ))
            all_rows.append({"table": "whatsapp_message_insights", "data": insight})
            
        url = res.get("paging", {}).get("next")
        params = None

    con.commit()
    con.close()

    # Update state
    state["last_sync_date"] = today_str
    save_state(uid, state)

    # Push to destination
    # We aggregate all rows or push by category. Standard seems to be pushing all rows.
    # But usually push_to_destination is called with (dest_cfg, source, rows)
    # Let's find destination config
    from backend.destinations.destination_router import push_to_destination
    
    # helper to get active destination from identity_server context if needed
    # but sync_whatsapp is usually called from universal_sync which handles its own?
    # Actually connectors like instagram.py handle it inside.
    
    from backend.security.secure_fetch import fetchone_secure
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
            "type": row["dest_type"],
            "host": row["host"],
            "port": row["port"],
            "username": row["username"],
            "password": row["password"],
            "database_name": row["database_name"]
        }

    dest_cfg = get_active_destination(uid)

    if dest_cfg:
        rows_pushed = push_to_destination(dest_cfg, SOURCE, all_rows)
    else:
        rows_pushed = 0

    return len(all_rows)

def disconnect_whatsapp(uid):
    con = get_db()
    cur = con.cursor()
    # Remove connection
    cur.execute("DELETE FROM whatsapp_connections WHERE uid=?", (uid,))
    # Remove job
    cur.execute("DELETE FROM connector_jobs WHERE uid=? AND source='whatsapp'", (uid,))
    # Disable connector initialization
    cur.execute("UPDATE google_connections SET enabled=0 WHERE uid=? AND source='whatsapp'", (uid,))
    # Remove state
    cur.execute("DELETE FROM connector_state WHERE uid=? AND source='whatsapp'", (uid,))
    
    con.commit()
    con.close()
