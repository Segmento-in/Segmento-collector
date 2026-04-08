import requests
import sqlite3
import datetime
import json
import os

DB = os.getenv("DB_PATH", "/tmp/identity.db")
BASE = "https://api.telegram.org"


# ---------------- DB ----------------

def db():
    return sqlite3.connect(DB, timeout=30)


# ---------------- GET USER BOT TOKEN ----------------

def get_bot_token(uid):

    con = db()
    cur = con.cursor()

    cur.execute("""
        SELECT bot_token
        FROM telegram_accounts
        WHERE uid=?
    """, (uid,))

    row = cur.fetchone()
    con.close()

    if not row:
        raise Exception("Telegram not connected")

    return row[0]


# ---------------- TELEGRAM API ----------------

def tg_get(uid, path, params=None):

    token = get_bot_token(uid)

    url = f"{BASE}/bot{token}/{path}"

    r = requests.get(url, params=params, timeout=20)

    if r.status_code != 200:
        raise Exception(r.text)

    return r.json()["result"]


# ---------------- SYNC MESSAGES ----------------

def sync_messages(uid, sync_type="historical"):

    con = db()
    cur = con.cursor()

    now = datetime.datetime.utcnow().isoformat()

    # -----------------------------------------
    # GET LAST OFFSET (INCREMENTAL STATE)
    # -----------------------------------------
    cur.execute("""
        SELECT last_update_id
        FROM telegram_state
        WHERE uid=?
    """, (uid,))
    row = cur.fetchone()

    last_offset = None
    if row:
        last_offset = row[0]

    # -----------------------------------------
    # INCREMENTAL MODE
    # -----------------------------------------
    if sync_type == "incremental" and last_offset:
        params = {
            "offset": last_offset,
            "limit": 100
        }
    else:
        params = {
            "limit": 100
        }

    updates = tg_get(uid, "getUpdates", params)

    inserted_rows = []
    max_update_id = None

    for u in updates:

        update_id = u["update_id"]
        msg = u.get("message")

        if not msg:
            continue

        chat = msg["chat"]

        cur.execute("""
            INSERT OR IGNORE INTO telegram_messages
            (uid, channel_id, message_id, text,
             author, date, raw_json, fetched_at)
            VALUES (?,?,?,?,?,?,?,?)
        """, (
            uid,
            chat["id"],
            msg["message_id"],
            msg.get("text"),
            msg.get("from", {}).get("username"),
            msg["date"],
            json.dumps(msg),
            now
        ))

        if cur.rowcount > 0:
            inserted_rows.append({
                "channel_id": chat["id"],
                "message_id": msg["message_id"],
                "text": msg.get("text"),
                "author": msg.get("from", {}).get("username"),
                "date": msg["date"]
            })

        if not max_update_id or update_id > max_update_id:
            max_update_id = update_id

    # -----------------------------------------
    # SAVE NEW OFFSET
    # -----------------------------------------
    if max_update_id:
        cur.execute("""
            INSERT OR REPLACE INTO telegram_state
            (uid, last_update_id)
            VALUES (?,?)
        """, (uid, max_update_id + 1))

    con.commit()
    con.close()

    return {
        "messages": len(inserted_rows),
        "rows": inserted_rows
    }