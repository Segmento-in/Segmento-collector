import requests
import sqlite3
import datetime
import json
import os
import time
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(BASE_DIR, "identity.db")


API = "https://discord.com/api/v10"


# ---------------- DB ----------------

def db():
    con = sqlite3.connect(
        DB,
        timeout=60,
        check_same_thread=False,
        isolation_level=None
    )

    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")

    return con

# ---------------- API (BOT AUTH) ----------------

def get_bot_token(uid):
    con = db()
    cur = con.cursor()

    cur.execute("""
        SELECT bot_token
        FROM discord_connections
        WHERE uid=?
    """, (uid,))

    row = cur.fetchone()
    con.close()

    if not row:
        raise Exception("Discord not connected")

    return row[0]


def discord_get(path, uid, params=None):

    bot_token = get_bot_token(uid)

    headers = {
        "Authorization": f"Bot {bot_token}"
    }

    url = f"{API}{path}"

    r = requests.get(url, headers=headers, params=params, timeout=20)

    if r.status_code == 429:
        data = r.json()
        retry = data.get("retry_after", 5)
        print(f"[DISCORD] Rate limited. Sleeping {retry} sec...")
        time.sleep(float(retry))
        return discord_get(path, uid, params)

    if r.status_code != 200:
        print("[DISCORD ERROR]", r.status_code, r.text[:300])
        return []

    return r.json()

# ---------------- SYNC GUILDS ----------------

def sync_guilds(uid):

    data = discord_get("/users/@me/guilds", uid)

    con = db()
    cur = con.cursor()

    now = datetime.datetime.now().isoformat()

    count = 0

    for g in data:

        cur.execute("""
        INSERT OR REPLACE INTO discord_guilds
        (uid, guild_id, name, raw_json, fetched_at)
        VALUES (?,?,?,?,?)
        """, (
            uid,
            g["id"],
            g["name"],
            json.dumps(g),
            now
        ))

        count += 1

    con.commit()
    con.close()

    return {"guilds": count}


# ---------------- SYNC CHANNELS ----------------

def sync_channels(guild_id, uid):
    
    data = discord_get(f"/guilds/{guild_id}/channels", uid)

    con = db()
    cur = con.cursor()

    now = datetime.datetime.now().isoformat()

    count = 0

    for c in data:

        cur.execute("""
        INSERT OR REPLACE INTO discord_channels
        (uid, channel_id, guild_id,
         name, type, raw_json, fetched_at)
        VALUES (?,?,?,?,?,?,?)
        """, (
            uid,
            c["id"],
            guild_id,
            c["name"],
            c["type"],
            json.dumps(c),
            now
        ))

        count += 1

    con.commit()
    con.close()

    return {"channels": count}


# ---------------- SYNC MESSAGES ----------------
def sync_messages(uid, channel_id, sync_type="historical"):

    con = db()
    cur = con.cursor()

    now = datetime.datetime.utcnow().isoformat()

    inserted_rows = []
    newest_id = None

    last_id = None

    cur.execute("""
        SELECT last_message_id
        FROM discord_state
        WHERE uid=? AND channel_id=?
    """, (uid, channel_id))

    row = cur.fetchone()
    if row:
        last_id = row[0]

    if sync_type == "incremental":

        if not last_id:
            con.close()
            return {"messages": 0, "rows": []}

        params = {
            "limit": 25,
            "after": last_id
        }

    else:
        params = {
            "limit": 50
        }

    data = discord_get(
        f"/channels/{channel_id}/messages",
        uid,
        params=params
    )

    for msg in data:

        message_id = msg["id"]

        cur.execute("""
            INSERT OR IGNORE INTO discord_messages
            (uid, channel_id, message_id, author, content,
             timestamp, raw_json, fetched_at)
            VALUES (?,?,?,?,?,?,?,?)
        """, (
            uid,
            channel_id,
            message_id,
            msg["author"]["username"],
            msg.get("content"),
            msg.get("timestamp"),
            json.dumps(msg),
            now
        ))

        if cur.rowcount > 0:

            inserted_rows.append({
                "channel_id": channel_id,
                "message_id": message_id,
                "author": msg["author"]["username"],
                "content": msg.get("content"),
                "timestamp": msg.get("timestamp")
            })

            if not newest_id or message_id > newest_id:
                newest_id = message_id

    if newest_id:
        cur.execute("""
            INSERT OR REPLACE INTO discord_state
            (uid, channel_id, last_message_id)
            VALUES (?,?,?)
        """, (uid, channel_id, newest_id))

    con.commit()
    con.close()

    return {
        "messages": len(inserted_rows),
        "rows": inserted_rows
    }