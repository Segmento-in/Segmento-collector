import requests
import sqlite3
import datetime
import json
import os
from dotenv import load_dotenv

load_dotenv()

DB = "identity.db"

CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")

TOKEN_URL = "https://id.twitch.tv/oauth2/token"
BASE = "https://api.twitch.tv/helix"


# ---------------- DB ----------------

def db():
    return sqlite3.connect(DB, timeout=30)


# ---------------- AUTH ----------------

_token = None
_token_expiry = None


def get_token():

    global _token, _token_expiry

    if _token and _token_expiry > datetime.datetime.now():
        return _token

    if not CLIENT_ID or not CLIENT_SECRET:
        raise Exception("Twitch credentials missing")

    r = requests.post(TOKEN_URL, params={
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials"
    })

    if r.status_code != 200:
        raise Exception(r.text)

    data = r.json()

    _token = data["access_token"]
    _token_expiry = datetime.datetime.now() + datetime.timedelta(
        seconds=data["expires_in"] - 60
    )

    return _token


# ---------------- API ----------------

def twitch_get(path, params=None):

    token = get_token()

    headers = {
        "Client-ID": CLIENT_ID,
        "Authorization": f"Bearer {token}"
    }

    url = f"{BASE}/{path}"

    r = requests.get(url, headers=headers, params=params, timeout=20)

    if r.status_code != 200:
        raise Exception(r.text)

    return r.json()["data"]


# ---------------- SYNC USER ----------------

def sync_user(uid, username):

    users = twitch_get("users", {
        "login": username
    })

    if not users:
        raise Exception("User not found")

    u = users[0]

    con = db()
    cur = con.cursor()

    now = datetime.datetime.now().isoformat()

    cur.execute("""
    INSERT OR REPLACE INTO twitch_users
    (uid, twitch_id, login, display_name,
     description, followers, view_count,
     raw_json, fetched_at)
    VALUES (?,?,?,?,?,?,?,?,?)
    """, (
        uid,
        u["id"],
        u["login"],
        u["display_name"],
        u["description"],
        None,
        u["view_count"],
        json.dumps(u),
        now
    ))

    con.commit()
    con.close()

    return {"user": u["display_name"]}


# ---------------- SYNC STREAM ----------------

def sync_stream(uid, username):

    users = twitch_get("users", {
        "login": username
    })

    if not users:
        raise Exception("User not found")

    user_id = users[0]["id"]

    streams = twitch_get("streams", {
        "user_id": user_id
    })

    if not streams:
        return {"stream": "offline"}

    s = streams[0]

    con = db()
    cur = con.cursor()

    now = datetime.datetime.now().isoformat()

    cur.execute("""
    INSERT INTO twitch_streams
    (uid, twitch_id, title, game_name,
     viewer_count, started_at,
     raw_json, fetched_at)
    VALUES (?,?,?,?,?,?,?,?)
    """, (
        uid,
        user_id,
        s["title"],
        s["game_name"],
        s["viewer_count"],
        s["started_at"],
        json.dumps(s),
        now
    ))

    con.commit()
    con.close()

    return {"stream": "live"}


def sync_videos(uid, username, limit=20, sync_type="historical"):

    # ---------------- GET USER ----------------
    users = twitch_get("users", {"login": username})

    if not users:
        raise Exception("User not found")

    user_id = users[0]["id"]

    # ---------------- GET LAST STATE ----------------
    last_video_date = None

    con = db()
    cur = con.cursor()

    if sync_type == "incremental":
        cur.execute("""
            SELECT state_json
            FROM connector_state
            WHERE uid=? AND source='twitch'
        """, (uid,))
        row = cur.fetchone()

        if row:
            state = json.loads(row[0])
            last_video_date = state.get("last_video_date")

    # ---------------- FETCH VIDEOS ----------------
    videos = twitch_get("videos", {
        "user_id": user_id,
        "first": limit
    })

    now = datetime.datetime.utcnow().isoformat()

    rows = []
    newest_created_at = last_video_date

    for v in videos:

        created_at = v.get("created_at")

        # Incremental filter
        if sync_type == "incremental" and last_video_date:
            if created_at <= last_video_date:
                continue

        row_dict = {
            "uid": uid,
            "twitch_id": user_id,
            "video_id": v.get("id"),
            "title": v.get("title"),
            "views": v.get("view_count"),
            "duration": v.get("duration"),
            "created_at": created_at
        }

        cur.execute("""
            INSERT OR IGNORE INTO twitch_videos
            (uid, twitch_id, video_id,
             title, views, duration,
             created_at, raw_json, fetched_at)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            uid,
            user_id,
            v.get("id"),
            v.get("title"),
            v.get("view_count"),
            v.get("duration"),
            created_at,
            json.dumps(v),
            now
        ))

        if cur.rowcount > 0:
            rows.append(row_dict)

            if not newest_created_at or created_at > newest_created_at:
                newest_created_at = created_at

    # ---------------- SAVE STATE ----------------
    if newest_created_at:
        cur.execute("""
            INSERT OR REPLACE INTO connector_state
            (uid, source, state_json, updated_at)
            VALUES (?, 'twitch', ?, ?)
        """, (
            uid,
            json.dumps({
                "username": username,
                "last_video_date": newest_created_at
            }),
            now
        ))

    con.commit()
    con.close()

    return {
        "videos": len(rows),
        "rows": rows
    }