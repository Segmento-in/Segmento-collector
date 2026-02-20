import requests
import sqlite3
import datetime
import json

DB = "identity.db"

TOKEN_URL = "https://id.twitch.tv/oauth2/token"
BASE = "https://api.twitch.tv/helix"


# ---------------- DB ----------------

def db():
    return sqlite3.connect(DB, timeout=30)


# ---------------- APP CREDENTIALS ----------------
# Stored in connector_configs

def get_app_credentials(uid):

    con = db()
    cur = con.cursor()

    cur.execute("""
        SELECT config_json
        FROM connector_configs
        WHERE uid=? AND connector='twitch'
        LIMIT 1
    """,(uid,))

    row = cur.fetchone()
    con.close()

    if not row:
        raise Exception("Twitch config missing")

    return json.loads(row[0])


# ---------------- TOKEN CACHE ----------------

_token_cache = {}


def get_token(uid):

    creds = get_app_credentials(uid)

    client_id = creds["client_id"]
    client_secret = creds["client_secret"]

    cache = _token_cache.get(uid)

    if cache and cache["expiry"] > datetime.datetime.utcnow():
        return cache["token"], client_id

    r = requests.post(
        TOKEN_URL,
        params={
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials"
        },
        timeout=20
    )

    if r.status_code != 200:
        raise Exception(r.text)

    data = r.json()

    token = data["access_token"]

    _token_cache[uid] = {
        "token": token,
        "expiry": datetime.datetime.utcnow()
        + datetime.timedelta(seconds=data["expires_in"] - 60)
    }

    return token, client_id


# ---------------- API ----------------

def twitch_get(uid, path, params=None):

    token, client_id = get_token(uid)

    headers = {
        "Client-ID": client_id,
        "Authorization": f"Bearer {token}"
    }

    r = requests.get(
        f"{BASE}/{path}",
        headers=headers,
        params=params,
        timeout=20
    )

    if r.status_code != 200:
        raise Exception(r.text)

    return r.json()["data"]


# ---------------- SYNC ----------------

def sync_videos(uid, username, sync_type="historical", limit=20):

    users = twitch_get(uid, "users", {"login": username})

    if not users:
        raise Exception("User not found")

    user_id = users[0]["id"]

    con = db()
    cur = con.cursor()

    last_video_date = None

    if sync_type == "incremental":
        cur.execute("""
            SELECT state_json
            FROM connector_state
            WHERE uid=? AND source='twitch'
        """,(uid,))
        row = cur.fetchone()

        if row:
            last_video_date = json.loads(row[0]).get("last_video_date")

    videos = twitch_get(uid,"videos",{
        "user_id":user_id,
        "first":limit
    })

    now=datetime.datetime.utcnow().isoformat()

    rows=[]
    newest=last_video_date

    for v in videos:

        created=v["created_at"]

        if sync_type=="incremental" and last_video_date:
            if created<=last_video_date:
                continue

        cur.execute("""
        INSERT OR IGNORE INTO twitch_videos
        (uid,twitch_id,video_id,title,
         views,duration,created_at,
         raw_json,fetched_at)
        VALUES (?,?,?,?,?,?,?,?,?)
        """,(
            uid,
            user_id,
            v["id"],
            v["title"],
            v["view_count"],
            v["duration"],
            created,
            json.dumps(v),
            now
        ))

        if cur.rowcount>0:
            rows.append({
                "video_id":v["id"],
                "title":v["title"],
                "views":v["view_count"]
            })

            if not newest or created>newest:
                newest=created

    if newest:
        cur.execute("""
        INSERT OR REPLACE INTO connector_state
        (uid,source,state_json,updated_at)
        VALUES (?,?,?,?)
        """,(
            uid,
            "twitch",
            json.dumps({
                "username":username,
                "last_video_date":newest
            }),
            now
        ))

    con.commit()
    con.close()

    return {
        "videos":len(rows),
        "rows":rows
    }