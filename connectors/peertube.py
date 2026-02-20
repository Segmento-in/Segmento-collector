import requests
import sqlite3
import json
from datetime import datetime

DB = "identity.db"

HEADERS = {
    "User-Agent": "SegmentoCollector/1.0",
    "Accept": "application/json"
}


# ------------------------------------------------
# DB
# ------------------------------------------------

def db():
    con = sqlite3.connect(DB, timeout=90,
                          check_same_thread=False,
                          isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con


# ------------------------------------------------
# CONFIG (SOURCE OF TRUTH)
# ------------------------------------------------

def get_instance(uid):

    con = db()
    cur = con.cursor()

    cur.execute("""
        SELECT config_json
        FROM connector_configs
        WHERE uid=? AND connector='peertube'
        LIMIT 1
    """, (uid,))

    row = cur.fetchone()
    con.close()

    if not row:
        raise Exception("PeerTube config missing")

    return json.loads(row[0])["instance"].rstrip("/")


# ------------------------------------------------
# HTTP
# ------------------------------------------------

def fetch(instance, path, params=None):

    try:
        r = requests.get(
            f"{instance}/api/v1{path}",
            headers=HEADERS,
            params=params,
            timeout=25
        )

        if r.status_code == 200:
            data = r.json()
            if isinstance(data, dict):
                return data.get("data", [])

    except Exception as e:
        print("PEERTUBE ERROR:", e)

    return []


# ------------------------------------------------
# SYNC
# ------------------------------------------------

def sync_peertube(uid, sync_type="historical", limit=50):

    instance = get_instance(uid)

    con = db()
    cur = con.cursor()

    last_ts = "1970-01-01T00:00:00Z"

    # incremental cursor
    cur.execute("""
        SELECT state_json
        FROM connector_state
        WHERE uid=? AND source='peertube'
    """, (uid,))

    row = cur.fetchone()

    if row:
        state = json.loads(row[0])
        last_ts = state.get("last_published_at", last_ts)

    con.close()

    videos = fetch(instance, "/videos", {
        "count": limit,
        "sort": "-publishedAt"
    })

    rows = []
    newest_ts = last_ts
    now = datetime.utcnow().isoformat()

    con = db()
    cur = con.cursor()

    for v in videos:

        ts = v.get("publishedAt")

        if sync_type == "incremental" and ts and ts <= last_ts:
            continue

        row_dict = {
            "uid": uid,
            "instance": instance,
            "video_id": v.get("uuid"),
            "name": v.get("name"),
            "description": v.get("description"),
            "duration": v.get("duration"),
            "views": v.get("views"),
            "likes": v.get("likes"),
            "dislikes": v.get("dislikes"),
            "published_at": ts,
            "channel_name": (v.get("channel") or {}).get("name"),
            "url": v.get("url")
        }

        cur.execute("""
            INSERT OR IGNORE INTO peertube_videos
            (uid,instance,video_id,name,description,
             duration,views,likes,dislikes,
             published_at,channel_name,url,
             raw_json,fetched_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            uid,
            instance,
            row_dict["video_id"],
            row_dict["name"],
            row_dict["description"],
            row_dict["duration"],
            row_dict["views"],
            row_dict["likes"],
            row_dict["dislikes"],
            row_dict["published_at"],
            row_dict["channel_name"],
            row_dict["url"],
            json.dumps(v),
            now
        ))

        if cur.rowcount > 0:
            rows.append(row_dict)

            if ts and ts > newest_ts:
                newest_ts = ts

    # save incremental state
    cur.execute("""
        INSERT OR REPLACE INTO connector_state
        (uid,source,state_json,updated_at)
        VALUES (?,?,?,?)
    """, (
        uid,
        "peertube",
        json.dumps({
            "instance": instance,
            "last_published_at": newest_ts
        }),
        now
    ))

    con.commit()
    con.close()

    return {
        "rows": rows,
        "count": len(rows)
    }