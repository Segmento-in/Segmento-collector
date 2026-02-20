import requests
import sqlite3
import json
import time
from datetime import datetime

DB = "identity.db"

BASE_HEADERS = {
    "User-Agent": "SegmentoCollector/1.0"
}

# DB

def db():
    con = sqlite3.connect(DB, timeout=90,
                          check_same_thread=False,
                          isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con

# CONFIG

def get_config(uid):

    con = db()
    cur = con.cursor()

    cur.execute("""
        SELECT config_json
        FROM connector_configs
        WHERE uid=? AND connector='discourse'
    """,(uid,))

    row = cur.fetchone()
    con.close()

    if not row:
        raise Exception("Discourse config missing")

    return json.loads(row[0])


def get_headers(uid):

    cfg = get_config(uid)

    headers = BASE_HEADERS.copy()

    if cfg.get("api_key"):
        headers["Api-Key"] = cfg["api_key"]
        headers["Api-Username"] = cfg.get("api_user","system")

    return headers


def get_forum(uid):
    return get_config(uid)["forum"].rstrip("/")

# HTTP

def safe_get(uid, path):

    url = f"{get_forum(uid)}{path}"

    try:
        r = requests.get(
            url,
            headers=get_headers(uid),
            timeout=25
        )

        if r.status_code == 200:
            return r.json()

        if r.status_code == 429:
            time.sleep(60)

    except Exception as e:
        print("DISCOURSE ERROR:", e)
        time.sleep(5)

    return None

# STATE

def get_last_topic(uid):

    con=db()
    cur=con.cursor()

    cur.execute("""
        SELECT state_json
        FROM connector_state
        WHERE uid=? AND source='discourse'
    """,(uid,))

    row=cur.fetchone()
    con.close()

    if row:
        return json.loads(row[0]).get("last_topic_id",0)

    return 0


def save_state(uid,tid):

    con=db()
    cur=con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_state
        (uid,source,state_json,updated_at)
        VALUES (?,?,?,?)
    """,(
        uid,
        "discourse",
        json.dumps({"last_topic_id":tid}),
        datetime.utcnow().isoformat()
    ))

    con.close()

# INSERTS

def insert_topics(uid,forum,rows):

    con=db()
    cur=con.cursor()
    now=datetime.utcnow().isoformat()

    cur.executemany("""
        INSERT OR IGNORE INTO discourse_topics
        (uid,forum,topic_id,title,
         posts_count,views,
         created_at,last_posted_at,
         slug,raw_json,fetched_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """,[(
        uid,
        forum,
        t["id"],
        t["title"],
        t["posts_count"],
        t["views"],
        t["created_at"],
        t["last_posted_at"],
        t["slug"],
        json.dumps(t),
        now
    ) for t in rows])

    con.close()

# SYNC

def sync_discourse(uid, sync_type="incremental"):

    forum=get_forum(uid)
    last_id=get_last_topic(uid)

    latest=safe_get(uid,"/latest.json")

    rows=[]
    new_topics=[]

    if latest:

        topics=latest.get(
            "topic_list",{}
        ).get("topics",[])

        for t in topics:

            tid=t.get("id",0)

            if sync_type=="incremental" and tid<=last_id:
                continue

            new_topics.append(t)

            rows.append({
                "uid":uid,
                "forum":forum,
                "topic_id":tid,
                "title":t.get("title"),
                "views":t.get("views")
            })

    if new_topics:
        insert_topics(uid,forum,new_topics)
        save_state(uid,max(t["id"] for t in new_topics))

    return {
        "rows":rows,
        "new_topics":len(new_topics)
    }