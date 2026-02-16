import requests,sqlite3,time,os,json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

DB="identity.db"

# Use a working default instance
INSTANCE=os.getenv("PEERTUBE_INSTANCE","https://framatube.org").rstrip("/")

API_BASE=INSTANCE+"/api/v1"

HEADERS={
    "User-Agent":"SegmentoCollector/1.0",
    "Accept":"application/json"
}


# ---------------- Utils ----------------

def safe_json(obj):
    try:
        return json.dumps(obj,ensure_ascii=False)
    except:
        return "{}"


# ---------------- DB ----------------

def db():
    con=sqlite3.connect(DB,timeout=90,check_same_thread=False,isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con


# ---------------- HTTP ----------------

def safe_get(path,params=None):

    url=API_BASE+path

    try:
        r=requests.get(url,headers=HEADERS,params=params,timeout=25)

        print("PEERTUBE URL:",r.url)
        print("PEERTUBE STATUS:",r.status_code)

        if r.status_code==200:
            return r.json()

        print("PEERTUBE BODY:",r.text[:200])

    except Exception as e:
        print("PEERTUBE ERROR:",e)

    return None


# ---------------- State ----------------

def get_last_time(uid):

    con=db()
    cur=con.cursor()

    cur.execute("""
    SELECT last_published_at
    FROM peertube_state
    WHERE uid=?
    """,(uid,))

    row=cur.fetchone()
    con.close()

    return row[0] if row and row[0] else "1970-01-01T00:00:00Z"


def save_last_time(uid,ts):

    con=db()
    cur=con.cursor()

    cur.execute("""
    INSERT OR REPLACE INTO peertube_state
    (uid,instance,last_published_at)
    VALUES(?,?,?)
    """,(uid,INSTANCE,ts))

    con.close()


# ---------------- Normalizer ----------------

def get_data(res):

    if not isinstance(res,dict):
        return []

    data=res.get("data")

    if isinstance(data,list):
        return data

    return []


# ---------------- Inserts ----------------

def insert_videos(uid,rows):

    if not rows:
        return

    con=db()
    cur=con.cursor()

    now=datetime.utcnow().isoformat()

    data=[]

    for v in rows:

        ch=v.get("channel") or {}

        data.append((
            uid,
            INSTANCE,
            v.get("uuid"),
            v.get("name"),
            v.get("description"),
            v.get("duration"),
            v.get("views"),
            v.get("likes"),
            v.get("dislikes"),
            v.get("publishedAt"),
            ch.get("name"),
            v.get("url"),
            str(v.get("category")),
            str(v.get("language")),
            safe_json(v),
            now
        ))

    cur.executemany("""
    INSERT OR IGNORE INTO peertube_videos
    (uid,instance,video_id,name,description,duration,views,likes,dislikes,
     published_at,channel_name,url,category,language,raw_json,fetched_at)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """,data)

    con.close()


def insert_channels(uid,rows):

    if not rows:
        return

    con=db()
    cur=con.cursor()

    now=datetime.utcnow().isoformat()

    data=[]

    for c in rows:

        data.append((
            uid,
            INSTANCE,
            c.get("uuid"),
            c.get("name"),
            c.get("displayName"),
            c.get("followersCount"),
            c.get("url"),
            safe_json(c),
            now
        ))

    cur.executemany("""
    INSERT OR IGNORE INTO peertube_channels
    (uid,instance,channel_id,name,display_name,followers,url,raw_json,fetched_at)
    VALUES (?,?,?,?,?,?,?,?,?)
    """,data)

    con.close()


# ---------------- Fetchers ----------------

def fetch_videos(params):

    res=safe_get("/videos",params)

    return get_data(res)


def fetch_channels():

    res=safe_get("/video-channels",{"count":50})

    return get_data(res)


def fetch_search(term,limit):

    res=safe_get("/search/videos",{
        "search":term,
        "count":limit
    })

    return get_data(res)


# ---------------- Main Sync ----------------

def sync_peertube(uid, sync_type="historical", limit=50):

    instance = INSTANCE
    last_ts = "1970-01-01T00:00:00Z"

    con = db()
    cur = con.cursor()

    cur.execute("""
        SELECT state_json
        FROM connector_state
        WHERE uid=? AND source='peertube'
    """, (uid,))

    row = cur.fetchone()

    if row:
        state = json.loads(row[0])
        instance = state.get("instance", INSTANCE)
        last_ts = state.get("last_published_at", last_ts)

    con.close()

    api_base = instance.rstrip("/") + "/api/v1"

    def fetch(path, params=None):
        try:
            r = requests.get(
                api_base + path,
                headers=HEADERS,
                params=params,
                timeout=25
            )

            if r.status_code == 200:
                data = r.json()
                return data.get("data", []) if isinstance(data, dict) else []

        except Exception as e:
            print("PEERTUBE ERROR:", e)

        return []

    latest = fetch("/videos", {
        "count": limit,
        "sort": "-publishedAt"
    })

    rows = []
    newest_ts = last_ts

    for v in latest:

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

        rows.append(row_dict)

        if ts and ts > newest_ts:
            newest_ts = ts

    if rows:

        con = db()
        cur = con.cursor()

        now = datetime.utcnow().isoformat()

        for r in rows:
            cur.execute("""
                INSERT OR IGNORE INTO peertube_videos
                (uid, instance, video_id, name, description,
                 duration, views, likes, dislikes,
                 published_at, channel_name, url,
                 raw_json, fetched_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                r["uid"],
                r["instance"],
                r["video_id"],
                r["name"],
                r["description"],
                r["duration"],
                r["views"],
                r["likes"],
                r["dislikes"],
                r["published_at"],
                r["channel_name"],
                r["url"],
                json.dumps(r),
                now
            ))

        new_state = {
            "instance": instance,
            "last_published_at": newest_ts
        }

        cur.execute("""
            INSERT OR REPLACE INTO connector_state
            (uid, source, state_json, updated_at)
            VALUES (?, 'peertube', ?, ?)
        """, (
            uid,
            json.dumps(new_state),
            now
        ))

        con.commit()
        con.close()

    return {
        "rows": rows,
        "count": len(rows)
    }