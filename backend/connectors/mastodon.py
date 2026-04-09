import requests,sqlite3,time,random,os,json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

DB = os.getenv("DB_PATH", "identity.db")
INSTANCE=os.getenv("MASTODON_INSTANCE","https://mastodon.social")

HEADERS={"User-Agent":"SegmentoCollector/1.0"}


def db():
    con=sqlite3.connect(DB,timeout=90,check_same_thread=False,isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con


def safe_get(url,params=None):
    try:
        r=requests.get(url,headers=HEADERS,params=params,timeout=15)
        if r.status_code==200:
            return r.json()
        if r.status_code in [429,403]:
            time.sleep(60+random.randint(5,15))
    except Exception:
        time.sleep(5)
    return None


def get_last_id(uid):
    con=db()
    cur=con.cursor()
    cur.execute("SELECT last_status_id FROM mastodon_state WHERE uid=?",(uid,))
    row=cur.fetchone()
    con.close()
    return row[0] if row and row[0] else "0"


def save_last_id(uid,sid):
    con=db()
    cur=con.cursor()
    cur.execute("""
    INSERT OR REPLACE INTO mastodon_state(uid,instance,last_status_id)
    VALUES(?,?,?)
    """,(uid,INSTANCE,sid))
    con.close()


def insert_statuses(uid,rows):
    con=db()
    cur=con.cursor()
    now=datetime.utcnow().isoformat()
    data=[]
    for s in rows:
        acc=s.get("account") or {}
        data.append((
            uid,
            INSTANCE,
            s.get("id"),
            s.get("content"),
            acc.get("username"),
            s.get("url"),
            s.get("replies_count"),
            s.get("reblogs_count"),
            s.get("favourites_count"),
            s.get("created_at"),
            s.get("visibility"),
            s.get("language"),
            json.dumps(s,ensure_ascii=False),
            now
        ))
    cur.executemany("""
    INSERT OR IGNORE INTO mastodon_statuses
    (uid,instance,status_id,content,author,url,replies,reblogs,favourites,
     created_at,visibility,language,raw_json,fetched_at)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """,data)
    con.close()


def insert_tags(uid,rows):
    con=db()
    cur=con.cursor()
    now=datetime.utcnow().isoformat()
    data=[]
    for t in rows:
        data.append((
            uid,
            INSTANCE,
            t.get("name"),
            t.get("url"),
            json.dumps(t.get("history"),ensure_ascii=False),
            json.dumps(t,ensure_ascii=False),
            now
        ))
    cur.executemany("""
    INSERT OR IGNORE INTO mastodon_tags
    (uid,instance,tag,url,history,raw_json,fetched_at)
    VALUES(?,?,?,?,?,?,?)
    """,data)
    con.close()


def fetch_public(limit=40):
    return safe_get(f"{INSTANCE}/api/v1/timelines/public",{"limit":limit}) or []


def fetch_local(limit=40):
    return safe_get(f"{INSTANCE}/api/v1/timelines/public",{"local":"true","limit":limit}) or []


def fetch_tags():
    return safe_get(f"{INSTANCE}/api/v1/trends/tags") or []


def fetch_trending():
    return safe_get(f"{INSTANCE}/api/v1/trends/statuses") or []

def sync_mastodon(uid, instance, sync_type="historical", limit=40):

    last_id = get_last_id(uid) if sync_type == "incremental" else "0"

    public = safe_get(f"{instance}/api/v1/timelines/public", {"limit": limit}) or []
    trending = safe_get(f"{instance}/api/v1/trends/statuses") or []

    all_statuses = public + trending

    new_statuses = []
    rows_for_destination = []

    for s in all_statuses:
        sid = s.get("id")
        if not sid:
            continue

        if sync_type == "incremental" and sid <= last_id:
            continue

        new_statuses.append(s)

        rows_for_destination.append({
            "status_id": sid,
            "content": s.get("content"),
            "author": (s.get("account") or {}).get("username"),
            "url": s.get("url"),
            "replies": s.get("replies_count"),
            "reblogs": s.get("reblogs_count"),
            "favourites": s.get("favourites_count"),
            "created_at": s.get("created_at"),
            "visibility": s.get("visibility"),
            "language": s.get("language")
        })

    if new_statuses:
        insert_statuses(uid, new_statuses)
        max_id = max(s["id"] for s in new_statuses)
        save_last_id(uid, max_id)

    return {
        "count": len(new_statuses),
        "rows": rows_for_destination
    }