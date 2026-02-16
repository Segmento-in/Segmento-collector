import requests,sqlite3,time,os,json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

DB="identity.db"

FORUM=os.getenv("DISCOURSE_FORUM","https://discuss.python.org").rstrip("/")

API_KEY=os.getenv("DISCOURSE_API_KEY")
API_USER=os.getenv("DISCOURSE_API_USER")


HEADERS={
    "User-Agent":"SegmentoCollector/1.0"
}

# Enable private access if key exists
if API_KEY and API_USER:
    HEADERS["Api-Key"]=API_KEY
    HEADERS["Api-Username"]=API_USER


# ---------------- DB ----------------

def db():
    con=sqlite3.connect(DB,timeout=90,check_same_thread=False,isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con


# ---------------- HTTP ----------------

def safe_get(url):
    try:
        r=requests.get(url,headers=HEADERS,timeout=20)
        if r.status_code==200:
            return r.json()
        if r.status_code==429:
            time.sleep(60)
    except Exception:
        time.sleep(5)
    return None


# ---------------- State ----------------

def get_last_topic(uid):
    con=db()
    cur=con.cursor()
    cur.execute("SELECT last_topic_id FROM discourse_state WHERE uid=?",(uid,))
    row=cur.fetchone()
    con.close()
    return row[0] if row and row[0] else 0


def save_last_topic(uid,tid):
    con=db()
    cur=con.cursor()
    cur.execute("""
    INSERT OR REPLACE INTO discourse_state(uid,forum,last_topic_id)
    VALUES(?,?,?)
    """,(uid,FORUM,tid))
    con.close()


# ---------------- Inserts ----------------

def insert_topics(uid,rows):
    con=db()
    cur=con.cursor()
    now=datetime.utcnow().isoformat()
    data=[]
    for t in rows:
        data.append((
            uid,
            FORUM,
            t.get("id"),
            t.get("title"),
            t.get("posts_count"),
            t.get("views"),
            t.get("created_at"),
            t.get("last_posted_at"),
            t.get("slug"),
            json.dumps(t,ensure_ascii=False),
            now
        ))
    cur.executemany("""
    INSERT OR IGNORE INTO discourse_topics
    (uid,forum,topic_id,title,posts_count,views,
     created_at,last_posted_at,slug,raw_json,fetched_at)
    VALUES(?,?,?,?,?,?,?,?,?,?,?)
    """,data)
    con.close()


def insert_categories(uid,rows):
    con=db()
    cur=con.cursor()
    now=datetime.utcnow().isoformat()
    data=[]
    for c in rows:
        data.append((
            uid,
            FORUM,
            c.get("id"),
            c.get("name"),
            c.get("description"),
            c.get("topic_count"),
            c.get("post_count"),
            json.dumps(c,ensure_ascii=False),
            now
        ))
    cur.executemany("""
    INSERT OR IGNORE INTO discourse_categories
    (uid,forum,category_id,name,description,
     topic_count,post_count,raw_json,fetched_at)
    VALUES(?,?,?,?,?,?,?,?,?)
    """,data)
    con.close()


def insert_users(uid,rows):
    con=db()
    cur=con.cursor()
    now=datetime.utcnow().isoformat()
    data=[]
    for u in rows:
        user=u.get("user") or {}
        data.append((
            uid,
            FORUM,
            user.get("id"),
            user.get("username"),
            user.get("name"),
            user.get("trust_level"),
            json.dumps(u,ensure_ascii=False),
            now
        ))
    cur.executemany("""
    INSERT OR IGNORE INTO discourse_users
    (uid,forum,user_id,username,name,
     trust_level,raw_json,fetched_at)
    VALUES(?,?,?,?,?,?,?,?)
    """,data)
    con.close()


# ---------------- Fetchers ----------------

def fetch_latest():
    return safe_get(f"{FORUM}/latest.json")


def fetch_top():
    return safe_get(f"{FORUM}/top.json")


def fetch_categories():
    return safe_get(f"{FORUM}/categories.json")


def fetch_users():
    return safe_get(f"{FORUM}/directory_items.json")


# ---------------- Main Sync ----------------

def sync_discourse(uid, sync_type="incremental"):

    last_id = 0

    if sync_type == "incremental":
        last_id = get_last_topic(uid)

    rows = []

    # -------- Latest Topics --------
    latest = fetch_latest()
    new_topics = []

    if latest:
        topics = latest.get("topic_list", {}).get("topics", [])

        for t in topics:
            tid = t.get("id", 0)

            if sync_type == "incremental":
                if tid <= last_id:
                    continue

            new_topics.append(t)

            rows.append({
                "uid": uid,
                "forum": FORUM,
                "topic_id": tid,
                "title": t.get("title"),
                "posts_count": t.get("posts_count"),
                "views": t.get("views"),
                "created_at": t.get("created_at"),
                "last_posted_at": t.get("last_posted_at"),
                "slug": t.get("slug")
            })

    if new_topics:
        insert_topics(uid, new_topics)
        save_last_topic(uid, max(t["id"] for t in new_topics))

    # -------- Categories --------
    cats = fetch_categories()
    categories = []

    if cats:
        categories = cats.get("category_list", {}).get("categories", [])
        if categories:
            insert_categories(uid, categories)

    # -------- Users --------
    users = fetch_users()
    user_rows = []

    if users:
        user_rows = users.get("directory_items", [])
        if user_rows:
            insert_users(uid, user_rows)

    return {
        "rows": rows,
        "new_topics": len(new_topics),
        "categories": len(categories),
        "users": len(user_rows)
    }