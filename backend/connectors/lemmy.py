import requests,sqlite3,time,os,json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

DB = os.getenv("DB_PATH", "/tmp/identity.db")

INSTANCE=os.getenv("LEMMY_INSTANCE","https://lemmy.world").rstrip("/")

HEADERS={
    "User-Agent":"SegmentoCollector/1.0"
}


# ---------------- DB ----------------

def db():
    con=sqlite3.connect(DB,timeout=90,check_same_thread=False,isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con


# ---------------- HTTP ----------------

def safe_get(url,params=None):
    try:
        r=requests.get(url,headers=HEADERS,params=params,timeout=20)
        if r.status_code==200:
            return r.json()
        if r.status_code==429:
            time.sleep(60)
    except Exception:
        time.sleep(5)
    return None


# ---------------- State ----------------

def get_last_post(uid):
    con=db()
    cur=con.cursor()
    cur.execute("SELECT last_post_id FROM lemmy_state WHERE uid=?",(uid,))
    row=cur.fetchone()
    con.close()
    return row[0] if row and row[0] else 0


def save_last_post(uid,pid):
    con=db()
    cur=con.cursor()
    cur.execute("""
    INSERT OR REPLACE INTO lemmy_state(uid,instance,last_post_id)
    VALUES(?,?,?)
    """,(uid,INSTANCE,pid))
    con.close()


# ---------------- Inserts ----------------

def insert_posts(uid,rows):
    con=db()
    cur=con.cursor()
    now=datetime.utcnow().isoformat()
    data=[]
    for r in rows:
        post=r.get("post") or {}
        creator=r.get("creator") or {}
        comm=r.get("community") or {}
        counts=r.get("counts") or {}

        data.append((
            uid,
            INSTANCE,
            post.get("id"),
            post.get("name"),
            post.get("url"),
            creator.get("name"),
            comm.get("name"),
            counts.get("score"),
            counts.get("comments"),
            post.get("published"),
            json.dumps(r,ensure_ascii=False),
            now
        ))
    cur.executemany("""
    INSERT OR IGNORE INTO lemmy_posts
    (uid,instance,post_id,name,url,creator,community,
     score,comments,published,raw_json,fetched_at)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
    """,data)
    con.close()


def insert_communities(uid,rows):
    con=db()
    cur=con.cursor()
    now=datetime.utcnow().isoformat()
    data=[]
    for r in rows:
        comm=r.get("community") or {}
        counts=r.get("counts") or {}

        data.append((
            uid,
            INSTANCE,
            comm.get("id"),
            comm.get("name"),
            comm.get("title"),
            counts.get("subscribers"),
            counts.get("posts"),
            counts.get("comments"),
            comm.get("published"),
            json.dumps(r,ensure_ascii=False),
            now
        ))
    cur.executemany("""
    INSERT OR IGNORE INTO lemmy_communities
    (uid,instance,community_id,name,title,subscribers,
     posts,comments,published,raw_json,fetched_at)
    VALUES(?,?,?,?,?,?,?,?,?,?,?)
    """,data)
    con.close()


def insert_users(uid,rows):
    con=db()
    cur=con.cursor()
    now=datetime.utcnow().isoformat()
    data=[]
    for r in rows:
        user=r.get("person") or {}
        counts=r.get("counts") or {}

        data.append((
            uid,
            INSTANCE,
            user.get("id"),
            user.get("name"),
            user.get("display_name"),
            counts.get("post_count"),
            counts.get("comment_count"),
            user.get("published"),
            json.dumps(r,ensure_ascii=False),
            now
        ))
    cur.executemany("""
    INSERT OR IGNORE INTO lemmy_users
    (uid,instance,user_id,username,display_name,
     posts,comments,published,raw_json,fetched_at)
    VALUES(?,?,?,?,?,?,?,?,?,?)
    """,data)
    con.close()


# ---------------- Fetchers ----------------

def fetch_posts(sort,limit):
    url=f"{INSTANCE}/api/v3/post/list"
    return safe_get(url,{
        "type_":"All",
        "sort":sort,
        "limit":limit
    })


def fetch_communities(limit):
    url=f"{INSTANCE}/api/v3/community/list"
    return safe_get(url,{"limit":limit})


def fetch_users(limit):
    url=f"{INSTANCE}/api/v3/user/list"
    return safe_get(url,{"limit":limit})


# ---------------- Main Sync ----------------

def sync_lemmy(uid,limit=30):

    last_id=get_last_post(uid)


    # Active
    active=fetch_posts("Active",limit)
    active_rows=[]

    if active:
        active_rows=active.get("posts",[])


    # Hot
    hot=fetch_posts("Hot",limit)
    hot_rows=[]

    if hot:
        hot_rows=hot.get("posts",[])


    # Merge posts
    all_posts=active_rows+hot_rows

    new_posts=[]

    for r in all_posts:
        post=r.get("post") or {}
        pid=post.get("id",0)
        if pid>last_id:
            new_posts.append(r)


    if new_posts:
        insert_posts(uid,new_posts)
        save_last_post(uid,max(r["post"]["id"] for r in new_posts))


    # Communities
    comm=fetch_communities(limit)
    comm_rows=[]

    if comm:
        comm_rows=comm.get("communities",[])
        if comm_rows:
            insert_communities(uid,comm_rows)


    # Users
    users=fetch_users(limit)
    user_rows=[]

    if users:
        user_rows=users.get("users",[])
        if user_rows:
            insert_users(uid,user_rows)


    return {
        "status":"ok",
        "new_posts":len(new_posts),
        "communities":len(comm_rows),
        "users":len(user_rows)
    }