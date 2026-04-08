import requests
import sqlite3
import datetime
import json
import time
import base64
import os


DB = os.getenv("DB_PATH", "/tmp/identity.db")
BASE_URL = "https://oauth.reddit.com"
TOKEN_URL = "https://www.reddit.com/api/v1/access_token"


# --------------------------------
# DB CONNECT
# --------------------------------

def db_connect():
    return sqlite3.connect(DB, timeout=30)


# --------------------------------
# AUTH - GET TOKEN
# --------------------------------

def get_access_token(uid):

    con = db_connect()
    cur = con.cursor()

    cur.execute("""
    SELECT access_token
    FROM reddit_accounts
    WHERE uid = ?
    ORDER BY id DESC
    LIMIT 1
    """, (uid,))

    row = cur.fetchone()
    con.close()

    if not row:
        raise Exception("Reddit not connected")

    return row[0]


# --------------------------------
# AUTH - INITIAL CONNECT
# --------------------------------

def connect_reddit(uid, client_id, client_secret, username, password):

    auth = base64.b64encode(
        f"{client_id}:{client_secret}".encode()
    ).decode()

    headers = {
        "Authorization": f"Basic {auth}",
        "User-Agent": "SegmentoBot/1.0"
    }

    data = {
        "grant_type": "password",
        "username": username,
        "password": password
    }

    r = requests.post(
        TOKEN_URL,
        headers=headers,
        data=data,
        timeout=30
    )

    if r.status_code != 200:
        raise Exception(r.text)

    token = r.json()

    expires_at = (
        datetime.datetime.now() +
        datetime.timedelta(seconds=token["expires_in"])
    ).isoformat()

    con = db_connect()
    cur = con.cursor()

    cur.execute("""
    INSERT INTO reddit_accounts
    (uid, access_token, refresh_token, expires_at, scopes, created_at)
    VALUES (?,?,?,?,?,?)
    """, (
        uid,
        token["access_token"],
        token.get("refresh_token"),
        expires_at,
        token.get("scope", ""),
        datetime.datetime.now().isoformat()
    ))

    con.commit()
    con.close()

    return {"status": "connected"}


# --------------------------------
# BASE API CALL
# --------------------------------

def reddit_get(path, token, params=None):

    headers = {
        "Authorization": f"Bearer {token}",
        "User-Agent": "SegmentoBot/1.0"
    }

    url = BASE_URL + path

    for i in range(3):

        r = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=30
        )

        # Rate limit
        if r.status_code == 429:
            time.sleep(2 ** i)
            continue

        if r.status_code != 200:
            raise Exception(r.text)

        return r.json()

    raise Exception("Rate limit exceeded")

# --------------------------------
# SYNC PROFILE
# --------------------------------

def sync_profile(uid):

    token = get_access_token(uid)

    data = reddit_get("/api/v1/me", token)

    con = db_connect()
    cur = con.cursor()

    now = datetime.datetime.now().isoformat()

    cur.execute("""
    INSERT OR REPLACE INTO reddit_profiles
    (uid, username, karma, created_utc, raw_json, fetched_at)
    VALUES (?,?,?,?,?,?)
    """, (
        uid,
        data.get("name"),
        data.get("total_karma"),
        data.get("created_utc"),
        json.dumps(data),
        now
    ))

    con.commit()
    con.close()

    return {"profile": "synced"}


# --------------------------------
# SYNC POSTS (SEARCH)
# --------------------------------

def sync_posts(uid, query="python", sync_type="historical"):

    token = get_access_token(uid)

    # ----------------------------
    # GET LAST STATE
    # ----------------------------

    last_created = None

    if sync_type == "incremental":
        con = db_connect()
        cur = con.cursor()

        cur.execute("""
        SELECT last_created_utc
        FROM reddit_state
        WHERE uid=?
        """, (uid,))

        row = cur.fetchone()
        con.close()

        if row:
            last_created = row[0]

    after = None
    rows = []

    con = db_connect()
    cur = con.cursor()

    now = datetime.datetime.utcnow().isoformat()

    newest_seen = last_created or 0

    while True:

        params = {
            "q": query,
            "limit": 100,
            "after": after,
            "sort": "new"
        }

        data = reddit_get("/search", token, params)

        children = data["data"]["children"]

        if not children:
            break

        for item in children:

            post = item["data"]
            created = post["created_utc"]

            # ----------------------------
            # INCREMENTAL LOGIC
            # ----------------------------
            if sync_type == "incremental" and last_created:
                if created <= last_created:
                    con.commit()
                    con.close()
                    return {"rows": rows}

            row_data = {
                "post_id": post["id"],
                "subreddit": post["subreddit"],
                "title": post["title"],
                "author": post["author"],
                "score": post["score"],
                "num_comments": post["num_comments"],
                "created_utc": created,
                "url": post["url"]
            }

            cur.execute("""
            INSERT OR IGNORE INTO reddit_posts
            (uid, post_id, subreddit, title, author,
             score, num_comments, created_utc,
             url, raw_json, fetched_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, (
                uid,
                row_data["post_id"],
                row_data["subreddit"],
                row_data["title"],
                row_data["author"],
                row_data["score"],
                row_data["num_comments"],
                row_data["created_utc"],
                row_data["url"],
                json.dumps(post),
                now
            ))

            rows.append(row_data)

            if created > newest_seen:
                newest_seen = created

        after = data["data"]["after"]

        if not after:
            break

    # ----------------------------
    # SAVE STATE
    # ----------------------------

    cur.execute("""
    INSERT OR REPLACE INTO reddit_state
    (uid, last_created_utc)
    VALUES (?,?)
    """, (uid, newest_seen))

    con.commit()
    con.close()

    return {"rows": rows}

# --------------------------------
# SYNC MESSAGES
# --------------------------------

def sync_messages(uid):

    token = get_access_token(uid)

    data = reddit_get("/message/inbox", token)

    con = db_connect()
    cur = con.cursor()

    now = datetime.datetime.now().isoformat()

    count = 0

    for msg in data["data"]["children"]:

        m = msg["data"]

        cur.execute("""
        INSERT OR REPLACE INTO reddit_messages
        (uid, message_id, author, subject, body,
         created_utc, raw_json, fetched_at)
        VALUES (?,?,?,?,?,?,?,?)
        """, (
            uid,
            m["id"],
            m.get("author"),
            m.get("subject"),
            m.get("body"),
            m.get("created_utc"),
            json.dumps(m),
            now
        ))

        count += 1

    con.commit()
    con.close()

    return {"messages": count}