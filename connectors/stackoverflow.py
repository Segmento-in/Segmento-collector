import requests
import sqlite3
import datetime
import json
import os
from dotenv import load_dotenv

load_dotenv()

DB = "identity.db"

API = "https://api.stackexchange.com/2.3"


# ---------------- DB ----------------

def db():
    con = sqlite3.connect(
        DB,
        timeout=90,
        check_same_thread=False,
        isolation_level=None   # autocommit
    )

    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")

    return con

def get_api_key(uid):

    con = sqlite3.connect(DB)
    cur = con.cursor()

    cur.execute("""
        SELECT api_key
        FROM connector_configs
        WHERE uid=? AND connector='stackoverflow'
    """, (uid,))

    row = cur.fetchone()
    con.close()

    if not row:
        raise Exception("StackOverflow API key missing")

    return row[0]

# ---------------- API ----------------
def so_get(uid, path, params=None):

    api_key = get_api_key(uid)

    p = params or {}

    p.update({
        "site": "stackoverflow",
        "key": api_key,
        "pagesize": 100
    })

    r = requests.get(API + path, params=p, timeout=20)

    if r.status_code != 200:
        raise Exception(r.text)

    return r.json()

# ---------------- STATE ----------------

def get_state(uid, ep):

    con = db()
    cur = con.cursor()

    cur.execute("""
    SELECT last_date FROM stack_state
    WHERE uid=? AND endpoint=?
    """, (uid, ep))

    row = cur.fetchone()
    con.close()

    return row[0] if row else 0


def save_state(uid, ep, date):

    con = db()
    cur = con.cursor()

    cur.execute("""
    INSERT OR REPLACE INTO stack_state
    (uid, endpoint, last_date)
    VALUES (?,?,?)
    """, (uid, ep, date))

    con.commit()
    con.close()


# ---------------- SYNC QUESTIONS ----------------

def sync_questions(uid, sync_type="historical"):

    if sync_type == "historical":
        save_state(uid, "questions", 0)
        last = 0
    else:
        last = get_state(uid, "questions")

    data = so_get(uid, "/questions", {
        "fromdate": last,
        "order": "asc",
        "sort": "creation"
    })

    items = data.get("items", [])

    con = db()
    cur = con.cursor()

    now = datetime.datetime.utcnow().isoformat()

    rows = []
    newest = last

    for q in items:

        ts = q["creation_date"]

        row = {
            "question_id": q["question_id"],
            "title": q["title"],
            "tags": ",".join(q.get("tags", [])),
            "score": q["score"],
            "owner": q["owner"].get("display_name"),
            "created_at": datetime.datetime.fromtimestamp(ts).isoformat(),
            "link": q["link"]
        }

        cur.execute("""
        INSERT OR IGNORE INTO stack_questions
        (uid, question_id, title, tags,
         score, owner, created_at,
         link, raw_json, fetched_at)
        VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            uid,
            row["question_id"],
            row["title"],
            row["tags"],
            row["score"],
            row["owner"],
            row["created_at"],
            row["link"],
            json.dumps(q),
            now
        ))

        if cur.rowcount > 0:
            rows.append(row)

        if ts > newest:
            newest = ts

    if newest > last:
        save_state(uid, "questions", newest + 1)

    con.commit()
    con.close()

    return {
        "questions": len(rows),
        "rows": rows
    }

# ---------------- SYNC ANSWERS ----------------

def sync_answers(uid, sync_type="historical"):

    if sync_type == "historical":
        save_state(uid, "answers", 0)
        last = 0
    else:
        last = get_state(uid, "answers")

    data = so_get(uid, "/answers", {
        "fromdate": last,
        "order": "asc",
        "sort": "creation"
    })

    items = data.get("items", [])

    con = db()
    cur = con.cursor()

    now = datetime.datetime.utcnow().isoformat()

    rows = []
    newest = last

    for a in items:

        ts = a["creation_date"]

        row = {
            "answer_id": a["answer_id"],
            "question_id": a["question_id"],
            "score": a["score"],
            "owner": a.get("owner", {}).get("display_name"),
            "created_at": datetime.datetime.fromtimestamp(ts).isoformat(),
            "link": a.get("link")
        }

        cur.execute("""
        INSERT OR IGNORE INTO stack_answers
        (uid, answer_id, question_id,
         score, owner, created_at,
         link, raw_json, fetched_at)
        VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            uid,
            row["answer_id"],
            row["question_id"],
            row["score"],
            row["owner"],
            row["created_at"],
            row["link"],
            json.dumps(a),
            now
        ))

        if cur.rowcount > 0:
            rows.append(row)

        if ts > newest:
            newest = ts

    if newest > last:
        save_state(uid, "answers", newest + 1)

    con.commit()
    con.close()

    return {
        "answers": len(rows),
        "rows": rows
    }

# ---------------- SYNC USERS ----------------
def sync_users(uid):

    data = so_get("/users", {"pagesize": 50})

    items = data.get("items", [])

    con = db()
    cur = con.cursor()

    now = datetime.datetime.utcnow().isoformat()

    rows = []

    for u in items:

        row = {
            "user_id": u["user_id"],
            "name": u["display_name"],
            "reputation": u["reputation"],
            "profile_url": u["link"]
        }

        cur.execute("""
        INSERT OR IGNORE INTO stack_users
        (uid, user_id, name,
         reputation, profile_url,
         raw_json, fetched_at)
        VALUES (?,?,?,?,?,?,?)
        """, (
            uid,
            row["user_id"],
            row["name"],
            row["reputation"],
            row["profile_url"],
            json.dumps(u),
            now
        ))

        if cur.rowcount > 0:
            rows.append(row)

    con.commit()
    con.close()

    return {
        "users": len(rows),
        "rows": rows
    }