import requests
import sqlite3
import datetime
import json
import os
import time

DB = "identity.db"
GRAPH_BASE = "https://graph.facebook.com/v19.0"


def db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


# --------------------------------------------
# STATE MANAGEMENT
# --------------------------------------------

def get_state(uid):

    con = db()
    cur = con.cursor()

    cur.execute("""
        SELECT state_json
        FROM connector_state
        WHERE uid=? AND source='facebook'
    """, (uid,))

    row = cur.fetchone()
    con.close()

    if not row:
        return {
            "last_post_time": "1970-01-01T00:00:00+0000",
            "after_cursor": None
        }

    return json.loads(row[0])


def save_state(uid, state):

    con = db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_state
        (uid, source, state_json, updated_at)
        VALUES (?, 'facebook', ?, ?)
    """, (
        uid,
        json.dumps(state),
        datetime.datetime.utcnow().isoformat()
    ))

    con.commit()
    con.close()


# --------------------------------------------
# TOKEN RETRIEVAL
# --------------------------------------------

def get_token(uid):

    con = db()
    cur = con.cursor()

    cur.execute("""
        SELECT page_access_token, page_id
        FROM facebook_connections
        WHERE uid=?
    """, (uid,))

    row = cur.fetchone()
    con.close()

    if not row:
        raise Exception("Facebook not connected")

    return row[0], row[1]


# --------------------------------------------
# INSERT POSTS
# --------------------------------------------

def insert_posts(uid, posts):

    if not posts:
        return

    con = db()
    cur = con.cursor()
    now = datetime.datetime.utcnow().isoformat()

    for post in posts:
        cur.execute("""
            INSERT OR IGNORE INTO facebook_page_posts
            (uid, post_id, page_id, message, story, created_time,
             privacy, attachments, message_tags,
             reactions_count, raw_json, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            uid,
            post.get("post_id"),
            post.get("page_id"),
            post.get("message"),
            post.get("story"),
            post.get("created_time"),
            post.get("privacy"),
            json.dumps(post.get("attachments")),
            json.dumps(post.get("message_tags")),
            post.get("reactions_count"),
            json.dumps(post),
            now
        ))

    con.commit()
    con.close()


# --------------------------------------------
# INSERT INSIGHTS
# --------------------------------------------

def insert_insights(uid, insights):

    if not insights:
        return

    con = db()
    cur = con.cursor()
    now = datetime.datetime.utcnow().isoformat()

    for metric in insights:
        cur.execute("""
            INSERT INTO facebook_page_insights
            (uid, page_id, metric_name, period,
             value, end_time, title, description,
             raw_json, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            uid,
            metric.get("page_id"),
            metric.get("metric_name"),
            metric.get("period"),
            json.dumps(metric.get("value")),
            metric.get("end_time"),
            metric.get("title"),
            metric.get("description"),
            json.dumps(metric),
            now
        ))

    con.commit()
    con.close()


# --------------------------------------------
# MAIN SYNC FUNCTION
# --------------------------------------------

def sync_facebook_pages(uid, sync_type="historical"):

    token, page_id = get_token(uid)

    state = get_state(uid)
    last_post_time = state.get("last_post_time")
    after_cursor = state.get("after_cursor")

    posts = []
    newest_time = last_post_time

    url = f"{GRAPH_BASE}/{page_id}/feed"

    params = {
        "access_token": token,
        "limit": 50,
        "fields": "id,message,story,created_time,privacy,attachments,message_tags,reactions.summary(true)"
    }

    if sync_type == "incremental":
        params["since"] = last_post_time

    if after_cursor:
        params["after"] = after_cursor

    while url:

        r = requests.get(url, params=params, timeout=30)

        if r.status_code == 429:
            time.sleep(2)
            continue

        data = r.json()

        for post in data.get("data", []):

            created = post.get("created_time")

            posts.append({
                "post_id": post.get("id"),
                "page_id": page_id,
                "message": post.get("message"),
                "story": post.get("story"),
                "created_time": created,
                "privacy": json.dumps(post.get("privacy")),
                "attachments": post.get("attachments"),
                "message_tags": post.get("message_tags"),
                "reactions_count": post.get("reactions", {}).get("summary", {}).get("total_count", 0)
            })

            if created and created > newest_time:
                newest_time = created

        paging = data.get("paging", {})
        url = paging.get("next")
        params = None

    insert_posts(uid, posts)

    # INSIGHTS FETCH
    insights = []

    try:
        ins = requests.get(
            f"{GRAPH_BASE}/{page_id}/insights",
            params={
                "access_token": token,
                "metric": "page_impressions,page_engaged_users",
                "period": "day"
            },
            timeout=30
        )

        ins_data = ins.json()

        for metric in ins_data.get("data", []):
            for val in metric.get("values", []):
                insights.append({
                    "page_id": page_id,
                    "metric_name": metric.get("name"),
                    "period": metric.get("period"),
                    "value": val.get("value"),
                    "end_time": val.get("end_time"),
                    "title": metric.get("title"),
                    "description": metric.get("description")
                })

    except:
        pass

    insert_insights(uid, insights)

    state["last_post_time"] = newest_time
    state["after_cursor"] = None
    save_state(uid, state)

    return {
        "rows": posts + insights,
        "posts": len(posts),
        "insights": len(insights)
    }