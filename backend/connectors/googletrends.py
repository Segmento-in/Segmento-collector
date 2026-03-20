from pytrends.request import TrendReq
from pytrends.exceptions import TooManyRequestsError

import sqlite3
import datetime
import json
import random
import time

DB = "identity.db"

def clean_pandas(obj):

    if hasattr(obj, "isoformat"):
        return obj.isoformat()

    if isinstance(obj, dict):
        return {k: clean_pandas(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [clean_pandas(x) for x in obj]

    return obj

# ---------------- PYTRENDS CLIENT ----------------

pytrends = TrendReq(
    hl="en-IN",
    tz=330,
    timeout=(10, 25),
)


# ---------------- DB ----------------

def db():
    return sqlite3.connect(DB, timeout=60, check_same_thread=False)


# ---------------- STATE ----------------

def get_state(uid, keyword):

    con = db()
    cur = con.cursor()

    cur.execute("""
    SELECT last_date
    FROM google_trends_state
    WHERE uid=? AND keyword=?
    """, (uid, keyword))

    row = cur.fetchone()
    con.close()

    return row[0] if row else None


def save_state(con, cur, uid, keyword, date):

    cur.execute("""
    INSERT OR REPLACE INTO google_trends_state
    (uid, keyword, last_date)
    VALUES (?,?,?)
    """, (uid, keyword, date))


# ---------------- SAFE PYTRENDS CALL ----------------

def safe_build_and_fetch(keyword, timeframe):

    last_error = None

    for i in range(3):

        try:

            pytrends.build_payload(
                [keyword],
                timeframe=timeframe,
                geo="IN"
            )

            df = pytrends.interest_over_time()
            if df is None or df.empty:
                raise Exception("Google Trends returned empty or blocked response")
            df = df.reset_index()

            return df

        except TooManyRequestsError as e:

            last_error = e

            wait = random.randint(25, 45)
            time.sleep(wait)

        except Exception as e:

            last_error = e
            time.sleep(10)

    raise Exception(f"Google Trends blocked: {last_error}")


def safe_fetch_related(keyword):

    last_error = None

    for i in range(3):

        try:

            pytrends.build_payload(
                [keyword],
                timeframe="now 7-d",
                geo="IN"
            )

            return pytrends.related_queries()

        except TooManyRequestsError as e:

            last_error = e

            wait = random.randint(25, 45)
            time.sleep(wait)

        except Exception as e:

            last_error = e
            time.sleep(10)

    raise Exception(f"Google Trends blocked: {last_error}")


# ---------------- SYNC INTEREST ----------------

def sync_interest(uid, keyword, sync_type="incremental"):

    if sync_type == "historical":
        timeframe = "today 12-m"
        last_date = None
    else:
        timeframe = "now 7-d"
        last_date = get_state(uid, keyword)

    print("Using timeframe:", timeframe, flush=True)

    df = safe_build_and_fetch(keyword, timeframe)

    rows_to_push = []

    for _, row in df.iterrows():
        date = str(row["date"])
        value = int(row[keyword])

        rows_to_push.append({
            "uid": uid,
            "keyword": keyword,
            "date": date,
            "value": value,
            "raw_json": "{}",
            "fetched_at": datetime.datetime.now().isoformat()
        })

    return {
        "interest_points": len(rows_to_push),
        "rows": rows_to_push
    }

# ---------------- SYNC RELATED ----------------

def sync_related(uid, keyword):

    related = safe_fetch_related(keyword)
    data = related.get(keyword)

    if not data:
        return {
            "keyword": keyword,
            "related_queries": 0,
            "rows": []
        }

    con = db()
    cur = con.cursor()

    now = datetime.datetime.now().isoformat()
    rows_to_push = []
    count = 0

    for t in ["top", "rising"]:

        df = data.get(t)

        if df is None or df.empty:
            continue

        for _, row in df.iterrows():

            row_dict = {
                "uid": uid,
                "keyword": keyword,
                "type": t,
                "query": row["query"],
                "value": int(row["value"]),
                "raw_json": json.dumps(clean_pandas(row.to_dict())),
                "fetched_at": now
            }

            cur.execute("""
                INSERT OR IGNORE INTO google_trends_related
                (uid, keyword, type, query, value,
                 raw_json, fetched_at)
                VALUES (?,?,?,?,?,?,?)
            """, (
                row_dict["uid"],
                row_dict["keyword"],
                row_dict["type"],
                row_dict["query"],
                row_dict["value"],
                row_dict["raw_json"],
                row_dict["fetched_at"]
            ))

            rows_to_push.append(row_dict)
            count += 1

    con.commit()
    con.close()

    time.sleep(random.randint(10, 20))

    return {
        "related_queries": count,
        "rows": rows_to_push
    }

from backend.destinations.destination_router import push_to_destination

SOURCE = "trends"


def get_active_destination(uid):

    con = db()
    cur = con.cursor()

    cur.execute("""
        SELECT dest_type, host, port, username, password, database_name
        FROM destination_configs
        WHERE uid=? AND source=?
        ORDER BY id DESC
        LIMIT 1
    """, (uid, SOURCE))

    row = cur.fetchone()
    con.close()

    if not row:
        return None

    return {
        "type": row[0],
        "host": row[1],
        "port": row[2],
        "username": row[3],
        "password": row[4],
        "database_name": row[5]
    }

from pytrends.request import TrendReq

def sync_trends(uid, keyword, sync_type="incremental"):

    pytrends = TrendReq(hl='en-US', tz=330)
    interest = sync_interest(uid, keyword, sync_type)
    related = sync_related(uid, keyword)

    print("Interest result:", interest, flush=True)
    print("Related result:", related, flush=True)

    dest = get_active_destination(uid)
    print("Destination:", dest, flush=True)

    if dest:

        print("Interest rows count:", len(interest.get("rows", [])), flush=True)
        print("Related rows count:", len(related.get("rows", [])), flush=True)

        if interest.get("rows"):
            push_to_destination(dest, "google_trends_interest", interest["rows"])

        if related.get("rows"):
            push_to_destination(dest, "google_trends_related", related["rows"])

    print("[TRENDS] Done", flush=True)

    return {
        "status": "success",
        "interest_points": interest.get("interest_points", 0),
        "related_queries": related.get("related_queries", 0)
    }