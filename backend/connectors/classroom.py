import sqlite3
import os
import json
import datetime
import time

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from backend.destinations.destination_router import push_to_destination
from backend.security.token_manager import ensure_valid_google_token

SOURCE = "classroom"
DB = os.getenv("DB_PATH", "/tmp/identity.db")


# ---------------- CONNECTION CHECK ---------------- #

def get_connected_user():
    con = sqlite3.connect(DB)
    cur = con.cursor()

    cur.execute("""
        SELECT uid
        FROM google_connections
        WHERE source=? AND enabled=1
        LIMIT 1
    """, (SOURCE,))

    row = cur.fetchone()
    con.close()

    return row[0] if row else None


# ---------------- AUTH ---------------- #

def get_creds(uid):

    con = sqlite3.connect(DB)
    cur = con.cursor()

    # OAuth token
    cur.execute("""
        SELECT access_token, refresh_token, scopes
        FROM google_accounts
        WHERE uid=? AND source='classroom'
        ORDER BY created_at DESC
        LIMIT 1
    """, (uid,))
    token = cur.fetchone()

    if not token:
        raise Exception("Classroom not authorized")

    access, refresh, scopes = token

    # Connector specific app creds
    cur.execute("""
        SELECT client_id, client_secret
        FROM connector_configs
        WHERE uid=? AND connector='classroom'
        LIMIT 1
    """, (uid,))
    app = cur.fetchone()

    con.close()

    if not app:
        raise Exception("Classroom app credentials missing")

    client_id, client_secret = app

    return Credentials(
        token=access,
        refresh_token=refresh,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=scopes.split(",") if scopes else None
    )


# ---------------- DESTINATION ---------------- #

def get_active_destination(uid):

    con = sqlite3.connect(DB)
    cur = con.cursor()

    cur.execute("""
        SELECT dest_type, host, port, username, password, database_name
        FROM destination_configs
        WHERE uid=? AND source=? AND is_active=1
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


# ---------------- PAGINATION ---------------- #

def fetch_all(func, key, **kwargs):

    rows = []
    token = None

    while True:
        res = func(pageToken=token, **kwargs).execute()
        rows.extend(res.get(key, []))
        token = res.get("nextPageToken")
        if not token:
            break

    return rows


# ---------------- MAIN SYNC ---------------- #

def sync_classroom(sync_type="incremental"):

    uid = get_connected_user()

    if not uid:
        return {"status": "error", "message": "Classroom not connected"}

    creds = get_creds(uid)
    creds = ensure_valid_google_token(creds, uid, "classroom")
    service = build("classroom", "v1", credentials=creds)

    con = sqlite3.connect(DB)
    cur = con.cursor()

    now = datetime.datetime.utcnow().isoformat()

    stats = {
        "status": "success",
        "courses": 0,
        "teachers": 0,
        "students": 0
    }

    rows_for_destination = []

    try:

        # ---------------- COURSES ---------------- #

        courses = fetch_all(
            service.courses().list,
            "courses",
            pageSize=100
        )

        for c in courses:

            cur.execute("""
                INSERT OR IGNORE INTO google_classroom_courses
                VALUES (NULL,?,?,?,?,?,?,?)
            """, (
                uid,
                c.get("id"),
                c.get("name"),
                c.get("courseState"),
                c.get("ownerId"),
                json.dumps(c),
                now
            ))

            rows_for_destination.append({
                "entity": "course",
                "course_id": c.get("id"),
                "name": c.get("name"),
                "state": c.get("courseState"),
                "owner_id": c.get("ownerId")
            })

            stats["courses"] += 1

        # ---------------- TEACHERS + STUDENTS ---------------- #

        for c in courses:

            course_id = c["id"]

            # -------- TEACHERS -------- #

            teachers = fetch_all(
                service.courses().teachers().list,
                "teachers",
                courseId=course_id
            )

            for t in teachers:

                p = t.get("profile", {})

                cur.execute("""
                    INSERT OR IGNORE INTO google_classroom_teachers
                    VALUES (NULL,?,?,?,?,?,?,?)
                """, (
                    uid,
                    course_id,
                    p.get("id"),
                    p.get("name", {}).get("fullName"),
                    p.get("emailAddress"),
                    json.dumps(t),
                    now
                ))

                rows_for_destination.append({
                    "entity": "teacher",
                    "course_id": course_id,
                    "user_id": p.get("id"),
                    "full_name": p.get("name", {}).get("fullName"),
                    "email": p.get("emailAddress")
                })

                stats["teachers"] += 1

            # -------- STUDENTS -------- #

            students = fetch_all(
                service.courses().students().list,
                "students",
                courseId=course_id
            )

            for s in students:

                p = s.get("profile", {})

                cur.execute("""
                    INSERT OR IGNORE INTO google_classroom_students
                    VALUES (NULL,?,?,?,?,?,?,?)
                """, (
                    uid,
                    course_id,
                    p.get("id"),
                    p.get("name", {}).get("fullName"),
                    p.get("emailAddress"),
                    json.dumps(s),
                    now
                ))

                rows_for_destination.append({
                    "entity": "student",
                    "course_id": course_id,
                    "user_id": p.get("id"),
                    "full_name": p.get("name", {}).get("fullName"),
                    "email": p.get("emailAddress")
                })

                stats["students"] += 1

            time.sleep(0.5)

        con.commit()

    except Exception as e:
        con.rollback()
        con.close()
        return {"status": "error", "message": str(e)}

    con.close()

    # ---------------- DESTINATION PUSH (FIXED NORMALIZATION) ---------------- #

    dest = get_active_destination(uid)

    if dest and rows_for_destination:

        # 🔥 IMPORTANT FIX:
        # Make all rows have identical keys (BigQuery requirement)

        all_keys = set()

        for r in rows_for_destination:
            all_keys.update(r.keys())

        normalized_rows = []

        for r in rows_for_destination:
            clean_row = {}
            for k in all_keys:
                v = r.get(k)

                if isinstance(v, (dict, list)):
                    clean_row[k] = json.dumps(v)
                elif v is not None:
                    clean_row[k] = str(v)
                else:
                    clean_row[k] = None

            normalized_rows.append(clean_row)

        push_to_destination(dest, SOURCE, normalized_rows)

    return stats
