import sqlite3
import os
import json
import datetime
import time

from google.oauth2.credentials import Credentials
from google.cloud import storage
from backend.destinations.destination_router import push_to_destination
from backend.security.token_manager import ensure_valid_google_token

SOURCE = "gcs"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.getenv("DB_PATH", "identity.db")


# ---------------- DB CONNECT ---------------- #

def db_connect():
    return sqlite3.connect(DB)


# ---------------- CONNECTED USER ---------------- #

def get_connected_user():
    con = db_connect()
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


# ---------------- DESTINATION ---------------- #

def get_active_destination(uid):
    con = db_connect()
    cur = con.cursor()

    cur.execute("""
        SELECT
            dest_type,
            host,
            port,
            username,
            password,
            database_name
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


# ---------------- AUTH ---------------- #

def get_creds():

    con = db_connect()
    cur = con.cursor()

    # ---------- CHECK CONNECTED ----------
    cur.execute("""
        SELECT uid
        FROM google_connections
        WHERE source=? AND enabled=1
        LIMIT 1
    """, (SOURCE,))

    row = cur.fetchone()

    if not row:
        con.close()
        return None, None

    uid = row[0]

    # ---------- FETCH TOKEN ----------
    cur.execute("""
        SELECT access_token, refresh_token, scopes
        FROM google_accounts
        WHERE source=?
        ORDER BY id DESC
        LIMIT 1
    """, (SOURCE,))

    token_row = cur.fetchone()
    con.close()

    if not token_row:
        return None, None

    access, refresh, scopes = token_row

    creds = Credentials(
        token=access,
        refresh_token=refresh,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=os.getenv("GOOGLE_CLIENT_ID"),
        client_secret=os.getenv("GOOGLE_CLIENT_SECRET"),
        scopes=scopes.split(",") if scopes else None
    )

    return uid, creds

# ---------------- SYNC ---------------- #

def sync_gcs(sync_type="incremental"):

    uid, creds = get_creds()

    if not uid:
        return {
            "status": "error",
            "message": "GCS connector not connected"
        }

    creds = ensure_valid_google_token(creds, uid, "gcs")

    project_id = os.getenv("GOOGLE_PROJECT_ID")

    if not project_id:
        return {
            "status": "error",
            "message": "GOOGLE_PROJECT_ID not set"
        }

    client = storage.Client(credentials=creds, project=project_id)

    con = db_connect()
    cur = con.cursor()

    now = datetime.datetime.utcnow().isoformat()

    bucket_rows = []
    object_rows = []

    bucket_count = 0
    object_count = 0

    buckets = list(client.list_buckets(project=project_id))

    for b in buckets:

        bucket_row = {
            "project_id": project_id,
            "bucket_name": b.name,
            "location": b.location,
            "storage_class": b.storage_class,
            "fetched_at": now
        }

        bucket_rows.append(bucket_row)

        cur.execute("""
            INSERT OR REPLACE INTO google_gcs_buckets
            (uid, project_id, bucket_name, location,
             storage_class, raw_json, fetched_at)
            VALUES (?,?,?,?,?,?,?)
        """, (
            uid,
            project_id,
            b.name,
            b.location,
            b.storage_class,
            json.dumps(b._properties, ensure_ascii=False),
            now
        ))

        bucket_count += 1

        blobs = client.list_blobs(b.name)

        for blob in blobs:

            object_row = {
                "bucket_name": b.name,
                "object_name": blob.name,
                "size": blob.size,
                "content_type": blob.content_type,
                "updated": blob.updated.isoformat() if blob.updated else None,
                "md5_hash": blob.md5_hash,
                "fetched_at": now
            }

            object_rows.append(object_row)

            cur.execute("""
                INSERT OR REPLACE INTO google_gcs_objects
                (uid, bucket_name, object_name,
                 size, content_type, updated,
                 md5_hash, raw_json, fetched_at)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, (
                uid,
                b.name,
                blob.name,
                blob.size,
                blob.content_type,
                blob.updated.isoformat() if blob.updated else None,
                blob.md5_hash,
                json.dumps(blob._properties, ensure_ascii=False),
                now
            ))

            object_count += 1

    con.commit()
    con.close()

    # ---------- PUSH TO DESTINATION ---------- #

    dest = get_active_destination(uid)

    if dest:
        if bucket_rows:
            push_to_destination(dest, "gcs_buckets", bucket_rows)

        if object_rows:
            push_to_destination(dest, "gcs_objects", object_rows)

    if bucket_count == 0:
        return {
            "status": "success",
            "buckets": 0,
            "objects": 0,
            "message": "No buckets found in project (billing may not be enabled)"
        }

    return {
        "status": "success",
        "buckets": bucket_count,
        "objects": object_count
    }
