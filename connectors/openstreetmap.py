import requests
import sqlite3
import time
import json
import xml.etree.ElementTree as ET
from datetime import datetime

DB = "identity.db"

BASE = "https://api.openstreetmap.org/api/0.6"

HEADERS = {
    "User-Agent": "SegmentoCollector/1.0"
}


# ------------------------------------------------
# DB
# ------------------------------------------------

def db():
    con = sqlite3.connect(
        DB,
        timeout=90,
        check_same_thread=False,
        isolation_level=None
    )
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con


# ------------------------------------------------
# HTTP
# ------------------------------------------------

def safe_get(url, params=None):

    try:
        r = requests.get(
            url,
            headers=HEADERS,
            params=params,
            timeout=25
        )

        if r.status_code == 200:
            if "xml" in r.headers.get("Content-Type", ""):
                return r.text
            return r.json()

        if r.status_code == 429:
            time.sleep(60)

    except Exception as e:
        print("OSM ERROR:", e)
        time.sleep(5)

    return None


# ------------------------------------------------
# STATE
# ------------------------------------------------

def get_state(uid):

    con = db()
    cur = con.cursor()

    cur.execute("""
        SELECT last_changeset_id,last_note_id
        FROM osm_state
        WHERE uid=?
    """, (uid,))

    row = cur.fetchone()
    con.close()

    if row:
        return row[0] or 0, row[1] or 0

    return 0, 0


def save_state(uid, cid, nid):

    con = db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO osm_state
        (uid,last_changeset_id,last_note_id)
        VALUES(?,?,?)
    """, (uid, cid, nid))

    con.close()


# ------------------------------------------------
# PARSERS
# ------------------------------------------------

def parse_changesets(xml_text):

    root = ET.fromstring(xml_text)
    rows = []

    for cs in root.findall("changeset"):
        rows.append({
            "id": int(cs.get("id", 0)),
            "user": cs.get("user"),
            "uid": cs.get("uid"),
            "created": cs.get("created_at"),
            "closed": cs.get("closed_at"),
            "raw": ET.tostring(cs, encoding="unicode")
        })

    return rows


def parse_notes(data):

    rows = []

    for f in data.get("features", []):

        p = f.get("properties", {})
        g = f.get("geometry", {}) or {}
        coords = g.get("coordinates", [None, None])

        rows.append({
            "id": p.get("id"),
            "status": p.get("status"),
            "lat": coords[1],
            "lon": coords[0],
            "created": p.get("date_created"),
            "closed": p.get("date_closed"),
            "comments": len(p.get("comments", [])),
            "raw": f
        })

    return rows


# ------------------------------------------------
# INSERTS
# ------------------------------------------------

def insert_changesets(uid, rows):

    con = db()
    cur = con.cursor()
    now = datetime.utcnow().isoformat()

    cur.executemany("""
        INSERT OR IGNORE INTO osm_changesets
        (uid,changeset_id,user,uid_osm,
         created_at,closed_at,
         raw_xml,fetched_at)
        VALUES (?,?,?,?,?,?,?,?)
    """, [
        (
            uid,
            r["id"],
            r["user"],
            r["uid"],
            r["created"],
            r["closed"],
            r["raw"],
            now
        )
        for r in rows
    ])

    con.close()


def insert_notes(uid, rows):

    con = db()
    cur = con.cursor()
    now = datetime.utcnow().isoformat()

    cur.executemany("""
        INSERT OR IGNORE INTO osm_notes
        (uid,note_id,status,lat,lon,
         created_at,closed_at,
         comments,raw_json,fetched_at)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    """, [
        (
            uid,
            r["id"],
            r["status"],
            r["lat"],
            r["lon"],
            r["created"],
            r["closed"],
            r["comments"],
            json.dumps(r["raw"]),
            now
        )
        for r in rows
    ])

    con.close()


# ------------------------------------------------
# MAIN SYNC
# ------------------------------------------------

def sync_openstreetmap(uid, sync_type="incremental", limit=50):

    last_cs, last_note = get_state(uid)

    rows_for_destination = []

    # ---------- CHANGESETS ----------
    xml = safe_get(
        f"{BASE}/changesets",
        {"closed": "true", "limit": limit}
    )

    new_cs = []

    if xml:
        parsed = parse_changesets(xml)

        for r in parsed:

            if sync_type == "incremental" and r["id"] <= last_cs:
                continue

            new_cs.append(r)

            rows_for_destination.append({
                "uid": uid,
                "type": "changeset",
                "changeset_id": r["id"]
            })

        if new_cs:
            insert_changesets(uid, new_cs)
            last_cs = max(r["id"] for r in new_cs)

    # ---------- NOTES ----------
    notes = safe_get(
        f"{BASE}/notes.json",
        {"limit": 100, "closed": -1}
    )

    new_notes = []

    if notes:
        parsed = parse_notes(notes)

        for r in parsed:

            if sync_type == "incremental" and r["id"] <= last_note:
                continue

            new_notes.append(r)

            rows_for_destination.append({
                "uid": uid,
                "type": "note",
                "note_id": r["id"]
            })

        if new_notes:
            insert_notes(uid, new_notes)
            last_note = max(r["id"] for r in new_notes)

    save_state(uid, last_cs, last_note)

    return {
        "rows": rows_for_destination,
        "new_changesets": len(new_cs),
        "new_notes": len(new_notes)
    }