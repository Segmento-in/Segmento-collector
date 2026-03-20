import requests
import sqlite3
import datetime
import json
import time

from backend.security.crypto import encrypt_value, decrypt_value
from backend.security.secure_fetch import fetchone_secure
from backend.destinations.destination_router import push_to_destination

DB = "identity.db"
SOURCE = "chartbeat"
BASE_URL = "https://api.chartbeat.com"


# ---------------- DB ----------------

def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


# ---------------- CREDENTIALS ----------------

def get_credentials(uid):
    con = get_db()
    cur = con.cursor()
    cur.execute("""
        SELECT api_key, scopes, access_token
        FROM connector_configs
        WHERE uid=? AND connector='chartbeat'
        LIMIT 1
    """, (uid,))
    row = fetchone_secure(cur)
    con.close()

    if not row:
        raise Exception("Chartbeat not configured")

    api_key  = decrypt_value(row["api_key"])
    host     = decrypt_value(row["scopes"])        # stored in scopes column
    query_id = row["access_token"]                 # optional — stored in access_token column
    if query_id:
        query_id = decrypt_value(query_id)

    return api_key, host, query_id


def save_credentials(uid, api_key, host, query_id=None):
    con = get_db()
    cur = con.cursor()

    enc_key      = encrypt_value(api_key)
    enc_host     = encrypt_value(host)
    enc_query_id = encrypt_value(query_id) if query_id else None

    cur.execute("""
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, api_key, scopes, access_token, status, created_at)
        VALUES (?, 'chartbeat', ?, ?, ?, 'configured', datetime('now'))
    """, (uid, enc_key, enc_host, enc_query_id))

    con.commit()
    con.close()


# ---------------- STATE ----------------

def get_state(uid):
    con = get_db()
    cur = con.cursor()
    cur.execute("""
        SELECT state_json
        FROM connector_state
        WHERE uid=? AND source='chartbeat'
        LIMIT 1
    """, (uid,))
    row = cur.fetchone()
    con.close()

    if not row:
        return {"last_sync_date": None, "last_query_id": None}

    try:
        return json.loads(row[0])
    except Exception:
        return {"last_sync_date": None, "last_query_id": None}


def save_state(uid, state):
    con = get_db()
    cur = con.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO connector_state
        (uid, source, state_json, updated_at)
        VALUES (?, 'chartbeat', ?, ?)
    """, (
        uid,
        json.dumps(state),
        datetime.datetime.utcnow().isoformat()
    ))
    con.commit()
    con.close()


# ---------------- DESTINATION ----------------

def get_active_destination(uid):
    con = get_db()
    cur = con.cursor()
    cur.execute("""
        SELECT dest_type, host, port, username, password, database_name
        FROM destination_configs
        WHERE uid=? AND source=? AND is_active=1
        LIMIT 1
    """, (uid, SOURCE))
    row = fetchone_secure(cur)
    con.close()

    if not row:
        return None

    return {
        "type":          row["dest_type"],
        "host":          row["host"],
        "port":          row["port"],
        "username":      row["username"],
        "password":      row["password"],
        "database_name": row["database_name"],
    }


# ---------------- HTTP HELPER ----------------

def _cb_get(api_key, path, params=None, retries=3):
    """GET request with Chartbeat auth header and retry/backoff."""
    url = BASE_URL + path
    headers = {
        "X-CB-AK": api_key,
        "Content-Type": "application/json",
    }

    for attempt in range(retries):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=30)

            if r.status_code == 429:
                wait = 2 ** attempt
                print(f"[CHARTBEAT] Rate limited. Waiting {wait}s…", flush=True)
                time.sleep(wait)
                continue

            if r.status_code in (401, 403):
                raise Exception(
                    f"Chartbeat auth error {r.status_code}: {r.text}"
                )

            if r.status_code >= 500:
                wait = 2 ** attempt
                print(f"[CHARTBEAT] Server error {r.status_code}. Retry in {wait}s…", flush=True)
                time.sleep(wait)
                continue

            r.raise_for_status()
            return r.json()

        except requests.exceptions.RequestException as e:
            if attempt == retries - 1:
                raise
            time.sleep(2 ** attempt)

    raise Exception("Chartbeat GET failed after retries")


def _cb_post(api_key, path, payload=None, retries=3):
    """POST request with Chartbeat auth header and retry/backoff."""
    url = BASE_URL + path
    headers = {
        "X-CB-AK": api_key,
        "Content-Type": "application/json",
    }

    for attempt in range(retries):
        try:
            r = requests.post(
                url, headers=headers,
                json=payload, timeout=30
            )

            if r.status_code == 429:
                wait = 2 ** attempt
                print(f"[CHARTBEAT] Rate limited. Waiting {wait}s…", flush=True)
                time.sleep(wait)
                continue

            if r.status_code in (401, 403):
                raise Exception(
                    f"Chartbeat auth error {r.status_code}: {r.text}"
                )

            if r.status_code >= 500:
                wait = 2 ** attempt
                print(f"[CHARTBEAT] Server error {r.status_code}. Retry in {wait}s…", flush=True)
                time.sleep(wait)
                continue

            r.raise_for_status()
            return r.json()

        except requests.exceptions.RequestException as e:
            if attempt == retries - 1:
                raise
            time.sleep(2 ** attempt)

    raise Exception("Chartbeat POST failed after retries")


# ---------------- INSERT HELPERS ----------------

def _insert_top_pages(uid, pages):
    if not pages:
        return

    con = get_db()
    cur = con.cursor()
    now = datetime.datetime.utcnow().isoformat()

    for page in pages:
        cur.execute("""
            INSERT OR REPLACE INTO chartbeat_top_pages
            (uid, path, title, concurrents, engaged_time,
             page_views, visits, raw_json, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            uid,
            page.get("path"),
            page.get("title"),
            page.get("concurrents"),
            page.get("engaged_time"),
            page.get("page_views"),
            page.get("visits"),
            json.dumps(page),
            now,
        ))

    con.commit()
    con.close()


def _insert_page_engagement(uid, rows):
    if not rows:
        return

    con = get_db()
    cur = con.cursor()
    now = datetime.datetime.utcnow().isoformat()

    for row in rows:
        cur.execute("""
            INSERT OR REPLACE INTO chartbeat_page_engagement
            (uid, path, title, author, section, device, referrer_type,
             page_views, page_uniques, page_avg_time, page_total_time,
             page_avg_scroll, page_scroll_starts, page_views_quality,
             date, raw_json, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            uid,
            row.get("path"),
            row.get("title"),
            row.get("author"),
            row.get("section"),
            row.get("device"),
            row.get("referrer_type"),
            row.get("page_views"),
            row.get("page_uniques"),
            row.get("page_avg_time"),
            row.get("page_total_time"),
            row.get("page_avg_scroll"),
            row.get("page_scroll_starts"),
            row.get("page_views_quality"),
            row.get("date"),
            json.dumps(row),
            now,
        ))

    con.commit()
    con.close()


def _insert_video_engagement(uid, rows):
    if not rows:
        return

    con = get_db()
    cur = con.cursor()
    now = datetime.datetime.utcnow().isoformat()

    for row in rows:
        cur.execute("""
            INSERT OR REPLACE INTO chartbeat_video_engagement
            (uid, video_title, video_path, play_state,
             video_plays, video_loads, video_play_rate,
             video_avg_time, raw_json, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            uid,
            row.get("video_title"),
            row.get("video_path"),
            row.get("play_state"),
            row.get("video_plays"),
            row.get("video_loads"),
            row.get("video_play_rate"),
            row.get("video_avg_time"),
            json.dumps(row),
            now,
        ))

    con.commit()
    con.close()


# ---------------- SYNC FUNCTIONS ----------------

def _fetch_top_pages(api_key, host):
    """Real-time top pages endpoint."""
    data = _cb_get(api_key, "/live/toppages/v3/", params={"host": host})

    pages = []
    for item in data.get("pages", []):
        stats  = item.get("stats", {})
        pages.append({
            "path":          item.get("path"),
            "title":         item.get("title"),
            "concurrents":   stats.get("c"),
            "engaged_time":  stats.get("t"),
            "page_views":    stats.get("pv"),
            "visits":        stats.get("v"),
        })

    return pages


def _fetch_page_engagement(api_key, host, start_date, end_date):
    """Historical page engagement via POST query."""
    payload = {
        "host":       host,
        "start_date": start_date,
        "end_date":   end_date,
        "metrics": [
            "page_views", "page_uniques", "page_avg_time",
            "page_total_time", "page_avg_scroll",
            "page_scroll_starts", "page_views_quality",
        ],
        "dimensions": ["path", "title", "author", "section", "device", "referrer_type"],
    }

    try:
        data = _cb_post(api_key, "/query/v2/submit/page/", payload=payload)
    except Exception as e:
        print(f"[CHARTBEAT] Page engagement query failed: {e}", flush=True)
        return []

    rows = []
    for item in data.get("data", []):
        rows.append({
            "path":               item.get("path"),
            "title":              item.get("title"),
            "author":             item.get("author"),
            "section":            item.get("section"),
            "device":             item.get("device"),
            "referrer_type":      item.get("referrer_type"),
            "page_views":         item.get("page_views"),
            "page_uniques":       item.get("page_uniques"),
            "page_avg_time":      item.get("page_avg_time"),
            "page_total_time":    item.get("page_total_time"),
            "page_avg_scroll":    item.get("page_avg_scroll"),
            "page_scroll_starts": item.get("page_scroll_starts"),
            "page_views_quality": item.get("page_views_quality"),
            "date":               start_date,
        })

    return rows


def _fetch_recurring(api_key, host, query_id):
    """Fetch results of a recurring historical query."""
    try:
        data = _cb_get(
            api_key,
            "/query/v2/recurring/fetch/",
            params={"host": host, "query_id": query_id},
        )
    except Exception as e:
        print(f"[CHARTBEAT] Recurring query fetch failed: {e}", flush=True)
        return []

    rows = []
    for item in data.get("data", []):
        rows.append({
            "path":               item.get("path"),
            "title":              item.get("title"),
            "author":             item.get("author"),
            "section":            item.get("section"),
            "device":             item.get("device"),
            "referrer_type":      item.get("referrer_type"),
            "page_views":         item.get("page_views"),
            "page_uniques":       item.get("page_uniques"),
            "page_avg_time":      item.get("page_avg_time"),
            "page_total_time":    item.get("page_total_time"),
            "page_avg_scroll":    item.get("page_avg_scroll"),
            "page_scroll_starts": item.get("page_scroll_starts"),
            "page_views_quality": item.get("page_views_quality"),
            "date":               item.get("date"),
        })

    return rows


def _fetch_video_engagement(api_key, video_host):
    """Real-time video engagement using video@host format."""
    try:
        data = _cb_get(
            api_key,
            "/live/video/v3/",
            params={"host": video_host},
        )
    except Exception as e:
        print(f"[CHARTBEAT] Video engagement fetch failed: {e}", flush=True)
        return []

    rows = []
    for item in data.get("videos", []):
        rows.append({
            "video_title":    item.get("title"),
            "video_path":     item.get("path"),
            "play_state":     item.get("play_state"),
            "video_plays":    item.get("plays"),
            "video_loads":    item.get("loads"),
            "video_play_rate": item.get("play_rate"),
            "video_avg_time": item.get("avg_time"),
        })

    return rows


# ---------------- MAIN SYNC ----------------

def connect_chartbeat(uid):
    """Validate credentials by hitting the live top pages endpoint."""
    try:
        api_key, host, _ = get_credentials(uid)
    except Exception as e:
        return {"status": "error", "message": str(e)}

    try:
        _cb_get(api_key, "/live/toppages/v3/", params={"host": host})
    except Exception as e:
        return {"status": "error", "message": f"Validation failed: {e}"}

    con = get_db()
    cur = con.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO google_connections
        (uid, source, enabled)
        VALUES (?, 'chartbeat', 1)
    """, (uid,))
    cur.execute("""
        INSERT OR REPLACE INTO chartbeat_connections
        (uid, host, connected_at)
        VALUES (?, ?, ?)
    """, (uid, host, datetime.datetime.utcnow().isoformat()))
    cur.execute("""
        UPDATE connector_configs
        SET status='connected'
        WHERE uid=? AND connector='chartbeat'
    """, (uid,))
    con.commit()
    con.close()

    return {"status": "success", "host": host}


def sync_chartbeat(uid, sync_type="historical"):
    try:
        api_key, host, query_id = get_credentials(uid)
    except Exception as e:
        return {"status": "error", "message": str(e)}

    state    = get_state(uid)
    today    = datetime.date.today()
    today_str = today.isoformat()

    all_rows = []

    # ── 1. Real-time top pages ──────────────────────────────────
    top_pages = _fetch_top_pages(api_key, host)
    _insert_top_pages(uid, top_pages)
    all_rows.extend(top_pages)
    print(f"[CHARTBEAT] Top pages fetched: {len(top_pages)}", flush=True)

    # ── 2. Historical page engagement ──────────────────────────
    engagement_rows = []

    if sync_type == "historical":
        start_date = (today - datetime.timedelta(days=30)).isoformat()
    else:
        start_date = state.get("last_sync_date") or (
            today - datetime.timedelta(days=1)
        ).isoformat()

    end_date = today_str

    if start_date < end_date:
        engagement_rows = _fetch_page_engagement(
            api_key, host, start_date, end_date
        )
        _insert_page_engagement(uid, engagement_rows)
        all_rows.extend(engagement_rows)
        print(f"[CHARTBEAT] Page engagement rows: {len(engagement_rows)}", flush=True)

    # ── 3. Recurring historical queries ────────────────────────
    recurring_rows = []
    effective_query_id = query_id or state.get("last_query_id")

    if effective_query_id:
        recurring_rows = _fetch_recurring(api_key, host, effective_query_id)
        _insert_page_engagement(uid, recurring_rows)
        all_rows.extend(recurring_rows)
        print(f"[CHARTBEAT] Recurring rows: {len(recurring_rows)}", flush=True)

    # ── 4. Video engagement (if video host format detected) ─────
    video_rows = []
    video_host = f"video@{host}"

    video_rows = _fetch_video_engagement(api_key, video_host)
    if video_rows:
        _insert_video_engagement(uid, video_rows)
        all_rows.extend(video_rows)
        print(f"[CHARTBEAT] Video rows: {len(video_rows)}", flush=True)

    # ── Push to destination ─────────────────────────────────────
    dest_cfg = get_active_destination(uid)

    rows_pushed = 0
    if dest_cfg and all_rows:
        rows_pushed = push_to_destination(dest_cfg, SOURCE, all_rows)
    elif not dest_cfg:
        print("[CHARTBEAT] No active destination configured.", flush=True)

    # ── Update state ────────────────────────────────────────────
    state["last_sync_date"] = today_str
    if effective_query_id:
        state["last_query_id"] = effective_query_id
    save_state(uid, state)

    return {
        "status":      "success",
        "rows_pushed": rows_pushed,
        "rows_found":  len(all_rows),
        "pages":       len(top_pages),
        "engagement":  len(engagement_rows) + len(recurring_rows),
        "videos":      len(video_rows),
        "sync_type":   sync_type,
    }


def disconnect_chartbeat(uid):
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source='chartbeat'
    """, (uid,))

    cur.execute("""
        DELETE FROM chartbeat_connections
        WHERE uid=?
    """, (uid,))

    cur.execute("""
        UPDATE connector_configs
        SET status='disconnected'
        WHERE uid=? AND connector='chartbeat'
    """, (uid,))

    con.commit()
    con.close()