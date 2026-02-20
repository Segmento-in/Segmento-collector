import requests
import sqlite3
import json
import time
from datetime import datetime, timedelta


DB = "identity.db"
BASE = "https://services.nvd.nist.gov/rest/json/cves/2.0"

# DB

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

# TIME

def utc_now():
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z")

# CONFIG LOAD

def load_config(uid):

    con = db()
    cur = con.cursor()

    cur.execute("""
        SELECT api_key, config_json
        FROM connector_configs
        WHERE uid=? AND connector='nvd'
        LIMIT 1
    """, (uid,))

    row = cur.fetchone()
    con.close()

    if not row:
        return {"error": "No NVD config"}

    api_key = row[0]

    config = json.loads(row[1] or "{}")
    keywords = config.get("keywords", [])
    return api_key, keywords

# STATE

def get_last_modified(uid):

    con = db()
    cur = con.cursor()

    cur.execute("""
        SELECT last_modified
        FROM nvd_state
        WHERE uid=?
    """, (uid,))

    row = cur.fetchone()
    con.close()

    if row:
        return row[0]

    return "1999-01-01T00:00:00.000Z"


def save_last_modified(uid, ts):

    con = db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO nvd_state(uid,last_modified)
        VALUES(?,?)
    """, (uid, ts))

    con.close()

# INSERT

def insert_cves(uid, rows):

    con = db()
    cur = con.cursor()

    now = datetime.utcnow().isoformat()

    data = []

    for v in rows:

        cve = v.get("cve", {})

        desc = ""
        if cve.get("descriptions"):
            desc = cve["descriptions"][0].get("value")

        severity = None
        score = None

        metrics = cve.get("metrics", {})

        for k in ["cvssMetricV31", "cvssMetricV30"]:
            if k in metrics:
                m = metrics[k][0]["cvssData"]
                severity = m.get("baseSeverity")
                score = m.get("baseScore")
                break

        ref_url = None
        refs = cve.get("references", [])
        if refs:
            ref_url = refs[0].get("url")

        data.append((
            uid,
            cve.get("id"),
            cve.get("sourceIdentifier"),
            cve.get("published"),
            cve.get("lastModified"),
            cve.get("vulnStatus"),
            desc,
            severity,
            score,
            ref_url,
            json.dumps(v, ensure_ascii=False),
            now
        ))

    cur.executemany("""
        INSERT OR IGNORE INTO nvd_cves
        (uid,cve_id,source_identifier,published,last_modified,
         vuln_status,description,severity,cvss_score,
         reference_url,raw_json,fetched_at)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
    """, data)

    con.close()

# SAFE REQUEST

def safe_get(headers, params):

    try:

        r = requests.get(
            BASE,
            headers=headers,
            params=params,
            timeout=40
        )

        if r.status_code == 200:
            return r.json()

        if r.status_code == 429:
            print("[NVD] Rate limited → sleeping")
            time.sleep(60)

    except Exception as e:
        print("[NVD ERROR]", e)
        time.sleep(5)

    return None

# MAIN SYNC

def sync_nvd(uid, sync_type="incremental"):

    api_key, keywords = load_config(uid)

    if not api_key:
        return {"error": "No API key configured"}

    headers = {
        "User-Agent": "SegmentoCollector/1.0",
        "X-Api-Key": api_key
    }

    # ---------------- Time Window ----------------

    if sync_type == "historical":
        start_date = "1999-01-01T00:00:00.000Z"
    else:
        start_date = get_last_modified(uid)

    end_date = utc_now()

    newest = start_date
    collected = []

    # ---------------- Fetch ----------------

    for kw in keywords:

        start_index = 0

        while True:

            params = {
                "keywordSearch": kw,
                "resultsPerPage": 200,
                "startIndex": start_index,
                "lastModStartDate": start_date,
                "lastModEndDate": end_date
            }

            data = safe_get(headers, params)

            if not data:
                break

            vulns = data.get("vulnerabilities", [])

            if not vulns:
                break

            for v in vulns:

                cve = v.get("cve", {})
                mod = cve.get("lastModified")

                if mod and mod > newest:
                    newest = mod

                collected.append(v)

            start_index += 200

            if start_index >= data.get("totalResults", 0):
                break

            time.sleep(1.2)

    # ---------------- Store ----------------

    if collected:
        insert_cves(uid, collected)
        save_last_modified(uid, newest)

    print(f"[NVD] CVEs collected: {len(collected)}")

    return {
        "rows": collected,
        "cves": len(collected),
        "sync_type": sync_type
    }