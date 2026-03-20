import requests
import sqlite3
import datetime
import json
import os

from urllib.parse import urlencode
from dotenv import load_dotenv
from backend.destinations.destination_router import push_to_destination


DB = "identity.db"
SOURCE = "github"

API = "https://api.github.com"


# ---------------- DB ---------------- #

def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


# ---------------- CONNECTION ---------------- #

def enable_connection(uid):
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO google_connections
        (uid, source, enabled)
        VALUES (?, ?, 1)
    """, (uid, SOURCE))

    con.commit()
    con.close()


def disable_connection(uid):
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source=?
    """, (uid, SOURCE))

    con.commit()
    con.close()


def is_connected(uid):
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT enabled FROM google_connections
        WHERE uid=? AND source=?
    """, (uid, SOURCE))

    row = cur.fetchone()
    con.close()

    return bool(row and row[0] == 1)

def get_repo_state(uid, repo_full):
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT last_commit_sha
        FROM github_state
        WHERE uid=? AND repo_full=?
    """, (uid, repo_full))

    row = cur.fetchone()
    con.close()

    return row[0] if row else None


def save_repo_state(uid, repo_full, last_sha):
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO github_state
        (uid, repo_full, last_commit_sha)
        VALUES (?, ?, ?)
    """, (uid, repo_full, last_sha))

    con.commit()
    con.close()


# ---------------- DESTINATION ---------------- #

def get_active_destination(uid):
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT dest_type, host, port, username, password, database_name
        FROM destination_configs
        WHERE uid=? AND source=? AND is_active=1
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

from backend.security.secure_fetch import fetchone_secure


def get_github_app(uid):

    con = sqlite3.connect("identity.db")
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
        SELECT client_id, client_secret
        FROM connector_configs
        WHERE uid=? AND connector='github'
        LIMIT 1
    """, (uid,))

    row = fetchone_secure(cur)
    con.close()

    if not row:
        raise Exception("GitHub app not configured")

    return {
        "client_id": row["client_id"],
        "client_secret": row["client_secret"]
    }

# ---------------- AUTH ---------------- #

def get_auth_url(uid):

    app = get_github_app(uid)

    params = {
        "client_id": app["client_id"],
        "scope": "repo read:user",
        "allow_signup": "true"
    }

    return (
        "https://github.com/login/oauth/authorize?"
        + urlencode(params)
    )

def exchange_code(uid, code):

    app = get_github_app(uid)

    r = requests.post(
        "https://github.com/login/oauth/access_token",
        headers={"Accept": "application/json"},
        data={
            "client_id": app["client_id"],
            "client_secret": app["client_secret"],
            "code": code
        },
        timeout=20
    )

    return r.json()

def save_token(uid, data):
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO github_tokens
        (uid, access_token, scope, token_type, fetched_at)
        VALUES (?,?,?,?,?)
    """, (
        uid,
        data["access_token"],
        data.get("scope"),
        data.get("token_type"),
        datetime.datetime.utcnow().isoformat()
    ))

    con.commit()
    con.close()
    enable_connection(uid)


def get_token(uid):
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT access_token FROM github_tokens
        WHERE uid=?
    """, (uid,))

    row = cur.fetchone()
    con.close()

    if not row:
        raise Exception("GitHub not connected")

    return row[0]


# ---------------- API ---------------- #

def gh_get(uid, path, params=None):
    token = get_token(uid)

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json"
    }

    r = requests.get(API + path, headers=headers, params=params, timeout=30)

    # Handle empty repository (409)
    if r.status_code == 409:
        return []

    # Handle 404 gracefully
    if r.status_code == 404:
        return []

    if r.status_code != 200:
        raise Exception(f"GitHub API Error {r.status_code}: {r.text}")

    return r.json()

# ---------------- MAIN SYNC ---------------- #

def sync_github(uid):

    if not is_connected(uid):
        return {"status": "error", "message": "GitHub not connected"}

    # Get sync type
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source=?
        LIMIT 1
    """, (uid, SOURCE))

    row = cur.fetchone()
    con.close()

    sync_type = row[0] if row else "historical"

    print(f"[GITHUB] Sync type: {sync_type}", flush=True)

    repos = gh_get(uid, "/user/repos", {"per_page": 100})

    all_rows = []
    total_new = 0

    for r in repos:

        repo_full = r["full_name"]

        # Get last synced SHA for this repo
        con = get_db()
        cur = con.cursor()

        cur.execute("""
            SELECT last_commit_sha
            FROM github_state
            WHERE uid=? AND repo_full=?
        """, (uid, repo_full))

        row = cur.fetchone()
        con.close()

        last_sha = row[0] if row else None

        try:
            commits = gh_get(
                uid,
                f"/repos/{repo_full}/commits",
                {"per_page": 100}
            )
        except Exception as e:
            print(f"[GITHUB] Skipping repo {repo_full}: {e}", flush=True)
            continue

        repo_new_commits = []

        for c in commits:

            sha = c["sha"]

            # Incremental stop condition
            if sync_type == "incremental" and last_sha:
                if sha == last_sha:
                    break

            repo_new_commits.append({
                "repo": repo_full,
                "sha": sha,
                "author": c["commit"]["author"]["name"],
                "message": c["commit"]["message"],
                "date": c["commit"]["author"]["date"]
            })

        if repo_new_commits:
            total_new += len(repo_new_commits)

            # Save newest SHA (first item returned is newest)
            newest_sha = repo_new_commits[0]["sha"]

            con = get_db()
            cur = con.cursor()

            cur.execute("""
                INSERT OR REPLACE INTO github_state
                (uid, repo_full, last_commit_sha)
                VALUES (?, ?, ?)
            """, (uid, repo_full, newest_sha))

            con.commit()
            con.close()

            all_rows.extend(repo_new_commits)

    # Push to destination
    dest_cfg = get_active_destination(uid)

    if not dest_cfg:
        return {"status": "error", "message": "No active destination"}

    inserted = push_to_destination(dest_cfg, SOURCE, all_rows)

    print(f"[GITHUB] New rows found: {total_new}", flush=True)
    print(f"[GITHUB] Rows pushed: {inserted}", flush=True)

    return {
        "status": "success",
        "rows_pushed": inserted,
        "rows_found": total_new,
        "sync_type": sync_type
    }