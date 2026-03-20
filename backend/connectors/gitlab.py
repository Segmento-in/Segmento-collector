import requests
import sqlite3
import datetime
import json
import os
from urllib.parse import urlencode
from dotenv import load_dotenv

load_dotenv()

DB = "identity.db"

API = "https://gitlab.com/api/v4"


# ---------------- DB ----------------

def db():
    return sqlite3.connect(DB, timeout=60, check_same_thread=False)

from backend.security.secure_fetch import fetchone_secure

def get_gitlab_app(uid):

    con = sqlite3.connect("identity.db")
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
        SELECT client_id, client_secret
        FROM connector_configs
        WHERE uid=? AND connector='gitlab'
        LIMIT 1
    """, (uid,))

    row = fetchone_secure(cur)   # AUTO DECRYPT
    con.close()

    if not row:
        raise Exception("GitLab app not configured")

    return {
        "client_id": row["client_id"],
        "client_secret": row["client_secret"]
    }

# ---------------- STATE ---------------- #

def get_project_state(uid, project_id):
    con = db()
    cur = con.cursor()

    cur.execute("""
        SELECT last_commit_sha, last_issue_updated
        FROM gitlab_state
        WHERE uid=? AND project_id=?
    """, (uid, project_id))

    row = cur.fetchone()
    con.close()

    if not row:
        return {}

    return {
        "last_commit_date": row[0],
        "last_issue_date": row[1]
    }


def save_project_state(uid, project_id, commit_date=None, issue_date=None):
    con = db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO gitlab_state
        (uid, project_id, last_commit_sha, last_issue_updated)
        VALUES (?, ?, ?, ?)
    """, (
        uid,
        project_id,
        commit_date,
        issue_date
    ))

    con.commit()
    con.close()


# ---------------- AUTH ----------------

def get_auth_url(uid):

    app = get_gitlab_app(uid)

    params = {
        "client_id": app["client_id"],
        "redirect_uri": "http://localhost:4000/gitlab/callback",
        "response_type": "code",
        "scope": "read_user read_api read_repository"
    }

    return "https://gitlab.com/oauth/authorize?" + urlencode(params)

def exchange_code(uid, code):

    app = get_gitlab_app(uid)

    r = requests.post(
        "https://gitlab.com/oauth/token",
        data={
            "client_id": app["client_id"],
            "client_secret": app["client_secret"],
            "code": code,
            "grant_type": "authorization_code",
            "redirect_uri": "http://localhost:4000/gitlab/callback"
        },
        timeout=20
    )

    data = r.json()
    print("GITLAB TOKEN:", data, flush=True)

    if "access_token" not in data:
        raise Exception(f"GitLab OAuth failed: {data}")

    return data

def save_token(uid, data):

    con = db()
    cur = con.cursor()

    now = datetime.datetime.now().isoformat()

    expires = datetime.datetime.now() + datetime.timedelta(
        seconds=data.get("expires_in", 0)
    )

    cur.execute("""
    INSERT OR REPLACE INTO gitlab_tokens
    (uid, access_token, refresh_token, expires_at, fetched_at)
    VALUES (?,?,?,?,?)
    """, (
        uid,
        data["access_token"],
        data.get("refresh_token"),
        expires.isoformat(),
        now
    ))

    con.commit()
    con.close()

def get_token(uid):

    con = db()
    cur = con.cursor()

    cur.execute("""
    SELECT access_token FROM gitlab_tokens
    WHERE uid=?
    """, (uid,))

    row = cur.fetchone()
    con.close()

    if not row:
        raise Exception("GitLab not connected")

    return row[0]


# ---------------- API ----------------

def gl_get(uid, path, params=None):

    token = get_token(uid)

    headers = {
        "Authorization": f"Bearer {token}"
    }

    r = requests.get(
        API + path,
        headers=headers,
        params=params,
        timeout=20
    )

    if r.status_code != 200:
        raise Exception(r.text)

    return r.json()


# ---------------- SYNC PROJECTS ----------------

def sync_projects(uid):

    data = gl_get(uid, "/projects", {"membership": True, "per_page": 100})

    con = db()
    cur = con.cursor()

    now = datetime.datetime.now().isoformat()

    count = 0

    for p in data:

        cur.execute("""
        INSERT OR IGNORE INTO gitlab_projects
        (uid, project_id, name, path,
         namespace, visibility,
         web_url, raw_json, fetched_at)
        VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            uid,
            p["id"],
            p["name"],
            p["path"],
            p["namespace"]["full_path"],
            p["visibility"],
            p["web_url"],
            json.dumps(p),
            now
        ))

        count += 1

    con.commit()
    con.close()

    return {"projects": count}


# ---------------- SYNC COMMITS ----------------

def sync_commits(uid, project_id, sync_type="historical", limit=200):

    state = get_project_state(uid, project_id)
    last_commit_date = state.get("last_commit_date")

    data = gl_get(
        uid,
        f"/projects/{project_id}/repository/commits",
        {"per_page": limit}
    )

    con = db()
    cur = con.cursor()

    now = datetime.datetime.utcnow().isoformat()

    count = 0
    newest_date = None
    new_rows = []

    for c in data:

        commit_date = c["created_at"]

        if sync_type == "incremental" and last_commit_date:
            if commit_date <= last_commit_date:
                break

        cur.execute("""
        INSERT OR IGNORE INTO gitlab_commits
        (uid, project_id, sha,
         author, message, date,
         web_url, raw_json, fetched_at)
        VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            uid,
            project_id,
            c["id"],
            c["author_name"],
            c["message"],
            commit_date,
            c["web_url"],
            json.dumps(c),
            now
        ))

        if cur.rowcount > 0:
            new_rows.append({
                "project_id": project_id,
                "sha": c["id"],
                "author": c["author_name"],
                "message": c["message"],
                "date": commit_date,
                "web_url": c["web_url"]
            })

        if not newest_date or commit_date > newest_date:
            newest_date = commit_date

        count += 1

    con.commit()
    con.close()

    if newest_date:
        save_project_state(uid, project_id, commit_date=newest_date, issue_date=state.get("last_issue_date"))

    return {"commits": count, "rows": new_rows}


# ---------------- SYNC ISSUES ----------------

def sync_issues(uid, project_id, sync_type="historical", limit=200):

    state = get_project_state(uid, project_id)
    last_issue_date = state.get("last_issue_date")

    data = gl_get(
        uid,
        f"/projects/{project_id}/issues",
        {"per_page": limit}
    )

    con = db()
    cur = con.cursor()

    now = datetime.datetime.utcnow().isoformat()

    count = 0
    newest_date = None
    new_rows = []

    for i in data:

        issue_date = i["created_at"]

        if sync_type == "incremental" and last_issue_date:
            if issue_date <= last_issue_date:
                break

        cur.execute("""
        INSERT OR IGNORE INTO gitlab_issues
        (uid, issue_id, project_id,
         title, state, author,
         created_at, web_url,
         raw_json, fetched_at)
        VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            uid,
            i["id"],
            project_id,
            i["title"],
            i["state"],
            i["author"]["username"],
            issue_date,
            i["web_url"],
            json.dumps(i),
            now
        ))

        if cur.rowcount > 0:
            new_rows.append({
                "project_id": project_id,
                "issue_id": i["id"],
                "title": i["title"],
                "state": i["state"],
                "author": i["author"]["username"],
                "created_at": issue_date,
                "web_url": i["web_url"]
            })

        if not newest_date or issue_date > newest_date:
            newest_date = issue_date

        count += 1

    con.commit()
    con.close()

    if newest_date:
        save_project_state(uid, project_id, commit_date=state.get("last_commit_date"), issue_date=newest_date)

    return {"issues": count, "rows": new_rows}

# ---------------- SYNC MERGE REQUESTS ----------------

def sync_mrs(uid, project_id, limit=200):

    data = gl_get(
        uid,
        f"/projects/{project_id}/merge_requests",
        {"per_page": limit, "state": "all"}
    )

    con = db()
    cur = con.cursor()

    now = datetime.datetime.now().isoformat()

    count = 0

    for m in data:

        cur.execute("""
        INSERT OR IGNORE INTO gitlab_merge_requests
        (uid, mr_id, project_id,
         title, state, author,
         created_at, web_url,
         raw_json, fetched_at)
        VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            uid,
            m["id"],
            project_id,
            m["title"],
            m["state"],
            m["author"]["username"],
            m["created_at"],
            m["web_url"],
            json.dumps(m),
            now
        ))

        count += 1

    con.commit()
    con.close()

    return {"merge_requests": count}