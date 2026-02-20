import sys
import os
import datetime

# Add project root to PYTHONPATH
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
import requests
import sqlite3

from flask import (
    Flask,
    render_template,
    redirect,
    jsonify,
    request
)

app = Flask(
    __name__,
    template_folder="templates",
    static_folder="static"
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "..", "identity.db")

# ================= BASIC =================
def get_google_status(source):

    uid = request.cookies.get("uid") or "demo_user"

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source=?
        LIMIT 1
    """, (uid, source))

    row = cur.fetchone()
    conn.close()

    return bool(row and row[0] == 1)

@app.route("/")
def home():
    return render_template("index.html")


@app.route("/tracking")
def tracking():
    return render_template("tracking.html")


@app.route("/connectors")
def connectors():
    return render_template("connectors.html")

@app.route("/api/status/<source>")
def generic_google_status(source):

    uid = request.cookies.get("uid") or "demo_user"

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source=?
        LIMIT 1
    """, (uid, source))

    row = cur.fetchone()
    conn.close()

    return jsonify({
        "connected": bool(row and row[0] == 1)
    })

@app.route("/connectors/<source>/job/save", methods=["POST"])
def ui_save_job(source):

    r = requests.post(
        f"http://localhost:4000/google/job/save/{source}",
        json=request.json
    )

    return jsonify(r.json())

@app.route("/connectors/<source>/job/get")
def ui_get_job(source):

    r = requests.get(
        f"http://localhost:4000/google/job/get/{source}"
    )

    return jsonify(r.json())

@app.route("/connectors/<source>/disconnect")
def ui_disconnect(source):

    r = requests.get(
        f"http://localhost:4000/connectors/{source}/disconnect",
        cookies=request.cookies
    )

    return jsonify(r.json())

# ================= GITHUB ========================

@app.route("/connectors/github")
def github_page():
    return render_template("connectors/github.html")


@app.route("/connectors/github/connect")
def github_connect():
    return redirect("http://localhost:4000/github/connect")

@app.route("/connectors/github/sync")
def github_sync():

    res = requests.get(
        "http://localhost:4000/connectors/github/sync",
        cookies=request.cookies
    )

    return jsonify(res.json())

@app.route("/dashboard/github")
def github_dashboard():
    return render_template("dashboards/github.html")

@app.route("/api/github/data/<table>")
def github_data(table):

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    if table == "repos":
        cur.execute("SELECT * FROM github_repos")

    elif table == "commits":
        cur.execute("SELECT * FROM github_commits")

    elif table == "issues":
        cur.execute("SELECT * FROM github_issues")

    else:
        return jsonify([])

    rows = cur.fetchall()

    conn.close()

    return jsonify([dict(r) for r in rows])

@app.route("/api/status/github")
def github_status_proxy():

    r = requests.get(
        "http://localhost:4000/api/status/github",
        cookies=request.cookies
    )

    return jsonify(r.json())

@app.route("/connectors/github/job/get")
def github_job_get_proxy():

    r = requests.get(
        "http://localhost:4000/connectors/github/job/get",
        cookies=request.cookies
    )

    return jsonify(r.json())

@app.route("/connectors/github/job/save", methods=["POST"])
def github_job_save_proxy():

    r = requests.post(
        "http://localhost:4000/connectors/github/job/save",
        json=request.get_json(),
        cookies=request.cookies
    )

    return jsonify(r.json())

@app.route("/connectors/github/save_app", methods=["POST"])
def github_save_app_proxy():

    r = requests.post(
        "http://localhost:4000/connectors/github/save_app",
        json=request.get_json(),
        cookies=request.cookies
    )

    return jsonify(r.json()), r.status_code

# ================= REDDIT ========================

@app.route("/connectors/reddit")
def reddit_page():
    return render_template("connectors/reddit.html")

@app.route("/connectors/reddit/connect")
def reddit_connect():

    r = requests.get(
        "http://localhost:4000/connectors/reddit/connect",
        cookies=request.cookies
    )

    return jsonify(r.json()), r.status_code

@app.route("/connectors/reddit/disconnect")
def reddit_disconnect():

    r = requests.get(
        "http://localhost:4000/connectors/reddit/disconnect",
        cookies=request.cookies
    )

    return jsonify(r.json())

@app.route("/connectors/reddit/save_config", methods=["POST"])
def reddit_save_config_proxy():

    r = requests.post(
        "http://localhost:4000/connectors/reddit/save_config",
        json=request.get_json(),
        cookies=request.cookies
    )

    return jsonify(r.json()), r.status_code

@app.route("/connectors/reddit/sync")
def reddit_sync():

    r = requests.get(
        "http://localhost:4000/connectors/reddit/sync",
        cookies=request.cookies
    )

    return jsonify(r.json())

# ---------- Reddit Dashboard ----------

@app.route("/dashboard/reddit")
def reddit_dashboard():
    return render_template("dashboards/reddit.html")

@app.route("/api/status/reddit")
def reddit_status():

    r = requests.get(
        "http://localhost:4000/api/status/reddit",
        cookies=request.cookies
    )

    return jsonify(r.json())

@app.route("/connectors/reddit/job/get")
def reddit_job_get():

    r = requests.get(
        "http://localhost:4000/connectors/reddit/job/get",
        cookies=request.cookies
    )

    return jsonify(r.json())

@app.route("/connectors/reddit/job/save", methods=["POST"])
def reddit_job_save():

    r = requests.post(
        "http://localhost:4000/connectors/reddit/job/save",
        json=request.get_json(),
        cookies=request.cookies
    )

    return jsonify(r.json())

# ---------- Reddit Data API ----------

@app.route("/api/reddit/data/<table>")
def reddit_data(table):

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    if table == "posts":
        cur.execute("SELECT * FROM reddit_posts")

    elif table == "messages":
        cur.execute("SELECT * FROM reddit_messages")

    elif table == "profile":
        cur.execute("SELECT * FROM reddit_profiles")

    else:
        return jsonify([])

    rows = cur.fetchall()

    conn.close()

    return jsonify([dict(r) for r in rows])

# ================= MEDIUM =================

@app.route("/connectors/medium")
def medium_page():
    return render_template("connectors/medium.html")

@app.route("/connectors/medium/connect", methods=["POST"])
def medium_connect():

    data = request.json

    r = requests.post(
        "http://localhost:4000/connectors/medium/connect",
        json=data,
        cookies=request.cookies
    )

    return jsonify(r.json())

@app.route("/connectors/medium/sync")
def medium_sync():

    r = requests.get(
        "http://localhost:4000/connectors/medium/sync",
        cookies=request.cookies
    )

    return jsonify(r.json())

@app.route("/dashboard/medium")
def medium_dashboard():
    return render_template("dashboards/medium.html")



@app.route("/api/status/medium")
def medium_status():

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""SELECT COUNT(DISTINCT uid) FROM medium_posts
                WHERE uid='demo_user'
                """)


    count = cur.fetchone()[0]

    conn.close()

    return jsonify({"connected": count > 0})



@app.route("/api/medium/data/posts")
def medium_data():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("SELECT * FROM medium_posts ORDER BY published DESC")

    rows = cur.fetchall()

    conn.close()

    return jsonify([dict(r) for r in rows])

# ================= GITLAB =================

@app.route("/connectors/gitlab")
def gitlab_page():
    return render_template("connectors/gitlab.html")

@app.route("/connectors/gitlab/save_app", methods=["POST"])
def gitlab_save_app_proxy():

    r = requests.post(
        "http://localhost:4000/connectors/gitlab/save_app",
        json=request.get_json(),
        cookies=request.cookies
    )

    return jsonify(r.json()), r.status_code

@app.route("/api/status/gitlab")
def gitlab_status_proxy():

    r = requests.get(
        "http://localhost:4000/api/status/gitlab",
        cookies=request.cookies
    )

    return jsonify(r.json())

@app.route("/connectors/gitlab/job/get")
def gitlab_job_get_proxy():

    r = requests.get(
        "http://localhost:4000/connectors/gitlab/job/get",
        cookies=request.cookies
    )

    return jsonify(r.json())


@app.route("/connectors/gitlab/job/save", methods=["POST"])
def gitlab_job_save_proxy():

    r = requests.post(
        "http://localhost:4000/connectors/gitlab/job/save",
        json=request.get_json(),
        cookies=request.cookies
    )

    return jsonify(r.json())

@app.route("/connectors/gitlab/connect")
def gitlab_connect():
    return redirect("http://localhost:4000/gitlab/connect")


@app.route("/connectors/gitlab/sync")
def gitlab_sync():

    try:
        r = requests.get(
            "http://localhost:4000/connectors/gitlab/sync",
            cookies=request.cookies,
            timeout=300
        )

        return jsonify(r.json()), r.status_code

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/dashboard/gitlab")
def gitlab_dashboard():
    return render_template("dashboards/gitlab.html")

@app.route("/api/gitlab/<table>")
def gitlab_data(table):

    conn = sqlite3.connect("../identity.db")
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()


    if table=="projects":
        cur.execute("SELECT * FROM gitlab_projects")

    elif table=="commits":
        cur.execute("SELECT * FROM gitlab_commits")

    elif table=="issues":
        cur.execute("SELECT * FROM gitlab_issues")

    elif table=="mrs":
        cur.execute("SELECT * FROM gitlab_merge_requests")

    else:
        return jsonify([])


    rows=cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])

# ================= DEVTO =================

@app.route("/connectors/devto")
def devto_page():
    return render_template("connectors/devto.html")

@app.route("/api/status/devto")
def devto_status_proxy():

    r = requests.get(
        "http://localhost:4000/api/status/devto",
        cookies=request.cookies
    )

    return jsonify(r.json())

@app.route("/connectors/devto/connect")
def devto_connect():

    r = requests.get(
        "http://localhost:4000/connectors/devto/connect",
        cookies=request.cookies
    )

    return redirect("/connectors/devto")


@app.route("/connectors/devto/disconnect")
def devto_disconnect():

    r = requests.get(
        "http://localhost:4000/connectors/devto/disconnect",
        cookies=request.cookies
    )

    return jsonify(r.json())


@app.route("/connectors/devto/sync")
def devto_sync():

    r = requests.get(
        "http://localhost:4000/connectors/devto/sync",
        cookies=request.cookies
    )

    return jsonify(r.json())


@app.route("/dashboard/devto")
def devto_dashboard():
    return render_template("dashboards/devto.html")

@app.route("/connectors/devto/job/get")
def devto_job_get_proxy():

    r = requests.get(
        "http://localhost:4000/connectors/devto/job/get",
        cookies=request.cookies
    )

    return jsonify(r.json())


@app.route("/connectors/devto/job/save", methods=["POST"])
def devto_job_save_proxy():

    r = requests.post(
        "http://localhost:4000/connectors/devto/job/save",
        json=request.get_json(),
        cookies=request.cookies
    )

    return jsonify(r.json())

# ================= STACKOVERFLOW =================

@app.route("/connectors/stackoverflow")
def stackoverflow_page():
    return render_template("connectors/stackoverflow.html")

@app.route("/connectors/stackoverflow/save_config", methods=["POST"])
def stackoverflow_save_config_proxy():

    r = requests.post(
        "http://localhost:4000/connectors/stackoverflow/save_config",
        json=request.get_json(),
        cookies=request.cookies
    )

    return jsonify(r.json()), r.status_code

# CONNECT
@app.route("/connectors/stackoverflow/connect")
def stackoverflow_connect():

    r = requests.get(
        "http://localhost:4000/connectors/stackoverflow/connect",
        cookies=request.cookies
    )

    if r.status_code != 200:
        return r.text, 400

    return redirect("/connectors/stackoverflow")


# DISCONNECT
@app.route("/connectors/stackoverflow/disconnect")
def stackoverflow_disconnect():

    r = requests.get(
        "http://localhost:4000/connectors/stackoverflow/disconnect",
        cookies=request.cookies
    )

    return jsonify(r.json())


# MANUAL SYNC
@app.route("/connectors/stackoverflow/sync")
def stackoverflow_sync():

    r = requests.get(
        "http://localhost:4000/connectors/stackoverflow/sync",
        cookies=request.cookies
    )

    return jsonify(r.json())


@app.route("/dashboard/stackoverflow")
def stackoverflow_dashboard():
    return render_template("dashboards/stackoverflow.html")


# ---------- STATUS ----------

@app.route("/api/status/stackoverflow")
def stackoverflow_status():

    r = requests.get(
        "http://localhost:4000/api/status/stackoverflow",
        cookies=request.cookies
    )

    return jsonify(r.json())


# ---------- DATA APIs ----------

@app.route("/api/stackoverflow/data/questions")
def stack_questions():

    conn = sqlite3.connect("../identity.db")
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM stack_questions
        ORDER BY fetched_at DESC
    """)

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])


@app.route("/api/stackoverflow/data/answers")
def stack_answers():

    conn = sqlite3.connect("../identity.db")
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM stack_answers
        ORDER BY fetched_at DESC
    """)

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])


@app.route("/api/stackoverflow/data/users")
def stack_users():

    conn = sqlite3.connect("../identity.db")
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM stack_users
        ORDER BY fetched_at DESC
    """)

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])

# ================= HACKERNEWS =================

@app.route("/connectors/hackernews")
def hackernews_page():
    return render_template("connectors/hackernews.html")


@app.route("/connectors/hackernews/connect")
def hackernews_connect():
    requests.get("http://localhost:4000/connectors/hackernews/connect")
    return redirect("/connectors/hackernews")

@app.route("/connectors/hackernews/sync")
def hackernews_sync():
    r = requests.get("http://localhost:4000/connectors/hackernews/sync")
    return jsonify(r.json())

@app.route("/dashboard/hackernews")
def hackernews_dashboard():
    return render_template("dashboards/hackernews.html")

# ---------- STATUS ----------

@app.route("/api/status/hackernews")
def hackernews_status():

    conn = sqlite3.connect("../identity.db")
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM hackernews_stories")

    count = cur.fetchone()[0]

    conn.close()

    return jsonify({"connected": count > 0})


# ---------- DATA API ----------

@app.route("/api/hackernews/data/stories")
def hackernews_stories():

    conn = sqlite3.connect("../identity.db")
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM hackernews_stories
        ORDER BY time DESC
    """)

    rows = cur.fetchall()

    conn.close()

    return jsonify([dict(r) for r in rows])

# ================= NVD =================

@app.route("/connectors/nvd")
def nvd_page():
    return render_template("connectors/nvd.html")

@app.route("/connectors/nvd/save_config", methods=["POST"])
def nvd_save_config_proxy():

    r = requests.post(
        "http://localhost:4000/connectors/nvd/save_config",
        json=request.get_json(),
        cookies=request.cookies
    )

    try:
        return jsonify(r.json()), r.status_code
    except:
        return jsonify({
            "error": "identity_server returned non-json",
            "status": r.status_code,
            "body": r.text
        }), r.status_code

# CONNECT = FIRST SYNC
@app.route("/connectors/nvd/connect")
def nvd_connect():

    r = requests.get(
        "http://localhost:4000/connectors/nvd/connect",
        cookies=request.cookies
    )

    if r.status_code != 200:
        return r.text, 400

    return redirect("/connectors/nvd")

# MANUAL SYNC
@app.route("/connectors/nvd/sync")
def nvd_sync():

    r = requests.get(
        "http://localhost:4000/connectors/nvd/sync",
        cookies=request.cookies
    )

    return jsonify(r.json())

@app.route("/dashboard/nvd")
def nvd_dashboard():
    return render_template("dashboards/nvd.html")


# ---------- STATUS ----------

@app.route("/api/status/nvd")
def nvd_status():

    r = requests.get("http://localhost:4000/api/status/nvd")

    return jsonify(r.json())

# ---------- DATA API ----------

@app.route("/api/nvd/data/cves")
def nvd_cves():

    conn = sqlite3.connect("../identity.db")
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM nvd_cves
        ORDER BY published DESC
    """)

    rows = cur.fetchall()

    conn.close()

    return jsonify([dict(r) for r in rows])

# ================= DISCORD =================

@app.route("/connectors/discord")
def discord_page():
    return render_template("connectors/discord.html")

@app.route("/connectors/discord/save_config", methods=["POST"])
def discord_save_config_proxy():

    r = requests.post(
        "http://localhost:4000/connectors/discord/save_config",
        json=request.get_json(),
        cookies=request.cookies
    )

    return jsonify(r.json()), r.status_code

@app.route("/connectors/discord/connect")
def discord_connect_proxy():

    r = requests.get(
        "http://localhost:4000/connectors/discord/connect"
    )

    if r.status_code != 200:
        return r.text, 400

    return redirect("/connectors/discord")

@app.route("/connectors/discord/disconnect")
def discord_disconnect_proxy():

    r = requests.get(
        "http://localhost:4000/connectors/discord/disconnect",
        cookies=request.cookies
    )

    return jsonify(r.json())

@app.route("/connectors/discord/sync")
def discord_sync():

    r = requests.get(
        "http://localhost:4000/connectors/discord/sync",
        cookies=request.cookies
    )

    return jsonify(r.json())

@app.route("/api/status/discord")
def discord_status_proxy():

    r = requests.get(
        "http://localhost:4000/api/status/discord",
        cookies=request.cookies
    )

    return jsonify(r.json())

# ================= TELEGRAM =================

@app.route("/connectors/telegram")
def telegram_page():
    return render_template("connectors/telegram.html")

@app.route("/connectors/telegram/connect")
def telegram_connect_proxy():

    r = requests.get(
        "http://localhost:4000/connectors/telegram/connect",
        cookies=request.cookies
    )

    if r.status_code != 200:
        return r.text, 400

    return redirect("/connectors/telegram")

@app.route("/connectors/telegram/disconnect")
def telegram_disconnect():

    r = requests.get(
        "http://localhost:4000/connectors/telegram/disconnect"
    )

    return jsonify(r.json())

@app.route("/connectors/telegram/sync")
def telegram_sync():

    r = requests.get(
        "http://localhost:4000/connectors/telegram/sync"
    )

    try:
        return jsonify(r.json())
    except:
        return jsonify({"error": "sync failed"}), 500

@app.route("/dashboard/telegram")
def telegram_dashboard():
    return render_template("dashboards/telegram.html")


# -------- STATUS --------

@app.route("/api/status/telegram")
def telegram_status_proxy():

    r = requests.get(
        "http://localhost:4000/api/status/telegram",
        cookies=request.cookies
    )

    return jsonify(r.json())

# -------- DATA APIs --------

@app.route("/api/telegram/channels")
def telegram_channels():

    conn = sqlite3.connect("../identity.db")
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("SELECT * FROM telegram_channels")

    rows = cur.fetchall()

    conn.close()

    return jsonify([dict(r) for r in rows])


@app.route("/api/telegram/messages/<cid>")
def telegram_messages(cid):

    # Trigger sync before fetch
    requests.get("http://localhost:4000/connectors/telegram/sync")

    conn = sqlite3.connect("../identity.db")
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM telegram_messages
        WHERE channel_id=?
        ORDER BY date DESC
        LIMIT 200
    """, (cid,))

    rows = cur.fetchall()

    conn.close()

    return jsonify([dict(r) for r in rows])

@app.route("/connectors/telegram/save_config", methods=["POST"])
def telegram_save_config_proxy():

    r = requests.post(
        "http://localhost:4000/connectors/telegram/save_config",
        json=request.get_json(),
        cookies=request.cookies
    )

    return jsonify(r.json()), r.status_code

# ================= TUMBLR =================

@app.route("/connectors/tumblr")
def tumblr_page():
    return render_template("connectors/tumblr.html")

@app.route("/connectors/tumblr/connect")
def tumblr_connect_proxy():

    r=requests.get(
        "http://localhost:4000/connectors/tumblr/connect",
        cookies=request.cookies
    )

    return redirect("/connectors/tumblr")

@app.route("/connectors/tumblr/save_config", methods=["POST"])
def tumblr_save_config_proxy():

    r = requests.post(
        "http://localhost:4000/connectors/tumblr/save_config",
        json=request.json,
        cookies=request.cookies
    )

    try:
        return jsonify(r.json()), r.status_code
    except:
        return jsonify({
            "error": "identity_server error",
            "raw": r.text
        }), r.status_code

@app.route("/connectors/tumblr/sync")
def tumblr_sync_proxy():

    r=requests.get(
        "http://localhost:4000/connectors/tumblr/sync",
        cookies=request.cookies
    )

    return jsonify(r.json())

@app.route("/dashboard/tumblr")
def tumblr_dashboard():
    return render_template("dashboards/tumblr.html")

@app.route("/connectors/tumblr/disconnect")
def tumblr_disconnect_proxy():

    r=requests.get(
        "http://localhost:4000/connectors/tumblr/disconnect",
        cookies=request.cookies
    )

    return jsonify(r.json())

# -------- STATUS --------

@app.route("/api/status/tumblr")
def tumblr_status_proxy():

    r = requests.get(
        "http://localhost:4000/api/status/tumblr",
        cookies=request.cookies
    )

    return jsonify(r.json())

# -------- DATA APIs --------

@app.route("/api/tumblr/blogs")
def tumblr_blogs():

    conn = sqlite3.connect("../identity.db")
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("SELECT * FROM tumblr_blogs")

    rows = cur.fetchall()

    conn.close()

    return jsonify([dict(r) for r in rows])


@app.route("/api/tumblr/posts/<blog>")
def tumblr_posts(blog):

    conn = sqlite3.connect("../identity.db")
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM tumblr_posts
        WHERE blog_name=?
        ORDER BY timestamp DESC
        LIMIT 200
    """, (blog,))

    rows = cur.fetchall()

    conn.close()

    return jsonify([dict(r) for r in rows])

# ================= MASTODON =================
@app.route("/connectors/mastodon")
def mastodon_page():
    return render_template("connectors/mastodon.html")

@app.route("/connectors/mastodon/connect", methods=["POST"])
def mastodon_connect():
    r = requests.post(
        "http://localhost:4000/connectors/mastodon/connect",
        json=request.json,
        cookies=request.cookies
    )
    return jsonify(r.json())

@app.route("/connectors/mastodon/disconnect")
def mastodon_disconnect():
    r = requests.get(
        "http://localhost:4000/connectors/mastodon/disconnect",
        cookies=request.cookies
    )
    return jsonify(r.json())

@app.route("/connectors/mastodon/sync")
def mastodon_sync():
    r = requests.get(
        "http://localhost:4000/connectors/mastodon/sync",
        cookies=request.cookies
    )
    return jsonify(r.json())

@app.route("/dashboard/mastodon")
def mastodon_dashboard():
    return render_template("dashboards/mastodon.html")


# -------- STATUS --------

@app.route("/api/status/mastodon")
def mastodon_status():

    con = sqlite3.connect("../identity.db")
    cur = con.cursor()

    cur.execute("SELECT COUNT(*) FROM mastodon_state")

    c = cur.fetchone()[0]

    con.close()

    return jsonify({
        "connected": c > 0
    })


# -------- DATA --------

@app.route("/api/mastodon/statuses")
def mastodon_statuses():

    con = sqlite3.connect("../identity.db")
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
    SELECT * FROM mastodon_statuses
    ORDER BY fetched_at DESC
    LIMIT 500
    """)

    rows = [dict(r) for r in cur.fetchall()]

    con.close()

    return jsonify(rows)


@app.route("/api/mastodon/tags")
def mastodon_tags():

    con = sqlite3.connect("../identity.db")
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
    SELECT * FROM mastodon_tags
    ORDER BY fetched_at DESC
    LIMIT 200
    """)

    rows = [dict(r) for r in cur.fetchall()]

    con.close()

    return jsonify(rows)

# ================= LEMMY =================

@app.route("/connectors/lemmy")
def lemmy_page():
    return render_template("connectors/lemmy.html")

@app.route("/connectors/lemmy/connect", methods=["POST"])
def lemmy_connect():

    r = requests.post(
        "http://localhost:4000/connectors/lemmy/connect",
        json=request.json,
        cookies=request.cookies
    )

    return jsonify(r.json())

@app.route("/connectors/lemmy/sync")
def lemmy_sync():

    r = requests.get(
        "http://localhost:4000/connectors/lemmy/sync",
        cookies=request.cookies
    )

    return jsonify(r.json())

@app.route("/dashboard/lemmy")
def lemmy_dashboard():
    return render_template("dashboards/lemmy.html")


# -------- STATUS --------

@app.route("/api/status/lemmy")
def lemmy_status():

    con = sqlite3.connect("../identity.db")
    cur = con.cursor()

    cur.execute("SELECT COUNT(*) FROM lemmy_state")

    c = cur.fetchone()[0]

    con.close()

    return jsonify({
        "connected": c > 0
    })


# -------- DATA --------

@app.route("/api/lemmy/posts")
def lemmy_posts():

    con = sqlite3.connect("../identity.db")
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
    SELECT * FROM lemmy_posts
    ORDER BY fetched_at DESC
    LIMIT 500
    """)

    rows = [dict(r) for r in cur.fetchall()]

    con.close()

    return jsonify(rows)


@app.route("/api/lemmy/communities")
def lemmy_communities():

    con = sqlite3.connect("../identity.db")
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
    SELECT * FROM lemmy_communities
    ORDER BY fetched_at DESC
    LIMIT 300
    """)

    rows = [dict(r) for r in cur.fetchall()]

    con.close()

    return jsonify(rows)


@app.route("/api/lemmy/users")
def lemmy_users():

    con = sqlite3.connect("../identity.db")
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
    SELECT * FROM lemmy_users
    ORDER BY fetched_at DESC
    LIMIT 300
    """)

    rows = [dict(r) for r in cur.fetchall()]

    con.close()

    return jsonify(rows)

# ================= PINTEREST =================

@app.route("/connectors/pinterest")
def pinterest_page():
    return render_template("connectors/pinterest.html")


@app.route("/connectors/pinterest/connect")
def pinterest_connect():
    return redirect("http://localhost:4000/connectors/pinterest/connect")

@app.route("/connectors/pinterest/sync")
def pinterest_sync():

    r = requests.get(
        "http://localhost:4000/connectors/pinterest/sync",
        cookies=request.cookies
    )

    return jsonify(r.json())

@app.route("/connectors/pinterest/disconnect")
def pinterest_disconnect():

    r = requests.get(
        "http://localhost:4000/connectors/pinterest/disconnect",
        cookies=request.cookies
    )

    return jsonify(r.json())

@app.route("/dashboard/pinterest")
def pinterest_dashboard():
    return render_template("dashboards/pinterest.html")


# -------- STATUS --------

@app.route("/api/status/pinterest")
def pinterest_status():

    uid = request.cookies.get("uid") or "demo_user"

    con = sqlite3.connect("../identity.db")
    cur = con.cursor()

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='pinterest'
        LIMIT 1
    """, (uid,))

    row = cur.fetchone()
    con.close()

    return jsonify({
        "connected": bool(row and row[0] == 1)
    })

# -------- DATA --------

@app.route("/api/pinterest/boards")
def pinterest_boards():

    con = sqlite3.connect("../identity.db")
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
    SELECT * FROM pinterest_boards
    ORDER BY fetched_at DESC
    """)

    rows = [dict(r) for r in cur.fetchall()]

    con.close()

    return jsonify(rows)


@app.route("/api/pinterest/pins")
def pinterest_pins():

    con = sqlite3.connect("../identity.db")
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
    SELECT * FROM pinterest_pins
    ORDER BY fetched_at DESC
    """)

    rows = [dict(r) for r in cur.fetchall()]

    con.close()

    return jsonify(rows)

# ================= TWITCH =================

@app.route("/connectors/twitch")
def twitch_page():
    return render_template("connectors/twitch.html")


@app.route("/connectors/twitch/connect", methods=["POST"])
def twitch_connect():

    r = requests.post(
        "http://localhost:4000/connectors/twitch/connect",
        json=request.json,
        cookies=request.cookies
    )

    return jsonify(r.json()), r.status_code

@app.route("/connectors/twitch/disconnect")
def twitch_disconnect():
    requests.get("http://localhost:4000/connectors/twitch/disconnect")
    return redirect("/connectors/twitch")

@app.route("/connectors/twitch/sync")
def twitch_sync():

    r = requests.get(
        "http://localhost:4000/connectors/twitch/sync"
    )

    return jsonify(r.json())

@app.route("/api/status/twitch")
def twitch_status():

    uid = request.cookies.get("uid") or "demo_user"

    con = sqlite3.connect("../identity.db")
    cur = con.cursor()

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='twitch'
        LIMIT 1
    """, (uid,))

    row = cur.fetchone()
    con.close()

    return jsonify({
        "connected": bool(row and row[0] == 1)
    })

# ================= PEERTUBE =================

@app.route("/connectors/peertube")
def peertube_page():
    return render_template("connectors/peertube.html")

@app.route("/connectors/peertube/connect", methods=["POST"])
def peertube_connect_proxy():

    r = requests.post(
        "http://localhost:4000/connectors/peertube/connect",
        json=request.json,
        cookies=request.cookies
    )

    return jsonify(r.json()), r.status_code

@app.route("/connectors/peertube/disconnect")
def peertube_disconnect_proxy():

    r = requests.get(
        "http://localhost:4000/connectors/peertube/disconnect",
        cookies=request.cookies
    )

    return jsonify(r.json()), r.status_code

@app.route("/connectors/peertube/sync")
def peertube_sync_proxy():

    r = requests.get(
        "http://localhost:4000/connectors/peertube/sync",
        cookies=request.cookies
    )

    return jsonify(r.json()), r.status_code


@app.route("/dashboard/peertube")
def peertube_dashboard():
    return render_template("dashboards/peertube.html")


# -------- STATUS --------

@app.route("/api/status/peertube")
def peertube_status():

    con = sqlite3.connect("../identity.db")
    cur = con.cursor()

    cur.execute("SELECT COUNT(*) FROM peertube_videos")

    c = cur.fetchone()[0]

    con.close()

    return jsonify({
        "connected": c > 0
    })


# -------- DATA --------

@app.route("/api/peertube/videos")
def peertube_videos():

    con = sqlite3.connect("../identity.db")
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
    SELECT * FROM peertube_videos
    ORDER BY fetched_at DESC
    """)

    rows = [dict(r) for r in cur.fetchall()]

    con.close()

    return jsonify(rows)


@app.route("/api/peertube/channels")
def peertube_channels():

    con = sqlite3.connect("../identity.db")
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
    SELECT * FROM peertube_channels
    ORDER BY fetched_at DESC
    """)

    rows = [dict(r) for r in cur.fetchall()]

    con.close()

    return jsonify(rows)

# ================= OPENSTREETMAP =================

@app.route("/connectors/openstreetmap")
def osm_page():
    return render_template("connectors/openstreetmap.html")


@app.route("/connectors/openstreetmap/connect", methods=["POST"])
def ui_osm_connect():

    r = requests.post(
        "http://localhost:4000/connectors/openstreetmap/connect",
        cookies=request.cookies
    )

    return jsonify(r.json())


@app.route("/connectors/openstreetmap/disconnect")
def ui_osm_disconnect():

    r = requests.get(
        "http://localhost:4000/connectors/openstreetmap/disconnect",
        cookies=request.cookies
    )

    return jsonify(r.json())


@app.route("/connectors/openstreetmap/sync")
def ui_osm_sync():

    r = requests.get(
        "http://localhost:4000/connectors/openstreetmap/sync",
        cookies=request.cookies
    )

    return jsonify(r.json())

@app.route("/dashboard/openstreetmap")
def osm_dashboard():
    return render_template("dashboards/openstreetmap.html")


# -------- STATUS --------

@app.route("/api/status/openstreetmap")
def osm_status():

    con = sqlite3.connect("../identity.db")
    cur = con.cursor()

    cur.execute("SELECT COUNT(*) FROM osm_changesets")

    c = cur.fetchone()[0]

    con.close()

    return jsonify({
        "connected": c > 0
    })


# -------- DATA --------

@app.route("/api/osm/changesets")
def osm_changesets():

    con = sqlite3.connect("../identity.db")
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
    SELECT * FROM osm_changesets
    ORDER BY fetched_at DESC
    """)

    rows = [dict(r) for r in cur.fetchall()]

    con.close()

    return jsonify(rows)


@app.route("/api/osm/notes")
def osm_notes():

    con = sqlite3.connect("../identity.db")
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
    SELECT * FROM osm_notes
    ORDER BY fetched_at DESC
    """)

    rows = [dict(r) for r in cur.fetchall()]

    con.close()

    return jsonify(rows)

# ================= WIKIPEDIA =================

@app.route("/connectors/wikipedia")
def wikipedia_page():
    return render_template("connectors/wikipedia.html")


# -------- CONNECT --------

@app.route("/connectors/wikipedia/connect", methods=["POST"])
def ui_wikipedia_connect():

    r = requests.post(
        "http://localhost:4000/connectors/wikipedia/connect",
        cookies=request.cookies
    )

    return jsonify(r.json()), r.status_code


# -------- DISCONNECT --------

@app.route("/connectors/wikipedia/disconnect")
def ui_wikipedia_disconnect():

    r = requests.get(
        "http://localhost:4000/connectors/wikipedia/disconnect",
        cookies=request.cookies
    )

    return jsonify(r.json()), r.status_code


# -------- SYNC --------

@app.route("/connectors/wikipedia/sync")
def ui_wikipedia_sync():

    r = requests.get(
        "http://localhost:4000/connectors/wikipedia/sync",
        cookies=request.cookies
    )

    return jsonify(r.json()), r.status_code


# -------- STATUS (Unified Pattern) --------

@app.route("/api/status/wikipedia")
def wikipedia_status():

    uid = request.cookies.get("uid") or "demo_user"

    con = sqlite3.connect("../identity.db")
    cur = con.cursor()

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='wikipedia'
    """, (uid,))

    row = cur.fetchone()
    con.close()

    return jsonify({
        "connected": bool(row and row[0] == 1)
    })


# -------- DASHBOARD --------

@app.route("/dashboard/wikipedia")
def wikipedia_dashboard():
    return render_template("dashboards/wikipedia.html")


# -------- DATA --------

@app.route("/api/wiki/recent")
def wiki_recent():

    con = sqlite3.connect("../identity.db")
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
        SELECT *
        FROM wikipedia_recent_changes
        ORDER BY fetched_at DESC
    """)

    rows = [dict(r) for r in cur.fetchall()]
    con.close()

    return jsonify(rows)


@app.route("/api/wiki/newpages")
def wiki_new():

    con = sqlite3.connect("../identity.db")
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
        SELECT *
        FROM wikipedia_new_pages
        ORDER BY fetched_at DESC
    """)

    rows = [dict(r) for r in cur.fetchall()]
    con.close()

    return jsonify(rows)


@app.route("/api/wiki/viewed")
def wiki_viewed():

    con = sqlite3.connect("../identity.db")
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
        SELECT *
        FROM wikipedia_most_viewed
        ORDER BY fetched_at DESC
    """)

    rows = [dict(r) for r in cur.fetchall()]
    con.close()

    return jsonify(rows)

# ================= PRODUCTHUNT =================

@app.route("/connectors/producthunt")
def producthunt_page():
    return render_template("connectors/producthunt.html")


# -------- CONNECT --------

@app.route("/connectors/producthunt/connect", methods=["POST"])
def ui_producthunt_connect():

    r = requests.post(
        "http://localhost:4000/connectors/producthunt/connect",
        cookies=request.cookies
    )

    return jsonify(r.json()), r.status_code


# -------- DISCONNECT --------

@app.route("/connectors/producthunt/disconnect")
def ui_producthunt_disconnect():

    r = requests.get(
        "http://localhost:4000/connectors/producthunt/disconnect",
        cookies=request.cookies
    )

    return jsonify(r.json()), r.status_code


# -------- SYNC --------

@app.route("/connectors/producthunt/sync")
def ui_producthunt_sync():

    r = requests.get(
        "http://localhost:4000/connectors/producthunt/sync",
        cookies=request.cookies
    )

    return jsonify(r.json()), r.status_code


# -------- STATUS (STANDARDIZED) --------

@app.route("/api/status/producthunt")
def ui_producthunt_status():

    uid = request.cookies.get("uid") or "demo_user"

    con = sqlite3.connect("../identity.db")
    cur = con.cursor()

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='producthunt'
    """, (uid,))

    row = cur.fetchone()
    con.close()

    return jsonify({
        "connected": bool(row and row[0] == 1)
    })


# -------- DASHBOARD --------

@app.route("/dashboard/producthunt")
def producthunt_dashboard():
    return render_template("dashboards/producthunt.html")


# -------- DATA APIs --------

@app.route("/api/producthunt/posts")
def ui_producthunt_posts():

    r = requests.get(
        "http://localhost:4000/producthunt/data/posts",
        cookies=request.cookies
    )

    return jsonify(r.json())


@app.route("/api/producthunt/topics")
def ui_producthunt_topics():

    r = requests.get(
        "http://localhost:4000/producthunt/data/topics",
        cookies=request.cookies
    )

    return jsonify(r.json())

# ============ DISCOURSE ============

# ================= DISCOURSE =================

@app.route("/connectors/discourse")
def discourse_page():
    return render_template("connectors/discourse.html")


@app.route("/connectors/discourse/connect", methods=["POST"])
def ui_discourse_connect():

    r = requests.post(
        "http://localhost:4000/connectors/discourse/connect",
        cookies=request.cookies
    )

    return jsonify(r.json()), r.status_code


@app.route("/connectors/discourse/disconnect")
def ui_discourse_disconnect():

    r = requests.get(
        "http://localhost:4000/connectors/discourse/disconnect",
        cookies=request.cookies
    )

    return jsonify(r.json()), r.status_code


@app.route("/connectors/discourse/sync")
def ui_discourse_sync():

    r = requests.get(
        "http://localhost:4000/connectors/discourse/sync",
        cookies=request.cookies
    )

    return jsonify(r.json()), r.status_code

@app.route("/dashboard/discourse")
def discourse_dashboard():
    return render_template("dashboards/discourse.html")

@app.route("/api/status/discourse")
def discourse_status():

    r = requests.get(
        "http://127.0.0.1:4000/discourse/data/topics",
        headers={
            "Cookie": request.headers.get("Cookie", "")
        }
    )

    data = r.json()

    return jsonify({
        "connected": len(data) > 0
    })

@app.route("/api/discourse/topics")
def ui_discourse_topics():

    r = requests.get(
        "http://127.0.0.1:4000/discourse/data/topics",
        headers={
            "Cookie": request.headers.get("Cookie", "")
        }
    )

    try:
        return jsonify(r.json())
    except:
        return jsonify([])


@app.route("/api/discourse/categories")
def ui_discourse_categories():

    r = requests.get(
        "http://127.0.0.1:4000/discourse/data/categories",
        headers={
            "Cookie": request.headers.get("Cookie", "")
        }
    )

    try:
        return jsonify(r.json())
    except:
        return jsonify([])

# ================= GMAIL ========================

@app.route("/connectors/gmail")
def gmail_page():
    return render_template("connectors/gmail.html")


# Redirect to Identity Server OAuth
@app.route("/connectors/gmail/connect")
def gmail_connect():
    return redirect("http://localhost:4000/google/connect?source=gmail")


# After OAuth redirect comes back here
@app.route("/connectors/gmail/callback")
def gmail_callback():

    code = request.args.get("code")

    if not code:
        return "Authorization failed", 400

    # Forward to identity server
    r = requests.get(
        f"http://localhost:4000/google/callback?code={code}&source=gmail"
    )

    if r.status_code != 200:
        return r.text, 400

    # NO AUTO SYNC HERE

    return redirect("/connectors/gmail")


@app.route("/connectors/gmail/sync")
def gmail_sync():

    r = requests.get(
        "http://localhost:4000/google/sync/gmail",
        timeout=120
    )

    try:
        return jsonify(r.json())
    except:
        return jsonify({"status": "error"}), 500


@app.route("/connectors/gmail/disconnect")
def gmail_disconnect():

    r = requests.get("http://localhost:4000/google/disconnect/gmail")

    return jsonify(r.json())

@app.route("/api/status/gmail")
def gmail_status():

    uid = request.cookies.get("uid") or "demo_user"
    source = "gmail"

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Check credentials saved
    cur.execute("""
        SELECT 1
        FROM connector_configs
        WHERE uid=? AND connector=?
        LIMIT 1
    """, (uid, source))
    creds = cur.fetchone()

    # Check connection enabled
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source=?
        LIMIT 1
    """, (uid, source))
    row = cur.fetchone()

    conn.close()

    connected = False
    if row and row[0] == 1:
        connected = True

    return jsonify({
        "connected": connected,
        "has_credentials": bool(creds)
    })

@app.route("/connectors/gmail/save_app", methods=["POST"])
def gmail_save_app_proxy():
    r = requests.post(
        "http://localhost:4000/connectors/gmail/save_app",
        json=request.get_json(),
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return jsonify(r.json()), r.status_code

@app.route("/api/gmail/data/<table>")
def gmail_data(table):

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    if table == "profile":
        cur.execute("SELECT * FROM google_gmail_profile")

    elif table == "labels":
        cur.execute("SELECT * FROM google_gmail_labels")

    elif table == "messages":
        cur.execute("SELECT * FROM google_gmail_messages")

    elif table == "details":
        cur.execute("SELECT * FROM google_gmail_message_details")

    else:
        return jsonify([])

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])

@app.route("/dashboard/gmail")
def gmail_dashboard():
    return render_template("dashboards/gmail.html")


# ================= GOOGLE DRIVE ========================

@app.route("/connectors/drive")
def drive_page():
    return render_template("connectors/drive.html")

@app.route("/connectors/drive/connect")
def drive_connect():
    return redirect("http://localhost:4000/google/connect?source=drive")

@app.route("/connectors/drive/sync")
def drive_sync():

    r = requests.get(
        "http://localhost:4000/google/sync/drive",
        timeout=120
    )

    # Safe handling
    try:
        return jsonify(r.json())
    except:
        return jsonify({
            "status": "error",
            "http_code": r.status_code,
            "raw": r.text
        }), 500


@app.route("/dashboard/drive")
def drive_dashboard():
    return render_template("dashboards/drive.html")


@app.route("/api/status/drive")
def drive_status():

    uid = request.cookies.get("uid") or "demo_user"
    source = "drive"

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Check credentials
    cur.execute("""
        SELECT 1
        FROM connector_configs
        WHERE uid=? AND connector=?
        LIMIT 1
    """, (uid, source))
    creds = cur.fetchone()

    # Check connection
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source=?
        LIMIT 1
    """, (uid, source))
    row = cur.fetchone()

    conn.close()

    connected = False
    if row and row[0] == 1:
        connected = True

    return jsonify({
        "connected": connected,
        "has_credentials": bool(creds)
    })

@app.route("/connectors/drive/save_app", methods=["POST"])
def drive_save_app_proxy():
    r = requests.post(
        "http://localhost:4000/connectors/drive/save_app",
        json=request.get_json(),
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return jsonify(r.json()), r.status_code

@app.route("/connectors/drive/disconnect")
def drive_disconnect():

    r = requests.get("http://localhost:4000/google/disconnect/drive")

    return jsonify(r.json())

@app.route("/api/drive/data/files")
def drive_files_data():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM drive_files
        ORDER BY fetched_at DESC
        LIMIT 500
    """)

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])

@app.route("/connectors/drive/job/get")
def drive_job_get_proxy():
    r = requests.get(
        "http://localhost:4000/connectors/drive/job/get",
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    try:
        return jsonify(r.json()), r.status_code
    except:
        return jsonify({
            "exists": False,
            "sync_type": "incremental",
            "schedule_time": None
        }), 200

@app.route("/connectors/drive/job/save", methods=["POST"])
def drive_job_save_proxy():
    r = requests.post(
        "http://localhost:4000/connectors/drive/job/save",
        json=request.get_json(),
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return jsonify(r.json()), r.status_code

# ================= GOOGLE CALENDAR ========================

@app.route("/connectors/calendar")
def calendar_page():
    return render_template("connectors/calendar.html")

@app.route("/connectors/calendar/connect")
def calendar_connect():
    return redirect("http://localhost:4000/google/connect?source=calendar")

@app.route("/connectors/calendar/save_app", methods=["POST"])
def calendar_save_app_proxy():
    r = requests.post(
        "http://localhost:4000/connectors/calendar/save_app",
        json=request.get_json(),
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return jsonify(r.json()), r.status_code

@app.route("/connectors/calendar/disconnect")
def calendar_disconnect():

    r = requests.get("http://localhost:4000/google/disconnect/calendar")

    return jsonify(r.json())

@app.route("/connectors/calendar/sync")
def calendar_sync():

    r = requests.get(
        "http://localhost:4000/google/sync/calendar",
        timeout=180
    )

    # Safe JSON handling
    try:
        return jsonify(r.json())
    except Exception as e:
        return jsonify({
            "status": "error",
            "http_code": r.status_code,
            "raw": r.text,
            "exception": str(e)
        }), 500


@app.route("/dashboard/calendar")
def calendar_dashboard():
    return render_template("dashboards/calendar.html")

@app.route("/api/status/calendar")
def calendar_status():

    uid = request.cookies.get("uid") or "demo_user"
    source = "calendar"

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Credentials
    cur.execute("""
        SELECT 1
        FROM connector_configs
        WHERE uid=? AND connector=?
        LIMIT 1
    """, (uid, source))
    creds = cur.fetchone()

    # Connection
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source=?
        LIMIT 1
    """, (uid, source))
    row = cur.fetchone()

    conn.close()

    connected = False
    if row and row[0] == 1:
        connected = True

    return jsonify({
        "connected": connected,
        "has_credentials": bool(creds)
    })

@app.route("/connectors/calendar/job/get")
def calendar_job_get_proxy():
    r = requests.get(
        "http://localhost:4000/connectors/calendar/job/get",
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return jsonify(r.json()), r.status_code


@app.route("/connectors/calendar/job/save", methods=["POST"])
def calendar_job_save_proxy():
    r = requests.post(
        "http://localhost:4000/connectors/calendar/job/save",
        json=request.get_json(),
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return jsonify(r.json()), r.status_code

@app.route("/api/calendar/data/<table>")
def calendar_data(table):

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    if table == "colors":
        cur.execute("SELECT * FROM google_calendar_colors")

    elif table == "settings":
        cur.execute("SELECT * FROM google_calendar_settings")

    elif table == "calendars":
        cur.execute("SELECT * FROM google_calendar_list")

    elif table == "events":
        cur.execute("""
            SELECT *
            FROM google_calendar_events
            ORDER BY start DESC
            LIMIT 1000
        """)

    else:
        return jsonify([])

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])

# ================= GOOGLE SHEETS ========================

@app.route("/connectors/sheets")
def sheets_page():
    return render_template("connectors/sheets.html")

@app.route("/connectors/sheets/connect")
def sheets_connect():
    return redirect("http://localhost:4000/google/connect?source=sheets")

@app.route("/connectors/sheets/save_app", methods=["POST"])
def sheets_save_app_proxy():
    r = requests.post(
        "http://localhost:4000/connectors/sheets/save_app",
        json=request.get_json(),
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return jsonify(r.json()), r.status_code

@app.route("/connectors/sheets/disconnect")
def sheets_disconnect():

    r = requests.get(
        "http://localhost:4000/google/disconnect/sheets"
    )

    return jsonify(r.json())

@app.route("/connectors/sheets/job/get")
def sheets_job_get_proxy():

    r = requests.get(
        "http://localhost:4000/connectors/sheets/job/get",
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return jsonify(r.json())


@app.route("/connectors/sheets/job/save", methods=["POST"])
def sheets_job_save_proxy():

    r = requests.post(
        "http://localhost:4000/connectors/sheets/job/save",
        json=request.get_json(),
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return jsonify(r.json()), r.status_code

@app.route("/connectors/sheets/sync")
def sheets_sync():

    r = requests.get(
        "http://localhost:4000/google/sync/sheets",
        timeout=120
    )

    # Safe JSON handling
    try:
        return jsonify(r.json())
    except Exception as e:
        return jsonify({
            "status": "error",
            "http_code": r.status_code,
            "raw": r.text,
            "exception": str(e)
        }), 500


@app.route("/dashboard/sheets")
def sheets_dashboard():
    return render_template("dashboards/sheets.html")


@app.route("/api/status/sheets")
def sheets_status():

    uid = request.cookies.get("uid") or "demo_user"
    source = "sheets"

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Check credentials
    cur.execute("""
        SELECT 1
        FROM connector_configs
        WHERE uid=? AND connector=?
        LIMIT 1
    """, (uid, source))
    creds = cur.fetchone()

    # Check connection
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source=?
        LIMIT 1
    """, (uid, source))
    row = cur.fetchone()

    conn.close()

    connected = False
    if row and row[0] == 1:
        connected = True

    return jsonify({
        "connected": connected,
        "has_credentials": bool(creds)
    })

@app.route("/api/sheets/data")
def sheets_data():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM sheets_data
        ORDER BY fetched_at DESC
        LIMIT 500
    """)

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])

# ================= GOOGLE FORMS ========================

@app.route("/connectors/forms")
def forms_page():
    return render_template("connectors/forms.html")

@app.route("/connectors/forms/sync")
def forms_sync():

    r = requests.get(
        "http://localhost:4000/google/sync/forms",
        timeout=180
    )

    return jsonify(r.json())

@app.route("/connectors/forms/connect")
def forms_connect():
    return redirect("http://localhost:4000/google/connect?source=forms")

@app.route("/connectors/forms/save_app", methods=["POST"])
def forms_save_app_proxy():
    r = requests.post(
        "http://localhost:4000/connectors/forms/save_app",
        json=request.get_json(),
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return jsonify(r.json()), r.status_code

@app.route("/connectors/forms/disconnect")
def forms_disconnect():

    r = requests.get(
        "http://localhost:4000/google/disconnect/forms"
    )

    return jsonify(r.json())

@app.route("/dashboard/forms")
def forms_dashboard():
    return render_template("dashboards/forms.html")

@app.route("/api/status/forms")
def forms_status():

    uid = request.cookies.get("uid") or "demo_user"
    source = "forms"

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Credentials
    cur.execute("""
        SELECT 1
        FROM connector_configs
        WHERE uid=? AND connector=?
        LIMIT 1
    """, (uid, source))
    creds = cur.fetchone()

    # Connection
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source=?
        LIMIT 1
    """, (uid, source))
    row = cur.fetchone()

    conn.close()

    connected = False
    if row and row[0] == 1:
        connected = True

    return jsonify({
        "connected": connected,
        "has_credentials": bool(creds)
    })

@app.route("/connectors/forms/job/get")
def forms_job_get_proxy():

    r = requests.get(
        "http://localhost:4000/connectors/forms/job/get",
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return jsonify(r.json())


@app.route("/connectors/forms/job/save", methods=["POST"])
def forms_job_save_proxy():

    r = requests.post(
        "http://localhost:4000/connectors/forms/job/save",
        json=request.get_json(),
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return jsonify(r.json()), r.status_code

@app.route("/api/forms/data/<table>")
def forms_data(table):

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    if table == "forms":

        cur.execute("""
            SELECT *
            FROM google_forms
            ORDER BY fetched_at DESC
        """)

    elif table == "responses":

        cur.execute("""
            SELECT *
            FROM google_form_responses
            ORDER BY fetched_at DESC
            LIMIT 1000
        """)

    else:
        return jsonify([])

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])


# ================= GOOGLE CONTACTS ========================

@app.route("/connectors/contacts")
def contacts_page():
    return render_template("connectors/contacts.html")

@app.route("/connectors/contacts/connect")
def contacts_connect():
    return redirect("http://localhost:4000/google/connect?source=contacts")

@app.route("/connectors/contacts/save_app", methods=["POST"])
def contacts_save_app_proxy():
    r = requests.post(
        "http://localhost:4000/connectors/contacts/save_app",
        json=request.get_json(),
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return jsonify(r.json()), r.status_code

@app.route("/connectors/contacts/disconnect")
def contacts_disconnect():
    r = requests.get(
        "http://localhost:4000/google/disconnect/contacts"
    )
    return jsonify(r.json())

@app.route("/connectors/contacts/sync")
def contacts_sync():

    r = requests.get(
        "http://localhost:4000/google/sync/contacts",
        timeout=180
    )

    return jsonify(r.json())

@app.route("/dashboard/contacts")
def contacts_dashboard():
    return render_template("dashboards/contacts.html")

@app.route("/api/status/contacts")
def contacts_status():

    uid = request.cookies.get("uid") or "demo_user"
    source = "contacts"

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Credentials
    cur.execute("""
        SELECT 1
        FROM connector_configs
        WHERE uid=? AND connector=?
        LIMIT 1
    """, (uid, source))
    creds = cur.fetchone()

    # Connection
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source=?
        LIMIT 1
    """, (uid, source))
    row = cur.fetchone()

    conn.close()

    connected = False
    if row and row[0] == 1:
        connected = True

    return jsonify({
        "connected": connected,
        "has_credentials": bool(creds)
    })

@app.route("/api/contacts/data")
def contacts_data():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM google_contacts_persons
        ORDER BY fetched_at DESC
        LIMIT 1000
    """)

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])

@app.route("/connectors/contacts/job/get")
def contacts_job_get_proxy():

    r = requests.get(
        "http://localhost:4000/connectors/contacts/job/get",
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return jsonify(r.json())


@app.route("/connectors/contacts/job/save", methods=["POST"])
def contacts_job_save_proxy():

    r = requests.post(
        "http://localhost:4000/connectors/contacts/job/save",
        json=request.get_json(),
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return jsonify(r.json()), r.status_code

# ================= GOOGLE TASKS ========================

@app.route("/connectors/tasks")
def tasks_page():
    return render_template("connectors/tasks.html")

@app.route("/connectors/tasks/connect")
def tasks_connect():
    return redirect("http://localhost:4000/google/connect?source=tasks")

@app.route("/connectors/tasks/save_app", methods=["POST"])
def tasks_save_app_proxy():
    r = requests.post(
        "http://localhost:4000/connectors/tasks/save_app",
        json=request.get_json(),
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return jsonify(r.json()), r.status_code

@app.route("/connectors/tasks/disconnect")
def tasks_disconnect():

    r = requests.get(
        "http://localhost:4000/google/disconnect/tasks"
    )

    return jsonify(r.json())

@app.route("/connectors/tasks/sync")
def tasks_sync():

    r = requests.get(
        "http://localhost:4000/google/sync/tasks",
        timeout=180
    )

    try:
        return jsonify(r.json())
    except Exception as e:
        return jsonify({
            "status": "error",
            "http_code": r.status_code,
            "raw": r.text,
            "exception": str(e)
        }), 500


@app.route("/dashboard/tasks")
def tasks_dashboard():
    return render_template("dashboards/tasks.html")


@app.route("/api/status/tasks")
def tasks_status():

    uid = request.cookies.get("uid") or "demo_user"
    source = "tasks"

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Credentials
    cur.execute("""
        SELECT 1
        FROM connector_configs
        WHERE uid=? AND connector=?
        LIMIT 1
    """, (uid, source))
    creds = cur.fetchone()

    # Connection
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source=?
        LIMIT 1
    """, (uid, source))
    row = cur.fetchone()

    conn.close()

    connected = False
    if row and row[0] == 1:
        connected = True

    return jsonify({
        "connected": connected,
        "has_credentials": bool(creds)
    })

@app.route("/connectors/tasks/job/get")
def tasks_job_get_proxy():

    r = requests.get(
        "http://localhost:4000/connectors/tasks/job/get",
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return jsonify(r.json())


@app.route("/connectors/tasks/job/save", methods=["POST"])
def tasks_job_save_proxy():

    r = requests.post(
        "http://localhost:4000/connectors/tasks/job/save",
        json=request.get_json(),
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return jsonify(r.json()), r.status_code

@app.route("/api/tasks/data/<table>")
def tasks_data(table):

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    if table == "lists":

        cur.execute("""
            SELECT *
            FROM google_tasks_lists
            ORDER BY fetched_at DESC
        """)

    elif table == "items":

        cur.execute("""
            SELECT *
            FROM google_tasks_items
            ORDER BY fetched_at DESC
            LIMIT 1000
        """)

    else:
        return jsonify([])

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])

# ================= GOOGLE GA4 ========================

@app.route("/connectors/ga4")
def ga4_page():
    return render_template("connectors/ga4.html")

@app.route("/connectors/ga4/connect")
def ga4_connect():
    return redirect("http://localhost:4000/google/connect?source=ga4")

@app.route("/connectors/ga4/save_app", methods=["POST"])
def ga4_save_app_proxy():

    r = requests.post(
        "http://localhost:4000/connectors/ga4/save_app",
        json=request.get_json(),
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return jsonify(r.json()), r.status_code

@app.route("/connectors/ga4/disconnect")
def ga4_disconnect():

    r = requests.get(
        "http://localhost:4000/google/disconnect/ga4"
    )

    return jsonify(r.json())

@app.route("/connectors/ga4/job/get")
def ga4_job_get_proxy():

    r = requests.get(
        "http://localhost:4000/connectors/ga4/job/get",
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return jsonify(r.json())


@app.route("/connectors/ga4/job/save", methods=["POST"])
def ga4_job_save_proxy():

    r = requests.post(
        "http://localhost:4000/connectors/ga4/job/save",
        json=request.get_json(),
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return jsonify(r.json()), r.status_code

@app.route("/connectors/ga4/sync")
def ga4_sync():

    r = requests.get(
        "http://localhost:4000/google/sync/ga4",
        timeout=180
    )

    try:
        return jsonify(r.json())
    except:
        return jsonify({
            "status": "error",
            "http_code": r.status_code,
            "raw": r.text
        }), 500

@app.route("/dashboard/ga4")
def ga4_dashboard():
    return render_template("dashboards/ga4.html")


@app.route("/api/status/ga4")
def ga4_status():

    uid = request.cookies.get("uid") or "demo_user"

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Check credentials
    cur.execute("""
        SELECT 1
        FROM connector_configs
        WHERE uid=? AND connector='ga4'
        LIMIT 1
    """, (uid,))
    creds = cur.fetchone()

    # Check connection
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='ga4'
        LIMIT 1
    """, (uid,))
    row = cur.fetchone()

    conn.close()

    return jsonify({
        "connected": bool(row and row[0] == 1),
        "has_credentials": bool(creds)
    })

@app.route("/api/ga4/data/<table>")
def ga4_data(table):

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()


    tables = {
        "overview": "ga4_website_overview",
        "devices": "ga4_devices",
        "locations": "ga4_locations",
        "traffic": "ga4_traffic_sources",
        "events": "ga4_events"
    }

    if table not in tables:
        return jsonify([])


    cur.execute(f"""
        SELECT *
        FROM {tables[table]}
        ORDER BY date DESC
        LIMIT 2000
    """)

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])

# ================= GOOGLE SEARCH CONSOLE ========================

@app.route("/connectors/search-console")
def gsc_page():
    return render_template("connectors/search_console.html")

@app.route("/connectors/search-console/connect")
def search_console_connect():
    return redirect("http://localhost:4000/google/connect?source=search-console")

@app.route("/connectors/search-console/save_app", methods=["POST"])
def search_console_save_app_proxy():

    r = requests.post(
        "http://localhost:4000/connectors/search-console/save_app",
        json=request.get_json(),
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return jsonify(r.json()), r.status_code

@app.route("/connectors/search-console/disconnect")
def search_console_disconnect():

    r = requests.get(
        "http://localhost:4000/google/disconnect/search-console"
    )

    return jsonify(r.json())

@app.route("/connectors/search-console/sync")
def ui_gsc_sync():

    site = request.args.get("site")
    sync_type = request.args.get("sync_type", "incremental")

    r = requests.get(
        "http://localhost:4000/connectors/search-console/sync",
        params={
            "site": site,
            "sync_type": sync_type
        }
    )

    return jsonify(r.json())

@app.route("/dashboard/search-console")
def gsc_dashboard():
    return render_template("dashboards/search_console.html")


@app.route("/api/status/search-console")
def search_console_status():

    uid = request.cookies.get("uid") or "demo_user"

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Credentials
    cur.execute("""
        SELECT 1
        FROM connector_configs
        WHERE uid=? AND connector='search-console'
        LIMIT 1
    """, (uid,))
    creds = cur.fetchone()

    # Connection
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='search-console'
        LIMIT 1
    """, (uid,))
    row = cur.fetchone()

    conn.close()

    return jsonify({
        "connected": bool(row and row[0] == 1),
        "has_credentials": bool(creds)
    })

@app.route("/api/search-console/data")
def gsc_data():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM google_search_console
        ORDER BY fetched_at DESC
        LIMIT 3000
    """)

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])

# ================= GOOGLE YOUTUBE ========================

@app.route("/connectors/youtube")
def youtube_page():
    return render_template("connectors/youtube.html")

@app.route("/connectors/youtube/connect")
def youtube_connect():
    return redirect("http://localhost:4000/google/connect?source=youtube")

@app.route("/connectors/youtube/save_app", methods=["POST"])
def youtube_save_app_proxy():

    r = requests.post(
        "http://localhost:4000/connectors/youtube/save_app",
        json=request.get_json(),
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return jsonify(r.json()), r.status_code

@app.route("/connectors/youtube/disconnect")
def youtube_disconnect():

    r = requests.get(
        "http://localhost:4000/google/disconnect/youtube"
    )

    return jsonify(r.json())

@app.route("/api/status/youtube")
def youtube_status():

    uid = request.cookies.get("uid") or "demo_user"

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Credentials
    cur.execute("""
        SELECT 1
        FROM connector_configs
        WHERE uid=? AND connector='youtube'
        LIMIT 1
    """, (uid,))
    creds = cur.fetchone()

    # Connection
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='youtube'
        LIMIT 1
    """, (uid,))
    row = cur.fetchone()

    conn.close()

    return jsonify({
        "connected": bool(row and row[0] == 1),
        "has_credentials": bool(creds)
    })

@app.route("/connectors/youtube/job/get")
def youtube_job_get_proxy():

    r = requests.get(
        "http://localhost:4000/connectors/youtube/job/get",
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    try:
        return jsonify(r.json()), r.status_code
    except:
        return jsonify({
            "exists": False,
            "sync_type": "incremental",
            "schedule_time": None
        }), 200

@app.route("/connectors/youtube/job/save", methods=["POST"])
def youtube_job_save_proxy():

    r = requests.post(
        "http://localhost:4000/connectors/youtube/job/save",
        json=request.get_json(),
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return jsonify(r.json()), r.status_code

@app.route("/connectors/youtube/sync")
def ui_youtube_sync():

    sync_type = request.args.get("sync_type", "incremental")

    r = requests.get(
        "http://localhost:4000/connectors/youtube/sync",
        params={"sync_type": sync_type}
    )

    return jsonify(r.json())

@app.route("/dashboard/youtube")
def youtube_dashboard():
    return render_template("dashboards/youtube.html")

@app.route("/api/youtube/data/<table>")
def youtube_data(table):

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()


    if table == "channels":

        cur.execute("""
            SELECT *
            FROM google_youtube_channels
            ORDER BY fetched_at DESC
        """)

    elif table == "videos":

        cur.execute("""
            SELECT *
            FROM google_youtube_videos
            ORDER BY published_at DESC
            LIMIT 2000
        """)

    else:
        return jsonify([])


    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])

# ================= GOOGLE TRENDS ========================

@app.route("/connectors/trends")
def trends_page():
    return render_template("connectors/trends.html")

@app.route("/connectors/trends/disconnect")
def trends_disconnect():

    r = requests.get("http://localhost:4000/google/disconnect/trends")

    return jsonify(r.json())

@app.route("/connectors/trends/sync")
def ui_trends_sync():

    keyword = request.args.get("keyword")
    sync_type = request.args.get("sync_type", "daily")

    r = requests.get(
        "http://localhost:4000/connectors/trends/sync",
        params={
            "keyword": keyword,
            "sync_type": sync_type
        }
    )

    return jsonify(r.json())

@app.route("/connectors/trends/connect")
def ui_trends_connect():

    r = requests.get(
        "http://localhost:4000/connectors/trends/connect"
    )

    return jsonify(r.json())

@app.route("/dashboard/trends")
def trends_dashboard():
    return render_template("dashboards/trends.html")

@app.route("/api/trends/data/<table>")
def trends_data(table):

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()


    if table == "interest":

        cur.execute("""
            SELECT *
            FROM google_trends_interest
            ORDER BY date DESC
            LIMIT 2000
        """)

    elif table == "related":

        cur.execute("""
            SELECT *
            FROM google_trends_related
            ORDER BY fetched_at DESC
            LIMIT 2000
        """)

    else:
        return jsonify([])


    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])

@app.route("/connectors/trends/job/get")
def trends_job_get_proxy():

    r = requests.get(
        "http://localhost:4000/connectors/trends/job/get",
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return jsonify(r.json())

@app.route("/connectors/trends/job/save", methods=["POST"])
def trends_job_save_proxy():

    r = requests.post(
        "http://localhost:4000/connectors/trends/job/save",
        json=request.get_json(),
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return jsonify(r.json())

# ================= GOOGLE NEWS ========================

@app.route("/connectors/news")
def news_page():
    return render_template("connectors/news.html")


@app.route("/connectors/news/connect", methods=["POST"])
def news_connect():
    r = requests.post(
        "http://localhost:4000/connectors/news/connect",
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return jsonify(r.json())


@app.route("/connectors/news/disconnect", methods=["POST"])
def news_disconnect():
    r = requests.post(
        "http://localhost:4000/connectors/news/disconnect",
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return jsonify(r.json())


@app.route("/connectors/news/sync")
def news_sync():

    keyword = request.args.get("keyword")
    sync_type = request.args.get("sync_type", "incremental")

    r = requests.get(
        "http://localhost:4000/connectors/news/sync",
        params={
            "keyword": keyword,
            "sync_type": sync_type
        },
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return jsonify(r.json())


@app.route("/connectors/news/job/get")
def news_job_get_proxy():
    r = requests.get(
        "http://localhost:4000/connectors/news/job/get",
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return jsonify(r.json())


@app.route("/connectors/news/job/save", methods=["POST"])
def news_job_save_proxy():
    r = requests.post(
        "http://localhost:4000/connectors/news/job/save",
        json=request.get_json(),
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return jsonify(r.json())


@app.route("/api/status/news")
def news_status_proxy():
    r = requests.get(
        "http://localhost:4000/api/status/news",
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return jsonify(r.json())

# ================= GOOGLE BOOKS ========================

@app.route("/connectors/books")
def books_page():
    return render_template("connectors/books.html")


@app.route("/connectors/books/connect")
def ui_books_connect():
    r = requests.get("http://localhost:4000/connectors/books/connect")
    return jsonify(r.json())


@app.route("/connectors/books/disconnect")
def ui_books_disconnect():
    r = requests.get("http://localhost:4000/connectors/books/disconnect")
    return jsonify(r.json())


@app.route("/connectors/books/sync")
def ui_books_sync():

    query = request.args.get("query")
    sync_type = request.args.get("sync_type", "incremental")

    r = requests.get(
        "http://localhost:4000/connectors/books/sync",
        params={
            "query": query,
            "sync_type": sync_type
        }
    )

    return jsonify(r.json())


@app.route("/connectors/books/job/get")
def ui_books_job_get():
    r = requests.get("http://localhost:4000/connectors/books/job/get")
    return jsonify(r.json())


@app.route("/connectors/books/job/save", methods=["POST"])
def ui_books_job_save():
    r = requests.post(
        "http://localhost:4000/connectors/books/job/save",
        json=request.json
    )
    return jsonify(r.json())


@app.route("/dashboard/books")
def books_dashboard():
    return render_template("dashboards/books.html")


@app.route("/api/status/books")
def books_status():
    r = requests.get("http://localhost:4000/api/status/books")
    return jsonify(r.json())


@app.route("/api/books/data")
def books_data():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM google_books_volumes
        ORDER BY fetched_at DESC
        LIMIT 2000
    """)

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])

# ================= GOOGLE WEBFONTS ========================

@app.route("/connectors/webfonts")
def webfonts_page():
    return render_template("connectors/webfonts.html")


@app.route("/connectors/webfonts/connect")
def webfonts_connect():
    r = requests.get(
        "http://localhost:4000/connectors/webfonts/connect",
        cookies=request.cookies
    )
    return jsonify(r.json())


@app.route("/connectors/webfonts/disconnect")
def webfonts_disconnect():
    r = requests.get(
        "http://localhost:4000/connectors/webfonts/disconnect",
        cookies=request.cookies
    )
    return jsonify(r.json())


@app.route("/connectors/webfonts/sync")
def webfonts_sync():
    r = requests.get(
        "http://localhost:4000/connectors/webfonts/sync",
        cookies=request.cookies,
        timeout=180
    )
    return jsonify(r.json())


@app.route("/connectors/webfonts/job/get")
def webfonts_job_get_proxy():
    r = requests.get(
        "http://localhost:4000/connectors/webfonts/job/get",
        cookies=request.cookies
    )
    return jsonify(r.json())


@app.route("/connectors/webfonts/job/save", methods=["POST"])
def webfonts_job_save_proxy():
    r = requests.post(
        "http://localhost:4000/connectors/webfonts/job/save",
        json=request.get_json(),
        cookies=request.cookies
    )
    return jsonify(r.json())


@app.route("/api/status/webfonts")
def webfonts_status():
    r = requests.get(
        "http://localhost:4000/api/status/webfonts",
        cookies=request.cookies
    )
    return jsonify(r.json())


@app.route("/api/webfonts/data")
def webfonts_data():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM google_webfonts
        ORDER BY fetched_at DESC
        LIMIT 1000
    """)

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])

@app.route("/connectors/webfonts/save_config", methods=["POST"])
def webfonts_save_config_proxy():

    r = requests.post(
        "http://localhost:4000/connectors/webfonts/save_config",
        json=request.get_json(),
        cookies=request.cookies
    )

    return jsonify(r.json()), r.status_code

# ================= GOOGLE PAGESPEED ========================

@app.route("/connectors/pagespeed")
def pagespeed_page():
    return render_template("connectors/pagespeed.html")


@app.route("/connectors/pagespeed/sync", methods=["POST"])
def pagespeed_sync():

    data = request.get_json()

    if not data:
        return jsonify({"error": "JSON body required"}), 400

    url = data.get("url")

    if not url:
        return jsonify({"error": "URL required"}), 400

    try:

        r = requests.post(
            "http://localhost:4000/google/sync/pagespeed",
            json={
                "urls": [url]
            },
            timeout=600
        )

        return jsonify(r.json())

    except Exception as e:

        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


@app.route("/dashboard/pagespeed")
def pagespeed_dashboard():
    return render_template("dashboards/pagespeed.html")


@app.route("/api/status/pagespeed")
def pagespeed_status():

    uid = request.cookies.get("uid") or "demo_user"

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # ---------- connection ----------
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='pagespeed'
        LIMIT 1
    """, (uid,))

    row = cur.fetchone()
    connected = bool(row and row[0] == 1)

    # ---------- api key ----------
    cur.execute("""
        SELECT api_key
        FROM connector_configs
        WHERE uid=? AND connector='pagespeed'
        LIMIT 1
    """, (uid,))

    key_row = cur.fetchone()
    api_key_saved = key_row is not None

    conn.close()

    return jsonify({
        "connected": connected,
        "api_key_saved": api_key_saved
    })

@app.route("/api/pagespeed/data")
def pagespeed_data():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM google_pagespeed
        ORDER BY fetched_at DESC
        LIMIT 1000
    """)

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])

@app.route("/connectors/pagespeed/connect")
def pagespeed_connect_proxy():
    r = requests.get(
        "http://localhost:4000/connectors/pagespeed/connect",
        cookies=request.cookies
    )
    return jsonify(r.json())

@app.route("/connectors/pagespeed/disconnect")
def pagespeed_disconnect_proxy():
    r = requests.get(
        "http://localhost:4000/connectors/pagespeed/disconnect",
        cookies=request.cookies
    )
    return jsonify(r.json())


@app.route("/connectors/pagespeed/save_config", methods=["POST"])
def pagespeed_save_config_proxy():

    r = requests.post(
        "http://localhost:4000/connectors/pagespeed/save_config",
        json=request.get_json(),
        cookies=request.cookies
    )

    return jsonify(r.json()), r.status_code

@app.route("/connectors/pagespeed/job/get")
def pagespeed_job_get_proxy():
    r = requests.get(
        "http://localhost:4000/connectors/pagespeed/job/get",
        cookies=request.cookies
    )
    return jsonify(r.json())

@app.route("/connectors/pagespeed/job/save", methods=["POST"])
def pagespeed_job_save_proxy():
    r = requests.post(
        "http://localhost:4000/connectors/pagespeed/job/save",
        json=request.get_json(),
        cookies=request.cookies
    )
    return jsonify(r.json())

# ================= GOOGLE CLOUD STORAGE =================

@app.route("/connectors/gcs")
def gcs_page():
    return render_template("connectors/gcs.html")


# ---- CONNECT (Google OAuth) ----
@app.route("/connectors/gcs/connect")
def gcs_connect():
    return redirect(
        "http://localhost:4000/google/connect?source=gcs"
    )

# ---- SYNC ----
@app.route("/connectors/gcs/sync")
def gcs_sync():

    sync_type = request.args.get("sync_type","incremental")

    r = requests.get(
        "http://localhost:4000/google/sync/gcs",
        params={"sync_type": sync_type}
    )

    return jsonify(r.json())

# ---- DASHBOARD ----
@app.route("/dashboard/gcs")
def gcs_dashboard():
    return render_template("dashboards/gcs.html")

# ---- DATA APIs ----
@app.route("/api/gcs/data/buckets")
def gcs_buckets():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM google_gcs_buckets
        ORDER BY fetched_at DESC
    """)

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])


@app.route("/api/gcs/data/objects")
def gcs_objects():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM google_gcs_objects
        ORDER BY fetched_at DESC
    """)

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])

@app.route("/api/status/gcs")
def gcs_status():

    uid=request.cookies.get("uid") or "demo_user"
    conn=sqlite3.connect(DB_PATH)
    cur=conn.cursor()

    # credentials saved?
    cur.execute("""
        SELECT 1 FROM connector_configs
        WHERE uid=? AND connector='gcs'
        LIMIT 1
    """,(uid,))
    creds=cur.fetchone()

    # connected?
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='gcs'
        LIMIT 1
    """,(uid,))
    row=cur.fetchone()

    conn.close()

    return jsonify({
        "connected": bool(row and row[0]==1),
        "has_credentials": bool(creds)
    })

@app.route("/connectors/gcs/disconnect")
def gcs_disconnect():

    r = requests.get(
        "http://localhost:4000/google/disconnect/gcs"
    )

    return jsonify(r.json())

@app.route("/connectors/gcs/job/get")
def gcs_job_get_proxy():

    r = requests.get(
        "http://localhost:4000/connectors/gcs/job/get",
        headers={"Cookie": request.headers.get("Cookie","")}
    )

    return jsonify(r.json())

@app.route("/connectors/gcs/job/save", methods=["POST"])
def gcs_job_save_proxy():

    r = requests.post(
        "http://localhost:4000/connectors/gcs/job/save",
        json=request.get_json(),
        headers={"Cookie": request.headers.get("Cookie","")}
    )

    return jsonify(r.json())

@app.route("/connectors/gcs/save_app",methods=["POST"])
def gcs_save_app_proxy():
    r=requests.post(
        "http://localhost:4000/connectors/gcs/save_app",
        json=request.get_json(),
        headers={"Cookie":request.headers.get("Cookie","")}
    )
    return jsonify(r.json()),r.status_code

# ================= GOOGLE CLASSROOM =================

@app.route("/connectors/classroom")
def classroom_page():
    return render_template("connectors/classroom.html")


@app.route("/connectors/classroom/connect")
def classroom_connect():
    return redirect(
        "http://localhost:4000/google/connect?source=classroom"
    )

@app.route("/connectors/classroom/save_app", methods=["POST"])
def classroom_save_app_proxy():

    r = requests.post(
        "http://localhost:4000/connectors/classroom/save_app",
        json=request.get_json(),
        headers={"Cookie": request.headers.get("Cookie","")}
    )

    return jsonify(r.json()), r.status_code

@app.route("/connectors/classroom/disconnect")
def classroom_disconnect():

    r = requests.get(
        "http://localhost:4000/google/disconnect/classroom"
    )

    return jsonify(r.json())


# ---- SYNC ----
@app.route("/connectors/classroom/sync")
def classroom_sync():

    r = requests.get(
        "http://localhost:4000/google/sync/classroom",
        timeout=300
    )

    if r.status_code != 200:
        return r.text, 400

    return r.json()


# ---- DASHBOARD ----
@app.route("/dashboard/classroom")
def classroom_dashboard():
    return render_template("dashboards/classroom.html")

@app.route("/api/status/classroom")
def classroom_status():

    uid = request.cookies.get("uid") or "demo_user"

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # credentials saved?
    cur.execute("""
        SELECT 1 FROM connector_configs
        WHERE uid=? AND connector='classroom'
        LIMIT 1
    """,(uid,))
    creds = cur.fetchone()

    # oauth connected?
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='classroom'
        LIMIT 1
    """,(uid,))
    row = cur.fetchone()

    conn.close()

    return jsonify({
        "connected": bool(row and row[0]==1),
        "has_credentials": bool(creds)
    })

# ---- DATA APIs ----

@app.route("/api/classroom/courses")
def classroom_courses():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM google_classroom_courses
        ORDER BY fetched_at DESC
    """)

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])


@app.route("/api/classroom/teachers")
def classroom_teachers():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM google_classroom_teachers
    """)

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])


@app.route("/api/classroom/students")
def classroom_students():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM google_classroom_students
    """)

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])


@app.route("/api/classroom/announcements")
def classroom_announcements():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM google_classroom_announcements
    """)

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])


@app.route("/api/classroom/coursework")
def classroom_coursework():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM google_classroom_coursework
    """)

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])


@app.route("/api/classroom/submissions")
def classroom_submissions():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM google_classroom_submissions
    """)

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])

# ================= GOOGLE FACT CHECK =================

@app.route("/connectors/factcheck")
def factcheck_page():
    return render_template("connectors/factcheck.html")

@app.route("/connectors/factcheck/connect")
def factcheck_connect():
    r = requests.get(
        "http://localhost:4000/connectors/factcheck/connect",
        cookies=request.cookies
    )
    return jsonify(r.json())

@app.route("/connectors/factcheck/disconnect")
def factcheck_disconnect():
    r = requests.get(
        "http://localhost:4000/connectors/factcheck/disconnect",
        cookies=request.cookies
    )
    return jsonify(r.json())

@app.route("/connectors/factcheck/sync")
def factcheck_sync():

    query = request.args.get("query")

    if not query:
        return jsonify({
            "status": "error",
            "message": "Query required"
        })


    try:

        r = requests.get(
            "http://127.0.0.1:4000/googlefactcheck/sync/claims",
            params={
                "q": query,
                "limit": 200
            },
            timeout=60
        )


        if r.status_code != 200:
            return jsonify({
                "status": "error",
                "message": r.text
            })


        data = r.json()


        return jsonify({
            "status": "ok",
            "data": data
        })


    except Exception as e:

        return jsonify({
            "status": "error",
            "message": str(e)
        })

# ---------- DASHBOARD ----------
@app.route("/dashboard/factcheck")
def factcheck_dashboard():
    return render_template("dashboards/factcheck.html")


# ---------- STATUS ----------
@app.route("/api/status/factcheck")
def factcheck_status():

    r = requests.get(
        "http://localhost:4000/api/status/factcheck",
        cookies=request.cookies
    )

    return jsonify(r.json())

@app.route("/connectors/factcheck/job/get")
def factcheck_job_get_proxy():

    r = requests.get(
        "http://localhost:4000/connectors/factcheck/job/get",
        cookies=request.cookies
    )

    return jsonify(r.json())

@app.route("/connectors/factcheck/job/save", methods=["POST"])
def factcheck_job_save_proxy():

    r = requests.post(
        "http://localhost:4000/connectors/factcheck/job/save",
        json=request.get_json(),
        cookies=request.cookies
    )

    return jsonify(r.json())

# ---------- DATA ----------
@app.route("/api/factcheck/claims")
def factcheck_claims():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM google_factcheck_claims
        ORDER BY fetched_at DESC
        LIMIT 500
    """)

    rows = cur.fetchall()

    conn.close()

    return jsonify([dict(r) for r in rows])

@app.route("/connectors/factcheck/save_config", methods=["POST"])
def factcheck_save_config_proxy():

    r = requests.post(
        "http://localhost:4000/connectors/factcheck/save_config",
        json=request.get_json(),
        cookies=request.cookies
    )

    return jsonify(r.json()), r.status_code

# ================= FACEBOOK PAGES=================

@app.route("/connectors/facebook")
def facebook_page():
    return render_template("connectors/facebookpages.html")


@app.route("/connectors/facebook/connect")
def facebook_connect():
    return redirect("http://localhost:4000/connectors/facebook/connect")


@app.route("/connectors/facebook/disconnect")
def facebook_disconnect():
    requests.get(
        "http://localhost:4000/connectors/facebook/disconnect",
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return redirect("/connectors/facebook")


@app.route("/connectors/facebook/sync")
def facebook_sync():
    r = requests.get(
        "http://localhost:4000/connectors/facebook/sync",
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return jsonify(r.json())

@app.route("/api/status/facebook")
def facebook_status():
    r = requests.get(
        "http://localhost:4000/api/status/facebook",
        cookies=request.cookies
    )
    return jsonify(r.json())

@app.route("/connectors/facebook/save_app", methods=["POST"])
def facebook_save_app_proxy():
    r = requests.post(
        "http://localhost:4000/connectors/facebook/save_app",
        json=request.get_json(),
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return jsonify(r.json()), r.status_code

@app.route("/connectors/facebook/job/get")
def facebook_job_get_proxy():
    r = requests.get(
        "http://localhost:4000/connectors/facebook/job/get",
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    try:
        return jsonify(r.json()), r.status_code
    except:
        return jsonify({
            "exists": False,
            "sync_type": "incremental",
            "schedule_time": None
        }), 200

@app.route("/connectors/facebook/job/save", methods=["POST"])
def facebook_job_save_proxy():
    r = requests.post(
        "http://localhost:4000/connectors/facebook/job/save",
        json=request.get_json(),
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return jsonify(r.json()), r.status_code

# ================= FACEBOOK ADS =================

@app.route("/connectors/facebook_ads")
def facebook_ads_page():
    return render_template("connectors/facebook_ads.html")

@app.route("/connectors/facebook_ads/connect")
def facebook_ads_connect():
    return redirect("http://localhost:4000/connectors/facebook_ads/connect")

@app.route("/connectors/facebook_ads/disconnect")
def facebook_ads_disconnect():
    requests.get(
        "http://localhost:4000/connectors/facebook_ads/disconnect",
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return redirect("/connectors/facebook_ads")

@app.route("/connectors/facebook_ads/sync")
def facebook_ads_sync():
    r = requests.get(
        "http://localhost:4000/connectors/facebook_ads/sync",
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return jsonify(r.json())

@app.route("/api/status/facebook_ads")
def facebook_ads_status():
    r = requests.get(
        "http://localhost:4000/api/status/facebook_ads",
        cookies=request.cookies
    )
    return jsonify(r.json())

@app.route("/connectors/facebook_ads/job/get")
def facebook_ads_job_get_proxy():
    r = requests.get(
        "http://localhost:4000/connectors/facebook_ads/job/get",
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    try:
        return jsonify(r.json()), r.status_code
    except:
        return jsonify({
            "exists": False,
            "sync_type": "incremental",
            "schedule_time": None
        }), 200

@app.route("/connectors/facebook_ads/job/save", methods=["POST"])
def facebook_ads_job_save_proxy():
    r = requests.post(
        "http://localhost:4000/connectors/facebook_ads/job/save",
        json=request.get_json(),
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return jsonify(r.json()), r.status_code

@app.route("/connectors/facebook_ads/save_app", methods=["POST"])
def facebook_ads_save_app_proxy():
    r = requests.post(
        "http://localhost:4000/connectors/facebook_ads/save_app",
        json=request.get_json(),
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return jsonify(r.json()), r.status_code

# ================= DESTINATION =================

@app.route("/destination/save", methods=["POST"])
def destination_save_proxy():
    r = requests.post(
        "http://localhost:4000/destination/save",
        json=request.get_json(),
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return jsonify(r.json()), r.status_code


@app.route("/destination/list/<source>")
def destination_list_proxy(source):
    r = requests.get(
        f"http://localhost:4000/destination/list/{source}",
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return jsonify(r.json()), r.status_code

# ================= MAIN ==========================

if __name__ == "__main__":
    app.run(port=3000, debug=True)