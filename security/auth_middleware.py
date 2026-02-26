from flask import request, g, redirect
import sqlite3

DB = "identity.db"


def get_db():
    return sqlite3.connect(DB)

# Resolve Logged User
def load_logged_user():

    session_id = request.cookies.get("segmento_session")

    if not session_id:
        g.user_id = None
        return

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT user_id
        FROM user_sessions
        WHERE session_id=?
        LIMIT 1
    """, (session_id,))

    row = cur.fetchone()
    con.close()

    if row:
        g.user_id = row[0]
    else:
        g.user_id = None

# Login Required Decorator
def require_login():

    if not getattr(g, "user_id", None):
        return redirect("/signup")