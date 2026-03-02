from flask import Blueprint, request, jsonify, make_response, redirect
import sqlite3
import uuid
import datetime
import os
from urllib.parse import quote_plus

from werkzeug.security import generate_password_hash
from security.secure_db import encrypt_payload
from werkzeug.security import check_password_hash

auth = Blueprint("auth", __name__)

DB = "identity.db"
UPLOAD_DIR = "uploads/company_logos"

os.makedirs(UPLOAD_DIR, exist_ok=True)
UI_BASE_URL = os.getenv("UI_BASE_URL", "http://localhost:3000")

# DB CONNECTION
def get_db():
    return sqlite3.connect(DB)


def sanitize_next_path(next_path):
    if not next_path:
        return "/"
    if not next_path.startswith("/") or next_path.startswith("//"):
        return "/"
    return next_path


def build_login_redirect(error_code, next_path="", auth_required=""):
    params = [f"error={error_code}"]

    safe_next = sanitize_next_path(next_path)
    if safe_next != "/":
        params.append(f"next={quote_plus(safe_next)}")

    if auth_required == "1":
        params.append("auth_required=1")

    return f"{UI_BASE_URL}/login?{'&'.join(params)}"

# ================= Signup =================
@auth.route("/auth/signup", methods=["POST"])
def signup():

    data = request.form
    next_url = sanitize_next_path(data.get("next"))

    # BASIC VALIDATION
    email = data.get("email")
    password = data.get("password")

    if not email or not password:
        return jsonify({
            "error": "Email and password are required."
        }), 400

    user_id = str(uuid.uuid4())

    first = data.get("first_name")
    last = data.get("last_name")

    company = data.get("company_name")
    company_size = data.get("company_size")
    country = data.get("country")
    phone = data.get("phone")

    is_individual = 1 if data.get(
        "is_individual") == "true" else 0

    # PASSWORD HASH + ENCRYPT
    password_hash = generate_password_hash(password)

    secured = encrypt_payload({
        "password": password_hash
    })

    # LOGO UPLOAD
    logo_path = None

    if "company_logo" in request.files:
        logo = request.files["company_logo"]

        if logo and logo.filename:
            filename = f"{user_id}_{logo.filename}"
            path = os.path.join(UPLOAD_DIR, filename)
            logo.save(path)
            logo_path = path

    con = get_db()
    cur = con.cursor()

    # CREATE USER
    try:

        cur.execute("""
            INSERT INTO users(
                id,email,password,
                first_name,last_name,
                company_name,company_size,
                country,phone,
                company_logo,
                is_individual,
                created_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            user_id,
            email,
            secured["password"],
            first,
            last,
            company,
            company_size,
            country,
            phone,
            logo_path,
            is_individual,
            datetime.datetime.utcnow().isoformat()
        ))

    except sqlite3.IntegrityError:

        con.close()

        return jsonify({
            "error": "Account already exists"
        }), 409

    session_id = str(uuid.uuid4())

    cur.execute("""
        INSERT INTO user_sessions(
            session_id,
            user_id,
            created_at,
            expires_at
        )
        VALUES(?,?,?,?)
    """, (
        session_id,
        user_id,
        datetime.datetime.utcnow().isoformat(),
        None
    ))

    con.commit()
    con.close()

    resp = make_response(jsonify({
        "success": True,
        "redirect": f"{UI_BASE_URL}{next_url}"
    }))

    resp.set_cookie(
        "segmento_session",
        session_id,
        httponly=True,
        samesite="Lax",
        path="/"
    )

    resp.set_cookie(
        "uid",
        user_id,
        httponly=False,
        samesite="Lax",
        path="/"
    )

    return resp

# ================= LOGIN =================

from security.crypto import decrypt_value


@auth.route("/auth/login", methods=["POST"])
def login():

    data = request.form

    email = data.get("email")
    password = data.get("password")
    next_url = data.get("next", "")
    auth_required = data.get("auth_required", "")

    if not email or not password:
        return redirect(build_login_redirect(
            "missing",
            next_url=next_url,
            auth_required=auth_required
        ))

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT id, password
        FROM users
        WHERE email=?
    """, (email,))

    row = cur.fetchone()

    if not row:
        con.close()
        return redirect(build_login_redirect(
            "invalid",
            next_url=next_url,
            auth_required=auth_required
        ))

    user_id = row[0]
    encrypted_hash = row[1]

    # DECRYPT STORED PASSWORD
    stored_hash = decrypt_value(encrypted_hash)

    # VERIFY PASSWORD
    if not check_password_hash(stored_hash, password):
        con.close()
        return redirect(build_login_redirect(
            "invalid",
            next_url=next_url,
            auth_required=auth_required
        ))

    # ---------- CREATE SESSION ----------
    session_id = str(uuid.uuid4())

    cur.execute("""
        INSERT INTO user_sessions
        VALUES(?,?,?,?)
    """, (
        session_id,
        user_id,
        datetime.datetime.utcnow().isoformat(),
        None
    ))

    con.commit()
    con.close()

    safe_next = sanitize_next_path(next_url)
    resp = make_response(redirect(f"{UI_BASE_URL}{safe_next}"))

    resp.set_cookie(
        "segmento_session",
        session_id,
        httponly=True,
        samesite="Lax",
        path="/"
    )

    resp.set_cookie(
        "uid",
        user_id,
        httponly=False,
        samesite="Lax",
        path="/"
    )

    return resp


@auth.route("/auth/logout")
def logout():

    session_id = request.cookies.get("segmento_session")

    if session_id:
        con = get_db()
        cur = con.cursor()

        cur.execute("""
            DELETE FROM user_sessions
            WHERE session_id=?
        """, (session_id,))

        con.commit()
        con.close()

    resp = make_response(jsonify({"status": "logged_out"}))

    resp.delete_cookie("segmento_session")

    return resp
