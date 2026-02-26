from flask import Blueprint, request, jsonify, make_response
import sqlite3
import uuid
import datetime
import os

from werkzeug.security import generate_password_hash
from security.secure_db import encrypt_payload

auth = Blueprint("auth", __name__)

DB = "identity.db"
UPLOAD_DIR = "uploads/company_logos"

os.makedirs(UPLOAD_DIR, exist_ok=True)

# DB CONNECTION
def get_db():
    return sqlite3.connect(DB)

# SIGNUP
@auth.route("/auth/signup", methods=["POST"])
def signup():

    data = request.form

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
        "redirect": "http://localhost:3000/connectors"
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