import sqlite3
import json
import datetime
import requests
import os
from flask import redirect, request, jsonify

# Amazon Seller Connector (SP-API)
# -------------------------------

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "identity.db")

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def save_app_amazon_seller(client_id, client_secret, seller_id, region):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM amazon_seller_config")
    cur.execute("INSERT INTO amazon_seller_config (client_id, client_secret, seller_id, region) VALUES (?, ?, ?, ?)", 
                (client_id, client_secret, seller_id, region))
    conn.commit()
    conn.close()
    return {"status": "success"}

def connect_amazon_seller():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT client_id FROM amazon_seller_config LIMIT 1")
    row = cur.fetchone()
    conn.close()
    
    if not row:
        return redirect("/connectors/amazon_seller?error=missing_creds")
        
    # Amazon Login with Amazon (LWA) setup
    client_id = row['client_id']
    redirect_uri = "http://localhost:4000/connectors/amazon_seller/callback"
    
    # NOTE: In production, you'd use the Amazon marketplace-specific Auth URL
    auth_url = (
        f"https://sellercentral.amazon.com/apps/authorize/consent"
        f"?application_id={client_id}"
        f"&state=amazon_state"
        f"&version=beta" # For SP-API
    )
    return redirect(auth_url)

def callback_amazon_seller():
    spapi_oauth_code = request.args.get("spapi_oauth_code")
    selling_partner_id = request.args.get("selling_partner_id")
    
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT client_id, client_secret FROM amazon_seller_config LIMIT 1")
    config = cur.fetchone()
    
    if not config or not spapi_oauth_code:
        return redirect("/connectors/amazon_seller?error=auth_failed")

    # Exchange code for LWA refresh token
    token_url = "https://api.amazon.com/auth/o2/token"
    payload = {
        "grant_type": "authorization_code",
        "code": spapi_oauth_code,
        "client_id": config['client_id'],
        "client_secret": config['client_secret'],
        "redirect_uri": "http://localhost:4000/connectors/amazon_seller/callback"
    }
    
    res = requests.post(token_url, data=payload)
    data = res.json()
    
    if "refresh_token" in data:
        cur.execute("DELETE FROM amazon_seller_auth")
        cur.execute(
            "INSERT INTO amazon_seller_auth (refresh_token, seller_id) VALUES (?, ?)",
            (data['refresh_token'], selling_partner_id)
        )
        conn.commit()
        conn.close()
        return redirect("/connectors/amazon_seller?connected=1")
    
    conn.close()
    return redirect("/connectors/amazon_seller?error=token_failed")

def disconnect_amazon_seller():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM amazon_seller_auth")
    conn.commit()
    conn.close()
    return {"status": "disconnected"}

def sync_amazon_seller():
    # SP-API logic requires converting refresh_token to access_token on every sync
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT b.client_id, b.client_secret, a.refresh_token FROM amazon_seller_auth a JOIN amazon_seller_config b LIMIT 1")
    auth = cur.fetchone()
    
    if not auth:
        return {"error": "not_connected"}
        
    rows_pushed = 20 # Example
    return {"status": "success", "rows_pushed": rows_pushed}
