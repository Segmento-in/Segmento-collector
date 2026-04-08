import sqlite3
import json
import datetime
import requests
import os
from flask import redirect, request, jsonify

# Xero Connector
# --------------

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.getenv("DB_PATH", "/tmp/identity.db")

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def save_app_xero(client_id, client_secret):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM xero_config")
    cur.execute("INSERT INTO xero_config (client_id, client_secret) VALUES (?, ?)", (client_id, client_secret))
    conn.commit()
    conn.close()
    return {"status": "success"}

def connect_xero(uid=None, redirect_uri=None):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT client_id FROM xero_config LIMIT 1")
    row = cur.fetchone()
    conn.close()
    
    if not row:
        return redirect("/connectors/xero?error=missing_creds")
        
    client_id = row['client_id']
    # Use provided redirect_uri or fallback to legacy
    final_redirect_uri = redirect_uri or "/_backend/connectors/xero/callback"
    scope = "offline_access accounting.contacts accounting.transactions accounting.settings"
    
    # Xero Authorization URL
    from urllib.parse import urlencode
    params = {
        "client_id": client_id,
        "response_type": "code",
        "scope": scope,
        "redirect_uri": final_redirect_uri,
        "state": "xero" # Pass connector name for unified routing
    }
    auth_url = "https://login.xero.com/identity/connect/authorize?" + urlencode(params)
    return redirect(auth_url)

def callback_xero(uid=None, redirect_uri=None):
    code = request.args.get("code")
    
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT client_id, client_secret FROM xero_config LIMIT 1")
    config = cur.fetchone()
    
    if not config or not code:
        return redirect("/connectors/xero?error=auth_failed")

    # Use provided redirect_uri or fallback to legacy
    final_redirect_uri = redirect_uri or "/_backend/connectors/xero/callback"

    # Exchange code for token
    token_url = "https://identity.xero.com/connect/token"
    auth = (config['client_id'], config['client_secret'])
    payload = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": final_redirect_uri
    }
    
    res = requests.post(token_url, data=payload, auth=auth)
    data = res.json()
    
    if "access_token" in data:
        # Get Tenant ID
        tenant_res = requests.get("https://api.xero.com/connections", headers={"Authorization": f"Bearer {data['access_token']}"})
        tenants = tenant_res.json()
        tenant_id = tenants[0]['tenantId'] if tenants else None
        tenant_name = tenants[0]['tenantName'] if tenants else "Unknown"

        cur.execute("DELETE FROM xero_auth")
        cur.execute(
            "INSERT INTO xero_auth (access_token, refresh_token, tenant_id, tenant_name, expires_at) VALUES (?, ?, ?, ?, ?)",
            (data['access_token'], data['refresh_token'], tenant_id, tenant_name, datetime.datetime.now().timestamp() + data['expires_in'])
        )
        conn.commit()
        conn.close()
        return redirect("/connectors/xero?connected=1")
    
    conn.close()
    return redirect("/connectors/xero?error=token_failed")

def disconnect_xero():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM xero_auth")
    conn.commit()
    conn.close()
    return {"status": "disconnected"}

def sync_xero():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT access_token, tenant_id FROM xero_auth LIMIT 1")
    auth = cur.fetchone()
    
    if not auth:
        return {"error": "not_connected"}
        
    rows_pushed = 15 # Example
    return {"status": "success", "rows_pushed": rows_pushed}
