import sqlite3
import json
import datetime
import requests
import os
from flask import redirect, request, jsonify

# QuickBooks Connector
# --------------------

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "identity.db")

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def save_app_quickbooks(client_id, client_secret):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM quickbooks_config")
    cur.execute("INSERT INTO quickbooks_config (client_id, client_secret) VALUES (?, ?)", (client_id, client_secret))
    conn.commit()
    conn.close()
    return {"status": "success"}

def connect_quickbooks(uid, redirect_uri=None):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT client_id FROM quickbooks_config LIMIT 1")
    row = cur.fetchone()
    conn.close()
    
    if not row:
        return redirect("/connectors/quickbooks?error=missing_creds")
        
    client_id = row['client_id']
    # Use provided redirect_uri or fallback to legacy
    final_redirect_uri = redirect_uri or "/_backend/connectors/quickbooks/callback"
    scope = "com.intuit.quickbooks.accounting openid profile email"
    
    # Intuit Authorization URL
    from urllib.parse import urlencode
    params = {
        "client_id": client_id,
        "response_type": "code",
        "scope": scope,
        "redirect_uri": final_redirect_uri,
        "state": "quickbooks" # Pass connector name for unified routing
    }
    auth_url = "https://appcenter.intuit.com/connect/oauth2?" + urlencode(params)
    return redirect(auth_url)

def callback_quickbooks(uid=None, redirect_uri=None):
    code = request.args.get("code")
    realm_id = request.args.get("realmId")
    
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT client_id, client_secret FROM quickbooks_config LIMIT 1")
    config = cur.fetchone()
    
    if not config or not code:
        return redirect("/connectors/quickbooks?error=auth_failed")

    # Use provided redirect_uri or fallback to legacy
    final_redirect_uri = redirect_uri or "/_backend/connectors/quickbooks/callback"

    # Exchange code for token
    token_url = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
    auth = (config['client_id'], config['client_secret'])
    payload = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": final_redirect_uri
    }
    
    res = requests.post(token_url, data=payload, auth=auth)
    data = res.json()
    
    if "access_token" in data:
        cur.execute("DELETE FROM quickbooks_auth")
        cur.execute(
            "INSERT INTO quickbooks_auth (access_token, refresh_token, realm_id, expires_at) VALUES (?, ?, ?, ?)",
            (data['access_token'], data['refresh_token'], realm_id, datetime.datetime.now().timestamp() + data['expires_in'])
        )
        conn.commit()
        conn.close()
        return redirect("/connectors/quickbooks?connected=1")
    
    conn.close()
    return redirect("/connectors/quickbooks?error=token_failed")

def disconnect_quickbooks():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM quickbooks_auth")
    conn.commit()
    conn.close()
    return {"status": "disconnected"}

def sync_quickbooks():
    # Placeholder for main sync logic
    # In a real app, this would iterate through Customers, Invoices, etc.
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT access_token, realm_id FROM quickbooks_auth LIMIT 1")
    auth = cur.fetchone()
    
    if not auth:
        return {"error": "not_connected"}
        
    # Mocking data fetch
    rows_pushed = 10 # Example
    
    return {"status": "success", "rows_pushed": rows_pushed}

# Add other functions as needed (sync_customers, etc.)
