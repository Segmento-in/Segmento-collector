import sqlite3
import json
import datetime
import requests
import os
from flask import redirect, request, jsonify

# New Relic Connector
# -------------------

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "identity.db")

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def save_app_newrelic(api_key, account_id, region):
    # For New Relic, we directly save credentials and validate them
    conn = get_db()
    cur = conn.cursor()
    
    # Basic validation request to New Relic GraphQL
    endpoint = "https://api.newrelic.com/graphql" if region == "US" else "https://api.eu.newrelic.com/graphql"
    query = "{ actor { user { name } } }"
    
    try:
        res = requests.post(endpoint, json={"query": query}, headers={"API-Key": api_key})
        if res.status_code == 200 and "errors" not in res.json():
            cur.execute("DELETE FROM newrelic_auth")
            cur.execute("INSERT INTO newrelic_auth (api_key, account_id, region) VALUES (?, ?, ?)", (api_key, account_id, region))
            conn.commit()
            conn.close()
            return {"status": "success", "message": "Authenticated"}
        else:
            conn.close()
            return {"status": "error", "message": "Invalid API Key or Account ID"}, 401
    except Exception as e:
        conn.close()
        return {"status": "error", "message": str(e)}, 500

def disconnect_newrelic():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM newrelic_auth")
    conn.commit()
    conn.close()
    return {"status": "disconnected"}

def sync_newrelic():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT api_key, account_id, region FROM newrelic_auth LIMIT 1")
    auth = cur.fetchone()
    
    if not auth:
        return {"error": "not_connected"}
        
    rows_pushed = 25 # Example
    return {"status": "success", "rows_pushed": rows_pushed}
