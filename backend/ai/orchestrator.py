"""
backend/ai/orchestrator.py

Main Orchestrator for AI-driven multi-step platform workflows.
Refined Version: 
- Supports choosing existing destinations.
- NL to 24-h time conversion for scheduling.
- Direct DB queries for usage statistics.
- Structured state management.
"""

import os
import json
import sqlite3
import datetime
import re
import importlib
import inspect
from typing import Optional, Dict, Any, List

from .registry import ALIAS_INDEX, get_connector_url, CONNECTORS
from .route_executor import call_connector_route
from .executor import execute_intent, normalize_source

# ──────────────────────────────────────────────────────────────────────────────
# 1. CONSTANTS & DB HELPERS
# ──────────────────────────────────────────────────────────────────────────────

DB_PATH = "identity.db"

def get_db():
    con = sqlite3.connect(DB_PATH, timeout=60, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con

CONNECTOR_MODULE_OVERRIDES = {
    "gmail": "google_gmail",
    "drive": "google_drive",
    "calendar": "google_calendar",
    "sheets": "google_sheets",
    "forms": "google_forms",
    "classroom": "classroom",
    "contacts": "google_contacts",
    "tasks": "google_tasks",
    "ga4": "google_ga4",
    "search-console": "google_search_console",
    "youtube": "google_youtube",
}

DESTINATION_FIELD_LABELS = {
    "postgres": {
        "host": "Host", "port": "Port", "username": "Username", "password": "Password", "database": "Database Name"
    },
    "mysql": {
        "host": "Host", "port": "Port", "username": "Username", "password": "Password", "database": "Database Name"
    },
    "redshift": {
        "host": "Host", "port": "Port", "username": "Username", "password": "Password", "database": "Database Name"
    },
    "clickhouse": {
        "host": "Host", "port": "Port", "username": "Username", "password": "Password", "database": "Database Name"
    },
    "snowflake": {
        "host": "Account / Host", "port": "Port", "username": "Username", "password": "Password", "database": "Database Name"
    },
    "mongodb": {
        "host": "Cluster URI", "database": "Database Name", "username": "Username", "password": "Password"
    },
    "elasticsearch": {
        "host": "Endpoint URL", "port": "Port", "database": "Index Name"
    },
    "duckdb": {
        "host": "File Path"
    },
    "bigquery": {
        "host": "Project ID", "database": "Dataset ID", "password": "Service Account JSON Key"
    },
    "s3": {
        "host": "Bucket Name", "port": "Region", "username": "Access Key ID", "password": "Secret Access Key"
    },
    "azure_datalake": {
        "host": "Storage Account", "port": "File System", "username": "Base Path", "password": "Account Key"
    },
    "databricks": {
        "host": "Workspace Hostname", "port": "HTTP Path", "database": "Catalog.Schema", "password": "Personal Access Token"
    },
    "gcs": {
        "host": "Bucket Name", "port": "Region", "password": "Service Account JSON Key"
    }
}

def _get_label(dtype: str, field: str) -> str:
    """Returns the UI label for a backend field, with fallback."""
    label = DESTINATION_FIELD_LABELS.get(dtype, {}).get(field)
    if label: return label
    return field.replace("_", " ").title()

# ──────────────────────────────────────────────────────────────────────────────
# 2. INTERNAL HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def _pretty(name: str) -> str:
    return name.replace("_", " ").replace("-", " ").title()

def _parse_credentials(message: str, pending_fields: list, dtype: Optional[str] = None) -> dict:
    """
    Parses user input. 
    1. Label-based logic (regex to find 'Label: Value' even if multi-line).
    2. Direct key:value fallback.
    3. Positional fallback (comma-separated).
    """
    found = {}
    
    # Map of Normalized Label -> Backend Key
    label_to_key = {}
    if dtype and dtype in DESTINATION_FIELD_LABELS:
        for k, v in DESTINATION_FIELD_LABELS[dtype].items():
            norm_label = v.lower().replace(" ", "").replace("-", "").replace("_", "")
            label_to_key[norm_label] = k
            # Add key itself as a synonym
            label_to_key[k.lower()] = k

    # ── 1. Label-Based Regex Extraction ──
    if label_to_key:
        # Create a reverse sorted list of labels (longest first for better matching)
        sorted_labels = sorted(DESTINATION_FIELD_LABELS[dtype].values(), key=len, reverse=True)
        # Add backend keys to candidate labels too
        candidates = sorted_labels + pending_fields
        
        # Pattern: (Candidate Label):\s*(Value until next label or end)
        labels_pattern = "|".join([re.escape(l) for l in candidates])
        matches = re.finditer(f"({labels_pattern}):\s*(.*?)(?={labels_pattern}:|$)", message, re.IGNORECASE | re.DOTALL)
        
        for match in matches:
            raw_label = match.group(1).lower().replace(" ", "").replace("-", "").replace("_", "")
            val = match.group(2).strip()
            
            # Remove trailing commas if any (from positional fallback or typing)
            if val.endswith(","): val = val[:-1].strip()
            
            key = label_to_key.get(raw_label) or raw_label
            if key in pending_fields:
                found[key] = val

    # ── 2. Positional Fallback (If no labels matched) ──
    if not found:
        # Simple split by comma or newline (but avoid splitting inside JSON)
        # Note: This positional logic is a fallback for simple values.
        # For JSON, labels are strictly required.
        parts = [p.strip() for p in re.split(r"[,|\n]", message) if p.strip()]
        if len(parts) == len(pending_fields):
            for i, val in enumerate(parts):
                found[pending_fields[i]] = val
        elif len(pending_fields) == 1:
            found[pending_fields[0]] = message.strip()

    return found

def _parse_time_to_24h(message: str) -> Optional[str]:
    """
    Parses '2 PM', '2:00 PM', '14:00' etc logic into HH:MM.
    """
    msg = message.upper().strip()
    
    # 1. Look for HH:MM AM/PM
    match = re.search(r"(\d{1,2}):?(\d{2})?\s*([AP]M)", msg)
    if match:
        hh = int(match.group(1))
        mm = int(match.group(2) or 0)
        meridiem = match.group(3)
        if meridiem == "PM" and hh < 12: hh += 12
        if meridiem == "AM" and hh == 12: hh = 0
        return f"{hh:02}:{mm:02}"
        
    # 2. Look for '2 PM'
    match = re.search(r"(\d{1,2})\s*([AP]M)", msg)
    if match:
        hh = int(match.group(1))
        meridiem = match.group(2)
        if meridiem == "PM" and hh < 12: hh += 12
        if meridiem == "AM" and hh == 12: hh = 0
        return f"{hh:02}:00"
        
    # 3. Look for HH:MM (already 24-h)
    match = re.search(r"(\d{1,2}):(\d{2})", msg)
    if match:
        hh = int(match.group(1))
        mm = int(match.group(2))
        if 0 <= hh <= 23 and 0 <= mm <= 59: return f"{hh:02}:{mm:02}"
        
    return None

# ──────────────────────────────────────────────────────────────────────────────
# 3. HANDLERS
# ──────────────────────────────────────────────────────────────────────────────

def _handle_destination(source: str, uid: str, chat_id: str, message: str, state: Optional[Dict]) -> Dict:
    # ── A. Resume ────────────────────────────────────────────────
    if state and state.get("flow") == "destination":
        step = state.get("step")
        metadata = state.get("metadata", {})
        dtype = metadata.get("type")
        
        if step == "awaiting_choice":
            choice = message.lower()
            if "new" in choice or "create" in choice:
                choices = ", ".join(DESTINATION_FIELD_LABELS.keys())
                return {
                    "type": "clarification",
                    "message": f"Which type of destination for **{_pretty(source)}**?\n\nChoices: {choices}",
                    "state": { "flow": "destination", "step": "awaiting_type", "connector": source, "metadata": {} }
                }
            
            match = re.search(r"(\d+)", message)
            if match:
                dest_id = match.group(1)
                con = get_db()
                con.execute("UPDATE destination_configs SET is_active=0 WHERE uid=? AND source=?", (uid, source))
                cur = con.execute("UPDATE destination_configs SET is_active=1 WHERE uid=? AND source=? AND id=?", (uid, source, dest_id))
                con.commit()
                if cur.rowcount > 0:
                    con.close()
                    return {"type": "status", "message": f"✅ Destination **#{dest_id}** is now active for **{_pretty(source)}**.", "state": None}
                con.close()
                return {"type": "error", "message": f"⚠️ Destination #{dest_id} not found for this connector.", "state": state}
            return {"type": "clarification", "message": "Please specify an ID from the list, or say 'create new'.", "state": state}

        if step == "awaiting_type":
            dtype = message.strip().lower()
            if dtype not in DESTINATION_FIELD_LABELS:
                choices = ", ".join(DESTINATION_FIELD_LABELS.keys())
                return {"type": "clarification", "message": f"⚠️ Unsupported type. Choices: {choices}", "state": state}
            
            # Fetch backend keys and map to labels
            backend_keys = list(DESTINATION_FIELD_LABELS[dtype].keys())
            labels_list = [f"• {_get_label(dtype, k)}" for k in backend_keys]
            examples = "\n".join([f"{_get_label(dtype, k).lower()}: xxx" for k in backend_keys])

            return {
                "type": "clarification",
                "message": f"Configuring **{dtype.title()}** for **{_pretty(source)}**.\n\nI need:\n" + "\n".join(labels_list) + 
                           f"\n\nYou can reply like:\n{examples}",
                "state": { "flow": "destination", "step": "awaiting_fields", "connector": source, "metadata": { "type": dtype, "pending": backend_keys, "provided": {} } }
            }

        if step == "awaiting_fields":
            pending = metadata.get("pending", [])
            provided = metadata.get("provided", {})
            provided.update(_parse_credentials(message, pending, dtype))
            
            missing = [k for k in pending if k not in provided]
            if missing:
                missing_labels = [f"• {_get_label(dtype, k)}" for k in missing]
                return {"type": "clarification", "message": "I still need:\n" + "\n".join(missing_labels), "state": state}
            
            # Cloud formats check
            if dtype in ("s3", "bigquery", "azure_datalake", "gcs", "databricks", "snowflake") and "format" not in provided:
                return {
                    "type": "clarification",
                    "message": "Select data format:\n• `parquet`\n• `json`\n• `iceberg`\n• `hudi`",
                    "state": { "flow": "destination", "step": "awaiting_format", "connector": source, "metadata": { **metadata, "provided": provided } }
                }
            
            # Final Save
            payload = { "source": source, "type": dtype, **provided }
            print(f"[DEST PAYLOAD] {payload}", flush=True) # Debug Log
            res = call_connector_route("/destination/save", uid, method="POST", json_data=payload)
            if res.get("ok"): return {"type": "status", "message": f"🚀 Successfully created and activated your new **{dtype.title()}** destination!", "state": None}
            err = res.get("error") or res.get("msg") or "Unknown API Error"
            return {"type": "error", "message": f"⚠️ Failed to save destination: {err}", "state": None}

        if step == "awaiting_format":
            fmt = message.strip().lower()
            if fmt not in ("parquet", "json", "iceberg", "hudi"):
                return {"type": "clarification", "message": "⚠️ Please select a valid format: `parquet`, `json`, `iceberg`, or `hudi`.", "state": state}
            
            metadata["provided"]["format"] = fmt
            # Final Save
            payload = { "source": source, "type": dtype, **metadata["provided"] }
            print(f"[DEST PAYLOAD] {payload}", flush=True) # Debug Log
            res = call_connector_route("/destination/save", uid, method="POST", json_data=payload)
            if res.get("ok"): return {"type": "status", "message": f"🚀 Successfully created and activated your new **{dtype.title()}** destination!", "state": None}
            err = res.get("error") or res.get("msg") or "Unknown API Error"
            return {"type": "error", "message": f"⚠️ Failed to save destination: {err}", "state": None}

    # ── B. New Flow ──────────────────────────────────────────────
    con = get_db()
    cur = con.execute("SELECT id, dest_type, host, is_active FROM destination_configs WHERE uid=? AND source=?", (uid, source))
    dests = cur.fetchall()
    con.close()
    
    if dests:
        list_str = "\n".join([f"• **#{d['id']}** ({d['dest_type']}) {'[ACTIVE]' if d['is_active'] else ''}" for d in dests])
        return {
            "type": "clarification",
            "message": f"I found existing destinations for **{_pretty(source)}**:\n\n{list_str}\n\nWould you like to **use an existing ID** or **create new**?",
            "state": { "flow": "destination", "step": "awaiting_choice", "connector": source, "metadata": {} }
        }
    
    choices = ", ".join(DESTINATION_FIELD_LABELS.keys())
    return {
        "type": "clarification",
        "message": f"No destinations found for **{_pretty(source)}**. Which type would you like to create?\n\nChoices: {choices}",
        "state": { "flow": "destination", "step": "awaiting_type", "connector": source, "metadata": {} }
    }

def _handle_schedule(source: str, uid: str, chat_id: str, message: str, state: Optional[Dict]) -> Dict:
    sched_time = _parse_time_to_24h(message)
    if sched_time:
        payload = {"schedule_time": sched_time, "sync_type": "incremental"}
        res = call_connector_route(f"/connectors/{source}/job/save", uid, method="POST", json_data=payload)
        if res.get("http_status") == 200:
            return {"type": "status", "message": f"📅 **{_pretty(source)}** scheduled for **{sched_time}** daily.", "state": None}
        return {"type": "error", "message": "⚠️ Internal error saving schedule."}
        
    return {
        "type": "clarification",
        "message": f"What time should I schedule **{_pretty(source)}**? (e.g. '2:30 PM' or '14:30')",
        "state": { "flow": "schedule", "step": "awaiting_time", "connector": source }
    }

def _handle_query(uid: str, message: str) -> Dict:
    con = get_db()
    msg = message.lower()
    
    if "how many" in msg or "connector" in msg:
        count = con.execute("SELECT COUNT(*) FROM connector_configs WHERE uid=? AND status='connected'", (uid,)).fetchone()[0]
        con.close()
        return {"type": "status", "message": f"🔗 You have **{count}** connectors fully connected right now."}
        
    if "rows" in msg or "records" in msg or "today" in msg:
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        total = con.execute("SELECT SUM(rows_pushed) FROM destination_push_logs WHERE uid=? AND pushed_at LIKE ?", (uid, f"{today}%")).fetchone()[0] or 0
        con.close()
        return {"type": "status", "message": f"📊 **{total}** rows were synced to your destinations today."}
        
    if "erd" in msg or "schema" in msg:
        # Search for backend files (logic to be expanded)
        con.close()
        return {"type": "status", "message": "🗺️ Entity Relationship Diagrams are available on connector pages. Which one are you interested in?"}
    
    con.close()
    return execute_intent({"action": "help"}, uid)

def _handle_sync(source: str, uid: str, chat_id: str) -> Dict:
    # 1. Connection check
    stat = call_connector_route(f"/api/status/{source}", uid)
    if not stat.get("connected"):
         return {"type": "error", "message": f"⚠️ **{_pretty(source)}** is not connected. Please say 'connect {source}' first."}
    
    # 2. Destination check
    con = get_db()
    active = con.execute("SELECT id FROM destination_configs WHERE uid=? AND source=? AND is_active=1", (uid, source)).fetchone()
    con.close()
    if not active:
        return _handle_destination(source, uid, chat_id, "", None)
        
    # 3. Exec Sync
    res = call_connector_route(f"/connectors/{source}/sync", uid)
    if res.get("http_status") == 200:
        return {"type": "sync", "message": f"🚀 Sync started for **{_pretty(source)}**!", "connectors": [source]}
    return {"type": "error", "message": f"⚠️ Sync failed: {res.get('error', 'Unknown')}"}

def _handle_connect(source: str, uid: str, chat_id: str, message: str, state: Optional[Dict]) -> Dict:
    # ── A. Resume (Credential Collection) ─────────────────────
    if state and state.get("flow") == "connect":
        metadata = state.get("metadata", {})
        pending = metadata.get("pending", [])
        provided = metadata.get("provided", {})
        
        provided.update(_parse_credentials(message, pending))
        missing = [f for f in pending if f not in provided]
        
        if not missing:
            res = call_connector_route(f"/connectors/{source}/connect", uid, method="POST", json_data=provided)
            if res.get("http_status") == 200:
                return {"type": "status", "message": f"✅ Successfully connected to **{_pretty(source)}**!", "state": None}
            return {"type": "error", "message": f"⚠️ Connection failed: {res.get('error', 'Unknown error')}", "state": None}
            
        return {
            "type": "clarification",
            "message": f"I still need: {', '.join([f'`{f}`' for f in missing])}.",
            "state": { "flow": "connect", "connector": source, "metadata": { "pending": pending, "provided": provided } }
        }

    # ── B. New Flow (2-Step Check) ───────────────────────────
    # Step 1: Status Check
    stat = call_connector_route(f"/api/status/{source}", uid)
    if stat.get("connected"):
        return {"type": "status", "message": f"✅ **{_pretty(source)}** is already connected. You can now run a sync or schedule a job!"}
    
    # Step 2: Attempt Connect (Discovery)
    res = call_connector_route(f"/connectors/{source}/connect", uid, method="POST", json_data={})
    
    # Handle Auth Required (OAuth flow)
    if res.get("error") == "auth_required":
        return {
            "type": "status", 
            "message": f"🔑 **{_pretty(source)}** requires authentication.\n\n[Connect via OAuth]({res.get('auth_url')})",
            "links": [res.get("auth_url")]
        }

    # Handle Missing Credentials
    if res.get("error") == "missing credentials":
        fields = res.get("required_fields", [])
        if not fields:
             # Last resort dynamic detection if API doesn't specify
             fields = ["access_token"] 
        return {
            "type": "clarification",
            "message": f"To connect **{_pretty(source)}**, I need the following credentials:\n\n" + 
                       "\n".join([f"• `{f}`" for f in fields]) + 
                       "\n\nPlease provide them as `key: value` or just the values in order.",
            "state": { "flow": "connect", "connector": source, "metadata": { "pending": fields, "provided": {} } }
        }
    
    if res.get("http_status") == 200:
        return {"type": "status", "message": f"✅ Successfully connected to **{_pretty(source)}**!"}
        
    return {"type": "error", "message": f"⚠️ Failed to connect to **{_pretty(source)}**: {res.get('error', 'Unknown Error')}"}

def _handle_disconnect(source: str, uid: str) -> Dict:
    res = call_connector_route(f"/connectors/{source}/disconnect", uid, method="POST")
    if res.get("http_status") == 200:
        return {"type": "status", "message": f"🔌 Successfully disconnected from **{_pretty(source)}**."}
    return {"type": "error", "message": f"⚠️ Failed to disconnect: {res.get('error', 'Unknown')}"}

# ──────────────────────────────────────────────────────────────────────────────
# 4. MAIN ENTRY POINT
# ──────────────────────────────────────────────────────────────────────────────

def orchestrate(intent: Dict, uid: str, chat_id: str, message: str, state: Optional[Dict]) -> Dict:
    action = intent.get("action", "unknown")
    connectors = intent.get("connectors", [])
    
    print(f"[ACTION] {action}", flush=True)

    # ── 1. Resume Check ───────────────────────────────────────
    if state and state.get("connector"):
        source = state["connector"]
        flow = state.get("flow", state.get("status")) # Backward compat
        
        if flow == "connect": return _handle_connect(source, uid, chat_id, message, state)
        if flow == "destination": return _handle_destination(source, uid, chat_id, message, state)
        if flow == "schedule": return _handle_schedule(source, uid, chat_id, message, state)

    # ── 2. New Commands ───────────────────────────────────────
    source = normalize_source(connectors[0]) if connectors else None

    if action == "connect" and source: return _handle_connect(source, uid, chat_id, message, None)
    if action == "disconnect" and source: return _handle_disconnect(source, uid)
    if action == "destination" and source: return _handle_destination(source, uid, chat_id, message, None)
    if action == "schedule" and source: return _handle_schedule(source, uid, chat_id, message, None)
    if action == "sync" and source: return _handle_sync(source, uid, chat_id)
    if action == "query": return _handle_query(uid, message)
    
    # Delegate to executor for stateless
    return execute_intent(intent, uid)
