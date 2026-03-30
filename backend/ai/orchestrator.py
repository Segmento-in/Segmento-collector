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
        "host": "Host",
        "port": "Port",
        "username": "Username",
        "password": "Password",
        "database": "Database Name",
    },
    "mysql": {
        "host": "Host",
        "port": "Port",
        "username": "Username",
        "password": "Password",
        "database": "Database Name",
    },
    "redshift": {
        "host": "Host",
        "port": "Port",
        "username": "Username",
        "password": "Password",
        "database": "Database Name",
    },
    "clickhouse": {
        "host": "Host",
        "port": "Port",
        "username": "Username",
        "password": "Password",
        "database": "Database Name",
    },
    "snowflake": {
        "host": "Account / Host",
        "port": "Port",
        "username": "Username",
        "password": "Password",
        "database": "Database Name",
    },
    "mongodb": {
        "host": "Cluster URI",
        "database": "Database Name",
        "username": "Username",
        "password": "Password",
    },
    "elasticsearch": {
        "host": "Endpoint URL",
        "port": "Port",
        "database": "Index Name",
    },
    "duckdb": {
        "host": "File Path",
    },
    "bigquery": {
        "host": "Project ID",
        "database": "Dataset ID",
        "password": "Service Account JSON Key",
    },
    "s3": {
        "host": "Bucket Name",
        "port": "Region",
        "username": "Access Key ID",
        "password": "Secret Access Key",
    },
    "azure_datalake": {
        "host": "Storage Account",
        "port": "File System",
        "username": "Base Path",
        "password": "Account Key",
    },
    "databricks": {
        "host": "Workspace Hostname",
        "port": "HTTP Path",
        "database": "Catalog.Schema",
        "password": "Personal Access Token",
    },
    "gcs": {
        "host": "Bucket Name",
        "port": "Region",
        "password": "Service Account JSON Key",
    },
}


def _get_label(dtype: str, field: str) -> str:
    label = DESTINATION_FIELD_LABELS.get(dtype, {}).get(field)
    if label:
        return label
    return field.replace("_", " ").title()


def _pretty(name: str) -> str:
    return name.replace("_", " ").replace("-", " ").title()


def _route_payload(res: Optional[Dict]) -> Dict:
    if not isinstance(res, dict):
        return {}
    data = res.get("data")
    return data if isinstance(data, dict) else {}


def _route_connected(res: Optional[Dict]) -> bool:
    payload = _route_payload(res)
    return bool((res or {}).get("connected") is True or payload.get("connected") is True)


def _route_auth_required(res: Optional[Dict]) -> bool:
    payload = _route_payload(res)
    return bool(payload.get("auth_required") is True)


def _route_redirect(res: Optional[Dict]) -> Optional[str]:
    payload = _route_payload(res)
    return payload.get("redirect") or payload.get("auth_url")


def _route_error(res: Optional[Dict]) -> str:
    payload = _route_payload(res)
    err = payload.get("error") or payload.get("message") or (res or {}).get("error") or "request failed"
    return str(err)


def _suggest_fix(reason: str) -> str:
    text = str(reason or "").strip().lower()
    if "missing credential" in text or "required" in text:
        return "Please provide credentials"
    if "auth_required" in text or "oauth" in text or "authentication" in text:
        return "Complete OAuth authentication"
    return "Please retry or reconnect"


def _response(
    rtype: str,
    message: str,
    connectors: Optional[List[str]] = None,
    links: Optional[List[str]] = None,
    data: Optional[Dict[str, Any]] = None,
    state: Optional[Dict[str, Any]] = None,
) -> Dict:
    return {
        "type": rtype,
        "message": message,
        "connectors": connectors or [],
        "links": links or [],
        "data": data,
        "state": state,
    }


def _error_response(action: str, source: Optional[str], reason: str, state: Optional[Dict[str, Any]] = None) -> Dict:
    target = _pretty(source) if source else "request"
    if action == "connect" and source:
        message = f"⚠️ Failed to connect to {target}\nReason: {reason}"
    else:
        message = f"⚠️ Failed to {action} {target}\nReason: {reason}"
    return _response(
        "error",
        message,
        connectors=[source] if source else [],
        data={"reason": reason, "fix": _suggest_fix(reason)},
        state=state,
    )


def _normalize_response(result: Optional[Dict]) -> Dict:
    result = result or {}
    normalized_type = result.get("type") or "message"
    if normalized_type not in {"message", "input_required", "redirect", "error"}:
        normalized_type = "message"

    return {
        "type": normalized_type,
        "message": result.get("message", ""),
        "connectors": result.get("connectors") or [],
        "links": result.get("links") or [],
        "data": result.get("data"),
        "state": result.get("state"),
    }


def _normalize_credential_key(raw_key: str, schema: Optional[Dict[str, str]] = None) -> str:
    text = str(raw_key or "").strip().lower().replace("_", " ").replace("-", " ")
    text = " ".join(text.split())
    if not text:
        return ""

    common_map = {
        "client id": "client_id",
        "clientid": "client_id",
        "client secret": "client_secret",
        "clientsecret": "client_secret",
        "api key": "api_key",
        "apikey": "api_key",
        "secret key": "secret_key",
        "secretkey": "secret_key",
        "access key": "access_key",
        "accesskey": "access_key",
        "access token": "access_token",
        "accesstoken": "access_token",
        "refresh token": "refresh_token",
        "refreshtoken": "refresh_token",
        "instance url": "instance_url",
        "base url": "base_url",
        "account id": "account_id",
        "tenant id": "tenant_id",
        "property id": "property_id",
        "seller id": "seller_id",
    }
    if text in common_map:
        return common_map[text]

    if schema:
        for field, label in schema.items():
            field_text = str(field).strip().lower().replace("_", " ").replace("-", " ")
            field_text = " ".join(field_text.split())
            label_text = str(label).strip().lower().replace("_", " ").replace("-", " ")
            label_text = " ".join(label_text.split())

            if text == field_text or text == label_text:
                return field
            if text.endswith(field_text):
                return field
            if label_text and text.endswith(label_text):
                return field

    return text.replace(" ", "_")


def _parse_connect_credentials(message: str, schema: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    message = (message or "").strip()
    if not message:
        return {}

    try:
        payload = json.loads(message)
        if isinstance(payload, dict):
            parsed = {}
            for k, v in payload.items():
                value = str(v).strip()
                if not value:
                    continue
                key = _normalize_credential_key(str(k), schema=schema)
                if key:
                    parsed[key] = value
            return parsed
    except Exception:
        pass

    found: Dict[str, str] = {}
    parts = [p.strip() for p in re.split(r"[\n,]", message) if p.strip()]
    for part in parts:
        if ":" not in part:
            continue
        key, value = part.split(":", 1)
        key = _normalize_credential_key(key, schema=schema)
        value = value.strip()
        if key and value:
            found[key] = value
    return found


def _connect_credentials_response(
    source: str,
    state: Dict[str, Any],
    schema: Optional[Dict[str, str]] = None,
    reason: str = "missing credentials",
    retry_hint: str = "",
) -> Dict:
    if not schema:
        return _response(
            "input_required",
            f"{_pretty(source)} requires credentials. Please provide required fields as `key: value` pairs.{retry_hint}",
            connectors=[source],
            data={"reason": reason, "fix": "Please provide credentials"},
            state=state,
        )

    labels = "\n".join([f"• {label}" for label in schema.values()])
    example = "\n".join([f"{field}: xxx" for field in schema.keys()])
    return _response(
        "input_required",
        f"{_pretty(source)} requires credentials.\n\nI need:\n{labels}\n\nYou can reply like:\n{example}{retry_hint}",
        connectors=[source],
        data={"reason": reason, "fix": "Please provide credentials"},
        state=state,
    )


def _parse_credentials(message: str, pending_fields: list, dtype: Optional[str] = None) -> dict:
    found = {}

    label_to_key = {}
    if dtype and dtype in DESTINATION_FIELD_LABELS:
        for k, v in DESTINATION_FIELD_LABELS[dtype].items():
            norm_label = v.lower().replace(" ", "").replace("-", "").replace("_", "")
            label_to_key[norm_label] = k
            label_to_key[k.lower()] = k

    if label_to_key:
        sorted_labels = sorted(DESTINATION_FIELD_LABELS[dtype].values(), key=len, reverse=True)
        candidates = sorted_labels + pending_fields
        labels_pattern = "|".join([re.escape(l) for l in candidates])
        matches = re.finditer(
            f"({labels_pattern}):\\s*(.*?)(?={labels_pattern}:|$)",
            message,
            re.IGNORECASE | re.DOTALL,
        )

        for match in matches:
            raw_label = match.group(1).lower().replace(" ", "").replace("-", "").replace("_", "")
            val = match.group(2).strip()
            if val.endswith(","):
                val = val[:-1].strip()

            key = label_to_key.get(raw_label) or raw_label
            if key in pending_fields:
                found[key] = val

    if not found:
        parts = [p.strip() for p in re.split(r"[,|\n]", message) if p.strip()]
        if len(parts) == len(pending_fields):
            for i, val in enumerate(parts):
                found[pending_fields[i]] = val
        elif len(pending_fields) == 1:
            found[pending_fields[0]] = message.strip()

    return found


def _parse_time_to_24h(message: str) -> Optional[str]:
    msg = message.upper().strip()

    match = re.search(r"(\d{1,2}):?(\d{2})?\s*([AP]M)", msg)
    if match:
        hh = int(match.group(1))
        mm = int(match.group(2) or 0)
        meridiem = match.group(3)
        if meridiem == "PM" and hh < 12:
            hh += 12
        if meridiem == "AM" and hh == 12:
            hh = 0
        return f"{hh:02}:{mm:02}"

    match = re.search(r"(\d{1,2})\s*([AP]M)", msg)
    if match:
        hh = int(match.group(1))
        meridiem = match.group(2)
        if meridiem == "PM" and hh < 12:
            hh += 12
        if meridiem == "AM" and hh == 12:
            hh = 0
        return f"{hh:02}:00"

    match = re.search(r"(\d{1,2}):(\d{2})", msg)
    if match:
        hh = int(match.group(1))
        mm = int(match.group(2))
        if 0 <= hh <= 23 and 0 <= mm <= 59:
            return f"{hh:02}:{mm:02}"

    return None


def _handle_destination(source: str, uid: str, chat_id: str, message: str, state: Optional[Dict]) -> Dict:
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
                    "state": {"flow": "destination", "step": "awaiting_type", "connector": source, "metadata": {}},
                }

            match = re.search(r"(\d+)", message)
            if match:
                dest_id = match.group(1)
                con = get_db()
                con.execute("UPDATE destination_configs SET is_active=0 WHERE uid=? AND source=?", (uid, source))
                cur = con.execute(
                    "UPDATE destination_configs SET is_active=1 WHERE uid=? AND source=? AND id=?",
                    (uid, source, dest_id),
                )
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

            backend_keys = list(DESTINATION_FIELD_LABELS[dtype].keys())
            labels_list = [f"• {_get_label(dtype, k)}" for k in backend_keys]
            examples = "\n".join([f"{_get_label(dtype, k).lower()}: xxx" for k in backend_keys])

            return {
                "type": "clarification",
                "message": f"Configuring **{dtype.title()}** for **{_pretty(source)}**.\n\nI need:\n" + "\n".join(labels_list) + f"\n\nYou can reply like:\n{examples}",
                "state": {"flow": "destination", "step": "awaiting_fields", "connector": source, "metadata": {"type": dtype, "pending": backend_keys, "provided": {}}},
            }

        if step == "awaiting_fields":
            pending = metadata.get("pending", [])
            provided = metadata.get("provided", {})
            provided.update(_parse_credentials(message, pending, dtype))

            missing = [k for k in pending if k not in provided]
            if missing:
                missing_labels = [f"• {_get_label(dtype, k)}" for k in missing]
                return {"type": "clarification", "message": "I still need:\n" + "\n".join(missing_labels), "state": state}

            if dtype in ("s3", "bigquery", "azure_datalake", "gcs", "databricks", "snowflake") and "format" not in provided:
                return {
                    "type": "clarification",
                    "message": "Select data format:\n• `parquet`\n• `json`\n• `iceberg`\n• `hudi`",
                    "state": {"flow": "destination", "step": "awaiting_format", "connector": source, "metadata": {**metadata, "provided": provided}},
                }

            payload = {"source": source, "type": dtype, **provided}
            print(f"[DEST PAYLOAD] {payload}", flush=True)
            res = call_connector_route("/destination/save", uid, method="POST", json_data=payload)
            if res.get("ok"):
                return {"type": "status", "message": f"🚀 Successfully created and activated your new **{dtype.title()}** destination!", "state": None}
            err = _route_error(res)
            return {"type": "error", "message": f"⚠️ Failed to save destination: {err}", "state": None}

        if step == "awaiting_format":
            fmt = message.strip().lower()
            if fmt not in ("parquet", "json", "iceberg", "hudi"):
                return {"type": "clarification", "message": "⚠️ Please select a valid format: `parquet`, `json`, `iceberg`, or `hudi`.", "state": state}

            metadata["provided"]["format"] = fmt
            payload = {"source": source, "type": dtype, **metadata["provided"]}
            print(f"[DEST PAYLOAD] {payload}", flush=True)
            res = call_connector_route("/destination/save", uid, method="POST", json_data=payload)
            if res.get("ok"):
                return {"type": "status", "message": f"🚀 Successfully created and activated your new **{dtype.title()}** destination!", "state": None}
            err = _route_error(res)
            return {"type": "error", "message": f"⚠️ Failed to save destination: {err}", "state": None}

    con = get_db()
    cur = con.execute("SELECT id, dest_type, host, is_active FROM destination_configs WHERE uid=? AND source=?", (uid, source))
    dests = cur.fetchall()
    con.close()

    if dests:
        list_str = "\n".join([f"• **#{d['id']}** ({d['dest_type']}) {'[ACTIVE]' if d['is_active'] else ''}" for d in dests])
        return {
            "type": "clarification",
            "message": f"I found existing destinations for **{_pretty(source)}**:\n\n{list_str}\n\nWould you like to **use an existing ID** or **create new**?",
            "state": {"flow": "destination", "step": "awaiting_choice", "connector": source, "metadata": {}},
        }

    choices = ", ".join(DESTINATION_FIELD_LABELS.keys())
    return {
        "type": "clarification",
        "message": f"No destinations found for **{_pretty(source)}**. Which type would you like to create?\n\nChoices: {choices}",
        "state": {"flow": "destination", "step": "awaiting_type", "connector": source, "metadata": {}},
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
        "state": {"flow": "schedule", "step": "awaiting_time", "connector": source},
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
        con.close()
        return {"type": "status", "message": "🗺️ Entity Relationship Diagrams are available on connector pages. Which one are you interested in?"}

    con.close()
    return execute_intent({"action": "help"}, uid)


def _handle_sync(source: str, uid: str, chat_id: str) -> Dict:
    stat = call_connector_route(f"/api/status/{source}", uid)
    if not _route_connected(stat):
        return _error_response("sync", source, f"{_pretty(source)} is not connected. Please connect it first.")

    con = get_db()
    active = con.execute("SELECT id FROM destination_configs WHERE uid=? AND source=? AND is_active=1", (uid, source)).fetchone()
    con.close()
    if not active:
        return _handle_destination(source, uid, chat_id, "", None)

    res = call_connector_route(f"/connectors/{source}/sync", uid)
    if res.get("http_status") == 200:
        return _response("message", f"🚀 Sync started for **{_pretty(source)}**!", connectors=[source])
    return _error_response("sync", source, _route_error(res))


def _handle_connect(source: str, uid: str, chat_id: str, message: str, state: Optional[Dict]) -> Dict:
    connect_state = {
        "flow": "connect",
        "source": source,
        "connector": source,
        "awaiting": "credentials",
    }

    if state and state.get("flow") == "connect":
        if state.get("post_oauth"):
            stat = call_connector_route(f"/api/status/{source}", uid)
            if _route_connected(stat):
                return _response(
                    "message",
                    f"✅ Successfully connected to **{_pretty(source)}**!",
                    connectors=[source],
                    state=None,
                )
            return _response(
                "message",
                f"⏳ Finishing authentication for {_pretty(source)}... please try again in a moment.",
                connectors=[source],
                state=state,
            )

        metadata = state.get("metadata", {})
        provided = metadata.get("provided", {})
        schema = metadata.get("schema") or {}
        attempts = metadata.get("attempts", 0) + 1
        parsed_credentials = _parse_connect_credentials(message, schema=schema)
        print("[PARSED CREDENTIALS]", parsed_credentials, flush=True)
        provided.update(parsed_credentials)
        retry_hint = " Make sure your credentials are correct (no extra spaces or invalid keys)." if attempts == 2 else ""

        if not parsed_credentials:
            return _response(
                "error",
                "⚠️ Could not understand credentials format",
                connectors=[source],
                data={"fix": "Please provide credentials as key: value (e.g., client_id: xxx)"},
                state={**connect_state, "metadata": {"provided": provided, "attempts": attempts, "schema": schema}},
            )

        required_fields = list(schema.keys()) if schema else []
        missing = [k for k in required_fields if k not in provided]
        if missing:
            missing_labels = [schema.get(k, k) for k in missing]
            return _response(
                "input_required",
                f"I still need:\n" + "\n".join([f"• {label}" for label in missing_labels]),
                connectors=[source],
                state={
                    **connect_state,
                    "metadata": {
                        "provided": provided,
                        "attempts": attempts,
                        "schema": schema,
                    },
                },
            )

        if attempts > 2:
            return _error_response(
                "connect",
                source,
                "Invalid credentials provided multiple times",
                state=None,
            )

        print("[FINAL CREDENTIAL PAYLOAD]", provided, flush=True)
        save_res = call_connector_route(f"/connectors/{source}/save_app", uid, method="POST", json_data=provided)
        if not save_res.get("ok"):
            reason = _route_error(save_res)
            if "missing credential" in reason.lower() or "required" in reason.lower():
                return _connect_credentials_response(
                    source,
                    {**connect_state, "metadata": {"provided": provided, "attempts": attempts, "schema": schema}},
                    schema=schema,
                    reason=reason,
                    retry_hint=retry_hint,
                )
            return _error_response("connect", source, reason, state={**connect_state, "metadata": {"provided": provided, "attempts": attempts, "schema": schema}})

        retry_res = call_connector_route(f"/connectors/{source}/connect", uid)
        print("[CONNECT RESPONSE]", retry_res, flush=True)
        print("[CONNECT PAYLOAD]", _route_payload(retry_res), flush=True)
        if _route_connected(retry_res):
            return _response("message", f"✅ Successfully connected to **{_pretty(source)}**!", connectors=[source], state=None)
        if _route_auth_required(retry_res):
            redirect_url = _route_redirect(retry_res)
            return _response(
                "redirect",
                f"Please complete authentication for {_pretty(source)}",
                connectors=[source],
                links=[redirect_url] if redirect_url else [],
                data={"reason": "auth_required", "fix": "Complete OAuth authentication"},
                state={"flow": "connect", "source": source, "post_oauth": True},
            )

        reason = _route_error(retry_res)
        if reason.lower() == "missing credentials":
            return _connect_credentials_response(
                source,
                {**connect_state, "metadata": {"provided": provided, "attempts": attempts, "schema": schema}},
                schema=schema,
                reason=reason,
                retry_hint=retry_hint,
            )
        return _error_response("connect", source, reason)

    stat = call_connector_route(f"/api/status/{source}", uid)
    if _route_connected(stat):
        return _response(
            "message",
            f"✅ **{_pretty(source)}** is already connected. You can now run a sync or schedule a job!",
            connectors=[source],
        )

    res = call_connector_route(f"/connectors/{source}/connect", uid)
    print("[CONNECT RESPONSE]", res, flush=True)
    print("[CONNECT PAYLOAD]", _route_payload(res), flush=True)
    if _route_connected(res):
        return _response("message", f"✅ Successfully connected to **{_pretty(source)}**!", connectors=[source])

    if _route_auth_required(res):
        redirect_url = _route_redirect(res)
        return _response(
            "redirect",
            f"Please complete authentication for {_pretty(source)}",
            connectors=[source],
            links=[redirect_url] if redirect_url else [],
            data={"reason": "auth_required", "fix": "Complete OAuth authentication"},
            state={"flow": "connect", "source": source, "post_oauth": True},
        )

    reason = _route_error(res)
    if reason.lower() == "missing credentials":
        schema = _route_payload(res).get("required_fields") or {}
        return _connect_credentials_response(
            source,
            {**connect_state, "metadata": {"provided": {}, "schema": schema}},
            schema=schema,
            reason=reason,
        )

    return _error_response("connect", source, reason)


def _handle_disconnect(source: str, uid: str) -> Dict:
    res = call_connector_route(f"/connectors/{source}/disconnect", uid, method="POST")
    if res.get("http_status") == 200:
        return _response("message", f"🔌 Successfully disconnected from **{_pretty(source)}**.", connectors=[source])
    return _error_response("disconnect", source, _route_error(res))


def orchestrate(intent: Dict, uid: str, chat_id: str, message: str, state: Optional[Dict]) -> Dict:
    action = intent.get("action", "unknown")
    connectors = intent.get("connectors", [])

    print(f"[ACTION] {action}", flush=True)

    if state and state.get("flow") and action not in (None, "unknown") and action != state.get("flow"):
        state = None

    if state and (state.get("source") or state.get("connector")):
        source = state.get("source") or state.get("connector")
        flow = state.get("flow", state.get("status"))

        if flow == "connect":
            return _normalize_response(_handle_connect(source, uid, chat_id, message, state))
        if flow == "destination":
            return _normalize_response(_handle_destination(source, uid, chat_id, message, state))
        if flow == "schedule":
            return _normalize_response(_handle_schedule(source, uid, chat_id, message, state))

    source = normalize_source(connectors[0]) if connectors else None

    if action == "connect" and source:
        return _normalize_response(_handle_connect(source, uid, chat_id, message, None))
    if action == "disconnect" and source:
        return _normalize_response(_handle_disconnect(source, uid))
    if action == "destination" and source:
        return _normalize_response(_handle_destination(source, uid, chat_id, message, None))
    if action == "schedule" and source:
        return _normalize_response(_handle_schedule(source, uid, chat_id, message, None))
    if action == "sync" and source:
        return _normalize_response(_handle_sync(source, uid, chat_id))
    if action == "sync" and not source:
        return _normalize_response(
            _response(
                "message",
                "Which connector would you like to sync?\n\nExample:\n• sync airtable\n• run sync for gmail",
            )
        )
    if action == "query":
        return _normalize_response(_handle_query(uid, message))

    return _normalize_response(execute_intent(intent, uid))
