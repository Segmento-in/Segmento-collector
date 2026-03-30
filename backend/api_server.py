import os
import logging
import importlib
import inspect
import re

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger('werkzeug')
log.setLevel(logging.DEBUG)

os.environ["OAUTHLIB_RELAX_TOKEN_SCOPE"] = "1"

import time
from flask import send_from_directory
from flask import Flask,request,redirect,make_response,jsonify,render_template_string
import sqlite3,uuid,datetime,os,json
import secrets
from flask_cors import CORS
import zoneinfo
from user_agents import parse
import pandas as pd
from tika import parser
import xmltodict
import requests
import datetime
import sqlite3
from urllib.parse import urlencode
from backend.destinations.destination_router import push_to_destination
# Google OAuth
from dotenv import load_dotenv
from google_auth_oauthlib.flow import Flow

# Scheduler
from backend.scheduler.scheduler import start_scheduler

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REAL_DB = os.path.join(PROJECT_ROOT, "identity.db")

_original_sqlite_connect = sqlite3.connect

def fixed_connect(db_path, *args, **kwargs):

    if isinstance(db_path, str) and db_path.endswith("identity.db"):
        db_path = REAL_DB

    return _original_sqlite_connect(db_path, *args, **kwargs)

sqlite3.connect = fixed_connect

# AI
from backend.ai.intent_engine import detect_intent
from backend.ai.llm_engine import call_llm
from backend.ai.executor import execute_intent
from backend.ai.orchestrator import orchestrate

#credentials security
from backend.security.secure_db import encrypt_payload
from backend.security.secure_db import decrypt_payload
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import auto_decrypt_row
from backend.security.secure_fetch import (
    fetchone_secure,
    fetchall_secure
)
from backend.security.auth_routes import auth
from backend.security.auth_middleware import load_logged_user


def resolve_intent(message: str) -> dict:
    try:
        llm_intent = call_llm(message)

        if llm_intent and llm_intent.get("action") not in (None, "unknown"):
            connector = llm_intent.get("connector")

            # Normalize any LLM-suggested connector through the registry first.
            from backend.ai.registry import ALIAS_INDEX

            canonical = None
            if connector:
                canonical = ALIAS_INDEX.get(str(connector).lower())

            return {
                "action": llm_intent.get("action"),
                "connectors": [canonical] if canonical else [],
                "raw": message,
            }
    except Exception as e:
        print(f"[LLM] Failed: {e}", flush=True)

    return detect_intent(message)

# Connectors
from backend.connectors.pinterest import (
    pinterest_get_auth_url,
    pinterest_exchange_code,
    pinterest_save_token,
    sync_pinterest
)
from backend.connectors.nvd import sync_nvd
from backend.connectors.openstreetmap import sync_openstreetmap
from backend.connectors.lemmy import sync_lemmy
from backend.connectors.discourse import sync_discourse
from backend.connectors.mastodon import sync_mastodon
from backend.connectors.peertube import sync_peertube
from backend.connectors.wikipedia import sync_wikipedia
from backend.connectors.producthunt import sync_producthunt
from backend.connectors.hackernews import sync_hackernews
from backend.connectors.google_youtube import sync_youtube
from backend.connectors.google_webfonts import sync_webfonts
from backend.connectors.google_gcs import sync_gcs
from backend.connectors.google_contacts import sync_contacts
from backend.connectors.google_tasks import sync_tasks
from backend.connectors.classroom import sync_classroom
from backend.connectors.google_gmail import sync_gmail
from backend.connectors.google_calendar import sync_calendar_files
from backend.connectors.google_pagespeed import sync_pagespeed
from backend.connectors.google_search_console import sync_search_console
from backend.connectors.google_drive import sync_drive_files
from backend.connectors.google_forms import sync_forms
from backend.connectors.google_sheets import sync_sheets_files
from backend.connectors.google_ga4 import sync_ga4
from backend.connectors.facebook_pages import sync_facebook_pages
from backend.connectors.tiktok import (
    get_tiktok_auth_url,
    handle_tiktok_oauth_callback,
    sync_tiktok,
    disconnect_tiktok
)
from backend.connectors.taboola import (
    connect_taboola,
    sync_taboola,
    disconnect_taboola,
)
from backend.connectors.outbrain import (
    connect_outbrain,
    sync_outbrain,
    disconnect_outbrain,
)
from backend.connectors.similarweb import (
    connect_similarweb,
    sync_similarweb,
    disconnect_similarweb,
)
from backend.connectors.bigquery import (
    connect_bigquery,
    sync_bigquery,
    disconnect_bigquery,
)
from backend.connectors.x import (
    get_x_auth_url,
    handle_x_oauth_callback,
    sync_x,
    disconnect_x
)
from backend.connectors.linkedin import (
    get_linkedin_auth_url,
    handle_linkedin_oauth_callback,
    sync_linkedin,
    disconnect_linkedin,
)
from backend.connectors.chartbeat import (
       connect_chartbeat,
       sync_chartbeat,
       disconnect_chartbeat,
)
from backend.connectors.stripe import (
    save_credentials as save_stripe_credentials,
    connect_stripe,
    sync_stripe,
    disconnect_stripe,
)
from backend.connectors.socialinsider import (
    connect_socialinsider,
    sync_socialinsider,
    disconnect_socialinsider,
)
from backend.connectors.aws_rds import (
    connect_rds,
    sync_rds,
    disconnect_rds,
    save_config as save_rds_config,
)
from backend.connectors.dynamodb import (
    connect_dynamodb,
    sync_dynamodb,
    disconnect_dynamodb,
    save_config as save_dynamodb_config,
)
from backend.connectors.slack import (
    connect_slack,
    sync_slack,
    disconnect_slack,
    save_config as save_slack_config,
)

from backend.connectors.looker import connect_looker, sync_looker, disconnect_looker, save_config as save_looker_config
from backend.connectors.superset import connect_superset, sync_superset, disconnect_superset, save_config as save_superset_config
from backend.connectors.azure_blob import connect_azure_blob, sync_azure_blob, disconnect_azure_blob, save_config as save_azure_blob_config
from backend.connectors.datadog import connect_datadog, sync_datadog, disconnect_datadog, save_config as save_datadog_config

from backend.connectors.notion import (
    connect_notion,
    sync_notion,
    disconnect_notion,
    save_config as save_notion_config,
)
from backend.connectors.hubspot import (
    connect_hubspot,
    sync_hubspot,
    disconnect_hubspot,
    save_config as save_hubspot_config,
)
from backend.connectors.airtable import (
    connect_airtable,
    sync_airtable,
    disconnect_airtable,
    save_config as save_airtable_config,
)
from backend.connectors.shopify import (
    connect_shopify,
    sync_shopify,
    disconnect_shopify,
    save_config as save_shopify_config,
)
from backend.connectors.zendesk import (
    connect_zendesk,
    sync_zendesk,
    disconnect_zendesk,
    save_config as save_zendesk_config,
)
from backend.connectors.intercom import (
    connect_intercom,
    sync_intercom,
    disconnect_intercom,
    save_config as save_intercom_config,
)
from backend.connectors.mailchimp import (
    connect_mailchimp,
    sync_mailchimp,
    disconnect_mailchimp,
    save_config as save_mailchimp_config,
)
from backend.connectors.twilio import (
    connect_twilio,
    sync_twilio,
    disconnect_twilio,
    save_config as save_twilio_config,
)
from backend.connectors.pipedrive import (
    connect_pipedrive,
    sync_pipedrive,
    disconnect_pipedrive,
    save_config as save_pipedrive_config,
)
from backend.connectors.freshdesk import (
    connect_freshdesk,
    sync_freshdesk,
    disconnect_freshdesk,
    save_config as save_freshdesk_config,
)
from backend.connectors.klaviyo import (
    connect_klaviyo,
    sync_klaviyo,
    disconnect_klaviyo,
    save_config as save_klaviyo_config,
)
from backend.connectors.amplitude import (
    connect_amplitude,
    sync_amplitude,
    disconnect_amplitude,
    save_config as save_amplitude_config,
)
from backend.connectors.salesforce import (
    save_config as save_salesforce_config,
    connect_salesforce,
    sync_salesforce,
    disconnect_salesforce,
)
from backend.connectors.jira import (
    save_config as save_jira_config,
    connect_jira,
    sync_jira,
    disconnect_jira,
)
from backend.connectors.zoho_crm import (
    save_config as save_zoho_crm_config,
    connect_zoho_crm,
    sync_zoho_crm,
    disconnect_zoho_crm,
)
from backend.connectors.paypal import (
    save_config as save_paypal_config,
    connect_paypal,
    sync_paypal,
    disconnect_paypal,
)
from backend.connectors.asana import (
    save_config as save_asana_config,
    connect_asana,
    sync_asana,
    disconnect_asana,
)
from backend.connectors.sendgrid import (
    save_config as save_sendgrid_config,
    connect_sendgrid,
    sync_sendgrid,
    disconnect_sendgrid,
)
from backend.connectors.tableau import (
    save_config as save_tableau_config,
    connect_tableau,
    sync_tableau,
    disconnect_tableau,
)
from backend.connectors.power_bi import (
    save_config as save_power_bi_config,
    connect_power_bi,
    sync_power_bi,
    disconnect_power_bi,
)
from backend.connectors.workday import (
    save_config as save_workday_config,
    connect_workday,
    sync_workday,
    disconnect_workday,
)
from backend.connectors.ebay import (
    save_config as save_ebay_config,
    connect_ebay,
    sync_ebay,
    disconnect_ebay,
)
from backend.connectors.mixpanel import (
    save_config as save_mixpanel_config,
    connect_mixpanel,
    sync_mixpanel,
    disconnect_mixpanel,
)
from backend.connectors.monday import (
    save_config as save_monday_config,
    connect_monday,
    sync_monday,
    disconnect_monday,
) 
from backend.connectors.clickup import (
    save_config as save_clickup_config,
    connect_clickup,
    sync_clickup,
    disconnect_clickup,
)
from backend.connectors.helpscout import (
    save_config as save_helpscout_config,
    connect_helpscout,
    sync_helpscout,
    disconnect_helpscout,
)
from backend.connectors.okta import (
    save_config as save_okta_config,
    connect_okta,
    sync_okta,
    disconnect_okta,
)
from backend.connectors.auth0 import (
    save_config as save_auth0_config,
    connect_auth0,
    sync_auth0,
    disconnect_auth0,
)
from backend.connectors.cloudflare import (
    save_config as save_cloudflare_config,
    connect_cloudflare,
    sync_cloudflare,
    disconnect_cloudflare,
)
from backend.connectors.sentry import (
    save_config as save_sentry_config,
    connect_sentry,
    sync_sentry,
    disconnect_sentry,
)
from backend.connectors.openai import (
    save_config as save_openai_config,
    connect_openai,
    sync_openai,
    disconnect_openai,
)
 
from backend.connectors.huggingface import (
    save_config as save_huggingface_config,
    connect_huggingface,
    sync_huggingface,
    disconnect_huggingface,
)
 
from backend.connectors.airflow import (
    save_config as save_airflow_config,
    connect_airflow,
    sync_airflow,
    disconnect_airflow,
)
 
from backend.connectors.kafka import (
    save_config as save_kafka_config,
    connect_kafka,
    sync_kafka,
    disconnect_kafka,
)
from backend.connectors.dbt import (
    connect_dbt,
    sync_dbt,
    disconnect_dbt,
    save_config as save_dbt_config,
)
from backend.connectors.typeform import (
    connect_typeform,
    sync_typeform,
    disconnect_typeform,
    save_config as save_typeform_config,
)
from backend.connectors.surveymonkey import (
    connect_surveymonkey,
    sync_surveymonkey,
    disconnect_surveymonkey,
    save_config as save_surveymonkey_config,
)
from backend.connectors.pinecone import (
    connect_pinecone,
    sync_pinecone,
    disconnect_pinecone,
    save_config as save_pinecone_config,
)
from backend.connectors.bitbucket import (
    connect_bitbucket,
    sync_bitbucket,
    disconnect_bitbucket,
    save_config as save_bitbucket_config,
)

from backend.connectors.vercel import (
    connect_vercel,
    sync_vercel,
    disconnect_vercel,
    save_config as save_vercel_config,
)

from backend.connectors.netlify import (
    connect_netlify,
    sync_netlify,
    disconnect_netlify,
    save_config as save_netlify_config,
)

from backend.connectors.linear import (
    connect_linear,
    sync_linear,
    disconnect_linear,
    save_config as save_linear_config,
)

from backend.connectors import quickbooks
from backend.connectors import xero
from backend.connectors import amazon_seller
from backend.connectors import newrelic

# ---------------- CONFIG ----------------
load_dotenv()

IST = zoneinfo.ZoneInfo("Asia/Kolkata")

app = Flask(__name__)
CORS(
    app,
    supports_credentials=True,
    origins=["http://localhost:3000", "http://127.0.0.1:3000"]
)

# Start scheduler once when API server starts
start_scheduler()

from flask import g

@app.before_request
def load_logged_user():

    g.user_id = None

    # Scheduler / internal UID
    internal_uid = request.headers.get("X-Internal-UID")
    if internal_uid:
        g.user_id = internal_uid
        return

    # Normal logged-in user via session
    session_id = request.cookies.get("segmento_session")
    if not session_id:
        return

    con = sqlite3.connect("identity.db")
    cur = con.cursor()

    cur.execute("""
        SELECT user_id
        FROM user_sessions
        WHERE session_id=?
    """, (session_id,))

    row = cur.fetchone()
    con.close()

    if row:
        g.user_id = row[0]

# ---------------- USAGE AUTO SYNC DETECTOR ----------------
@app.before_request
def usage_sync_start():

    path = request.path

    if "/sync/" not in path:
        return

    uid = get_uid()
    if not uid:
        return

    try:
        source = path.split("/sync/")[-1]

        mode = request.args.get("mode", "manual")

        if mode not in ["manual", "scheduled"]:
            mode = "manual"

        g.sync_run_id = log_sync_start(
            uid,
            source,
            mode
        )

        print(f"[USAGE] Sync START → {source} ({mode})", flush=True)

    except Exception as e:
        print("[USAGE START ERROR]", e, flush=True)

# ---------------- API USAGE LOGGER ----------------

@app.before_request
def log_api_usage():

    path = request.path

    # ignore static + health endpoints
    if (
        path.startswith("/static")
        or path.startswith("/__")
        or "favicon" in path
    ):
        return

    uid = getattr(g, "user_id", None)

    if not uid:
        return

    try:

        con = get_db()
        cur = con.cursor()

        cur.execute("""
            INSERT INTO api_usage_logs
            (uid, endpoint, method, created_at)
            VALUES (?, ?, ?, ?)
        """, (
            uid,
            path,
            request.method,
            datetime.datetime.now(
                datetime.UTC
            ).isoformat()
        ))

        con.commit()
        con.close()

    except Exception as e:
        print("[API LOG ERROR]", e, flush=True)

@app.after_request
def usage_sync_finish(response):

    if not hasattr(g, "sync_run_id"):
        return response

    try:

        status = "success" if response.status_code == 200 else "failed"

        log_sync_finish(
            g.sync_run_id,
            0,
            status
        )

        print("[USAGE] Sync FINISHED", flush=True)

    except Exception as e:
        print("[USAGE FINISH ERROR]", e, flush=True)

    return response

@app.route("/__ping")
def ping():
    return "IDENTITY OK"

@auth.route("/auth/me")
def me():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "unauthorized"}), 401

    return jsonify({
        "user_id": uid
    })

app.register_blueprint(auth)

UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# ✓ CHANGED: Use absolute path to root database, not relative "identity.db"
DB = REAL_DB


# ---------------- GOOGLE CONFIG ----------------

GOOGLE_SCOPES = [

    # Contacts
    "https://www.googleapis.com/auth/contacts.readonly",

    # Cloud Storage
    "https://www.googleapis.com/auth/devstorage.read_only",

    # Tasks
    "https://www.googleapis.com/auth/tasks.readonly",

    # Classroom
    "https://www.googleapis.com/auth/classroom.courses.readonly",
    "https://www.googleapis.com/auth/classroom.rosters.readonly",
    "https://www.googleapis.com/auth/classroom.coursework.students.readonly",
    "https://www.googleapis.com/auth/classroom.student-submissions.students.readonly",
    "https://www.googleapis.com/auth/classroom.announcements.readonly",

    # Youtube
    "https://www.googleapis.com/auth/youtube.readonly",

    # Gmail
    "https://www.googleapis.com/auth/gmail.readonly",

    # Drive & Sheets
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets.readonly",

    # Search Console
    "https://www.googleapis.com/auth/webmasters.readonly",

    # Analytics
    "https://www.googleapis.com/auth/analytics.readonly",

    # Forms
    "https://www.googleapis.com/auth/forms.responses.readonly",
    "https://www.googleapis.com/auth/forms.body.readonly",

    # Calendar
    "https://www.googleapis.com/auth/calendar.readonly"
]

GOOGLE_CLIENT_CONFIG = {
    "web": {
        "client_id": os.getenv("GOOGLE_CLIENT_ID"),
        "client_secret": os.getenv("GOOGLE_CLIENT_SECRET"),
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "redirect_uris": [os.getenv("GOOGLE_REDIRECT_URI")]
    }
}

#-------------Helper Fucntion---------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "identity.db")
def get_db():

    con = sqlite3.connect(
        DB,
        timeout=60,
        isolation_level=None,   # autocommit
        check_same_thread=False
    )

    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    con.execute("PRAGMA synchronous=NORMAL;")

    return con

from flask import request, g


def get_uid():

    # Priority 1 — Logged web user
    if getattr(g, "user_id", None):
        return g.user_id

    # Priority 2 — Scheduler / internal calls
    internal_uid = request.headers.get("X-Internal-UID")
    if internal_uid:
        return internal_uid

    # Priority 3 — legacy cookie fallback
    uid_cookie = request.cookies.get("uid")
    if uid_cookie:
        return uid_cookie

    return None

# CONNECTOR INITIALIZATION
def ensure_connector_initialized(uid, source):

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR IGNORE INTO google_connections
        (uid, source, enabled)
        VALUES (?, ?, 0)
    """, (uid, source))

    con.commit()
    con.close()


GOOGLE_OAUTH_SOURCES = {
    "gmail",
    "drive",
    "calendar",
    "sheets",
    "forms",
    "classroom",
    "contacts",
    "tasks",
    "ga4",
    "search-console",
    "youtube",
}

OAUTH_SOURCES = GOOGLE_OAUTH_SOURCES | {
    "github",
    "instagram",
    "tiktok",
    "x",
    "linkedin",
}

CONNECT_MODULE_OVERRIDES = {
    "search-console": "google_search_console",
    "books": "googlebooks",
    "factcheck": "googlefactcheck",
    "news": "googlenews",
    "webfonts": "google_webfonts",
}

CONNECT_FUNCTION_OVERRIDES = {
    "aws_rds": "connect_rds",
}

OAUTH_REDIRECTS = {
    "github": "/github/connect",
    "instagram": "/instagram/connect",
    "tiktok": "/connectors/tiktok/connect",
    "x": "/connectors/x/connect",
    "linkedin": "/connectors/linkedin/connect",
}


def _is_internal_ai_request():
    return bool(request.headers.get("X-Internal-UID"))


def _has_connector_config(uid, source):
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT 1
        FROM connector_configs
        WHERE uid=? AND connector=?
        LIMIT 1
        """,
        (uid, source),
    )
    row = fetchone_secure(cur)
    con.close()
    return bool(row)


def _is_connector_enabled(uid, source):
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source=?
        LIMIT 1
        """,
        (uid, source),
    )
    row = fetchone_secure(cur)
    con.close()
    return bool(row and row.get("enabled") == 1)


def _oauth_redirect_for(source):
    if source in GOOGLE_OAUTH_SOURCES:
        return f"/google/connect?source={source}"
    return OAUTH_REDIRECTS.get(source)


def _has_google_oauth_completion(uid, source):
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT 1
        FROM google_accounts
        WHERE uid=? AND source=?
        LIMIT 1
        """,
        (uid, source),
    )
    row = fetchone_secure(cur)
    con.close()
    return bool(row)


def _has_oauth_completion(uid, source):
    con = get_db()
    cur = con.cursor()

    try:
        if source in GOOGLE_OAUTH_SOURCES:
            cur.execute(
                """
                SELECT access_token
                FROM google_accounts
                WHERE uid=? AND source=?
                LIMIT 1
                """,
                (uid, source),
            )
        elif source == "github":
            cur.execute("SELECT access_token FROM github_tokens WHERE uid=? LIMIT 1", (uid,))
        elif source == "instagram":
            cur.execute(
                """
                SELECT access_token
                FROM instagram_connections
                WHERE uid=?
                LIMIT 1
                """,
                (uid,),
            )
        elif source == "tiktok":
            cur.execute(
                """
                SELECT access_token
                FROM tiktok_connections
                WHERE uid=?
                LIMIT 1
                """,
                (uid,),
            )
        elif source == "x":
            cur.execute(
                """
                SELECT access_token
                FROM x_connections
                WHERE uid=?
                LIMIT 1
                """,
                (uid,),
            )
        elif source == "linkedin":
            cur.execute(
                """
                SELECT access_token
                FROM linkedin_connections
                WHERE uid=?
                LIMIT 1
                """,
                (uid,),
            )
        else:
            return False

        row = fetchone_secure(cur)
        if not row:
            return False
        value = next(iter(row.values())) if isinstance(row, dict) else row[0]
        return bool(value and str(value).strip())
    finally:
        con.close()


def _load_connector_module(source):
    module_name = CONNECT_MODULE_OVERRIDES.get(source, source.replace("-", "_"))
    return importlib.import_module(f"backend.connectors.{module_name}")


def _get_connector_connect_callable(source):
    module = _load_connector_module(source)
    function_name = CONNECT_FUNCTION_OVERRIDES.get(
        source,
        f"connect_{source.replace('-', '_')}",
    )
    return getattr(module, function_name, None)


def _run_connector_connect_probe(uid, source):
    try:
        connect_fn = _get_connector_connect_callable(source)
    except Exception:
        return None

    if not callable(connect_fn):
        return None

    try:
        return connect_fn(uid)
    except TypeError:
        return None
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


def _normalize_probe_error(result):
    if not isinstance(result, dict):
        return "connection failed"

    error = result.get("error") or result.get("message") or "connection failed"
    error_text = str(error).strip().lower()
    if "missing" in error_text and "credential" in error_text:
        return "missing credentials"
    if "not configured" in error_text:
        return "missing credentials"
    return str(error)


def _credential_template_path(source):
    try:
        from backend.ai.registry import get_connector_url
        template_name = get_connector_url(source).rstrip("/").split("/")[-1]
    except Exception:
        template_name = source.replace("-", "_")

    connectors_dir = os.path.join(PROJECT_ROOT, "frontend", "templates", "connectors")
    candidates = [
        template_name,
        source,
        source.replace("-", "_"),
        source.replace("_", ""),
        source.replace("-", ""),
    ]

    seen = set()
    for candidate in candidates:
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        path = os.path.join(connectors_dir, f"{candidate}.html")
        if os.path.exists(path):
            return path
    return None


def _to_snake_case(name):
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", str(name))
    return text.replace("-", "_").strip().lower()


def _extract_template_field_labels(source):
    path = _credential_template_path(source)
    if not path:
        return {}, []

    try:
        html = open(path, "r", encoding="utf-8").read()
    except Exception:
        return {}, []

    pattern = re.compile(
        r"<label[^>]*>(.*?)</label>\s*<(?:input|select|textarea)[^>]*id=\"([^\"]+)\"",
        re.IGNORECASE | re.DOTALL,
    )

    by_key = {}
    ordered = []
    for raw_label, raw_id in pattern.findall(html):
        label = re.sub(r"<[^>]+>", "", raw_label)
        label = " ".join(label.split()).strip()
        field_key = _to_snake_case(raw_id)
        if not label or not field_key:
            continue
        if field_key not in by_key:
            by_key[field_key] = label
            ordered.append(label)

    return by_key, ordered


def get_required_fields(source):
    route_path = f"/connectors/{source}/save_app"
    endpoint = None
    for rule in app.url_map.iter_rules():
        if rule.rule == route_path:
            endpoint = rule.endpoint
            break

    if not endpoint:
        return {}

    view_fn = app.view_functions.get(endpoint)
    if not view_fn:
        return {}

    try:
        src = inspect.getsource(view_fn)
    except Exception:
        return {}

    fields = []
    for field in re.findall(r"data\.get\(\s*[\"']([a-zA-Z0-9_]+)[\"']", src):
        if field not in fields:
            fields.append(field)

    if not fields:
        return {}

    labels_by_key, ordered_labels = _extract_template_field_labels(source)
    required = {}
    for idx, field in enumerate(fields):
        label = labels_by_key.get(field)
        if not label and idx < len(ordered_labels):
            label = ordered_labels[idx]
        if not label:
            label = field.replace("_", " ").title()
        required[field] = label

    return required


def _resolve_connector_contract(uid, source):
    has_config = _has_connector_config(uid, source)

    if source in OAUTH_SOURCES:
        if not has_config:
            return {
                "connected": False,
                "auth_required": False,
                "credentials_required": True,
            }
        oauth_done = _has_oauth_completion(uid, source)
        if not oauth_done:
            return {
                "connected": False,
                "auth_required": True,
                "credentials_required": False,
            }
        return {
            "connected": True,
            "auth_required": False,
            "credentials_required": False,
        }

    if not has_config:
        return {
            "connected": False,
            "auth_required": False,
            "credentials_required": True,
        }

    return {
        "connected": True,
        "auth_required": False,
        "credentials_required": False,
    }


def _normalize_connect_response_payload(data, status_code, location=None):
    source = None
    path = request.path.rstrip("/")
    if path.startswith("/connectors/") and path.endswith("/connect"):
        parts = path.split("/")
        if len(parts) >= 4:
            source = parts[2]

    uid = getattr(g, "user_id", None)
    if uid and source:
        state = _resolve_connector_contract(uid, source)
        print("[CONNECTOR CONTRACT]", {
            "source": source,
            "state": state
        }, flush=True)
        if state["connected"]:
            return {"connected": True}
        if state["credentials_required"]:
            payload = {"connected": False, "error": "missing credentials"}
            required_fields = get_required_fields(source)
            if required_fields:
                payload["required_fields"] = required_fields
            return payload
        if state["auth_required"]:
            return {
                "connected": False,
                "auth_required": True,
                "redirect": _oauth_redirect_for(source) or location,
            }
        return {"connected": False, "error": "connection failed"}

    if location:
        return {
            "connected": False,
            "auth_required": True,
            "redirect": location,
        }

    if not isinstance(data, dict):
        return {"connected": False, "error": "Invalid connector response"}

    if data.get("connected") is True:
        return {"connected": True}

    if data.get("auth_required") and data.get("redirect"):
        return {
            "connected": False,
            "auth_required": True,
            "redirect": data["redirect"],
        }

    if data.get("status") in ("success", "connected"):
        return {"connected": True}

    error = data.get("error") or data.get("message")
    if status_code >= 400 and error:
        return {"connected": False, "error": str(error)}

    if error:
        return {"connected": False, "error": str(error)}

    normalized_error = _normalize_probe_error(data)
    if normalized_error == "missing credentials":
        payload = {"connected": False, "error": "missing credentials"}
        required_fields = get_required_fields(source)
        if required_fields:
            payload["required_fields"] = required_fields
        return payload

    return {"connected": False, "error": normalized_error}


@app.after_request
def normalize_connector_contracts(response):
    path = request.path.rstrip("/")

    if path.startswith("/api/status/") and response.status_code < 400:
        uid = getattr(g, "user_id", None)
        source = path.split("/", 3)[-1]
        if uid and source:
            state = _resolve_connector_contract(uid, source)
            normalized = jsonify(state)
            normalized.status_code = response.status_code
            return normalized

    if (
        _is_internal_ai_request()
        and path.startswith("/connectors/")
        and path.endswith("/connect")
    ):
        location = response.headers.get("Location") if 300 <= response.status_code < 400 else None
        data = response.get_json(silent=True) or {}
        payload = _normalize_connect_response_payload(data, response.status_code, location=location)
        status_code = response.status_code
        if payload.get("connected") is True:
            status_code = 200
        elif payload.get("auth_required"):
            status_code = 200
        elif payload.get("error") == "missing credentials":
            status_code = 400
        normalized = jsonify(payload)
        normalized.status_code = status_code
        return normalized

    return response

# ---------------- USAGE: SYNC RUN LOGGER ----------------

def log_sync_start(uid, source, sync_type):
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT INTO sync_runs
        (uid, source, sync_type, started_at, status)
        VALUES (?, ?, ?, ?, ?)
    """, (
        uid,
        source,
        sync_type,
        datetime.datetime.now(datetime.UTC).isoformat(),
        "running"
    ))

    run_id = cur.lastrowid
    con.commit()
    con.close()

    return run_id


def log_sync_finish(run_id, rows_synced, status, error=None):
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        UPDATE sync_runs
        SET rows_synced=?,
            finished_at=?,
            status=?,
            error=?
        WHERE id=?
    """, (
        rows_synced,
        datetime.datetime.now(datetime.UTC).isoformat(),
        status,
        error,
        run_id
    ))

    con.commit()
    con.close()

# ---------------- DB INIT ----------------

def init_db():

    con = get_db()
    cur = con.cursor()

    # AI Companion
    cur.execute("""
    CREATE TABLE IF NOT EXISTS ai_chats(
        id TEXT PRIMARY KEY,
        user_id TEXT,
        title TEXT,
        created_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS ai_messages(
        id TEXT PRIMARY KEY,
        chat_id TEXT,
        role TEXT,
        content TEXT,
        created_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS ai_state(
        chat_id TEXT PRIMARY KEY,
        uid TEXT,
        state_json TEXT,
        updated_at TEXT
    )
    """)

    # Visits
    cur.execute("""
    CREATE TABLE IF NOT EXISTS visits(
    id INTEGER PRIMARY KEY,
    uid TEXT,domain TEXT,browser TEXT,os TEXT,device TEXT,ip TEXT,
    screen TEXT,language TEXT,timezone TEXT,
    referrer TEXT,page_url TEXT,user_agent TEXT,
    name TEXT,age INTEGER,gender TEXT,city TEXT,country TEXT,profession TEXT,
    ts TEXT)
    """)


    # Identity
    cur.execute("""
    CREATE TABLE IF NOT EXISTS identity_map(
    id INTEGER PRIMARY KEY,
    uid TEXT,email TEXT,device_id TEXT,
    session_id TEXT,external_id TEXT,created_at TEXT)
    """)


    # Events
    cur.execute("""
    CREATE TABLE IF NOT EXISTS web_events(
    id INTEGER PRIMARY KEY,
    uid TEXT,domain TEXT,event TEXT,
    device_id TEXT,session_id TEXT,
    meta TEXT,ts TEXT)
    """)


    # Files
    cur.execute("""
    CREATE TABLE IF NOT EXISTS file_data(
    id INTEGER PRIMARY KEY,
    uid TEXT,filename TEXT,filetype TEXT,content TEXT,ts TEXT)
    """)

    # Forms
    cur.execute("""
    CREATE TABLE IF NOT EXISTS form_data(
    id INTEGER PRIMARY KEY,
    uid TEXT,form_name TEXT,data TEXT,ts TEXT)
    """)

    # ---------------- PLATFORM USERS ----------------

    cur.execute("""
    CREATE TABLE IF NOT EXISTS users(
        id TEXT PRIMARY KEY,
        email TEXT UNIQUE NOT NULL,
        password TEXT,
        first_name TEXT,
        last_name TEXT,

        company_name TEXT,
        company_size TEXT,
        country TEXT,
        phone TEXT,

        company_logo TEXT,

        is_individual INTEGER DEFAULT 0,

        created_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS user_sessions(
        session_id TEXT PRIMARY KEY,
        user_id TEXT,
        created_at TEXT,
        expires_at TEXT
    )
    """)

    # Google Accounts
    cur.execute("""
    CREATE TABLE IF NOT EXISTS google_accounts(
    id INTEGER PRIMARY KEY,
    uid TEXT,
    source TEXT,
    access_token TEXT,
    refresh_token TEXT,
    scopes TEXT,
    created_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS google_connections (
    uid TEXT,
    source TEXT,
    enabled INTEGER DEFAULT 1,
    PRIMARY KEY (uid, source)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS connector_jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        source TEXT,
        sync_type TEXT,
        schedule_time TEXT,
        enabled INTEGER DEFAULT 1,
        created_at TEXT,
        last_run_at TEXT,
        PRIMARY KEY (uid, source)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS connector_state (
        uid TEXT,
        source TEXT,
        state_json TEXT,
        updated_at TEXT,
        PRIMARY KEY (uid, source)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS destination_configs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        source TEXT,
        dest_type TEXT,

        host TEXT,
        port INTEGER,
        username TEXT,
        password TEXT,
        database_name TEXT,

        created_at TEXT,
        is_active INTEGER DEFAULT 1,
        format TEXT DEFAULT 'parquet'
    )
    """)


    # Drive
    cur.execute("""
    CREATE TABLE IF NOT EXISTS drive_files(
    id INTEGER PRIMARY KEY,
    uid TEXT,
    file_id TEXT,
    name TEXT,
    mime_type TEXT,
    size INTEGER,
    created_time TEXT,
    fetched_at TEXT
    )
    """)


    # Sheets
    cur.execute("""
    CREATE TABLE IF NOT EXISTS sheets_data(
    id INTEGER PRIMARY KEY,
    uid TEXT,
    sheet_id TEXT,
    name TEXT,
    created_time TEXT,
    modified_time TEXT,
    fetched_at TEXT
    )
    """)


    # Search Console
    cur.execute("""
    CREATE TABLE IF NOT EXISTS gsc_queries(
    id INTEGER PRIMARY KEY,
    site TEXT,
    query TEXT,
    clicks INTEGER,
    impressions INTEGER,
    position REAL,
    fetched_at TEXT
    )
    """)

    # GA4 Website Overview
    cur.execute("""
    CREATE TABLE IF NOT EXISTS ga4_website_overview(
    id INTEGER PRIMARY KEY,
    uid TEXT,
    property_id TEXT,
    date TEXT,
    total_users INTEGER,
    new_users INTEGER,
    sessions INTEGER,
    views INTEGER,
    bounce_rate REAL,
    avg_session_duration REAL,
    fetched_at TEXT
    )
    """)

    # GA4 Devices
    cur.execute("""
    CREATE TABLE IF NOT EXISTS ga4_devices(
    id INTEGER PRIMARY KEY,
    uid TEXT,
    property_id TEXT,
    date TEXT,
    device TEXT,
    os TEXT,
    browser TEXT,
    users INTEGER,
    sessions INTEGER
    )
    """)


    # GA4 Locations
    cur.execute("""
    CREATE TABLE IF NOT EXISTS ga4_locations(
    id INTEGER PRIMARY KEY,
    uid TEXT,
    property_id TEXT,
    date TEXT,
    country TEXT,
    region TEXT,
    city TEXT,
    users INTEGER,
    sessions INTEGER
    )
    """)


    # GA4 Traffic
    cur.execute("""
    CREATE TABLE IF NOT EXISTS ga4_traffic_sources(
    id INTEGER PRIMARY KEY,
    uid TEXT,
    property_id TEXT,
    date TEXT,
    source TEXT,
    medium TEXT,
    users INTEGER,
    sessions INTEGER
    )
    """)


    # GA4 Events
    cur.execute("""
    CREATE TABLE IF NOT EXISTS ga4_events(
    id INTEGER PRIMARY KEY,
    uid TEXT,
    property_id TEXT,
    date TEXT,
    event TEXT,
    count INTEGER,
    users INTEGER,
    fetched_at TEXT
    )
    """)

    # Google Search Console
    cur.execute("""
    CREATE TABLE IF NOT EXISTS google_search_console(
    id INTEGER PRIMARY KEY,
    uid TEXT,
    site_url TEXT,
    query TEXT,
    page TEXT,
    country TEXT,
    device TEXT,
    clicks INTEGER,
    impressions INTEGER,
    ctr REAL,
    position REAL,
    fetched_at TEXT
    )
    """)

    # PageSpeed
    cur.execute("""
    CREATE TABLE IF NOT EXISTS google_pagespeed(
    id INTEGER PRIMARY KEY,
    uid TEXT,
    url TEXT,
    strategy TEXT,
    categories TEXT,
    performance_score REAL,
    seo_score REAL,
    accessibility_score REAL,
    best_practices_score REAL,
    pwa_score REAL,
    raw_response TEXT,
    fetched_at TEXT
    )
    """)

    #google_forms
    cur.execute("""
    CREATE TABLE IF NOT EXISTS google_forms (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    uid TEXT,
    form_id TEXT,
    title TEXT,
    description TEXT,
    responder_uri TEXT,
    raw_json TEXT,
    fetched_at TEXT
    )
    """)

    #google_responses
    cur.execute("""
    CREATE TABLE IF NOT EXISTS google_form_responses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    uid TEXT,
    form_id TEXT,
    response_id TEXT,
    create_time TEXT,
    last_submitted_time TEXT,
    answers_json TEXT,
    raw_json TEXT,
    fetched_at TEXT
    )
    """)

    #google_calendar_colors
    cur.execute("""
    CREATE TABLE IF NOT EXISTS google_calendar_colors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    uid TEXT,
    raw_json TEXT,
    fetched_at TEXT
    )
    """)

    #google_calendar_settings
    cur.execute("""
    CREATE TABLE IF NOT EXISTS google_calendar_settings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    uid TEXT,
    setting_id TEXT,
    value TEXT,
    raw_json TEXT,
    fetched_at TEXT
    )
    """)

    #google_calendar_list
    cur.execute("""
    CREATE TABLE IF NOT EXISTS google_calendar_list (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    uid TEXT,
    calendar_id TEXT,
    summary TEXT,
    time_zone TEXT,
    access_role TEXT,
    raw_json TEXT,
    fetched_at TEXT
    )
    """)

    # socialinsider
    cur.execute("""
    CREATE TABLE IF NOT EXISTS socialinsider_connections (
        uid TEXT PRIMARY KEY,
        api_key TEXT,
        platform TEXT,
        handle TEXT,
        created_at TEXT
    )
    """)

    # slack
    cur.execute("""
    CREATE TABLE IF NOT EXISTS slack_channels (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        channel_id TEXT,
        name TEXT,
        is_private INTEGER,
        is_archived INTEGER,
        member_count INTEGER,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)
    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS slack_users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        user_id TEXT,
        name TEXT,
        real_name TEXT,
        is_bot INTEGER,
        deleted INTEGER,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)
    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS slack_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        message_id TEXT,
        channel_id TEXT,
        user_id TEXT,
        ts TEXT,
        text TEXT,
        type TEXT,
        subtype TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS socialinsider_posts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        platform TEXT,
        handle TEXT,
        post_id TEXT,
        publish_date TEXT,
        content_type TEXT,
        engagement INTEGER,
        reach INTEGER,
        impressions INTEGER,
        saves INTEGER,
        video_views INTEGER,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS socialinsider_profile_insights (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        platform TEXT,
        handle TEXT,
        follower_count INTEGER,
        follower_growth INTEGER,
        gender_distribution TEXT,
        age_distribution TEXT,
        geo_distribution TEXT,
        industry TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    #google_calendar_events
    cur.execute("""
    CREATE TABLE IF NOT EXISTS google_calendar_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    uid TEXT,
    calendar_id TEXT,
    event_id TEXT,
    summary TEXT,
    start_time TEXT,
    end_time TEXT,
    status TEXT,
    raw_json TEXT,
    fetched_at TEXT
    )
    """)

    #google_gmail_profile
    cur.execute("""
    CREATE TABLE IF NOT EXISTS google_gmail_profile (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    uid TEXT,
    email_address TEXT,
    messages_total INTEGER,
    threads_total INTEGER,
    raw_json TEXT,
    fetched_at TEXT
    )
    """)

    #google_gmail_labels
    cur.execute("""
    CREATE TABLE IF NOT EXISTS google_gmail_labels (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    uid TEXT,
    label_id TEXT,
    name TEXT,
    type TEXT,
    raw_json TEXT,
    fetched_at TEXT
    )
    """)

    #google_gmail_messages
    cur.execute("""
    CREATE TABLE IF NOT EXISTS google_gmail_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    uid TEXT,
    message_id TEXT,
    thread_id TEXT,
    snippet TEXT,
    internal_date TEXT,
    raw_json TEXT,
    fetched_at TEXT
    )
    """)

    #google_gmail_message_details       
    cur.execute("""
    CREATE TABLE IF NOT EXISTS google_gmail_message_details (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    uid TEXT,
    message_id TEXT,
    thread_id TEXT,
    subject TEXT,
    sender TEXT,
    to_email TEXT,
    snippet TEXT,
    internal_date TEXT,
    payload_json TEXT,
    raw_json TEXT,
    fetched_at TEXT
    )
    """)

    # ---------------- GOOGLE CLASSROOM ---------------- #

    # Courses
    cur.execute("""
    CREATE TABLE IF NOT EXISTS google_classroom_courses (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        course_id TEXT UNIQUE,
        name TEXT,
        state TEXT,
        owner_id TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    # Teachers
    cur.execute("""
    CREATE TABLE IF NOT EXISTS google_classroom_teachers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        course_id TEXT,
        user_id TEXT,
        full_name TEXT,
        email TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    # Students
    cur.execute("""
    CREATE TABLE IF NOT EXISTS google_classroom_students (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        course_id TEXT,
        user_id TEXT,
        full_name TEXT,
        email TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    # Announcements
    cur.execute("""
    CREATE TABLE IF NOT EXISTS google_classroom_announcements (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        course_id TEXT,
        announcement_id TEXT,
        text TEXT,
        state TEXT,
        created_at TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    # Coursework
    cur.execute("""
    CREATE TABLE IF NOT EXISTS google_classroom_coursework (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        course_id TEXT,
        work_id TEXT,
        title TEXT,
        work_type TEXT,
        state TEXT,
        max_points REAL,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    # Submissions
    cur.execute("""
    CREATE TABLE IF NOT EXISTS google_classroom_submissions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        course_id TEXT,
        work_id TEXT,
        submission_id TEXT,
        user_id TEXT,
        state TEXT,
        assigned_grade REAL,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    # google_tasks_lists
    cur.execute("""
    CREATE TABLE IF NOT EXISTS google_tasks_lists (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        list_id TEXT UNIQUE,
        title TEXT,
        updated TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    # google_tasks_items
    cur.execute("""
    CREATE TABLE IF NOT EXISTS google_tasks_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        task_id TEXT UNIQUE,
        list_id TEXT,
        title TEXT,
        status TEXT,
        due TEXT,
        completed TEXT,
        updated TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    #google_contacts_persons
    cur.execute("""
    CREATE TABLE IF NOT EXISTS google_contacts_persons (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        resource_name TEXT UNIQUE,
        etag TEXT,
        names TEXT,
        emails TEXT,
        phone_numbers TEXT,
        organizations TEXT,
        addresses TEXT,
        metadata_json TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    #google_gcs_buckets
    cur.execute("""
    CREATE TABLE IF NOT EXISTS google_gcs_buckets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        project_id TEXT,
        bucket_name TEXT,
        location TEXT,
        storage_class TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    #google_gcs_objects
    cur.execute("""
    CREATE TABLE IF NOT EXISTS google_gcs_objects (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        bucket_name TEXT,
        object_name TEXT,
        size INTEGER,
        content_type TEXT,
        updated TEXT,
        md5_hash TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    # google_webfonts
    cur.execute("""
    CREATE TABLE IF NOT EXISTS google_webfonts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        family TEXT,
        category TEXT,
        version TEXT,
        kind TEXT,
        last_modified TEXT,
        variants TEXT,
        subsets TEXT,
        files_json TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    # Google YouTube Channels
    cur.execute("""
    CREATE TABLE IF NOT EXISTS google_youtube_channels (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        channel_id TEXT,
        title TEXT,
        description TEXT,
        subscribers INTEGER,
        views INTEGER,
        videos INTEGER,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    # Google YouTube Videos
    cur.execute("""
    CREATE TABLE IF NOT EXISTS google_youtube_videos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        channel_id TEXT,
        video_id TEXT,
        title TEXT,
        description TEXT,
        published_at TEXT,
        views INTEGER,
        likes INTEGER,
        comments INTEGER,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    # ---------------- REDDIT TABLES ----------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS reddit_accounts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        access_token TEXT,
        refresh_token TEXT,
        expires_at TEXT,
        scopes TEXT,
        created_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS reddit_profiles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        username TEXT,
        karma INTEGER,
        created_utc TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS reddit_posts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        post_id TEXT UNIQUE,
        subreddit TEXT,
        title TEXT,
        author TEXT,
        score INTEGER,
        num_comments INTEGER,
        created_utc TEXT,
        url TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS reddit_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        message_id TEXT UNIQUE,
        author TEXT,
        subject TEXT,
        body TEXT,
        created_utc TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS reddit_tokens (
        uid TEXT PRIMARY KEY,
        client_id TEXT,
        client_secret TEXT,
        username TEXT,
        password TEXT,
        access_token TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS reddit_state (
        uid TEXT PRIMARY KEY,
        last_created_utc INTEGER
    )
    """)


    # ---------------- TELEGRAM TABLES ----------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS telegram_accounts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        bot_token TEXT,
        created_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS telegram_channels (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        channel_id TEXT,
        username TEXT,
        title TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS telegram_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        channel_id TEXT,
        message_id TEXT,
        text TEXT,
        author TEXT,
        date INTEGER,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS telegram_state (
        uid TEXT PRIMARY KEY,
        last_update_id INTEGER
    )
    """)

    # ---------------- MEDIUM ----------------

    cur.execute("""
    CREATE TABLE IF NOT EXISTS medium_posts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        title TEXT,
        link TEXT,
        author TEXT,
        published TEXT,
        summary TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS medium_accounts (
        uid TEXT PRIMARY KEY,
        username TEXT,
        created_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS medium_state (
        uid TEXT PRIMARY KEY,
        last_published TEXT
    )
    """)


    # ---------------- QUORA ----------------

    cur.execute("""
    CREATE TABLE IF NOT EXISTS quora_answers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        question TEXT,
        answer_url TEXT,
        author TEXT,
        upvotes INTEGER,
        content TEXT,
        raw_html TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS quora_profile (
        uid TEXT PRIMARY KEY,
        username TEXT,
        full_name TEXT,
        bio TEXT,
        followers INTEGER,
        raw_html TEXT,
        fetched_at TEXT
    )
    """)

    # ---------------- TUMBLR ----------------

    cur.execute("""
    CREATE TABLE IF NOT EXISTS tumblr_blogs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        blog_name TEXT,
        title TEXT,
        description TEXT,
        posts_count INTEGER,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS tumblr_posts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        blog_name TEXT,
        post_id TEXT,
        post_type TEXT,
        title TEXT,
        body TEXT,
        url TEXT,
        timestamp INTEGER,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS tumblr_state (
        uid TEXT,
        blog_name TEXT,
        last_post_id INTEGER,
        PRIMARY KEY(uid, blog_name)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS tumblr_accounts (
        uid TEXT PRIMARY KEY,
        api_key TEXT,
        created_at TEXT
    )
    """)

    # ---------------- TWITCH ----------------

    cur.execute("""
    CREATE TABLE IF NOT EXISTS twitch_users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        twitch_id TEXT,
        login TEXT,
        display_name TEXT,
        description TEXT,
        followers INTEGER,
        view_count INTEGER,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS twitch_streams (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        twitch_id TEXT,
        title TEXT,
        game_name TEXT,
        viewer_count INTEGER,
        started_at TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS twitch_videos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        twitch_id TEXT,
        video_id TEXT,
        title TEXT,
        views INTEGER,
        duration TEXT,
        created_at TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    # ---------------- DISCORD ----------------

    cur.execute("""
    CREATE TABLE IF NOT EXISTS discord_guilds (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        guild_id TEXT,
        name TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS discord_channels (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        channel_id TEXT,
        guild_id TEXT,
        name TEXT,
        type INTEGER,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS discord_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        channel_id TEXT,
        message_id TEXT,
        author TEXT,
        content TEXT,
        timestamp TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS discord_connections (
        uid TEXT PRIMARY KEY,
        bot_token TEXT,
        created_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS discord_state (
        uid TEXT,
        channel_id TEXT,
        last_message_id TEXT,
        PRIMARY KEY(uid, channel_id)
    )
    """)

    # ---------------- GOOGLE BOOKS ----------------

    cur.execute("""
    CREATE TABLE IF NOT EXISTS google_books_volumes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        volume_id TEXT UNIQUE,
        title TEXT,
        authors TEXT,
        publisher TEXT,
        published_date TEXT,
        description TEXT,
        page_count INTEGER,
        categories TEXT,
        language TEXT,
        preview_link TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS google_books_state (
        uid TEXT,
        query TEXT,
        last_index INTEGER,
        PRIMARY KEY (uid, query)
    )
    """)

    # ---------------- GOOGLE FACT CHECK ----------------

    cur.execute("""
    CREATE TABLE IF NOT EXISTS google_factcheck_claims (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        claim_id TEXT UNIQUE,
        text TEXT,
        claimant TEXT,
        claim_date TEXT,
        rating TEXT,
        review_publisher TEXT,
        review_url TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS google_factcheck_state (
        uid TEXT,
        query TEXT,
        next_page_token TEXT,
        PRIMARY KEY (uid, query)
    )
    """)

    # ---------------- GOOGLE NEWS ----------------

    cur.execute("""
    CREATE TABLE IF NOT EXISTS google_news_articles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        article_id TEXT UNIQUE,
        query TEXT,
        title TEXT,
        link TEXT,
        source TEXT,
        published TEXT,
        summary TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS google_news_state (
        uid TEXT,
        query TEXT,
        last_published TEXT,
        PRIMARY KEY (uid, query)
    )
    """)

    # ---------------- GOOGLE TRENDS ----------------

    cur.execute("""
    CREATE TABLE IF NOT EXISTS google_trends_interest (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        keyword TEXT,
        date TEXT,
        value INTEGER,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS google_trends_related (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        keyword TEXT,
        type TEXT,
        query TEXT,
        value INTEGER,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS google_trends_state (
        uid TEXT,
        keyword TEXT,
        last_date TEXT,
        PRIMARY KEY (uid, keyword)
    )
    """)

    # ---------------- DEV.TO ----------------

    cur.execute("""
    CREATE TABLE IF NOT EXISTS devto_articles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        article_id INTEGER UNIQUE,
        title TEXT,
        url TEXT,
        author TEXT,
        published_at TEXT,
        tags TEXT,
        reactions INTEGER,
        comments INTEGER,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    # ---------------- WHATSAPP ----------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS whatsapp_connections (
        uid TEXT PRIMARY KEY,
        access_token_encrypted TEXT,
        waba_id TEXT,
        phone_number_id TEXT,
        connected_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS whatsapp_business_accounts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        waba_id TEXT,
        name TEXT,
        currency TEXT,
        timezone_id TEXT,
        messaging_limit TEXT,
        verification_status TEXT,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid, waba_id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS whatsapp_phone_numbers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        waba_id TEXT,
        phone_number_id TEXT,
        display_phone_number TEXT,
        verified_name TEXT,
        quality_rating TEXT,
        status TEXT,
        platform_type TEXT,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid, phone_number_id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS whatsapp_message_templates (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        waba_id TEXT,
        template_name TEXT,
        namespace TEXT,
        category TEXT,
        language TEXT,
        status TEXT,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid, waba_id, template_name, language)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS whatsapp_conversation_analytics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        phone_number_id TEXT,
        conversation_id TEXT,
        category TEXT,
        origin_type TEXT,
        start_time TEXT,
        end_time TEXT,
        messages_sent INTEGER,
        messages_received INTEGER,
        date TEXT,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid, conversation_id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS whatsapp_message_insights (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        phone_number_id TEXT,
        sent INTEGER,
        delivered INTEGER,
        read INTEGER,
        failed INTEGER,
        date TEXT,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid, phone_number_id, date)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS devto_tags (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        name TEXT UNIQUE,
        popularity INTEGER,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS devto_state (
        uid TEXT,
        endpoint TEXT,
        last_page INTEGER,
        PRIMARY KEY (uid, endpoint)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS github_tokens (
        uid TEXT PRIMARY KEY,
        access_token TEXT,
        scope TEXT,
        token_type TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS github_repos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        repo_id INTEGER UNIQUE,
        name TEXT,
        full_name TEXT,
        private INTEGER,
        url TEXT,
        stars INTEGER,
        forks INTEGER,
        language TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS github_commits (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        repo_full TEXT,
        sha TEXT UNIQUE,
        author TEXT,
        message TEXT,
        date TEXT,
        url TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS github_issues (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        issue_id INTEGER UNIQUE,
        repo_full TEXT,
        title TEXT,
        state TEXT,
        author TEXT,
        created_at TEXT,
        url TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS github_state (
        uid TEXT,
        repo_full TEXT,
        last_commit_sha TEXT,
        last_issue_updated TEXT,
        PRIMARY KEY (uid, repo_full)
    )
    """)

    # ---------------- GITLAB ----------------

    cur.execute("""
    CREATE TABLE IF NOT EXISTS gitlab_tokens (
        uid TEXT PRIMARY KEY,
        access_token TEXT,
        refresh_token TEXT,
        expires_at TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS gitlab_projects (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        project_id INTEGER UNIQUE,
        name TEXT,
        path TEXT,
        namespace TEXT,
        visibility TEXT,
        web_url TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS gitlab_commits (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        project_id INTEGER,
        sha TEXT UNIQUE,
        author TEXT,
        message TEXT,
        date TEXT,
        web_url TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS gitlab_issues (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        issue_id INTEGER UNIQUE,
        project_id INTEGER,
        title TEXT,
        state TEXT,
        author TEXT,
        created_at TEXT,
        web_url TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS gitlab_merge_requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        mr_id INTEGER UNIQUE,
        project_id INTEGER,
        title TEXT,
        state TEXT,
        author TEXT,
        created_at TEXT,
        web_url TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS gitlab_state (
        uid TEXT,
        project_id INTEGER,
        last_commit_sha TEXT,
        last_issue_updated TEXT,
        PRIMARY KEY (uid, project_id)
    )
    """)

    # ---------------- STACKOVERFLOW ----------------

    cur.execute("""
    CREATE TABLE IF NOT EXISTS stack_tokens (
        uid TEXT PRIMARY KEY,
        access_token TEXT,
        expires_at TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS stack_questions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        question_id INTEGER UNIQUE,
        title TEXT,
        tags TEXT,
        score INTEGER,
        owner TEXT,
        created_at TEXT,
        link TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS stack_answers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        answer_id INTEGER UNIQUE,
        question_id INTEGER,
        score INTEGER,
        owner TEXT,
        created_at TEXT,
        link TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS stack_users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        user_id INTEGER UNIQUE,
        name TEXT,
        reputation INTEGER,
        profile_url TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS stack_state (
        uid TEXT,
        endpoint TEXT,
        last_date INTEGER,
        PRIMARY KEY (uid, endpoint)
    )
    """)

    # ---------------- HACKERNEWS ----------------

    cur.execute("""
    CREATE TABLE IF NOT EXISTS hackernews_stories (
        id INTEGER PRIMARY KEY AUTOINCREMENT,

        uid TEXT,

        story_id INTEGER,

        title TEXT,
        author TEXT,
        url TEXT,

        score INTEGER,
        descendants INTEGER,

        type TEXT,
        time INTEGER,

        raw_json TEXT,

        fetched_at TEXT,

        UNIQUE(uid, story_id)
    )
    """)


    cur.execute("""
    CREATE TABLE IF NOT EXISTS hackernews_state (
        uid TEXT PRIMARY KEY,

        last_story_id INTEGER
    )
    """)

    # ---------------- PRODUCTHUNT ----------------

    cur.execute("""
    CREATE TABLE IF NOT EXISTS producthunt_tokens (
        uid TEXT PRIMARY KEY,
        access_token TEXT,
        fetched_at TEXT
    )
    """)


    cur.execute("""
    CREATE TABLE IF NOT EXISTS producthunt_posts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        post_id TEXT,
        name TEXT,
        tagline TEXT,
        votes INTEGER,
        comments INTEGER,
        created_at TEXT,
        url TEXT,
        topics TEXT,
        makers TEXT,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid, post_id)
    )
    """)


    cur.execute("""
    CREATE TABLE IF NOT EXISTS producthunt_topics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        topic_id TEXT,
        name TEXT,
        slug TEXT,
        followers INTEGER,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid, topic_id)
    )
    """)


    cur.execute("""
    CREATE TABLE IF NOT EXISTS producthunt_state (
        uid TEXT PRIMARY KEY,
        last_post_time TEXT
    )
    """)

    # ---------------- WIKIPEDIA ----------------

    cur.execute("""
    CREATE TABLE IF NOT EXISTS wikipedia_recent_changes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        rcid INTEGER,
        title TEXT,
        user TEXT,
        comment TEXT,
        timestamp TEXT,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid, rcid)
    )
    """)


    cur.execute("""
    CREATE TABLE IF NOT EXISTS wikipedia_new_pages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        page_id INTEGER,
        title TEXT,
        creator TEXT,
        created_at TEXT,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid, page_id)
    )
    """)


    cur.execute("""
    CREATE TABLE IF NOT EXISTS wikipedia_most_viewed (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        article TEXT,
        views INTEGER,
        rank INTEGER,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid, article)
    )
    """)


    cur.execute("""
    CREATE TABLE IF NOT EXISTS wikipedia_state (
        uid TEXT PRIMARY KEY,
        last_rc_timestamp TEXT
    )
    """)

    # ---------------- PEERTUBE ----------------

    cur.execute("""
    CREATE TABLE IF NOT EXISTS peertube_videos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        instance TEXT,
        video_id TEXT,
        name TEXT,
        description TEXT,
        duration INTEGER,
        views INTEGER,
        likes INTEGER,
        dislikes INTEGER,
        published_at TEXT,
        channel_name TEXT,
        url TEXT,
        category TEXT,
        language TEXT,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid, instance, video_id)
    )
    """)


    cur.execute("""
    CREATE TABLE IF NOT EXISTS peertube_channels (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        instance TEXT,
        channel_id TEXT,
        name TEXT,
        display_name TEXT,
        followers INTEGER,
        url TEXT,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid, instance, channel_id)
    )
    """)


    cur.execute("""
    CREATE TABLE IF NOT EXISTS peertube_state (
        uid TEXT PRIMARY KEY,
        instance TEXT,
        last_published_at TEXT
    )
    """)

    # ---------------- MASTODON ----------------

    cur.execute("""
    CREATE TABLE IF NOT EXISTS mastodon_statuses(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        instance TEXT,
        status_id TEXT,
        content TEXT,
        author TEXT,
        url TEXT,
        replies INTEGER,
        reblogs INTEGER,
        favourites INTEGER,
        created_at TEXT,
        visibility TEXT,
        language TEXT,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid,instance,status_id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS mastodon_tags(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        instance TEXT,
        tag TEXT,
        url TEXT,
        history TEXT,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid,instance,tag)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS mastodon_state(
        uid TEXT PRIMARY KEY,
        instance TEXT,
        last_status_id TEXT
    )
    """)

    # ---------------- DISCOURSE ----------------

    cur.execute("""
    CREATE TABLE IF NOT EXISTS discourse_topics(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        forum TEXT,
        topic_id INTEGER,
        title TEXT,
        posts_count INTEGER,
        views INTEGER,
        created_at TEXT,
        last_posted_at TEXT,
        slug TEXT,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid,forum,topic_id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS discourse_categories(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        forum TEXT,
        category_id INTEGER,
        name TEXT,
        description TEXT,
        topic_count INTEGER,
        post_count INTEGER,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid,forum,category_id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS discourse_users(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        forum TEXT,
        user_id INTEGER,
        username TEXT,
        name TEXT,
        trust_level INTEGER,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid,forum,user_id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS discourse_state(
        uid TEXT PRIMARY KEY,
        forum TEXT,
        last_topic_id INTEGER
    )
    """)

    # ---------------- LEMMY ----------------

    cur.execute("""
    CREATE TABLE IF NOT EXISTS lemmy_posts(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        instance TEXT,
        post_id INTEGER,
        name TEXT,
        url TEXT,
        creator TEXT,
        community TEXT,
        score INTEGER,
        comments INTEGER,
        published TEXT,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid,instance,post_id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS lemmy_communities(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        instance TEXT,
        community_id INTEGER,
        name TEXT,
        title TEXT,
        subscribers INTEGER,
        posts INTEGER,
        comments INTEGER,
        published TEXT,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid,instance,community_id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS lemmy_users(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        instance TEXT,
        user_id INTEGER,
        username TEXT,
        display_name TEXT,
        posts INTEGER,
        comments INTEGER,
        published TEXT,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid,instance,user_id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS lemmy_state(
        uid TEXT PRIMARY KEY,
        instance TEXT,
        last_post_id INTEGER
    )
    """)

    # ---------------- OPENSTREETMAP ----------------

    cur.execute("""
    CREATE TABLE IF NOT EXISTS osm_changesets(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        changeset_id INTEGER,
        user TEXT,
        uid_osm INTEGER,
        created_at TEXT,
        closed_at TEXT,
        min_lat REAL,
        min_lon REAL,
        max_lat REAL,
        max_lon REAL,
        raw_xml TEXT,
        fetched_at TEXT,
        UNIQUE(uid,changeset_id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS osm_notes(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        note_id INTEGER,
        status TEXT,
        lat REAL,
        lon REAL,
        created_at TEXT,
        closed_at TEXT,
        comments INTEGER,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid,note_id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS osm_state(
        uid TEXT PRIMARY KEY,
        last_changeset_id INTEGER,
        last_note_id INTEGER
    )
    """)

    # ---------------- NVD ----------------

    cur.execute("""
    CREATE TABLE IF NOT EXISTS nvd_cves(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        cve_id TEXT,
        source_identifier TEXT,
        published TEXT,
        last_modified TEXT,
        vuln_status TEXT,
        description TEXT,
        severity TEXT,
        cvss_score REAL,
        reference_url TEXT,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid,cve_id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS nvd_state(
        uid TEXT PRIMARY KEY,
        last_modified TEXT
    )
    """)

    # ---------------- PINTEREST ----------------

    cur.execute("""
    CREATE TABLE IF NOT EXISTS pinterest_tokens(
        uid TEXT PRIMARY KEY,
        access_token TEXT,
        refresh_token TEXT,
        expires_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS pinterest_boards(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        board_id TEXT,
        name TEXT,
        description TEXT,
        privacy TEXT,
        url TEXT,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid,board_id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS pinterest_pins(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        pin_id TEXT,
        board_id TEXT,
        title TEXT,
        description TEXT,
        link TEXT,
        media_url TEXT,
        created_at TEXT,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid,pin_id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS pinterest_state(
        uid TEXT PRIMARY KEY,
        last_pin_time TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS connector_configs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        connector TEXT,
        client_id TEXT,
        client_secret TEXT,
        api_key TEXT,
        access_token TEXT,
        refresh_token TEXT,
        scopes TEXT,
        status TEXT,
        created_at TEXT
    )
    """)

    # ---------------- Meta ----------------

    cur.execute("""
    CREATE TABLE IF NOT EXISTS facebook_connections (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        page_id TEXT,
        page_name TEXT,
        page_access_token TEXT,
        connected_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS instagram_connections (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT UNIQUE,
        ig_account_id TEXT,
        access_token TEXT,
        connected_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS facebook_pages_metadata (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        page_id TEXT,
        name TEXT,
        category TEXT,
        link TEXT,
        tasks TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS facebook_page_posts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        post_id TEXT UNIQUE,
        page_id TEXT,
        message TEXT,
        story TEXT,
        created_time TEXT,
        privacy TEXT,
        attachments TEXT,
        message_tags TEXT,
        reactions_count INTEGER,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS facebook_post_comments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        comment_id TEXT UNIQUE,
        post_id TEXT,
        from_id TEXT,
        from_name TEXT,
        message TEXT,
        created_time TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS facebook_page_insights (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        page_id TEXT,
        metric_name TEXT,
        period TEXT,
        value TEXT,
        end_time TEXT,
        title TEXT,
        description TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS facebook_reactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        reaction_id TEXT UNIQUE,
        post_id TEXT,
        comment_id TEXT,
        type TEXT,
        user_psid TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS facebook_app_credentials (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    uid TEXT,
    app_id TEXT,
    app_secret TEXT,
    redirect_uri TEXT,
    created_at TEXT
    )
    """)

    # ---------------- FACEBOOK ADS ----------------

    cur.execute("""
    CREATE TABLE IF NOT EXISTS facebook_ads_connections (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        ad_account_id TEXT,
        ad_account_name TEXT,
        access_token TEXT,
        connected_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS facebook_ad_accounts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        account_id TEXT UNIQUE,
        name TEXT,
        account_status TEXT,
        currency TEXT,
        timezone_name TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS facebook_ad_campaigns (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        campaign_id TEXT UNIQUE,
        account_id TEXT,
        name TEXT,
        status TEXT,
        objective TEXT,
        daily_budget TEXT,
        start_time TEXT,
        stop_time TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS facebook_ad_sets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        adset_id TEXT UNIQUE,
        campaign_id TEXT,
        name TEXT,
        status TEXT,
        daily_budget TEXT,
        optimization_goal TEXT,
        start_time TEXT,
        end_time TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS facebook_ads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        ad_id TEXT UNIQUE,
        adset_id TEXT,
        name TEXT,
        status TEXT,
        creative_id TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS facebook_ad_creatives (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        creative_id TEXT UNIQUE,
        name TEXT,
        object_story_id TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS facebook_ads_insights (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        account_id TEXT,
        campaign_id TEXT,
        adset_id TEXT,
        ad_id TEXT,
        date_start TEXT,
        date_stop TEXT,
        impressions TEXT,
        clicks TEXT,
        spend TEXT,
        ctr TEXT,
        cpc TEXT,
        cpm TEXT,
        reach TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    # ---------------- TIKTOK BUSINESS ----------------

    cur.execute("""
    CREATE TABLE IF NOT EXISTS tiktok_connections (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT UNIQUE,
        advertiser_id TEXT,
        access_token TEXT,
        refresh_token TEXT,
        expires_at TEXT,
        connected_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS tiktok_campaigns (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        advertiser_id TEXT,
        campaign_id TEXT,
        campaign_name TEXT,
        objective TEXT,
        status TEXT,
        budget TEXT,
        budget_type TEXT,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid, campaign_id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS tiktok_ads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        advertiser_id TEXT,
        ad_id TEXT,
        campaign_id TEXT,
        adgroup_id TEXT,
        ad_name TEXT,
        status TEXT,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid, ad_id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS tiktok_reports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        advertiser_id TEXT,
        ad_id TEXT,
        campaign_id TEXT,
        adgroup_id TEXT,
        stat_time_day TEXT,
        impressions TEXT,
        clicks TEXT,
        spend TEXT,
        ctr TEXT,
        cpc TEXT,
        cpm TEXT,
        conversions TEXT,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid, ad_id, stat_time_day)
    )
    """)

    # ---------------- TABOOLA BACKSTAGE ----------------

    cur.execute("""
    CREATE TABLE IF NOT EXISTS taboola_connections (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT UNIQUE,
        account_id TEXT,
        access_token TEXT,
        expires_at TEXT,
        connected_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS taboola_campaign_reports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        account_id TEXT,
        dimension TEXT,
        dimension_value TEXT,
        campaign_id TEXT,
        campaign_name TEXT,
        impressions TEXT,
        clicks TEXT,
        ctr TEXT,
        spent TEXT,
        cpc TEXT,
        cpm TEXT,
        conversions TEXT,
        conversion_rate TEXT,
        roas TEXT,
        date TEXT,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid, account_id, dimension, dimension_value, campaign_id, date)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS taboola_ads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        account_id TEXT,
        campaign_id TEXT,
        campaign_name TEXT,
        item_id TEXT,
        item_name TEXT,
        thumbnail_url TEXT,
        url TEXT,
        impressions TEXT,
        clicks TEXT,
        ctr TEXT,
        cpc TEXT,
        spent TEXT,
        conversions TEXT,
        date TEXT,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid, account_id, campaign_id, item_id, date)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS taboola_publisher_revenue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        account_id TEXT,
        dimension TEXT,
        dimension_value TEXT,
        page_views TEXT,
        page_views_with_ads_pct TEXT,
        ad_revenue TEXT,
        ad_rpm TEXT,
        ad_cpc TEXT,
        date TEXT,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid, account_id, dimension, dimension_value, date)
    )
    """)

    # ---------------- OUTBRAIN AMPLIFY ----------------

    cur.execute("""
    CREATE TABLE IF NOT EXISTS outbrain_connections (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT UNIQUE,
        marketer_id TEXT,
        access_token TEXT,
        expires_at TEXT,
        connected_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS outbrain_marketers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        marketer_id TEXT,
        name TEXT,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid, marketer_id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS outbrain_campaign_reports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        marketer_id TEXT,
        campaign_id TEXT,
        campaign_name TEXT,
        breakdown TEXT,
        impressions TEXT,
        clicks TEXT,
        ctr TEXT,
        spend TEXT,
        conversions TEXT,
        date TEXT,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid, marketer_id, campaign_id, breakdown, date)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS outbrain_ads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        marketer_id TEXT,
        campaign_id TEXT,
        promoted_link_id TEXT,
        promoted_link_text TEXT,
        impressions TEXT,
        clicks TEXT,
        ctr TEXT,
        spend TEXT,
        conversions TEXT,
        date TEXT,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid, marketer_id, campaign_id, promoted_link_id, date)
    )
    """)

    # ---------------- SIMILARWEB ----------------

    cur.execute("""
    CREATE TABLE IF NOT EXISTS similarweb_connections (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT UNIQUE,
        domain TEXT,
        connected_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS similarweb_domain_overview (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        domain TEXT,
        date TEXT,
        visits TEXT,
        desktop_share TEXT,
        mobile_share TEXT,
        pages_per_visit TEXT,
        visit_duration TEXT,
        bounce_rate TEXT,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid, domain, date)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS similarweb_traffic_sources (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        domain TEXT,
        date TEXT,
        direct_share TEXT,
        referral_share TEXT,
        organic_search_share TEXT,
        paid_search_share TEXT,
        social_share TEXT,
        mail_share TEXT,
        display_share TEXT,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid, domain, date)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS similarweb_referrals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        domain TEXT,
        date TEXT,
        referring_domain TEXT,
        referral_share TEXT,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid, domain, date, referring_domain)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS similarweb_search_keywords (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        domain TEXT,
        date TEXT,
        keyword TEXT,
        search_volume TEXT,
        traffic_share TEXT,
        cpc TEXT,
        organic_vs_paid TEXT,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid, domain, date, keyword)
    )
    """)

    # ---------------- X (TWITTER) ----------------

    cur.execute("""
    CREATE TABLE IF NOT EXISTS x_connections (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT UNIQUE,
        x_user_id TEXT,
        username TEXT,
        access_token TEXT,
        refresh_token TEXT,
        expires_at TEXT,
        connected_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS x_users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        user_id TEXT,
        username TEXT,
        display_name TEXT,
        bio TEXT,
        location TEXT,
        followers_count INTEGER,
        following_count INTEGER,
        tweet_count INTEGER,
        profile_image_url TEXT,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid, user_id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS x_tweets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        tweet_id TEXT,
        author_id TEXT,
        text TEXT,
        like_count INTEGER,
        retweet_count INTEGER,
        reply_count INTEGER,
        media_ids TEXT,
        created_at TEXT,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid, tweet_id)
    )
    """)

    # ---------------- LINKEDIN MARKETING ----------------

    cur.execute("""
    CREATE TABLE IF NOT EXISTS linkedin_connections (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT UNIQUE,
        linkedin_member_id TEXT,
        access_token TEXT,
        refresh_token TEXT,
        expires_at TEXT,
        linkedin_version TEXT,
        connected_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS linkedin_ad_accounts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        account_id TEXT,
        name TEXT,
        status TEXT,
        type TEXT,
        currency TEXT,
        test TEXT,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid, account_id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS linkedin_campaigns (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        account_id TEXT,
        campaign_id TEXT,
        name TEXT,
        status TEXT,
        daily_budget TEXT,
        objective TEXT,
        start_date TEXT,
        end_date TEXT,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid, campaign_id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS linkedin_creatives (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        account_id TEXT,
        creative_id TEXT,
        campaign_id TEXT,
        intended_status TEXT,
        is_serving TEXT,
        review_status TEXT,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid, creative_id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS linkedin_ad_analytics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        account_id TEXT,
        pivot_type TEXT,
        pivot_value TEXT,
        impressions TEXT,
        clicks TEXT,
        cost_in_local_currency TEXT,
        date_start TEXT,
        date_end TEXT,
        raw_json TEXT,
        fetched_at TEXT,
        UNIQUE(uid, account_id, pivot_type, pivot_value, date_start, date_end)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS chartbeat_connections (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        uid          TEXT UNIQUE,
        host         TEXT,
        connected_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS stripe_connections (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        uid          TEXT UNIQUE,
        account_id   TEXT,
        display_name TEXT,
        connected_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS chartbeat_top_pages (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        uid          TEXT,
        path         TEXT,
        title        TEXT,
        concurrents  INTEGER,
        engaged_time REAL,
        page_views   INTEGER,
        visits       INTEGER,
        raw_json     TEXT,
        fetched_at   TEXT,
        UNIQUE(uid, path, fetched_at)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS chartbeat_page_engagement (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        uid                 TEXT,
        path                TEXT,
        title               TEXT,
        author              TEXT,
        section             TEXT,
        device              TEXT,
        referrer_type       TEXT,
        page_views          INTEGER,
        page_uniques        INTEGER,
        page_avg_time       REAL,
        page_total_time     REAL,
        page_avg_scroll     REAL,
        page_scroll_starts  INTEGER,
        page_views_quality  REAL,
        date                TEXT,
        raw_json            TEXT,
        fetched_at          TEXT,
        UNIQUE(uid, path, device, referrer_type, date)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS chartbeat_video_engagement (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        uid              TEXT,
        video_title      TEXT,
        video_path       TEXT,
        play_state       TEXT,
        video_plays      INTEGER,
        video_loads      INTEGER,
        video_play_rate  REAL,
        video_avg_time   REAL,
        raw_json         TEXT,
        fetched_at       TEXT,
        UNIQUE(uid, video_path, play_state, fetched_at)
    )
    """)

    # ---------------- USAGE : SYNC RUN HISTORY ----------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS sync_runs(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        source TEXT,
        sync_type TEXT,
        rows_synced INTEGER DEFAULT 0,
        started_at TEXT,
        finished_at TEXT,
        status TEXT,
        error TEXT
    )
    """)

    # ---------------- USAGE : DESTINATION PUSH LOGS ----------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS destination_push_logs(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        source TEXT,
        destination_type TEXT,
        rows_pushed INTEGER,
        pushed_at TEXT,
        status TEXT,
        error TEXT
    )
    """)

    # ---------------- USAGE : API CALL TRACKING ----------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS api_usage_logs(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        endpoint TEXT,
        method TEXT,
        created_at TEXT
    )
    """)
    # ---------------- QUICKBOOKS ----------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS quickbooks_config (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id TEXT,
        client_secret TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS quickbooks_auth (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        access_token TEXT,
        refresh_token TEXT,
        realm_id TEXT,
        expires_at REAL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # ---------------- XERO ----------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS xero_config (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id TEXT,
        client_secret TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS xero_auth (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        access_token TEXT,
        refresh_token TEXT,
        tenant_id TEXT,
        tenant_name TEXT,
        expires_at REAL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # ---------------- AMAZON SELLER ----------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS amazon_seller_config (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id TEXT,
        client_secret TEXT,
        seller_id TEXT,
        region TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS amazon_seller_auth (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        refresh_token TEXT,
        seller_id TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # ---------------- NEW RELIC ----------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS newrelic_auth (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        api_key TEXT,
        account_id TEXT,
        region TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    con.commit()
    con.close()

init_db()

# ---------------- IDENTITY ----------------

@app.route("/sync")
def sync():

    r = request.args.get("return_url")
    uid = request.cookies.get("uid") or str(uuid.uuid4())

    resp = make_response(redirect(f"{r}?uid={uid}"))
    resp.set_cookie("uid", uid, max_age=30*24*3600)

    return resp



IFRAME = """<script>
window.parent.postMessage({type:"IDENTITY_SYNC",uid:"{{uid}}"},"*");
</script>"""


@app.route("/iframe_sync")
def iframe_sync():

    uid = request.cookies.get("uid") or str(uuid.uuid4())

    resp = make_response(render_template_string(IFRAME, uid=uid))
    resp.set_cookie("uid", uid, max_age=30*24*3600)

    return resp



# ---------------- WEB TRACKING ----------------

@app.route("/record",methods=["POST"])
def record():

    d = request.get_json() or {}

    uid = d.get("uid")
    domain = d.get("domain")
    email = d.get("email")

    did = d.get("device_id")
    sid = d.get("session_id")

    event = d.get("event_type","page")
    meta = d.get("meta",{})


    if not uid or not domain:
        return jsonify({"error":"missing"}),400


    ua = request.headers.get("User-Agent")
    ip = request.remote_addr
    p = parse(ua)


    browser = p.browser.family
    os_name = p.os.family

    device = "Mobile" if p.is_mobile else "Tablet" if p.is_tablet else "Desktop"

    ts = datetime.datetime.now(IST).isoformat()


    con = get_db()
    cur = con.cursor()


    cur.execute("""
    INSERT INTO visits
    (uid,domain,browser,os,device,ip,
    screen,language,timezone,
    referrer,page_url,user_agent,ts)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
    """,(uid,domain,browser,os_name,device,ip,
    meta.get("screen"),meta.get("language"),meta.get("timezone"),
    meta.get("referrer"),meta.get("page_url"),ua,ts))


    cur.execute("""
    INSERT INTO identity_map
    (uid,email,device_id,session_id,external_id,created_at)
    VALUES(?,?,?,?,?,?)
    """,(uid,email,did,sid,None,ts))


    cur.execute("""
    INSERT INTO web_events
    (uid,domain,event,device_id,session_id,meta,ts)
    VALUES(?,?,?,?,?,?,?)
    """,(uid,domain,event,did,sid,json.dumps(meta),ts))


    con.commit()
    con.close()


    return jsonify({"status":"stored"})



# ---------------- PROFILE ----------------

@app.route("/profile",methods=["POST"])
def profile():

    d = request.get_json() or {}

    uid = d.get("uid")
    email = d.get("email")


    if not uid:
        return jsonify({"error":"uid"}),400


    con = get_db()
    cur = con.cursor()


    cur.execute("""
    UPDATE visits SET
    name=?,age=?,gender=?,city=?,country=?,profession=?
    WHERE uid=?
    """,(d.get("name"),d.get("age"),d.get("gender"),
         d.get("city"),d.get("country"),d.get("profession"),uid))


    if email:
        cur.execute("UPDATE identity_map SET email=? WHERE uid=?",(email,uid))

    con.commit()
    con.close()


    return jsonify({"status":"saved"})



# ---------------- FILE UPLOAD ----------------

@app.route("/upload",methods=["POST"])
def upload():

    uid = request.form.get("uid")
    f = request.files.get("file")


    if not f:
        return "No file",400


    path = os.path.join(UPLOAD_FOLDER,f.filename)
    f.save(path)


    c = ""


    if f.filename.endswith((".csv",".xlsx")):

        df = pd.read_csv(path) if f.filename.endswith(".csv") else pd.read_excel(path)
        c = df.to_json()


    elif f.filename.endswith(".json"):

        c = json.dumps(json.load(open(path)))


    elif f.filename.endswith(".xml"):

        c = json.dumps(xmltodict.parse(open(path).read()))


    else:

        c = parser.from_file(path).get("content","")


    ts = datetime.datetime.now(IST).isoformat()


    con = get_db()
    cur = con.cursor()


    cur.execute("""
    INSERT INTO file_data
    VALUES(NULL,?,?,?,?,?)
    """,(uid,f.filename,f.filename.split(".")[-1],c,ts))


    con.commit()
    con.close()


    return jsonify({"status":"uploaded"})



# ---------------- API COLLECT ----------------

@app.route("/api/collect",methods=["POST"])
def api():

    d = request.get_json()
    ts = datetime.datetime.now(IST).isoformat()


    con = get_db()
    cur = con.cursor()


    cur.execute("""
    INSERT INTO api_data VALUES(NULL,?,?,?,?)
    """,(d.get("source"),d.get("endpoint"),
         json.dumps(d.get("data")),ts))


    con.commit()
    con.close()


    return jsonify({"status":"ok"})



# ---------------- FORM ----------------

@app.route("/form/submit",methods=["POST"])
def form():

    d = request.get_json()
    ts = datetime.datetime.now(IST).isoformat()


    con = get_db()
    cur = con.cursor()


    cur.execute("""
    INSERT INTO form_data VALUES(NULL,?,?,?,?)
    """,(d.get("uid"),d.get("form"),json.dumps(d),ts))


    con.commit()
    con.close()


    return jsonify({"status":"saved"})

@app.route("/api/status/<source>")
def connector_status(source):

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    return jsonify(_resolve_connector_contract(uid, source))

# ---------------- GOOGLE OAUTH ----------------

@app.route("/google/connect")
def google_connect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    source = request.args.get("source")

    if not source:
        return "Missing source parameter", 400

    # Fetch Google App Credentials from DB
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT client_id, client_secret
        FROM connector_configs
        WHERE uid=? AND connector=?
    """, (uid, source))

    row = fetchone_secure(cur)

    con.close()

    if not row:
        return "Google App credentials not saved", 400

    client_id = row["client_id"]
    client_secret = row["client_secret"]

    # Define scopes dynamically (for now only gmail)
    if source == "gmail":
        scopes = ["https://www.googleapis.com/auth/gmail.readonly"]

    elif source == "drive":
        scopes = ["https://www.googleapis.com/auth/drive.readonly"]

    elif source == "calendar":
        scopes = ["https://www.googleapis.com/auth/calendar.readonly"]

    elif source == "sheets":
        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

    elif source == "forms":
        scopes = [
            "https://www.googleapis.com/auth/forms.responses.readonly",
            "https://www.googleapis.com/auth/forms.body.readonly"
        ]

    elif source == "classroom":
        scopes = [
            "https://www.googleapis.com/auth/classroom.courses.readonly",
            "https://www.googleapis.com/auth/classroom.rosters.readonly",
            "https://www.googleapis.com/auth/classroom.coursework.students.readonly",
            "https://www.googleapis.com/auth/classroom.student-submissions.students.readonly",
            "https://www.googleapis.com/auth/classroom.announcements.readonly",
    ]
        
    elif source == "contacts":
        scopes = ["https://www.googleapis.com/auth/contacts.readonly"]

    elif source == "tasks":
        scopes = ["https://www.googleapis.com/auth/tasks.readonly"]

    elif source == "ga4":
        scopes = ["https://www.googleapis.com/auth/analytics.readonly"]

    elif source == "search-console":
        scopes = ["https://www.googleapis.com/auth/webmasters.readonly"]

    elif source == "youtube":
        scopes = ["https://www.googleapis.com/auth/youtube.readonly"]

    else:
        return "Unsupported Google connector", 400

    flow = Flow.from_client_config(
        {
            "web": {
                "client_id": client_id,
                "client_secret": client_secret,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token"
            }
        },
        scopes=scopes,
        redirect_uri=request.host_url.rstrip("/") + "/oauth2callback"
    )

    auth_url, state = flow.authorization_url(
        access_type="offline",
        prompt="consent",
        include_granted_scopes="true",
        state=source
    )

    return redirect(auth_url)


@app.route("/oauth2callback")
def google_callback():

    code = request.args.get("code")

    if not code:
        return "No code", 400

    source = request.args.get("state") or "gmail"
    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    # Fetch Google App Credentials from DB
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT client_id, client_secret
        FROM connector_configs
        WHERE uid=? AND connector=?
    """, (uid, source))

    row = fetchone_secure(cur)

    if not row:
        return "Google App credentials not saved", 400

    client_id = row["client_id"]
    client_secret = row["client_secret"]

    # Define scopes
    if source == "gmail":
        scopes = ["https://www.googleapis.com/auth/gmail.readonly"]

    elif source == "drive":
        scopes = ["https://www.googleapis.com/auth/drive.readonly"]

    elif source == "calendar":
        scopes = ["https://www.googleapis.com/auth/calendar.readonly"]

    elif source == "sheets":
        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    
    elif source == "forms":
        scopes = [
            "https://www.googleapis.com/auth/forms.responses.readonly",
            "https://www.googleapis.com/auth/forms.body.readonly"
        ]

    elif source == "classroom":
        scopes = [
            "https://www.googleapis.com/auth/classroom.courses.readonly",
            "https://www.googleapis.com/auth/classroom.rosters.readonly",
            "https://www.googleapis.com/auth/classroom.coursework.students.readonly",
            "https://www.googleapis.com/auth/classroom.student-submissions.students.readonly",
            "https://www.googleapis.com/auth/classroom.announcements.readonly",
    ]
        
    elif source == "contacts":
        scopes = ["https://www.googleapis.com/auth/contacts.readonly"]

    elif source == "tasks":
        scopes = ["https://www.googleapis.com/auth/tasks.readonly"]

    elif source == "ga4":
        scopes = ["https://www.googleapis.com/auth/analytics.readonly"]

    elif source == "search-console":
        scopes = ["https://www.googleapis.com/auth/webmasters.readonly"]

    elif source == "youtube":
        scopes = ["https://www.googleapis.com/auth/youtube.readonly"]

    else:
        con.close()
        return "Unsupported Google connector", 400

    flow = Flow.from_client_config(
        {
            "web": {
                "client_id": client_id,
                "client_secret": client_secret,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token"
            }
        },
        scopes=scopes,
        redirect_uri=request.host_url.rstrip("/") + "/oauth2callback"
    )

    flow.fetch_token(
        code=code,
        include_client_id=True
    )

    creds = flow.credentials

    try:

        # Remove old token
        cur.execute("""
            DELETE FROM google_accounts
            WHERE uid=? AND source=?
        """, (uid, source))

        # Save new token
        cur.execute("""
            INSERT INTO google_accounts
            (uid, source, access_token, refresh_token, scopes, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            uid,
            source,
            creds.token,
            creds.refresh_token,
            ",".join(creds.scopes),
            datetime.datetime.now(datetime.UTC).isoformat()
        ))

        # Enable connector
        cur.execute("""
            INSERT OR REPLACE INTO google_connections
            (uid, source, enabled)
            VALUES (?, ?, 1)
        """, (uid, source))

        con.commit()
        con.close()

    finally:
        con.close()

    return redirect(
        f"http://localhost:3000/connectors/{source}"
    )

@app.route("/ai/connector/<connector>/<action>", methods=["GET", "POST"])
def ai_connector_router(connector, action):
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401

    try:
        # -------- GOOGLE SPECIAL CASE --------
        if connector == "google_gmail":
            if action == "sync":
                return sync_gmail_route()
            elif action == "connect":
                # Gmail uses OAuth → redirect flow
                return redirect("/google/connect/gmail")

        if connector == "google_drive":
            if action == "sync":
                return jsonify(sync_drive_files(uid))

        if connector == "google_calendar":
            if action == "sync":
                return jsonify(sync_calendar_files(uid))

        # -------- GENERIC CONNECTORS --------
        module_name = f"backend.connectors.{connector}"
        module = __import__(module_name, fromlist=["*"])

        func_name = f"{action}_{connector}"

        if hasattr(module, func_name):
            func = getattr(module, func_name)

            try:
                result = func(uid)
            except TypeError:
                result = func()

            return jsonify(result)

        return jsonify({
            "error": f"{action} not supported for {connector}"
        }), 400

    except Exception as e:
        return jsonify({
            "error": str(e)
        }), 500

@app.route("/connectors/gmail/connect")
def gmail_connect_alt():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401

    if not _has_connector_config(uid, "gmail"):
        return jsonify({"connected": False, "error": "credentials_required"}), 400

    auth_url = "/google/connect?source=gmail"
    if _is_internal_ai_request():
        return jsonify({
            "connected": False,
            "auth_required": True,
            "redirect": auth_url,
        })
    return redirect(auth_url)

@app.route("/connectors/gmail/status")
def gmail_status_alt():
    return gmail_status()

    if action == "status":
        try:
            return call_connector_route(f"/connectors/{connector}/status", uid)
        except:
            return jsonify({"status": "unknown"})

@app.route("/ai/sync/<source>")
def ai_sync(source):
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401

    # call existing sync route internally
    res = call_existing_sync(source, uid)

    # AFTER sync, read rows from DB or router
    rows_pushed = get_last_sync_rows(uid, source)

    return jsonify({
        "status": "ok",
        "rows_pushed": rows_pushed
    })
        
# ---------------- DRIVE ----------------

@app.route("/google/sync/drive")
def sync_drive():

    from backend.connectors.google_drive import sync_drive_files

    return jsonify(sync_drive_files())

# ---------------- DRIVE SAVE APP CREDENTIALS ----------------

@app.route("/connectors/drive/save_app", methods=["POST"])
def drive_save_app():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = encrypt_payload(request.get_json())

    client_id = data.get("client_id")
    client_secret = data.get("client_secret")

    if not client_id or not client_secret:
        return jsonify({"error": "Client ID and Secret required"}), 400

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, client_id, client_secret, created_at)
        VALUES (?, 'drive', ?, ?, ?)
    """, (
        uid,
        client_id,
        client_secret,
        datetime.datetime.now(datetime.UTC).isoformat()
    ))

    con.commit()
    con.close()

    ensure_connector_initialized(uid, "drive")
    return jsonify({"status": "saved"})

# ---------------- SHEETS ----------------

@app.route("/google/sync/sheets")
def sync_sheets():
    return jsonify(sync_sheets_files())

# ---------------- SHEETS SAVE APP ----------------

@app.route("/connectors/sheets/save_app", methods=["POST"])
def sheets_save_app():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = encrypt_payload(request.get_json())

    client_id = data.get("client_id")
    client_secret = data.get("client_secret")

    if not client_id or not client_secret:
        return jsonify({"error": "Client ID and Secret required"}), 400

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, client_id, client_secret, created_at)
        VALUES (?, 'sheets', ?, ?, ?)
    """, (
        uid,
        client_id,
        client_secret,
        datetime.datetime.now(datetime.UTC).isoformat()
    ))

    con.commit()
    con.close()

    ensure_connector_initialized(uid, "sheets")
    return jsonify({"status": "saved"})

# ---------------- SHEETS DISCONNECT ----------------

@app.route("/google/disconnect/sheets")
def disconnect_sheets():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source='sheets'
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "ok"})

# ---------------- SHEETS JOB GET ----------------

@app.route("/connectors/sheets/job/get")
def sheets_job_get():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='sheets'
    """, (uid,))

    row = fetchone_secure(cur)
    con.close()

    if not row:
        return jsonify({"exists": False})

    return jsonify({
        "exists": True,
        "sync_type": row[0],
        "schedule_time": row[1]
    })


# ---------------- SHEETS JOB SAVE ----------------

@app.route("/connectors/sheets/job/save", methods=["POST"])
def sheets_job_save():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
        
    data = request.get_json()

    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time, enabled)
        VALUES (?, 'sheets', ?, ?, 1)
    """, (uid, sync_type, schedule_time))

    con.commit()
    con.close()

    return jsonify({"status": "ok"})


# ---------- GA4 Sync ----------
from backend.connectors.google_ga4 import sync_ga4 as run_ga4_sync


@app.route("/google/sync/ga4")
def google_ga4_sync():

    try:
        result = sync_ga4()

        # Always return JSON
        return jsonify(result), 200

    except Exception as e:

        print("[GA4 SYNC ERROR]", str(e), flush=True)

        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

# ---------------- GA4 SAVE APP ----------------

@app.route("/connectors/ga4/save_app", methods=["POST"])
def ga4_save_app():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = encrypt_payload(request.get_json())

    client_id = data.get("client_id")
    client_secret = data.get("client_secret")
    property_id = data.get("property_id")

    if not client_id or not client_secret or not property_id:
        return jsonify({"error": "Client ID, Secret and Property ID required"}), 400

    config = {
        "property_id": property_id
    }

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, client_id, client_secret, config_json, created_at)
        VALUES (?, 'ga4', ?, ?, ?, ?)
    """, (
        uid,
        client_id,
        client_secret,
        json.dumps(config),
        datetime.datetime.now(datetime.UTC).isoformat()
    ))

    con.commit()
    con.close()

    ensure_connector_initialized(uid, "ga4")
    return jsonify({"status": "saved"})

# ---------------- GA4 JOB GET ----------------

@app.route("/connectors/ga4/job/get")
def ga4_job_get():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='ga4'
    """, (uid,))

    row = fetchone_secure(cur)
    con.close()

    if not row:
        return jsonify({"exists": False})

    return jsonify({
        "exists": True,
        "sync_type": row[0],
        "schedule_time": row[1]
    })


# ---------------- GA4 JOB SAVE ----------------

@app.route("/connectors/ga4/job/save", methods=["POST"])
def ga4_job_save():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    
    data = request.get_json()

    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time, enabled)
        VALUES (?, 'ga4', ?, ?, 1)
    """, (uid, sync_type, schedule_time))

    con.commit()
    con.close()

    return jsonify({"status": "ok"})

# ---------- SEARCH CONSOLE ----------
@app.route("/connectors/search-console/sync")
def gsc_sync():

    site = request.args.get("site")
    sync_type = request.args.get("sync_type", "incremental")

    if not site:
        return jsonify({"status": "error", "message": "Missing site URL"})

    result = sync_search_console(site, sync_type)

    return jsonify(result)

# ---------------- SEARCH CONSOLE SAVE APP ----------------

@app.route("/connectors/search-console/save_app", methods=["POST"])
def search_console_save_app():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = encrypt_payload(request.get_json())

    client_id = data.get("client_id")
    client_secret = data.get("client_secret")

    if not client_id or not client_secret:
        return jsonify({"error": "Client ID and Secret required"}), 400

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, client_id, client_secret, created_at)
        VALUES (?, 'search-console', ?, ?, ?)
    """, (
        uid,
        client_id,
        client_secret,
        datetime.datetime.now(datetime.UTC).isoformat()
    ))

    con.commit()
    con.close()

    ensure_connector_initialized(uid, "search-console")
    return jsonify({"status": "saved"})

# ---------------- SEARCH CONSOLE JOB GET ----------------

@app.route("/connectors/search-console/job/get")
def gsc_job_get():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='search-console'
    """, (uid,))

    row = fetchone_secure(cur)
    con.close()

    if not row:
        return jsonify({"exists": False})

    return jsonify({
        "exists": True,
        "sync_type": row[0],
        "schedule_time": row[1]
    })


# ---------------- SEARCH CONSOLE JOB SAVE ----------------

@app.route("/connectors/search-console/job/save", methods=["POST"])
def gsc_job_save():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    
    data = request.get_json()

    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time, enabled)
        VALUES (?, 'search-console', ?, ?, 1)
    """, (uid, sync_type, schedule_time))

    con.commit()
    con.close()

    return jsonify({"status": "ok"})

@app.route("/api/status/search-console")
def search_console_status():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='search-console'
    """, (uid,))

    row = fetchone_secure(cur)
    con.close()

    return jsonify({
        "connected": True if row and row[0] == 1 else False
    })

@app.route("/google/disconnect/search-console")
def disconnect_search_console():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source='search-console'
    """, (uid,))

    cur.execute("""
        DELETE FROM google_accounts
        WHERE uid=? AND source='search-console'
    """, (uid,))

    cur.execute("""
        UPDATE connector_jobs
        SET enabled=0
        WHERE uid=? AND source='search-console'
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "ok"})

@app.route("/connectors/pagespeed/sync")
def pagespeed_sync():

    url = request.args.get("url")
    sync_type = request.args.get("sync_type", "incremental")

    if not url:
        return jsonify({"error": "url required"}), 400

    result = sync_pagespeed(url, sync_type)

    return jsonify(result)

# ---------------- GOOGLE PAGESPEED CONNECT ----------------
@app.route("/connectors/pagespeed/connect")
def pagespeed_connect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # ensure API key exists first
    cur.execute("""
        SELECT api_key
        FROM connector_configs
        WHERE uid=? AND connector='pagespeed'
        LIMIT 1
    """, (uid,))

    key = cur.fetchone()

    if not key:
        con.close()
        return jsonify({
            "status": "error",
            "message": "Save API key first"
        }), 400

    # enable only after credential exists
    cur.execute("""
        INSERT OR REPLACE INTO google_connections
        (uid, source, enabled)
        VALUES (?, 'pagespeed', 1)
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "connected"})

@app.route("/connectors/pagespeed/disconnect")
def pagespeed_disconnect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source='pagespeed'
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "disconnected"})

@app.route("/connectors/pagespeed/save_config", methods=["POST"])
def pagespeed_save_config():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = encrypt_payload(request.get_json())

    api_key = data.get("api_key")

    if not api_key:
        return jsonify({"error": "API key required"}), 400

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, api_key, created_at)
        VALUES (?, 'pagespeed', ?, datetime('now'))
    """, (uid, api_key))

    con.commit()
    con.close()

    return jsonify({"status": "saved"})

@app.route("/api/status/pagespeed")
def pagespeed_status():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # ---------- connection ----------
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='pagespeed'
        LIMIT 1
    """, (uid,))
    row = fetchone_secure(cur)

    connected = bool(row and row[0] == 1)

    # ---------- api key ----------
    cur.execute("""
        SELECT api_key
        FROM connector_configs
        WHERE uid=? AND connector='pagespeed'
        LIMIT 1
    """, (uid,))
    key_row = fetchone_secure(cur)

    api_key_saved = key_row is not None

    con.close()

    return jsonify({
        "connected": connected,
        "api_key_saved": api_key_saved
    })

@app.route("/connectors/pagespeed/job/get")
def pagespeed_job_get():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='pagespeed'
    """, (uid,))

    row = fetchone_secure(cur)
    con.close()

    if not row:
        return jsonify({"exists": False})

    return jsonify({
        "exists": True,
        "sync_type": row[0],
        "schedule_time": row[1]
    })

@app.route("/connectors/pagespeed/job/save", methods=["POST"])
def pagespeed_job_save():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    
    data = request.get_json()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time)
        VALUES (?, 'pagespeed', ?, ?)
    """, (
        uid,
        data.get("sync_type"),
        data.get("schedule_time")
    ))

    con.commit()
    con.close()

    return jsonify({"status": "job_saved"})

@app.route("/google/sync/forms")
def sync_forms_api():

    try:
        return jsonify(sync_forms())

    except Exception as e:
        return jsonify({
            "status": "failed",
            "error": str(e)
        }), 500

# ---------------- FORMS SAVE APP ----------------

@app.route("/connectors/forms/save_app", methods=["POST"])
def forms_save_app():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    data = encrypt_payload(request.get_json())

    client_id = data.get("client_id")
    client_secret = data.get("client_secret")

    if not client_id or not client_secret:
        return jsonify({"error": "Client ID and Secret required"}), 400

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, client_id, client_secret, config_json, created_at)
        VALUES (?, 'forms', ?, ?, ?, ?)
    """, (
        uid,
        client_id,
        client_secret,
        "{}",
        datetime.datetime.now(datetime.UTC).isoformat()
    ))

    con.commit()
    con.close()

    ensure_connector_initialized(uid, "forms")

    return jsonify({"status": "saved"})

# ---------------- FORMS DISCONNECT ----------------

@app.route("/google/disconnect/forms")
def disconnect_forms():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source='forms'
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "ok"})

# ---------------- FORMS JOB GET ----------------

@app.route("/connectors/forms/job/get")
def forms_job_get():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='forms'
    """, (uid,))

    row = fetchone_secure(cur)
    con.close()

    if not row:
        return jsonify({"exists": False})

    return jsonify({
        "exists": True,
        "sync_type": row[0],
        "schedule_time": row[1]
    })


# ---------------- FORMS JOB SAVE ----------------

@app.route("/connectors/forms/job/save", methods=["POST"])
def forms_job_save():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    
    data = request.get_json()

    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time, enabled)
        VALUES (?, 'forms', ?, ?, 1)
    """, (uid, sync_type, schedule_time))

    con.commit()
    con.close()

    return jsonify({"status": "ok"})

# ---------------- CALENDAR ----------------

@app.route("/google/sync/calendar")
def sync_calendar_files():

    from backend.connectors.google_calendar import sync_calendar_files

    return jsonify(sync_calendar_files())
    
@app.route("/google/sync/gmail")
def sync_gmail_route():

    try:

        result = sync_gmail()

        return jsonify({
            "status": "ok",
            "messages": result["messages"]
        })

    except Exception as e:

        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


@app.route("/google/disconnect/gmail")
def google_disconnect_gmail():

    uid = getattr(g, "user_id", None)
    source = "gmail"
    print("DISCONNECT USER:", g.user_id, flush=True)
    con = get_db()
    cur = con.cursor()

    try:

        # Disable connection
        cur.execute("""
            UPDATE google_connections
            SET enabled = 0
            WHERE uid=? AND source=?
        """, (uid, source))


        # Disable scheduled job
        cur.execute("""
            UPDATE connector_jobs
            SET enabled = 0
            WHERE uid=? AND source=?
        """, (uid, source))


        # Remove OAuth tokens
        cur.execute("""
            DELETE FROM google_accounts
            WHERE uid=? AND source=?
        """, (uid, source))


        con.commit()

    except Exception as e:

        con.rollback()

        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500
 
    finally:
        con.close()

    return jsonify({
        "status": "ok",
        "message": "Gmail disconnected successfully"
    })

# ---------------- GMAIL SAVE APP CREDENTIALS ----------------

@app.route("/connectors/gmail/save_app", methods=["POST"])
def gmail_save_app():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = encrypt_payload(request.get_json())

    client_id = data.get("client_id")
    client_secret = data.get("client_secret")

    if not client_id or not client_secret:
        return jsonify({"error": "Client ID and Secret required"}), 400

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, client_id, client_secret, created_at)
        VALUES (?, 'gmail', ?, ?, ?)
    """, (
        uid,
        client_id,
        client_secret,
        datetime.datetime.now(datetime.UTC).isoformat()
    ))

    con.commit()
    con.close()

    ensure_connector_initialized(uid, "gmail")
    return jsonify({"status": "saved"})

# ---------------- CALENDAR SAVE APP ----------------

@app.route("/connectors/calendar/save_app", methods=["POST"])
def calendar_save_app():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = encrypt_payload(request.get_json())

    client_id = data.get("client_id")
    client_secret = data.get("client_secret")

    if not client_id or not client_secret:
        return jsonify({"error": "Client ID and Secret required"}), 400

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, client_id, client_secret, created_at)
        VALUES (?, 'calendar', ?, ?, ?)
    """, (
        uid,
        client_id,
        client_secret,
        datetime.datetime.now(datetime.UTC).isoformat()
    ))

    con.commit()
    con.close()

    ensure_connector_initialized(uid, "calendar")
    return jsonify({"status": "saved"})

@app.route("/google/disconnect/drive")
def disconnect_drive():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # Disable connection
    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source='drive'
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "ok"})

# ---------------- GA4 DISCONNECT ----------------

@app.route("/google/disconnect/ga4")
def google_disconnect_ga4():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # Disable connection
    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source='ga4'
    """, (uid,))

    # Remove token
    cur.execute("""
        DELETE FROM google_accounts
        WHERE uid=? AND source='ga4'
    """, (uid,))

    # Disable scheduled job
    cur.execute("""
        UPDATE connector_jobs
        SET enabled=0
        WHERE uid=? AND source='ga4'
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "ok"})

# ---------------- CALENDAR DISCONNECT ----------------

@app.route("/google/disconnect/calendar")
def disconnect_calendar():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source='calendar'
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "ok"})

# ---------------- TASKS DISCONNECT ----------------

@app.route("/google/disconnect/tasks")
def disconnect_tasks():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source='tasks'
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "ok"})

@app.route("/google/disconnect/<source>")
def google_disconnect(source):

    print("DISCONNECT CALLED:", source, flush=True)

    con = sqlite3.connect(DB)
    cur = con.cursor()

    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE source=?
    """, (source,))

    con.commit()
    con.close()

    return jsonify({"status": "disconnected"})

#============== connectors ==============
@app.route("/connectors/classroom/sync")
def classroom_sync():

    sync_type = request.args.get("sync_type", "incremental")

    result = sync_classroom(sync_type)

    return jsonify(result)

@app.route("/connectors/classroom/save_app", methods=["POST"])
def classroom_save_app():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = encrypt_payload(request.get_json())

    client_id = data.get("client_id")
    client_secret = data.get("client_secret")

    if not client_id or not client_secret:
        return jsonify({"error": "Missing credentials"}), 400

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, client_id, client_secret, created_at)
        VALUES (?, 'classroom', ?, ?, ?)
    """, (
        uid,
        client_id,
        client_secret,
        datetime.datetime.now(datetime.UTC).isoformat()
    ))

    con.commit()
    con.close()

    ensure_connector_initialized(uid, "classroom")
    return jsonify({"status": "saved"})

@app.route("/google/disconnect/classroom")
def disconnect_classroom():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source='classroom'
    """, (uid,))

    cur.execute("""
        DELETE FROM google_accounts
        WHERE uid=? AND source='classroom'
    """, (uid,))

    cur.execute("""
        UPDATE connector_jobs
        SET enabled=0
        WHERE uid=? AND source='classroom'
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "ok"})

@app.route("/connectors/classroom/job/get")
def classroom_job_get():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='classroom'
    """, (uid,))

    row = fetchone_secure(cur)
    con.close()

    if not row:
        return jsonify({"exists": False})

    return jsonify({
        "exists": True,
        "sync_type": row[0],
        "schedule_time": row[1]
    })

@app.route("/connectors/classroom/job/save", methods=["POST"])
def classroom_job_save():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = request.get_json()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time)
        VALUES (?, 'classroom', ?, ?)
    """, (
        uid,
        data.get("sync_type","incremental"),
        data.get("schedule_time")
    ))

    con.commit()
    con.close()

    return jsonify({"status":"job_saved"})

@app.route("/google/sync/tasks")
def google_sync_tasks():

    try:
        result = sync_tasks()
        return jsonify(result)

    except Exception as e:

        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

# ---------------- TASKS SAVE APP ----------------

@app.route("/connectors/tasks/save_app", methods=["POST"])
def tasks_save_app():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = encrypt_payload(request.get_json())

    client_id = data.get("client_id")
    client_secret = data.get("client_secret")

    if not client_id or not client_secret:
        return jsonify({"error": "Client ID and Secret required"}), 400

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, client_id, client_secret, created_at)
        VALUES (?, 'tasks', ?, ?, ?)
    """, (
        uid,
        client_id,
        client_secret,
        datetime.datetime.now(datetime.UTC).isoformat()
    ))

    con.commit()
    con.close()

    ensure_connector_initialized(uid, "tasks")
    return jsonify({"status": "saved"})

# ---------------- TASKS JOB GET ----------------

@app.route("/connectors/tasks/job/get")
def tasks_job_get():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='tasks'
    """, (uid,))

    row = fetchone_secure(cur)
    con.close()

    if not row:
        return jsonify({"exists": False})

    return jsonify({
        "exists": True,
        "sync_type": row[0],
        "schedule_time": row[1]
    })


# ---------------- TASKS JOB SAVE ----------------

@app.route("/connectors/tasks/job/save", methods=["POST"])
def tasks_job_save():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = request.get_json()

    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time, enabled)
        VALUES (?, 'tasks', ?, ?, 1)
    """, (uid, sync_type, schedule_time))

    con.commit()
    con.close()

    return jsonify({"status": "ok"})

@app.route("/google/sync/contacts")
def sync_contacts_api():
    print("[SERVER] Triggering contacts sync", flush=True)
    return jsonify(sync_contacts())

# ---------------- CONTACTS SAVE APP ----------------

@app.route("/connectors/contacts/save_app", methods=["POST"])
def contacts_save_app():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = encrypt_payload(request.get_json())

    client_id = data.get("client_id")
    client_secret = data.get("client_secret")

    if not client_id or not client_secret:
        return jsonify({"error": "Client ID and Secret required"}), 400

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, client_id, client_secret, created_at)
        VALUES (?, 'contacts', ?, ?, ?)
    """, (
        uid,
        client_id,
        client_secret,
        datetime.datetime.now(datetime.UTC).isoformat()
    ))

    con.commit()
    con.close()

    ensure_connector_initialized(uid, "contacts")
    return jsonify({"status": "saved"})

# ---------------- CONTACTS DISCONNECT ----------------

@app.route("/google/disconnect/contacts")
def disconnect_contacts():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source='contacts'
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "ok"})

# ---------------- CONTACTS JOB GET ----------------

@app.route("/connectors/contacts/job/get")
def contacts_job_get():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='contacts'
    """, (uid,))

    row = fetchone_secure(cur)
    con.close()

    if not row:
        return jsonify({"exists": False})

    return jsonify({
        "exists": True,
        "sync_type": row[0],
        "schedule_time": row[1]
    })


# ---------------- CONTACTS JOB SAVE ----------------

@app.route("/connectors/contacts/job/save", methods=["POST"])
def contacts_job_save():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = request.get_json()

    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time, enabled)
        VALUES (?, 'contacts', ?, ?, 1)
    """, (uid, sync_type, schedule_time))

    con.commit()
    con.close()

    return jsonify({"status": "ok"})

# ---------------- GOOGLE GCS SYNC ----------------

@app.route("/google/sync/gcs")
def google_sync_gcs():

    try:
        from backend.connectors.google_gcs import sync_gcs

        result = sync_gcs()

        return jsonify(result)

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500
    
# ---------------- GCS DISCONNECT ----------------

@app.route("/google/disconnect/gcs")
def disconnect_gcs():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source='gcs'
    """, (uid,))

    cur.execute("""
        DELETE FROM google_accounts
        WHERE uid=? AND source='gcs'
    """, (uid,))

    cur.execute("""
        UPDATE connector_jobs
        SET enabled=0
        WHERE uid=? AND source='gcs'
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "ok"})

@app.route("/connectors/gcs/job/get")
def gcs_job_get():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='gcs'
    """, (uid,))

    row = fetchone_secure(cur)
    con.close()

    if not row:
        return jsonify({"exists": False})

    return jsonify({
        "exists": True,
        "sync_type": row[0],
        "schedule_time": row[1]
    })

@app.route("/connectors/gcs/job/save", methods=["POST"])
def gcs_job_save():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = request.get_json()

    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time)
        VALUES (?, 'gcs', ?, ?)
    """, (uid, sync_type, schedule_time))

    con.commit()
    con.close()

    return jsonify({"status": "job_saved"})


@app.route("/connectors/gcs/save_app", methods=["POST"])
def gcs_save_app():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = encrypt_payload(request.get_json())

    client_id = data.get("client_id")
    client_secret = data.get("client_secret")

    if not client_id or not client_secret:
        return jsonify({"error":"missing"}),400

    con=get_db()
    cur=con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_configs
        (uid,connector,client_id,client_secret,created_at)
        VALUES (?,?,?, ?,?)
    """,(
        uid,
        "gcs",
        client_id,
        client_secret,
        datetime.datetime.now(datetime.UTC).isoformat()
    ))

    con.commit()
    con.close()

    ensure_connector_initialized(uid, "gcs")
    return jsonify({"status":"saved"})

@app.route("/google/sync/webfonts", methods=["GET", "POST"])
def google_sync_webfonts():

    try:

        result = sync_webfonts()

        return jsonify({
            "status": "ok",
            "data": result
        })

    except Exception as e:

        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500
    
from backend.connectors.google_webfonts import sync_webfonts

@app.route("/connectors/webfonts/sync")
def webfonts_sync_route():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='webfonts'
    """, (uid,))

    row = fetchone_secure(cur)
    con.close()

    if not row or row[0] != 1:
        return jsonify({"error": "WebFonts not connected"}), 400

    result = sync_webfonts()

    return jsonify(result)

# ---------------- GOOGLE WEBFONTS CONNECT ----------------

@app.route("/connectors/webfonts/connect")
def webfonts_connect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO google_connections (uid, source, enabled)
        VALUES (?, 'webfonts', 1)
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "connected"})

# ---------------- GOOGLE WEBFONTS DISCONNECT ----------------

@app.route("/connectors/webfonts/disconnect")
def webfonts_disconnect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        UPDATE google_connections
        SET enabled = 0
        WHERE uid=? AND source='webfonts'
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "disconnected"})

@app.route("/api/status/webfonts")
def webfonts_status():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # ---------- connection ----------
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='webfonts'
        LIMIT 1
    """, (uid,))
    row = fetchone_secure(cur)

    connected = bool(row and row[0] == 1)

    # ---------- API KEY ----------
    cur.execute("""
        SELECT api_key
        FROM connector_configs
        WHERE uid=? AND connector='webfonts'
        LIMIT 1
    """, (uid,))

    key_row = fetchone_secure(cur)

    api_key_saved = bool(
        key_row and key_row[0]
    )

    con.close()

    return jsonify({
        "connected": connected,
        "api_key_saved": api_key_saved
    })

@app.route("/connectors/webfonts/job/get")
def webfonts_job_get():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='webfonts'
    """, (uid,))

    row = fetchone_secure(cur)
    con.close()

    if not row:
        return jsonify({"exists": False})

    return jsonify({
        "exists": True,
        "sync_type": row[0],
        "schedule_time": row[1]
    })

@app.route("/connectors/webfonts/job/save", methods=["POST"])
def webfonts_job_save():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = request.get_json()

    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time)
        VALUES (?, 'webfonts', ?, ?)
    """, (uid, sync_type, schedule_time))

    con.commit()
    con.close()

    return jsonify({"status": "job_saved"})

# ---------------- GOOGLE WEBFONTS SAVE CONFIG ----------------

@app.route("/connectors/webfonts/save_config", methods=["POST"])
def webfonts_save_config():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = encrypt_payload(request.get_json())

    api_key = data.get("api_key")

    if not api_key:
        return jsonify({"error": "API key required"}), 400

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, api_key, created_at)
        VALUES (?, 'webfonts', ?, datetime('now'))
    """, (uid, api_key))

    con.commit()
    con.close()

    return jsonify({"status": "saved"})

@app.route("/connectors/youtube/sync")
def youtube_sync():

    sync_type = request.args.get("sync_type", "incremental")

    result = sync_youtube(sync_type)

    return jsonify(result)

# ---------------- YOUTUBE SAVE APP ----------------

@app.route("/connectors/youtube/save_app", methods=["POST"])
def youtube_save_app():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = encrypt_payload(request.get_json())

    client_id = data.get("client_id")
    client_secret = data.get("client_secret")

    if not client_id or not client_secret:
        return jsonify({"error": "Client ID and Secret required"}), 400

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, client_id, client_secret, created_at)
        VALUES (?, 'youtube', ?, ?, ?)
    """, (
        uid,
        client_id,
        client_secret,
        datetime.datetime.now(datetime.UTC).isoformat()
    ))

    con.commit()
    con.close()

    ensure_connector_initialized(uid, "youtube")
    return jsonify({"status": "saved"})

@app.route("/google/disconnect/youtube")
def disconnect_youtube():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source='youtube'
    """, (uid,))

    cur.execute("""
        DELETE FROM google_accounts
        WHERE uid=? AND source='youtube'
    """, (uid,))

    cur.execute("""
        UPDATE connector_jobs
        SET enabled=0
        WHERE uid=? AND source='youtube'
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "ok"})

# ---------------- YOUTUBE JOB GET ----------------

@app.route("/connectors/youtube/job/get")
def youtube_job_get():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='youtube'
    """, (uid,))

    row = fetchone_secure(cur)
    con.close()

    if not row:
        return jsonify({"exists": False})

    return jsonify({
        "exists": True,
        "sync_type": row[0],
        "schedule_time": row[1]
    })


# ---------------- YOUTUBE JOB SAVE ----------------

@app.route("/connectors/youtube/job/save", methods=["POST"])
def youtube_job_save():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = request.get_json()

    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time)
        VALUES (?, 'youtube', ?, ?)
    """, (uid, sync_type, schedule_time))

    con.commit()
    con.close()

    return jsonify({"status": "job_saved"})

#----------------Reddit----------------
@app.route("/connectors/reddit/connect")
def reddit_connect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT client_id, client_secret, config_json
        FROM connector_configs
        WHERE uid=? AND connector='reddit'
        LIMIT 1
    """, (uid,))

    row = fetchone_secure(cur)

    if not row:
        return jsonify({"error":"Config missing"}),400

    client_id, client_secret, cfg = row
    cfg=json.loads(cfg)

    from backend.connectors.reddit import connect_reddit

    connect_reddit(
        uid,
        client_id,
        client_secret,
        cfg["username"],
        cfg["password"]
    )

    cur.execute("""
        INSERT OR REPLACE INTO google_connections
        (uid,source,enabled)
        VALUES (?, 'reddit',1)
    """,(uid,))

    con.commit()
    con.close()

    return jsonify({"status":"connected"})

@app.route("/connectors/reddit/disconnect")
def reddit_disconnect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source='reddit'
    """, (uid,))

    cur.execute("DELETE FROM reddit_tokens WHERE uid=?", (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "disconnected"})

@app.route("/api/status/reddit")
def reddit_status():

    uid=get_uid()

    con=get_db()
    cur=con.cursor()

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='reddit'
    """,(uid,))
    conn=cur.fetchone()

    cur.execute("""
        SELECT client_id
        FROM connector_configs
        WHERE uid=? AND connector='reddit'
    """,(uid,))
    cfg=cur.fetchone()

    con.close()

    return jsonify({
        "connected": bool(conn and conn[0]==1),
        "has_credentials": bool(cfg)
    })

@app.route("/connectors/reddit/save_config", methods=["POST"])
def reddit_save_config():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = encrypt_payload(request.get_json())

    client_id = data.get("client_id")
    client_secret = data.get("client_secret")
    username = data.get("username")
    password = data.get("password")

    if not all([client_id, client_secret, username, password]):
        return jsonify({"error": "Missing fields"}), 400

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, client_id, client_secret, config_json, created_at)
        VALUES (?, 'reddit', ?, ?, ?, datetime('now'))
    """, (
        uid,
        client_id,
        client_secret,
        json.dumps({
            "username": username,
            "password": password
        })
    ))

    con.commit()
    con.close()

    return jsonify({"status": "saved"})

@app.route("/connectors/reddit/job/get")
def reddit_job_get():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='reddit'
        LIMIT 1
    """, (uid,))

    row = fetchone_secure(cur)
    con.close()

    if not row:
        return jsonify({})

    return jsonify({
        "sync_type": row[0],
        "schedule_time": row[1]
    })

@app.route("/connectors/reddit/job/save", methods=["POST"])
def reddit_job_save():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time)
        VALUES (?, 'reddit', ?, ?)
    """, (
        uid,
        data.get("sync_type"),
        data.get("schedule_time")
    ))

    con.commit()
    con.close()

    return jsonify({"status": "saved"})

@app.route("/connectors/reddit/sync")
def reddit_sync_universal():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # ----------------------------
    # CHECK CONNECTION
    # ----------------------------
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='reddit'
    """, (uid,))

    row = fetchone_secure(cur)

    if not row or row[0] != 1:
        con.close()
        return jsonify({"error": "not connected"}), 400

    # ----------------------------
    # GET SYNC TYPE
    # ----------------------------
    cur.execute("""
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source='reddit'
        LIMIT 1
    """, (uid,))

    job = cur.fetchone()
    sync_type = job[0] if job else "historical"

    con.close()

    # ----------------------------
    # IMPORT REAL FUNCTIONS
    # ----------------------------
    from backend.connectors.reddit import (
        sync_posts,
        sync_profile,
        sync_messages
    )

    # ----------------------------
    # RUN SYNC
    # ----------------------------
    profile_data = sync_profile(uid)
    posts_data = sync_posts(uid, query="python", sync_type=sync_type)
    messages_data = sync_messages(uid)

    post_rows = posts_data.get("rows", [])

    # ----------------------------
    # DESTINATION
    # ----------------------------
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT dest_type, host, port, username, password, database_name, format
        FROM destination_configs
        WHERE uid=? AND source=? AND is_active=1
        LIMIT 1
    """, (uid, "reddit"))

    dest_row = fetchone_secure(cur)
    con.close()

    if not dest_row:
        return jsonify({
            "posts": len(post_rows),
            "rows_pushed": 0,
            "sync_type": sync_type,
            "message": "No active destination"
        })

    dest = {
        "type": dest_row[0],
        "host": dest_row[1],
        "port": dest_row[2],
        "username": dest_row[3],
        "password": dest_row[4],
        "database_name": dest_row[5]
    }

    from backend.destinations.destination_router import push_to_destination

    pushed = 0

    if post_rows:
        pushed += push_to_destination(dest, "reddit_posts", post_rows)

    print(f"[REDDIT] Sync type: {sync_type}", flush=True)
    print(f"[REDDIT] Posts fetched: {len(post_rows)}", flush=True)
    print(f"[REDDIT] Rows pushed: {pushed}", flush=True)

    return jsonify({
        "posts": len(post_rows),
        "rows_pushed": pushed,
        "sync_type": sync_type
    })

@app.route("/reddit/sync/profile")
def reddit_profile():

    uid = request.args.get("uid")

    from backend.connectors.reddit import sync_profile

    return sync_profile(uid)

@app.route("/reddit/sync/posts")
def reddit_posts():

    uid = request.args.get("uid")
    q = request.args.get("q", "python")

    from backend.connectors.reddit import sync_posts

    return sync_posts(uid, q)

@app.route("/reddit/sync/messages")
def reddit_messages():

    uid = request.args.get("uid")

    from backend.connectors.reddit import sync_messages

    return sync_messages(uid)

# ---------------- TELEGRAM ----------------

@app.route("/connectors/telegram/sync")
def telegram_sync_universal():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT COUNT(*)
        FROM telegram_accounts
        WHERE uid=?
    """, (uid,))
    if cur.fetchone()[0] == 0:
        con.close()
        return jsonify({"error": "not connected"}), 400

    cur.execute("""
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source='telegram'
        LIMIT 1
    """, (uid,))
    job = cur.fetchone()
    sync_type = job[0] if job else "historical"

    con.close()

    from backend.connectors.telegram import sync_messages

    res = sync_messages(uid, sync_type)

    total_messages = res["messages"]
    rows = res["rows"]

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT dest_type, host, port, username, password, database_name, format
        FROM destination_configs
        WHERE uid=? AND source='telegram' AND is_active=1
        LIMIT 1
    """, (uid,))

    dest_row = fetchone_secure(cur)
    con.close()

    if not dest_row:
        return jsonify({
            "messages": total_messages,
            "rows_pushed": 0,
            "sync_type": sync_type
        })

    dest = {
        "type": dest_row[0],
        "host": dest_row[1],
        "port": dest_row[2],
        "username": dest_row[3],
        "password": dest_row[4],
        "database_name": dest_row[5]
    }

    from backend.destinations.destination_router import push_to_destination

    pushed = 0
    if rows:
        pushed = push_to_destination(dest, "telegram_messages", rows)

    return jsonify({
        "messages": total_messages,
        "rows_pushed": pushed,
        "sync_type": sync_type
    })

@app.route("/connectors/telegram/connect")
def telegram_connect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # read saved token
    cur.execute("""
        SELECT api_key
        FROM connector_configs
        WHERE uid=? AND connector='telegram'
        LIMIT 1
    """,(uid,))

    row = fetchone_secure(cur)

    if not row:
        return jsonify({"error":"config missing"}),400

    bot_token = row[0]

    # store runtime connection
    cur.execute("""
        INSERT OR REPLACE INTO telegram_accounts
        (uid, bot_token, created_at)
        VALUES (?,?,?)
    """,(
        uid,
        bot_token,
        datetime.datetime.now(datetime.UTC).isoformat()
    ))

    # enable connector
    cur.execute("""
        INSERT OR REPLACE INTO google_connections
        (uid,source,enabled)
        VALUES (?, 'telegram',1)
    """,(uid,))

    con.commit()
    con.close()

    return jsonify({"status":"connected"})

@app.route("/connectors/telegram/disconnect")
def telegram_disconnect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("DELETE FROM telegram_accounts WHERE uid=?", (uid,))

    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source='telegram'
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "disconnected"})

@app.route("/api/status/telegram")
def telegram_status():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # enabled
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='telegram'
    """,(uid,))
    conn = cur.fetchone()

    # credentials saved
    cur.execute("""
        SELECT api_key
        FROM connector_configs
        WHERE uid=? AND connector='telegram'
    """,(uid,))
    cfg = cur.fetchone()

    con.close()

    return jsonify({
        "connected": bool(conn and conn[0]==1),
        "has_credentials": bool(cfg and cfg[0])
    })

# ---------------- TELEGRAM SAVE CONFIG ----------------

@app.route("/connectors/telegram/save_config", methods=["POST"])
def telegram_save_config():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = encrypt_payload(request.get_json())

    bot_token = data.get("bot_token")

    if not bot_token:
        return jsonify({"error": "bot_token required"}), 400

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, api_key, created_at)
        VALUES (?, 'telegram', ?, datetime('now'))
    """, (uid, bot_token))

    con.commit()
    con.close()

    return jsonify({"status": "saved"})
# ---------------- MEDIUM ----------------

@app.route("/medium/sync")
def medium_sync():

    uid = getattr(g, "user_id", None)
    username = request.args.get("username")

    from backend.connectors.medium import sync_user

    return jsonify(sync_user(uid, username))

@app.route("/connectors/medium/connect")
def medium_connect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT config_json
        FROM connector_configs
        WHERE uid=? AND connector='medium'
    """,(uid,))

    if not cur.fetchone():
        con.close()
        return jsonify({"error":"config missing"}),400

    cur.execute("""
        INSERT OR REPLACE INTO google_connections
        (uid, source, enabled)
        VALUES (?, 'medium', 1)
    """,(uid,))

    con.commit()
    con.close()

    return jsonify({"status":"connected"})

@app.route("/connectors/medium/save_config", methods=["POST"])
def medium_save_config():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = encrypt_payload(request.get_json())

    username = data.get("username")

    if not username:
        return jsonify({"error": "username required"}), 400

    con = get_db()
    cur = con.cursor()

    # check existing
    cur.execute("""
        SELECT id
        FROM connector_configs
        WHERE uid=? AND connector='medium'
        LIMIT 1
    """,(uid,))

    row = fetchone_secure(cur)

    if row:
        # UPDATE
        cur.execute("""
            UPDATE connector_configs
            SET config_json=?,
                created_at=datetime('now')
            WHERE uid=? AND connector='medium'
        """,(
            json.dumps({"username": username}),
            uid
        ))
    else:
        # INSERT
        cur.execute("""
            INSERT INTO connector_configs
            (uid, connector, config_json, created_at)
            VALUES (?, 'medium', ?, datetime('now'))
        """,(
            uid,
            json.dumps({"username": username})
        ))

    con.commit()
    con.close()

    return jsonify({"status":"saved"})

@app.route("/connectors/medium/disconnect")
def medium_disconnect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("DELETE FROM medium_accounts WHERE uid=?", (uid,))
    cur.execute("DELETE FROM medium_state WHERE uid=?", (uid,))

    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source='medium'
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "disconnected"})

@app.route("/api/status/medium")
def medium_status():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # connection
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='medium'
    """,(uid,))
    conn = cur.fetchone()

    # credentials
    cur.execute("""
        SELECT config_json
        FROM connector_configs
        WHERE uid=? AND connector='medium'
    """,(uid,))
    cfg = cur.fetchone()

    con.close()

    return jsonify({
        "connected": bool(conn and conn[0]==1),
        "has_credentials": bool(cfg)
    })

@app.route("/connectors/medium/sync")
def medium_sync_universal():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source='medium'
        LIMIT 1
    """, (uid,))
    job = cur.fetchone()
    sync_type = job[0] if job else "historical"

    con.close()

    from backend.connectors.medium import sync_medium

    res = sync_medium(uid, sync_type)

    total = res["posts"]
    rows = res["rows"]

    # Destination
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT dest_type, host, port, username, password, database_name, format
        FROM destination_configs
        WHERE uid=? AND source='medium' AND is_active=1
        LIMIT 1
    """, (uid,))

    dest_row = fetchone_secure(cur)
    con.close()

    if not dest_row:
        return jsonify({
            "posts": total,
            "rows_pushed": 0,
            "sync_type": sync_type
        })

    dest = {
        "type": dest_row[0],
        "host": dest_row[1],
        "port": dest_row[2],
        "username": dest_row[3],
        "password": dest_row[4],
        "database_name": dest_row[5]
    }

    from backend.destinations.destination_router import push_to_destination

    pushed = 0
    if rows:
        pushed = push_to_destination(dest, "medium_posts", rows)

    return jsonify({
        "posts": total,
        "rows_pushed": pushed,
        "sync_type": sync_type
    })


# ---------------- QUORA (DISABLED) ----------------

@app.route("/quora/sync/profile")
def quora_sync_profile():

    return jsonify({
        "status": "blocked",
        "platform": "quora",
        "reason": "Quora blocks automated access (HTTP 403). No public API."
    })


@app.route("/quora/sync/answers")
def quora_sync_answers():

    return jsonify({
        "status": "blocked",
        "platform": "quora",
        "reason": "Quora blocks automated access (HTTP 403). No public API."
    })

# ---------------- TUMBLR ----------------

@app.route("/tumblr/sync/blog")
def tumblr_sync_blog():

    uid = request.cookies.get("uid")
    blog = request.args.get("blog")

    from backend.connectors.tumblr import sync_blog

    return jsonify(sync_blog(uid, blog))


@app.route("/tumblr/sync/posts")
def tumblr_sync_posts():

    uid = request.cookies.get("uid")
    blog = request.args.get("blog")

    from backend.connectors.tumblr import sync_posts

    return jsonify(sync_posts(uid, blog))

# ---------------- TWITCH ----------------

@app.route("/connectors/twitch/connect")
def twitch_connect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT 1
        FROM connector_configs
        WHERE uid=? AND connector='twitch'
    """,(uid,))

    if not cur.fetchone():
        con.close()
        return jsonify({"error":"config missing"}),400

    cur.execute("""
        INSERT OR REPLACE INTO google_connections
        (uid,source,enabled)
        VALUES (?,?,1)
    """,(uid,"twitch"))

    con.commit()
    con.close()

    return jsonify({"status":"connected"})

@app.route("/api/status/twitch")
def twitch_status():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='twitch'
    """,(uid,))

    connected = bool(
        (row := cur.fetchone()) and row[0]==1
    )

    cur.execute("""
        SELECT 1
        FROM connector_configs
        WHERE uid=? AND connector='twitch'
    """,(uid,))

    has_credentials = bool(cur.fetchone())

    con.close()

    return jsonify({
        "connected":connected,
        "has_credentials":has_credentials
    })

@app.route("/connectors/twitch/disconnect")
def twitch_disconnect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # Only disable connector
    cur.execute("""
        UPDATE google_connections
        SET enabled = 0
        WHERE uid=? AND source='twitch'
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "disconnected"})

@app.route("/connectors/twitch/sync")
def twitch_sync():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # Check if enabled
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='twitch'
    """, (uid,))
    row = fetchone_secure(cur)

    if not row or row[0] != 1:
        con.close()
        return jsonify({"error": "Twitch not connected"}), 400

    # Get username from connector_state
    cur.execute("""
        SELECT config_json
        FROM connector_configs
        WHERE uid=? AND source='twitch'
    """, (uid,))
    row = fetchone_secure(cur)

    if not row:
        con.close()
        return jsonify({"error": "Username missing"}), 400

    cfg = json.loads(row[0])
    username = cfg.get("username")

    if not username:
        con.close()
        return jsonify({"error": "Username missing"}), 400

    # Get sync type from job
    sync_type = get_connector_sync_type(uid, "twitch")

    con.close()

    # Run connector
    from backend.connectors.twitch import sync_videos

    result = sync_videos(
        uid=uid,
        username=username,
        sync_type=sync_type
    )

    rows = result.get("rows", [])

    # Check destination
    con = get_db()
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
        SELECT *
        FROM destination_configs
        WHERE uid=? AND source='twitch'
        ORDER BY id DESC
        LIMIT 1
    """, (uid,))

    row = fetchone_secure(cur)
    con.close()

    dest = None

    if row:
        dest = dict(row)

        # Normalize key name for router
        if "dest_type" in dest:
            dest["type"] = dest["dest_type"]

    if dest and rows:
        inserted = push_to_destination(
            dest,
            "twitch_videos",
            rows
        )

        return jsonify({
            "status": "pushed_to_destination",
            "rows": inserted
        })

    return jsonify({
        "status": "stored_locally",
        "videos": result.get("videos", 0)
    })

@app.route("/connectors/twitch/save_config", methods=["POST"])
def twitch_save_config():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = encrypt_payload(request.get_json()) or {}

    username = data.get("username")

    if not username:
        return jsonify({"error":"username required"}),400

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, config_json, created_at)
        VALUES (?,?,?,datetime('now'))
    """,(
        uid,
        "twitch",
        json.dumps({"username": username})
    ))

    con.commit()
    con.close()

    return jsonify({"status":"saved"})

#------------------- Tumblr --------------------------

@app.route("/connectors/tumblr/save_config", methods=["POST"])
def tumblr_save_config():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = encrypt_payload(request.get_json())

    api_key = data.get("api_key")

    if not api_key:
        return jsonify({"error":"API key required"}),400

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, api_key, created_at)
        VALUES (?, 'tumblr', ?, datetime('now'))
    """,(uid, api_key))

    con.commit()
    con.close()

    return jsonify({"status":"saved"})
        
@app.route("/connectors/tumblr/connect")
def tumblr_connect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # check config exists
    cur.execute("""
        SELECT api_key
        FROM connector_configs
        WHERE uid=? AND connector='tumblr'
    """,(uid,))

    row = fetchone_secure(cur)

    if not row:
        con.close()
        return jsonify({"error":"config missing"}),400

    cur.execute("""
        INSERT OR REPLACE INTO google_connections
        (uid, source, enabled)
        VALUES (?, 'tumblr', 1)
    """,(uid,))

    con.commit()
    con.close()

    return jsonify({"status":"connected"})

@app.route("/connectors/tumblr/disconnect")
def tumblr_disconnect():

    uid=get_uid()

    con=get_db()
    cur=con.cursor()

    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source='tumblr'
    """,(uid,))

    con.commit()
    con.close()

    return jsonify({"status":"disconnected"})

@app.route("/connectors/tumblr/sync")
def tumblr_sync_universal():

    uid=get_uid()

    con=get_db()
    cur=con.cursor()

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='tumblr'
    """,(uid,))

    if not cur.fetchone():
        con.close()
        return jsonify({"error":"not connected"}),400

    cur.execute("""
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source='tumblr'
        LIMIT 1
    """,(uid,))

    job=cur.fetchone()
    sync_type=job[0] if job else "historical"

    con.close()

    from backend.connectors.tumblr import sync_posts

    result=sync_posts(uid,sync_type)

    rows=result.get("rows",[])

    return jsonify({
        "posts":len(rows),
        "sync_type":sync_type
    })

@app.route("/api/status/tumblr")
def tumblr_status():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # ---------- connection ----------
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='tumblr'
        LIMIT 1
    """,(uid,))
    conn = cur.fetchone()

    connected = bool(conn and conn[0] == 1)

    # ---------- credentials ----------
    cur.execute("""
        SELECT api_key
        FROM connector_configs
        WHERE uid=? AND connector='tumblr'
        LIMIT 1
    """,(uid,))
    cfg = cur.fetchone()

    has_credentials = bool(cfg and cfg[0])

    con.close()

    return jsonify({
        "connected": connected,
        "has_credentials": has_credentials
    })

# ---------------- DISCORD (BOT MODE) ----------------

@app.route("/connectors/discord/connect")
def discord_connect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT api_key
        FROM connector_configs
        WHERE uid=? AND connector='discord'
        LIMIT 1
    """, (uid,))

    row = fetchone_secure(cur)

    if not row:
        return jsonify({"error":"token missing"}),400

    bot_token = row[0]

    cur.execute("""
        INSERT OR REPLACE INTO discord_connections
        (uid, bot_token, created_at)
        VALUES (?,?,?)
    """, (
        uid,
        bot_token,
        datetime.datetime.now(datetime.UTC).isoformat()
    ))

    cur.execute("""
        INSERT OR REPLACE INTO google_connections
        (uid, source, enabled)
        VALUES (?, 'discord', 1)
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status":"connected"})

@app.route("/connectors/discord/disconnect")
def discord_disconnect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("DELETE FROM discord_connections WHERE uid=?", (uid,))

    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source='discord'
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "disconnected"})

@app.route("/connectors/discord/sync")
def discord_sync_universal():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    print("SYNC UID:", uid, flush=True)

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='discord'
    """, (uid,))
    row = fetchone_secure(cur)

    if not row or row[0] != 1:
        con.close()
        return jsonify({"error": "not connected"}), 400

    cur.execute("""
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source='discord'
        LIMIT 1
    """, (uid,))
    job = cur.fetchone()
    sync_type = job[0] if job else "historical"

    from backend.connectors.discord import sync_guilds, sync_channels, sync_messages

    sync_guilds(uid)

    cur.execute("SELECT guild_id FROM discord_guilds WHERE uid=?", (uid,))
    guilds = cur.fetchall()

    total_messages = 0
    all_rows = []

    # Reduced safety limits (faster sync)
    max_global_messages = 200
    max_channels_per_guild = 3

    for g in guilds:

        guild_id = g[0]

        sync_channels(guild_id, uid)

        cur.execute("""
            SELECT channel_id
            FROM discord_channels
            WHERE uid=? AND guild_id=? AND type=0
            LIMIT ?
        """, (uid, guild_id, max_channels_per_guild))

        channels = cur.fetchall()

        for c in channels:

            if total_messages >= max_global_messages:
                break

            channel_id = c[0]

            try:
                res = sync_messages(uid, channel_id, sync_type)

                total_messages += res.get("messages", 0)
                all_rows.extend(res.get("rows", []))

            except Exception as e:
                print("Channel error:", channel_id, str(e), flush=True)
                continue

        if total_messages >= max_global_messages:
            break

    cur.execute("""
        SELECT dest_type, host, port, username, password, database_name, format
        FROM destination_configs
        WHERE uid=? AND source=? AND is_active=1
        LIMIT 1
    """, (uid, "discord"))

    dest_row = fetchone_secure(cur)
    con.close()

    if not dest_row:
        return jsonify({
            "messages": total_messages,
            "rows_pushed": 0,
            "sync_type": sync_type
        })

    dest = {
        "type": dest_row[0],
        "host": dest_row[1],
        "port": dest_row[2],
        "username": dest_row[3],
        "password": dest_row[4],
        "database_name": dest_row[5]
    }

    from backend.destinations.destination_router import push_to_destination

    pushed = 0

    if all_rows:
        pushed = push_to_destination(dest, "discord_messages", all_rows)

    print(f"[DISCORD] Sync type: {sync_type}", flush=True)
    print(f"[DISCORD] Messages inserted: {total_messages}", flush=True)
    print(f"[DISCORD] Rows pushed: {pushed}", flush=True)

    return jsonify({
        "messages": total_messages,
        "rows_pushed": pushed,
        "sync_type": sync_type
    })

@app.route("/api/status/discord")
def discord_status():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # connection
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='discord'
    """,(uid,))
    conn = cur.fetchone()

    # token saved
    cur.execute("""
        SELECT api_key
        FROM connector_configs
        WHERE uid=? AND connector='discord'
    """,(uid,))
    cfg = cur.fetchone()

    con.close()

    return jsonify({
        "connected": bool(conn and conn[0]==1),
        "has_credentials": bool(cfg and cfg[0])
    })

# ---------------- DISCORD SAVE CONFIG ----------------

@app.route("/connectors/discord/save_config", methods=["POST"])
def discord_save_config():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = encrypt_payload(request.get_json())

    bot_token = data.get("bot_token")

    if not bot_token:
        return jsonify({"error": "bot_token required"}), 400

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, api_key, created_at)
        VALUES (?, 'discord', ?, datetime('now'))
    """, (uid, bot_token))

    con.commit()
    con.close()

    return jsonify({"status": "saved"})

# ---------------- GOOGLE BOOKS ----------------

from backend.connectors.googlebooks import sync_books


@app.route("/connectors/books/connect")
def connect_books():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO google_connections(uid, source, enabled)
        VALUES (?, 'books', 1)
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "connected"})


@app.route("/connectors/books/disconnect")
def disconnect_books():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source='books'
    """, (uid,))

    cur.execute("""
        UPDATE connector_jobs
        SET enabled=0
        WHERE uid=? AND source='books'
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "disconnected"})


@app.route("/connectors/books/sync")
def books_sync():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    query = request.args.get("query")
    sync_type = request.args.get("sync_type", "incremental")

    if not query:
        return jsonify({"status": "error", "message": "query required"})

    return jsonify(sync_books(query, sync_type))


@app.route("/connectors/books/job/get")
def get_books_job():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='books'
    """, (uid,))

    row = fetchone_secure(cur)
    con.close()

    if not row:
        return jsonify({
            "sync_type": "incremental",
            "schedule_time": "00:00"
        })

    return jsonify({
        "sync_type": row[0],
        "schedule_time": row[1]
    })


@app.route("/connectors/books/job/save", methods=["POST"])
def save_books_job():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json

    sync_type = data.get("sync_type")
    schedule_time = data.get("schedule_time")

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT INTO connector_jobs
        (uid, source, sync_type, schedule_time, enabled)
        VALUES (?, 'books', ?, ?, 1)
    """, (uid, sync_type, schedule_time))

    con.commit()
    con.close()

    return jsonify({"status": "saved"})


@app.route("/api/status/books")
def books_status():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='books'
        LIMIT 1
    """, (uid,))

    row = fetchone_secure(cur)
    con.close()

    return jsonify({
        "connected": bool(row and row[0] == 1)
    })

# ---------------- GOOGLE FACT CHECK ----------------

from backend.connectors.googlefactcheck import sync_factcheck

@app.route("/connectors/factcheck/sync")
def factcheck_sync():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    query = request.args.get("query")
    sync_type = request.args.get("sync_type", "incremental")

    if not query:
        return jsonify({
            "status": "error",
            "message": "query required"
        })

    result = sync_factcheck(uid, query, sync_type)

    return jsonify(result)

@app.route("/connectors/factcheck/save_config", methods=["POST"])
def factcheck_save_config():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = encrypt_payload(request.get_json())

    api_key = data.get("api_key")

    if not api_key:
        return jsonify({"error": "API key required"}), 400

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, api_key, created_at)
        VALUES (?, 'factcheck', ?, datetime('now'))
    """, (uid, api_key))

    con.commit()
    con.close()

    return jsonify({"status": "saved"})

@app.route("/connectors/factcheck/connect")
def factcheck_connect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO google_connections
        (uid, source, enabled)
        VALUES (?, 'factcheck', 1)
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "connected"})

@app.route("/connectors/factcheck/disconnect")
def factcheck_disconnect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source='factcheck'
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "disconnected"})

@app.route("/api/status/factcheck")
def factcheck_status():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # connection
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='factcheck'
        LIMIT 1
    """, (uid,))
    row = fetchone_secure(cur)

    connected = bool(row and row[0] == 1)

    # api key
    cur.execute("""
        SELECT api_key
        FROM connector_configs
        WHERE uid=? AND connector='factcheck'
        LIMIT 1
    """, (uid,))
    key = cur.fetchone()

    con.close()

    return jsonify({
        "connected": connected,
        "api_key_saved": key is not None
    })

@app.route("/connectors/factcheck/job/get")
def factcheck_job_get():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='factcheck'
        LIMIT 1
    """, (uid,))

    row = fetchone_secure(cur)
    con.close()

    if not row:
        return jsonify({"exists": False})

    return jsonify({
        "exists": True,
        "sync_type": row[0],
        "schedule_time": row[1]
    })

@app.route("/connectors/factcheck/job/save", methods=["POST"])
def factcheck_job_save():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = request.get_json()

    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time)
        VALUES (?, 'factcheck', ?, ?)
    """, (
        uid,
        sync_type,
        schedule_time
    ))

    con.commit()
    con.close()

    return jsonify({"status": "job_saved"})

# ---------------- GOOGLE NEWS ----------------

@app.route("/googlenews/sync/articles")
def googlenews_sync():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    query = request.args.get("q")
    limit = int(request.args.get("limit", 100))

    from backend.connectors.googlenews import sync_articles

    return jsonify(sync_articles(uid, query, limit))

from backend.connectors.googlenews import sync_news

@app.route("/connectors/news/sync", methods=["GET"])
def news_sync():

    keyword = request.args.get("keyword")
    sync_type = request.args.get("sync_type", "incremental")

    if not keyword:
        return jsonify({"error": "keyword required"}), 400

    result = sync_news(keyword, sync_type)

    return jsonify(result)

# ---------------- GOOGLE NEWS CONNECT ----------------

@app.route("/connectors/news/connect", methods=["POST"])
def connect_news():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO google_connections
        (uid, source, enabled)
        VALUES (?, 'news', 1)
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "connected"})


# ---------------- GOOGLE NEWS DISCONNECT ----------------

@app.route("/connectors/news/disconnect", methods=["POST"])
def disconnect_news():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source='news'
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "disconnected"})


# ---------------- GOOGLE NEWS STATUS ----------------

@app.route("/api/status/news")
def news_status():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='news'
        LIMIT 1
    """, (uid,))

    row = fetchone_secure(cur)
    con.close()

    return jsonify({
        "connected": bool(row and row[0] == 1)
    })

# ---------------- GOOGLE NEWS JOB GET ----------------

@app.route("/connectors/news/job/get")
def news_job_get():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='news'
    """, (uid,))

    row = fetchone_secure(cur)
    con.close()

    if not row:
        return jsonify({
            "exists": False,
            "sync_type": "incremental",
            "schedule_time": "00:00"
        })

    return jsonify({
        "exists": True,
        "sync_type": row[0],
        "schedule_time": row[1]
    })


# ---------------- GOOGLE NEWS JOB SAVE ----------------

@app.route("/connectors/news/job/save", methods=["POST"])
def news_job_save():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = request.get_json()

    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time)
        VALUES (?, 'news', ?, ?)
    """, (uid, sync_type, schedule_time))

    con.commit()
    con.close()

    return jsonify({"status": "saved"})

# ---------------- GOOGLE TRENDS ----------------

@app.route("/googletrends/sync/interest")
def googletrends_sync_interest():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    keyword = request.args.get("q")

    from backend.connectors.googletrends import sync_interest

    return jsonify(sync_interest(uid, keyword))


@app.route("/googletrends/sync/related")
def googletrends_sync_related():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    keyword = request.args.get("q")

    from backend.connectors.googletrends import sync_related

    return jsonify(sync_related(uid, keyword))

from backend.connectors.googletrends import sync_trends


@app.route("/connectors/trends/sync", methods=["GET"])
def trends_sync():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    keyword = request.args.get("keyword")
    sync_type = request.args.get("sync_type", "incremental")

    if not keyword:
        return jsonify({"status": "error", "message": "Keyword required"})

    print("CALLING SYNC TRENDS NOW", flush=True)
    result = sync_trends(uid, keyword, sync_type)

    return jsonify(result)

@app.route("/connectors/trends/connect", methods=["POST"])
def trends_connect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO google_connections(uid, source, enabled)
        VALUES (?, 'trends', 1)
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "connected"})

# ---------------- TRENDS STATUS ----------------

@app.route("/api/status/trends")
def trends_status():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='trends'
        LIMIT 1
    """, (uid,))

    row = fetchone_secure(cur)
    con.close()

    return jsonify({
        "connected": bool(row and row[0] == 1)
    })

# ---------------- TRENDS DISCONNECT ----------------

@app.route("/google/disconnect/trends")
def disconnect_trends():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source='trends'
    """, (uid,))

    cur.execute("""
        UPDATE connector_jobs
        SET enabled=0
        WHERE uid=? AND source='trends'
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "ok"})

@app.route("/connectors/trends/job/get")
def trends_job_get():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='trends'
    """, (uid,))

    row = fetchone_secure(cur)
    con.close()

    if not row:
        return jsonify({"exists": False})

    return jsonify({
        "exists": True,
        "sync_type": row[0],
        "schedule_time": row[1]
    })

@app.route("/connectors/trends/job/save", methods=["POST"])
def trends_job_save():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = request.get_json()

    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time)
        VALUES (?, 'trends', ?, ?)
    """, (uid, sync_type, schedule_time))

    con.commit()
    con.close()

    return jsonify({"status": "job_saved"})

# ---------------- DEV.TO ----------------

@app.route("/connectors/devto/connect")
def devto_connect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO google_connections
        (uid, source, enabled)
        VALUES (?, 'devto', 1)
    """, (uid,))

    con.commit()
    con.close()

    return redirect("http://localhost:3000/connectors/devto")

# ---------------- DEVTO STATUS ----------------

@app.route("/api/status/devto")
def devto_status():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # API connectors have implicit credentials
    has_credentials = True

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='devto'
        LIMIT 1
    """, (uid,))

    row = fetchone_secure(cur)
    con.close()

    return jsonify({
        "has_credentials": has_credentials,
        "connected": bool(row and row[0] == 1)
    })

@app.route("/connectors/devto/disconnect")
def devto_disconnect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source='devto'
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "disconnected"})


@app.route("/connectors/devto/job/get")
def devto_job_get():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='devto'
    """, (uid,))

    row = fetchone_secure(cur)
    con.close()

    if not row:
        return jsonify({})

    return jsonify({
        "sync_type": row[0],
        "schedule_time": row[1]
    })


@app.route("/connectors/devto/job/save", methods=["POST"])
def devto_job_save():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time)
        VALUES (?, 'devto', ?, ?)
    """, (
        uid,
        data.get("sync_type"),
        data.get("schedule_time")
    ))

    con.commit()
    con.close()

    return jsonify({"status": "saved"})


@app.route("/connectors/devto/sync")
def devto_sync_universal():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # ---- Check connection ----
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='devto'
    """, (uid,))

    row = fetchone_secure(cur)

    if not row or row[0] != 1:
        con.close()
        return jsonify({"error": "not connected"}), 400

    # ---- Get sync type ----
    cur.execute("""
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source='devto'
        LIMIT 1
    """, (uid,))

    job = cur.fetchone()
    sync_type = job[0] if job else "historical"

    con.close()

    # ---- Run connector ----
    from backend.connectors.devto import sync_articles, sync_tags

    articles_data = sync_articles(uid, sync_type=sync_type)
    tags_data = sync_tags(uid)

    article_rows = articles_data.get("rows", [])
    tag_rows = tags_data.get("rows", [])

    # ---- Get destination ----
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT
            dest_type,
            host,
            port,
            username,
            password,
            database_name,
            format
        FROM destination_configs
        WHERE uid=? AND source=? AND is_active=1
        LIMIT 1
    """, (uid, "devto"))

    dest_row = fetchone_secure(cur)
    con.close()

    if not dest_row:
        return jsonify({
            "articles": articles_data.get("articles", 0),
            "tags": tags_data.get("tags", 0),
            "rows_pushed": 0,
            "sync_type": sync_type,
            "message": "No active destination"
        })

    dest = {
        "type": dest_row[0],
        "host": dest_row[1],
        "port": dest_row[2],
        "username": dest_row[3],
        "password": dest_row[4],
        "database_name": dest_row[5]
    }

    # ---- Push ONLY NEW ROWS ----
    from backend.destinations.destination_router import push_to_destination

    pushed = 0

    if article_rows:
        pushed += push_to_destination(dest, "devto_articles", article_rows)

    if tag_rows:
        pushed += push_to_destination(dest, "devto_tags", tag_rows)

    print(f"[DEVTO] Sync type: {sync_type}", flush=True)
    print(f"[DEVTO] Articles found: {len(article_rows)}", flush=True)
    print(f"[DEVTO] Tags found: {len(tag_rows)}", flush=True)
    print(f"[DEVTO] Rows pushed: {pushed}", flush=True)

    return jsonify({
        "articles": len(article_rows),
        "tags": len(tag_rows),
        "rows_pushed": pushed,
        "sync_type": sync_type
    })

# ---------------- GITHUB ----------------

from backend.connectors.github import (
    get_auth_url,
    exchange_code,
    save_token,
    sync_github,
    disable_connection
)

@app.route("/github/connect")
def github_connect():
    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    return redirect(get_auth_url(uid))

@app.route("/github/callback")
def github_callback():

    code = request.args.get("code")
    if not code:
        return "Authorization failed", 400

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    token = exchange_code(uid, code)

    if not token.get("access_token"):
        return "Token exchange failed", 400

    save_token(uid, token)

    return redirect("http://localhost:3000/connectors/github")


@app.route("/connectors/github/disconnect")
def github_disconnect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # Disable connection
    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source='github'
    """, (uid,))

    # DELETE stored GitHub token
    cur.execute("""
        DELETE FROM github_tokens
        WHERE uid=?
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "disconnected"})

@app.route("/connectors/github/sync")
def github_sync():
    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    return jsonify(sync_github(uid))

@app.route("/connectors/github/save_app", methods=["POST"])
def github_save_app():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = encrypt_payload(request.get_json())

    client_id = data.get("client_id")
    client_secret = data.get("client_secret")

    if not client_id or not client_secret:
        return jsonify({"error": "Client ID & Secret required"}), 400

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, client_id, client_secret, created_at)
        VALUES (?, 'github', ?, ?, datetime('now'))
    """, (
        uid,
        client_id,
        client_secret
    ))

    con.commit()
    con.close()

    ensure_connector_initialized(uid, "github")
    return jsonify({"status": "saved"})

@app.route("/api/status/github")
def github_status():

    uid = getattr(g, "user_id", None)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # credentials exist?
    cur.execute("""
        SELECT 1
        FROM connector_configs
        WHERE uid=? AND connector='github'
        LIMIT 1
    """, (uid,))
    creds = cur.fetchone()

    # connection enabled?
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='github'
        LIMIT 1
    """, (uid,))
    row = fetchone_secure(cur)

    conn.close()

    return jsonify({
        "has_credentials": bool(creds),
        "connected": bool(row and row["enabled"] == 1)
    })

@app.route("/connectors/github/job/get")
def github_job_get():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='github'
    """, (uid,))

    row = fetchone_secure(cur)
    con.close()

    if not row:
        return jsonify({"exists": False})

    return jsonify({
        "exists": True,
        "sync_type": row["sync_type"],
        "schedule_time": row[1]
    })

@app.route("/connectors/github/job/save", methods=["POST"])
def github_job_save():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = request.get_json()

    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time)
        VALUES (?, 'github', ?, ?)
    """, (uid, sync_type, schedule_time))

    con.commit()
    con.close()

    return jsonify({"status": "job_saved"})

# ---------------- INSTAGRAM ----------------

from backend.connectors.instagram import (
    get_instagram_auth_url,
    handle_oauth_callback,
    sync_instagram,
    disconnect_instagram
)

@app.route("/connectors/instagram/save_app", methods=["POST"])
def instagram_save_app():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json() or {}

    app_id = data.get("app_id")
    app_secret = data.get("app_secret")
    redirect_uri = data.get("redirect_uri") or (request.host_url.rstrip("/") + "/instagram/callback")

    if not app_id or not app_secret:
        return jsonify({"error": "App ID & App Secret required"}), 400

    con = get_db()
    cur = con.cursor()

    # Store encrypted values in connector_configs (while preserving existing schema).
    cur.execute("""
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, client_id, client_secret, scopes, created_at)
        VALUES (?, 'instagram', ?, ?, ?, datetime('now'))
    """, (
        uid,
        encrypt_value(app_id),
        encrypt_value(app_secret),
        encrypt_value(redirect_uri)
    ))

    con.commit()
    con.close()

    ensure_connector_initialized(uid, "instagram")
    return jsonify({"status": "saved"})

@app.route("/instagram/connect")
def instagram_connect():
    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    try:
        return redirect(get_instagram_auth_url(uid))
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route("/instagram/callback")
def instagram_callback():
    uid = getattr(g, "user_id", None)
    code = request.args.get("code")

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    if not code:
        return "Authorization failed", 400

    result = handle_oauth_callback(uid, code)

    if result.get("status") != "success":
        return jsonify(result), 400

    return redirect("http://localhost:3000/connectors/instagram")

@app.route("/connectors/instagram/disconnect")
def instagram_disconnect():
    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    disconnect_instagram(uid)
    return jsonify({"status": "disconnected"})

@app.route("/connectors/instagram/sync")
def instagram_sync_route():
    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()
    cur.execute("""
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source='instagram'
        LIMIT 1
    """, (uid,))
    row = fetchone_secure(cur)
    con.close()

    sync_type = row["sync_type"] if row and row.get("sync_type") else "historical"
    return jsonify(sync_instagram(uid, sync_type=sync_type))

@app.route("/api/status/instagram")
def instagram_status():
    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT 1
        FROM connector_configs
        WHERE uid=? AND connector='instagram'
        LIMIT 1
    """, (uid,))
    creds = fetchone_secure(cur)

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='instagram'
        LIMIT 1
    """, (uid,))
    conn = fetchone_secure(cur)

    cur.execute("""
        SELECT ig_account_id
        FROM instagram_connections
        WHERE uid=?
        LIMIT 1
    """, (uid,))
    ig_row = fetchone_secure(cur)

    con.close()

    return jsonify({
        "has_credentials": bool(creds),
        "connected": bool(conn and conn["enabled"] == 1),
        "ig_account_id": ig_row["ig_account_id"] if ig_row else None
    })

@app.route("/connectors/instagram/job/get")
def instagram_job_get():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    try:
        cur.execute("""
            SELECT sync_type, schedule_time
            FROM connector_jobs
            WHERE uid=? AND source='instagram'
        """, (uid,))

        row = fetchone_secure(cur)
    finally:
        con.close()

    if not row:
        return jsonify({"exists": False})

    return jsonify({
        "exists": True,
        "sync_type": row["sync_type"],
        "schedule_time": row["schedule_time"]
    })

@app.route("/connectors/instagram/job/save", methods=["POST"])
def instagram_job_save():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json()
    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time)
        VALUES (?, 'instagram', ?, ?)
    """, (uid, sync_type, schedule_time))

    con.commit()
    con.close()

    return jsonify({"status": "job_saved"})

# ---------------- TIKTOK BUSINESS ----------------

@app.route("/connectors/tiktok/save_app", methods=["POST"])
def tiktok_save_app():

    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json() or {}

    client_key = data.get("client_key")
    client_secret = data.get("client_secret")
    advertiser_id = data.get("advertiser_id")
    scopes = data.get("scopes") or "user.info.basic,video.list,ads.read"
    redirect_uri = data.get("redirect_uri") or (request.host_url.rstrip("/") + "/connectors/tiktok/callback")

    if not client_key or not client_secret or not advertiser_id:
        return jsonify({"error": "client_key, client_secret and advertiser_id are required"}), 400

    con = get_db()
    cur = con.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, client_id, client_secret, api_key, scopes, status, created_at)
        VALUES (?, 'tiktok', ?, ?, ?, ?, 'configured', datetime('now'))
    """, (
        uid,
        encrypt_value(client_key),
        encrypt_value(client_secret),
        encrypt_value(advertiser_id),
        encrypt_value(redirect_uri)
    ))

    con.commit()
    con.close()

    ensure_connector_initialized(uid, "tiktok")
    return jsonify({"status": "saved"})


@app.route("/connectors/tiktok/connect")
def tiktok_connect():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    try:
        return redirect(get_tiktok_auth_url(uid))
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/connectors/tiktok/callback")
def tiktok_callback():
    uid = getattr(g, "user_id", None)
    code = request.args.get("code")

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    if not code:
        return "Authorization failed", 400

    result = handle_tiktok_oauth_callback(uid, code)
    if result.get("status") != "success":
        return jsonify(result), 400

    return redirect("http://localhost:3000/connectors/tiktok")


@app.route("/connectors/tiktok/disconnect")
def tiktok_disconnect():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    disconnect_tiktok(uid)
    return jsonify({"status": "disconnected"})


@app.route("/connectors/tiktok/sync")
def tiktok_sync_route():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()
    cur.execute("""
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source='tiktok'
        LIMIT 1
    """, (uid,))
    row = fetchone_secure(cur)
    con.close()

    sync_type = row["sync_type"] if row and row.get("sync_type") else "historical"
    return jsonify(sync_tiktok(uid, sync_type=sync_type))


@app.route("/api/status/tiktok")
def tiktok_status():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT 1
        FROM connector_configs
        WHERE uid=? AND connector='tiktok'
        LIMIT 1
    """, (uid,))
    creds = fetchone_secure(cur)

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='tiktok'
        LIMIT 1
    """, (uid,))
    conn = fetchone_secure(cur)

    cur.execute("""
        SELECT advertiser_id, expires_at
        FROM tiktok_connections
        WHERE uid=?
        LIMIT 1
    """, (uid,))
    tk = fetchone_secure(cur)
    con.close()

    return jsonify({
        "has_credentials": bool(creds),
        "connected": bool(conn and conn["enabled"] == 1),
        "advertiser_id": tk["advertiser_id"] if tk else None,
        "expires_at": tk["expires_at"] if tk else None
    })


@app.route("/connectors/tiktok/job/get")
def tiktok_job_get():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()
    try:
        cur.execute("""
            SELECT sync_type, schedule_time
            FROM connector_jobs
            WHERE uid=? AND source='tiktok'
        """, (uid,))
        row = fetchone_secure(cur)
    finally:
        con.close()

    if not row:
        return jsonify({"exists": False})

    return jsonify({
        "exists": True,
        "sync_type": row["sync_type"],
        "schedule_time": row["schedule_time"]
    })


@app.route("/connectors/tiktok/job/save", methods=["POST"])
def tiktok_job_save():

    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json()
    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")

    con = get_db()
    cur = con.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time)
        VALUES (?, 'tiktok', ?, ?)
    """, (uid, sync_type, schedule_time))

    con.commit()
    con.close()
    return jsonify({"status": "job_saved"})

# ---------------- TABOOLA BACKSTAGE ----------------

@app.route("/connectors/taboola/save_app", methods=["POST"])
def taboola_save_app():

    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json() or {}
    client_id = data.get("client_id")
    client_secret = data.get("client_secret")
    account_id = data.get("account_id")

    if not client_id or not client_secret or not account_id:
        return jsonify({"error": "client_id, client_secret and account_id are required"}), 400

    con = get_db()
    cur = con.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, client_id, client_secret, api_key, status, created_at)
        VALUES (?, 'taboola', ?, ?, ?, 'configured', datetime('now'))
    """, (
        uid,
        encrypt_value(client_id),
        encrypt_value(client_secret),
        encrypt_value(account_id)
    ))
    con.commit()
    con.close()

    ensure_connector_initialized(uid, "taboola")
    return jsonify({"status": "saved"})


@app.route("/connectors/taboola/connect")
def taboola_connect():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    result = connect_taboola(uid)
    if result.get("status") != "success":
        return jsonify(result), 400
    return jsonify(result)


@app.route("/connectors/taboola/disconnect")
def taboola_disconnect():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    disconnect_taboola(uid)
    return jsonify({"status": "disconnected"})


@app.route("/connectors/taboola/sync")
def taboola_sync_route():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()
    cur.execute("""
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source='taboola'
        LIMIT 1
    """, (uid,))
    row = fetchone_secure(cur)
    con.close()

    sync_type = row["sync_type"] if row and row.get("sync_type") else "historical"
    return jsonify(sync_taboola(uid, sync_type=sync_type))


@app.route("/api/status/taboola")
def taboola_status():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT 1
        FROM connector_configs
        WHERE uid=? AND connector='taboola'
        LIMIT 1
    """, (uid,))
    creds = fetchone_secure(cur)

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='taboola'
        LIMIT 1
    """, (uid,))
    conn = fetchone_secure(cur)

    cur.execute("""
        SELECT account_id, expires_at
        FROM taboola_connections
        WHERE uid=?
        LIMIT 1
    """, (uid,))
    tb = fetchone_secure(cur)
    con.close()

    return jsonify({
        "has_credentials": bool(creds),
        "connected": bool(conn and conn["enabled"] == 1),
        "account_id": tb["account_id"] if tb else None,
        "expires_at": tb["expires_at"] if tb else None
    })


@app.route("/connectors/taboola/job/get")
def taboola_job_get():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()
    try:
        cur.execute("""
            SELECT sync_type, schedule_time
            FROM connector_jobs
            WHERE uid=? AND source='taboola'
        """, (uid,))
        row = fetchone_secure(cur)
    finally:
        con.close()

    if not row:
        return jsonify({"exists": False})

    return jsonify({
        "exists": True,
        "sync_type": row["sync_type"],
        "schedule_time": row["schedule_time"]
    })


@app.route("/connectors/taboola/job/save", methods=["POST"])
def taboola_job_save():

    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json() or {}
    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")

    con = get_db()
    cur = con.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time)
        VALUES (?, 'taboola', ?, ?)
    """, (uid, sync_type, schedule_time))

    con.commit()
    con.close()
    return jsonify({"status": "job_saved"})

# ---------------- OUTBRAIN AMPLIFY ----------------

@app.route("/connectors/outbrain/save_app", methods=["POST"])
def outbrain_save_app():

    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json() or {}
    username = data.get("username")
    password = data.get("password")
    marketer_id = data.get("marketer_id")

    if not username or not password or not marketer_id:
        return jsonify({"error": "username, password and marketer_id are required"}), 400

    con = get_db()
    cur = con.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, client_id, client_secret, api_key, status, created_at)
        VALUES (?, 'outbrain', ?, ?, ?, 'configured', datetime('now'))
    """, (
        uid,
        encrypt_value(username),
        encrypt_value(password),
        encrypt_value(marketer_id)
    ))
    con.commit()
    con.close()

    ensure_connector_initialized(uid, "outbrain")
    return jsonify({"status": "saved"})


@app.route("/connectors/outbrain/connect")
def outbrain_connect():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    result = connect_outbrain(uid)
    if result.get("status") != "success":
        return jsonify(result), 400
    return jsonify(result)


@app.route("/connectors/outbrain/disconnect")
def outbrain_disconnect():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    disconnect_outbrain(uid)
    return jsonify({"status": "disconnected"})


@app.route("/connectors/outbrain/sync")
def outbrain_sync_route():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()
    cur.execute("""
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source='outbrain'
        LIMIT 1
    """, (uid,))
    row = fetchone_secure(cur)
    con.close()

    sync_type = row["sync_type"] if row and row.get("sync_type") else "historical"
    return jsonify(sync_outbrain(uid, sync_type=sync_type))


@app.route("/api/status/outbrain")
def outbrain_status():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT 1
        FROM connector_configs
        WHERE uid=? AND connector='outbrain'
        LIMIT 1
    """, (uid,))
    creds = fetchone_secure(cur)

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='outbrain'
        LIMIT 1
    """, (uid,))
    conn = fetchone_secure(cur)

    cur.execute("""
        SELECT marketer_id, expires_at
        FROM outbrain_connections
        WHERE uid=?
        LIMIT 1
    """, (uid,))
    ob = fetchone_secure(cur)
    con.close()

    return jsonify({
        "has_credentials": bool(creds),
        "connected": bool(conn and conn["enabled"] == 1),
        "marketer_id": ob["marketer_id"] if ob else None,
        "expires_at": ob["expires_at"] if ob else None
    })


@app.route("/connectors/outbrain/job/get")
def outbrain_job_get():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()
    try:
        cur.execute("""
            SELECT sync_type, schedule_time
            FROM connector_jobs
            WHERE uid=? AND source='outbrain'
        """, (uid,))
        row = fetchone_secure(cur)
    finally:
        con.close()

    if not row:
        return jsonify({"exists": False})

    return jsonify({
        "exists": True,
        "sync_type": row["sync_type"],
        "schedule_time": row["schedule_time"]
    })


@app.route("/connectors/outbrain/job/save", methods=["POST"])
def outbrain_job_save():

    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json() or {}
    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")

    con = get_db()
    cur = con.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time)
        VALUES (?, 'outbrain', ?, ?)
    """, (uid, sync_type, schedule_time))

    con.commit()
    con.close()
    return jsonify({"status": "job_saved"})

# ---------------- SIMILARWEB ----------------

@app.route("/connectors/similarweb/save_app", methods=["POST"])
def similarweb_save_app():

    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json() or {}
    api_key = data.get("api_key")
    domain = data.get("domain")

    if not api_key or not domain:
        return jsonify({"error": "api_key and domain are required"}), 400

    con = get_db()
    cur = con.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, api_key, scopes, status, created_at)
        VALUES (?, 'similarweb', ?, ?, 'configured', datetime('now'))
    """, (
        uid,
        encrypt_value(api_key),
        encrypt_value(domain)
    ))
    con.commit()
    con.close()

    ensure_connector_initialized(uid, "similarweb")
    return jsonify({"status": "saved"})


@app.route("/connectors/similarweb/connect")
def similarweb_connect():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    result = connect_similarweb(uid)
    if result.get("status") != "success":
        return jsonify(result), 400
    return jsonify(result)


@app.route("/connectors/similarweb/disconnect")
def similarweb_disconnect():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    disconnect_similarweb(uid)
    return jsonify({"status": "disconnected"})


@app.route("/connectors/similarweb/sync")
def similarweb_sync_route():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()
    cur.execute("""
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source='similarweb'
        LIMIT 1
    """, (uid,))
    row = fetchone_secure(cur)
    con.close()

    sync_type = row["sync_type"] if row and row.get("sync_type") else "historical"
    return jsonify(sync_similarweb(uid, sync_type=sync_type))


@app.route("/api/status/similarweb")
def similarweb_status():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT 1
        FROM connector_configs
        WHERE uid=? AND connector='similarweb'
        LIMIT 1
    """, (uid,))
    creds = fetchone_secure(cur)

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='similarweb'
        LIMIT 1
    """, (uid,))
    conn = fetchone_secure(cur)

    cur.execute("""
        SELECT domain
        FROM similarweb_connections
        WHERE uid=?
        LIMIT 1
    """, (uid,))
    sw = fetchone_secure(cur)
    con.close()

    return jsonify({
        "has_credentials": bool(creds),
        "connected": bool(conn and conn["enabled"] == 1),
        "domain": sw["domain"] if sw else None
    })


@app.route("/connectors/similarweb/job/get")
def similarweb_job_get():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()
    try:
        cur.execute("""
            SELECT sync_type, schedule_time
            FROM connector_jobs
            WHERE uid=? AND source='similarweb'
        """, (uid,))
        row = fetchone_secure(cur)
    finally:
        con.close()

    if not row:
        return jsonify({"exists": False})

    return jsonify({
        "exists": True,
        "sync_type": row["sync_type"],
        "schedule_time": row["schedule_time"]
    })


@app.route("/connectors/similarweb/job/save", methods=["POST"])
def similarweb_job_save():

    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json() or {}
    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")

    con = get_db()
    cur = con.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time)
        VALUES (?, 'similarweb', ?, ?)
    """, (uid, sync_type, schedule_time))

    con.commit()
    con.close()
    return jsonify({"status": "job_saved"})


# ---------------- BIGQUERY DESTINATION ----------------

@app.route("/connectors/bigquery/save_app", methods=["POST"])
def bigquery_save_app():

    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    payload = request.get_json() or {}
    project_id = (payload.get("project_id") or "").strip()
    dataset_id = (payload.get("dataset_id") or "").strip()
    sa_json_raw = (payload.get("service_account_json") or "").strip()

    if not project_id or not dataset_id or not sa_json_raw:
        return jsonify({
            "error": "project_id, dataset_id and service_account_json are required"
        }), 400

    try:
        sa_info = json.loads(sa_json_raw)
    except Exception:
        return jsonify({"error": "Invalid service_account_json"}), 400

    required_keys = ["type", "project_id", "private_key", "client_email", "token_uri"]
    missing = [k for k in required_keys if k not in sa_info]
    if missing:
        return jsonify({
            "error": f"service_account_json missing keys: {', '.join(missing)}"
        }), 400

    # Normalize configuration – store everything in encrypted config_json
    config = {
        "project_id": project_id or sa_info.get("project_id"),
        "dataset_id": dataset_id,
        "service_account": sa_info,
    }

    secured = encrypt_payload({
        "config_json": json.dumps(config)
    })

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, config_json, status, created_at)
        VALUES (?, 'bigquery', ?, 'configured', datetime('now'))
    """, (
        uid,
        secured.get("config_json"),
    ))

    con.commit()
    con.close()

    ensure_connector_initialized(uid, "bigquery")
    return jsonify({"status": "saved"})


@app.route("/connectors/bigquery/connect")
def bigquery_connect():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    result = connect_bigquery(uid)
    status = result.get("status")
    if status != "success":
        return jsonify(result), 400
    return jsonify(result)


@app.route("/connectors/bigquery/disconnect")
def bigquery_disconnect():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    disconnect_bigquery(uid)
    return jsonify({"status": "disconnected"})


@app.route("/connectors/bigquery/sync")
def bigquery_sync_route():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    # BigQuery is a destination connector – sync here is a validation/schema refresh
    return jsonify(sync_bigquery(uid, sync_type="incremental"))


@app.route("/api/status/bigquery")
def bigquery_status():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT config_json, status
        FROM connector_configs
        WHERE uid=? AND connector='bigquery'
        LIMIT 1
    """, (uid,))
    cfg_row = fetchone_secure(cur)

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='bigquery'
        LIMIT 1
    """, (uid,))
    conn_row = fetchone_secure(cur)

    con.close()

    project_id = None
    dataset_id = None

    if cfg_row and cfg_row.get("config_json"):
        try:
            cfg = json.loads(cfg_row["config_json"])
            project_id = cfg.get("project_id")
            dataset_id = cfg.get("dataset_id")
        except Exception:
            import traceback; traceback.print_exc()
            print('Exception caught', flush=True)

    return jsonify({
        "has_credentials": bool(cfg_row),
        "connected": bool(conn_row and conn_row.get("enabled") == 1),
        "project_id": project_id,
        "dataset_id": dataset_id,
        "status": (cfg_row.get("status") if cfg_row else None)
    })


@app.route("/connectors/bigquery/job/get")
def bigquery_job_get():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()
    try:
        cur.execute("""
            SELECT sync_type, schedule_time
            FROM connector_jobs
            WHERE uid=? AND source='bigquery'
        """, (uid,))
        row = fetchone_secure(cur)
    finally:
        con.close()

    if not row:
        return jsonify({"exists": False})

    return jsonify({
        "exists": True,
        "sync_type": row.get("sync_type"),
        "schedule_time": row.get("schedule_time")
    })


@app.route("/connectors/bigquery/job/save", methods=["POST"])
def bigquery_job_save():

    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json() or {}
    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")

    con = get_db()
    cur = con.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time)
        VALUES (?, 'bigquery', ?, ?)
    """, (uid, sync_type, schedule_time))

    con.commit()
    con.close()
    return jsonify({"status": "job_saved"})


# ---------------- X (TWITTER) ----------------

@app.route("/connectors/x/save_app", methods=["POST"])
def x_save_app():

    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json() or {}
    client_id = data.get("client_id")
    client_secret = data.get("client_secret")
    redirect_uri = data.get("redirect_uri") or (request.host_url.rstrip("/") + "/connectors/x/callback")

    if not client_id or not client_secret:
        return jsonify({"error": "client_id and client_secret are required"}), 400

    con = get_db()
    cur = con.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, client_id, client_secret, scopes, status, created_at)
        VALUES (?, 'x', ?, ?, ?, 'configured', datetime('now'))
    """, (
        uid,
        encrypt_value(client_id),
        encrypt_value(client_secret),
        encrypt_value(redirect_uri)
    ))

    con.commit()
    con.close()

    ensure_connector_initialized(uid, "x")
    return jsonify({"status": "saved"})


@app.route("/connectors/x/connect")
def x_connect():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    try:
        return redirect(get_x_auth_url(uid))
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/connectors/x/callback")
def x_callback():
    uid = getattr(g, "user_id", None)
    code = request.args.get("code")

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    if not code:
        return "Authorization failed", 400

    result = handle_x_oauth_callback(uid, code)
    if result.get("status") != "success":
        return jsonify(result), 400

    return redirect("http://localhost:3000/connectors/x")


@app.route("/connectors/x/disconnect")
def x_disconnect():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    disconnect_x(uid)
    return jsonify({"status": "disconnected"})


@app.route("/connectors/x/sync")
def x_sync_route():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()
    cur.execute("""
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source='x'
        LIMIT 1
    """, (uid,))
    row = fetchone_secure(cur)
    con.close()

    sync_type = row["sync_type"] if row and row.get("sync_type") else "historical"
    return jsonify(sync_x(uid, sync_type=sync_type))


@app.route("/api/status/x")
def x_status():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT 1
        FROM connector_configs
        WHERE uid=? AND connector='x'
        LIMIT 1
    """, (uid,))
    creds = fetchone_secure(cur)

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='x'
        LIMIT 1
    """, (uid,))
    conn = fetchone_secure(cur)

    cur.execute("""
        SELECT username, x_user_id, expires_at
        FROM x_connections
        WHERE uid=?
        LIMIT 1
    """, (uid,))
    xr = fetchone_secure(cur)
    con.close()

    return jsonify({
        "has_credentials": bool(creds),
        "connected": bool(conn and conn["enabled"] == 1),
        "username": xr["username"] if xr else None,
        "x_user_id": xr["x_user_id"] if xr else None,
        "expires_at": xr["expires_at"] if xr else None
    })


@app.route("/connectors/x/job/get")
def x_job_get():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()
    try:
        cur.execute("""
            SELECT sync_type, schedule_time
            FROM connector_jobs
            WHERE uid=? AND source='x'
        """, (uid,))
        row = fetchone_secure(cur)
    finally:
        con.close()

    if not row:
        return jsonify({"exists": False})

    return jsonify({
        "exists": True,
        "sync_type": row["sync_type"],
        "schedule_time": row["schedule_time"]
    })


@app.route("/connectors/x/job/save", methods=["POST"])
def x_job_save():

    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json() or {}
    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")

    con = get_db()
    cur = con.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time)
        VALUES (?, 'x', ?, ?)
    """, (uid, sync_type, schedule_time))

    con.commit()
    con.close()
    return jsonify({"status": "job_saved"})

# ---------------- LINKEDIN MARKETING ----------------

@app.route("/connectors/linkedin/save_app", methods=["POST"])
def linkedin_save_app():

    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json() or {}
    client_id = data.get("client_id")
    client_secret = data.get("client_secret")
    redirect_uri = data.get("redirect_uri") or (request.host_url.rstrip("/") + "/connectors/linkedin/callback")
    linkedin_version = data.get("linkedin_version") or "202503"

    if not client_id or not client_secret:
        return jsonify({"error": "client_id and client_secret are required"}), 400

    con = get_db()
    cur = con.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, client_id, client_secret, api_key, scopes, status, created_at)
        VALUES (?, 'linkedin', ?, ?, ?, ?, 'configured', datetime('now'))
    """, (
        uid,
        encrypt_value(client_id),
        encrypt_value(client_secret),
        encrypt_value(linkedin_version),
        encrypt_value(redirect_uri),
    ))
    con.commit()
    con.close()

    ensure_connector_initialized(uid, "linkedin")
    return jsonify({"status": "saved"})


@app.route("/connectors/linkedin/connect")
def linkedin_connect():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    try:
        oauth_state = secrets.token_urlsafe(24)
        state = get_connector_state(uid, "linkedin") or {}
        state["oauth_state"] = oauth_state
        state["oauth_state_expires_at"] = (
            datetime.datetime.now(datetime.UTC) + datetime.timedelta(minutes=10)
        ).isoformat()
        save_connector_state(uid, "linkedin", state)
        return redirect(get_linkedin_auth_url(uid, oauth_state))
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/connectors/linkedin/callback")
def linkedin_callback():
    uid = getattr(g, "user_id", None)
    code = request.args.get("code")
    returned_state = request.args.get("state")

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    if not code:
        return "Authorization failed", 400

    stored_state = get_connector_state(uid, "linkedin") or {}
    expected_state = stored_state.get("oauth_state")
    expires_at = stored_state.get("oauth_state_expires_at")
    expires_dt = None
    if expires_at:
        try:
            expires_dt = datetime.datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
            if expires_dt.tzinfo is None:
                expires_dt = expires_dt.replace(tzinfo=datetime.UTC)
        except Exception:
            expires_dt = None

    if not expected_state or not returned_state or returned_state != expected_state:
        return jsonify({"error": "Invalid OAuth state"}), 400
    if not expires_dt or expires_dt < datetime.datetime.now(datetime.UTC):
        return jsonify({"error": "OAuth state expired"}), 400

    result = handle_linkedin_oauth_callback(uid, code)
    if result.get("status") != "success":
        return jsonify(result), 400

    stored_state.pop("oauth_state", None)
    stored_state.pop("oauth_state_expires_at", None)
    save_connector_state(uid, "linkedin", stored_state)

    return redirect("http://localhost:3000/connectors/linkedin")


@app.route("/connectors/linkedin/disconnect")
def linkedin_disconnect():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    disconnect_linkedin(uid)
    return jsonify({"status": "disconnected"})


@app.route("/connectors/linkedin/sync")
def linkedin_sync_route():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()
    cur.execute("""
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source='linkedin'
        LIMIT 1
    """, (uid,))
    row = fetchone_secure(cur)
    con.close()

    sync_type = row["sync_type"] if row and row.get("sync_type") else "historical"
    return jsonify(sync_linkedin(uid, sync_type=sync_type))


@app.route("/api/status/linkedin")
def linkedin_status():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT 1
        FROM connector_configs
        WHERE uid=? AND connector='linkedin'
        LIMIT 1
    """, (uid,))
    creds = fetchone_secure(cur)

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='linkedin'
        LIMIT 1
    """, (uid,))
    conn = fetchone_secure(cur)

    cur.execute("""
        SELECT linkedin_member_id, expires_at
        FROM linkedin_connections
        WHERE uid=?
        LIMIT 1
    """, (uid,))
    lk = fetchone_secure(cur)
    con.close()

    return jsonify({
        "has_credentials": bool(creds),
        "connected": bool(conn and conn["enabled"] == 1),
        "linkedin_member_id": lk["linkedin_member_id"] if lk else None,
        "expires_at": lk["expires_at"] if lk else None,
    })


@app.route("/connectors/linkedin/job/get")
def linkedin_job_get():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()
    try:
        cur.execute("""
            SELECT sync_type, schedule_time
            FROM connector_jobs
            WHERE uid=? AND source='linkedin'
        """, (uid,))
        row = fetchone_secure(cur)
    finally:
        con.close()

    if not row:
        return jsonify({"exists": False})

    return jsonify({
        "exists": True,
        "sync_type": row["sync_type"],
        "schedule_time": row["schedule_time"],
    })


@app.route("/connectors/linkedin/job/save", methods=["POST"])
def linkedin_job_save():

    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json() or {}
    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")

    con = get_db()
    cur = con.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time)
        VALUES (?, 'linkedin', ?, ?)
    """, (uid, sync_type, schedule_time))

    con.commit()
    con.close()
    return jsonify({"status": "job_saved"})

# ---------------- GITLAB ----------------

@app.route("/gitlab/connect")
def gitlab_connect():

    from backend.connectors.gitlab import get_auth_url

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    return redirect(get_auth_url(uid))

# ---------------- GITLAB SAVE CONFIG ----------------

@app.route("/connectors/gitlab/save_app", methods=["POST"])
def gitlab_save_app():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = encrypt_payload(request.get_json())

    client_id = data.get("client_id")
    client_secret = data.get("client_secret")

    if not client_id or not client_secret:
        return jsonify({
            "error": "Client ID & Secret required"
        }), 400

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, client_id, client_secret, created_at)
        VALUES (?, 'gitlab', ?, ?, datetime('now'))
    """, (
        uid,
        client_id,
        client_secret
    ))

    con.commit()
    con.close()

    ensure_connector_initialized(uid, "gitlab")
    return jsonify({"status": "saved"})

@app.route("/connectors/gitlab/disconnect")
def gitlab_disconnect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # 1. Disable connection
    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source='gitlab'
    """, (uid,))

    # 2. Delete token
    cur.execute("""
        DELETE FROM gitlab_tokens
        WHERE uid=?
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "disconnected"})

@app.route("/gitlab/callback")
def gitlab_callback():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    code = request.args.get("code")

    from backend.connectors.gitlab import exchange_code, save_token

    data = exchange_code(uid, code)
    save_token(uid, data)

    # Enable connection
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO google_connections
        (uid, source, enabled)
        VALUES (?, 'gitlab', 1)
    """, (uid,))

    con.commit()
    con.close()

    return redirect("http://localhost:3000/connectors/gitlab")

@app.route("/api/status/gitlab")
def gitlab_status():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # credentials exist?
    cur.execute("""
        SELECT 1
        FROM connector_configs
        WHERE uid=? AND connector='gitlab'
        LIMIT 1
    """, (uid,))
    creds = cur.fetchone()

    # connection enabled?
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='gitlab'
        LIMIT 1
    """, (uid,))
    row = fetchone_secure(cur)

    con.close()

    return jsonify({
        "has_credentials": bool(creds),
        "connected": bool(row and row[0] == 1)
    })

@app.route("/connectors/gitlab/job/get")
def gitlab_job_get():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='gitlab'
    """, (uid,))

    row = fetchone_secure(cur)
    con.close()

    if not row:
        return jsonify({"exists": False})

    return jsonify({
        "exists": True,
        "sync_type": row[0],
        "schedule_time": row[1]
    })

@app.route("/connectors/gitlab/job/save", methods=["POST"])
def gitlab_job_save():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = request.get_json()

    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time)
        VALUES (?, 'gitlab', ?, ?)
    """, (
        uid,
        sync_type,
        schedule_time
    ))

    con.commit()
    con.close()

    return jsonify({"status": "job_saved"})

@app.route("/connectors/gitlab/sync")
def gitlab_sync_universal():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='gitlab'
    """, (uid,))

    row = fetchone_secure(cur)

    if not row or row[0] != 1:
        con.close()
        return jsonify({"error": "not connected"}), 400

    cur.execute("""
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source='gitlab'
        LIMIT 1
    """, (uid,))

    job = cur.fetchone()
    sync_type = job[0] if job else "historical"

    con.close()

    from backend.connectors.gitlab import (
        sync_projects,
        sync_commits,
        sync_issues,
        sync_mrs
    )

    projects_data = sync_projects(uid)

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT project_id
        FROM gitlab_projects
        WHERE uid=?
    """, (uid,))

    project_ids = [r[0] for r in cur.fetchall()]
    con.close()

    new_rows = []

    for pid in project_ids:

        commits = sync_commits(uid, pid, sync_type)
        issues = sync_issues(uid, pid, sync_type)
        mrs = sync_mrs(uid, pid)

        new_rows += commits.get("rows", [])
        new_rows += issues.get("rows", [])
        new_rows += mrs.get("rows", [])

    from backend.destinations.destination_router import push_to_destination

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT dest_type, host, port, username, password, database_name, format
        FROM destination_configs
        WHERE uid=? AND source='gitlab' AND is_active=1
        LIMIT 1
    """, (uid,))

    dest_row = fetchone_secure(cur)
    con.close()

    if not dest_row:
        return jsonify({
            "projects": len(project_ids),
            "rows_pushed": 0,
            "sync_type": sync_type
        })

    dest = {
        "type": dest_row[0],
        "host": dest_row[1],
        "port": dest_row[2],
        "username": dest_row[3],
        "password": dest_row[4],
        "database_name": dest_row[5]
    }

    pushed = 0

    if new_rows:
        pushed = push_to_destination(dest, "gitlab_data", new_rows)
    else:
        pushed = 0

    print(f"[GITLAB] Sync type: {sync_type}", flush=True)
    print(f"[GITLAB] New rows found: {len(new_rows)}", flush=True)
    print(f"[GITLAB] Rows pushed: {pushed}", flush=True)

    return jsonify({
        "projects": len(project_ids),
        "rows_pushed": pushed,
        "rows_found": len(new_rows),
        "sync_type": sync_type
    })

# ---------------- STACKOVERFLOW ----------------

@app.route("/connectors/stackoverflow/connect")
def stackoverflow_connect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO google_connections
        (uid, source, enabled)
        VALUES (?, 'stackoverflow', 1)
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "connected"})

@app.route("/api/status/stackoverflow")
def stackoverflow_status():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # connection
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='stackoverflow'
        LIMIT 1
    """, (uid,))
    row = fetchone_secure(cur)

    connected = bool(row and row[0] == 1)

    # api key
    cur.execute("""
        SELECT api_key
        FROM connector_configs
        WHERE uid=? AND connector='stackoverflow'
        LIMIT 1
    """, (uid,))

    key_row = fetchone_secure(cur)

    api_key_saved = bool(key_row and key_row[0])

    con.close()

    return jsonify({
        "has_credentials": api_key_saved,
        "connected": connected
    })

@app.route("/connectors/stackoverflow/disconnect")
def stackoverflow_disconnect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source='stackoverflow'
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "disconnected"})


@app.route("/connectors/stackoverflow/job/get")
def stackoverflow_job_get():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='stackoverflow'
        LIMIT 1
    """, (uid,))

    row = fetchone_secure(cur)
    con.close()

    if not row:
        return jsonify({})

    return jsonify({
        "sync_type": row[0],
        "schedule_time": row[1]
    })


@app.route("/connectors/stackoverflow/job/save", methods=["POST"])
def stackoverflow_job_save():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time)
        VALUES (?, 'stackoverflow', ?, ?)
    """, (
        uid,
        data.get("sync_type"),
        data.get("schedule_time")
    ))

    con.commit()
    con.close()

    return jsonify({"status": "saved"})


@app.route("/connectors/stackoverflow/sync")
def stackoverflow_sync_universal():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # Check connection
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='stackoverflow'
    """, (uid,))

    row = fetchone_secure(cur)

    if not row or row[0] != 1:
        con.close()
        return jsonify({"error": "not connected"}), 400

    # Get sync type
    cur.execute("""
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source='stackoverflow'
        LIMIT 1
    """, (uid,))

    job = cur.fetchone()
    sync_type = job[0] if job else "historical"

    con.close()

    from backend.connectors.stackoverflow import (
        sync_questions,
        sync_answers,
        sync_users
    )

    q_data = sync_questions(uid, sync_type)
    a_data = sync_answers(uid, sync_type)
    u_data = sync_users(uid)

    q_rows = q_data.get("rows", [])
    a_rows = a_data.get("rows", [])
    u_rows = u_data.get("rows", [])

    # Destination
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT dest_type, host, port, username, password, database_name, format
        FROM destination_configs
        WHERE uid=? AND source=? AND is_active=1
        LIMIT 1
    """, (uid, "stackoverflow"))

    dest_row = fetchone_secure(cur)
    con.close()

    if not dest_row:
        return jsonify({
            "rows_pushed": 0,
            "sync_type": sync_type
        })

    dest = {
        "type": dest_row[0],
        "host": dest_row[1],
        "port": dest_row[2],
        "username": dest_row[3],
        "password": dest_row[4],
        "database_name": dest_row[5]
    }

    from backend.destinations.destination_router import push_to_destination

    pushed = 0

    if q_rows:
        pushed += push_to_destination(dest, "stack_questions", q_rows)

    if a_rows:
        pushed += push_to_destination(dest, "stack_answers", a_rows)

    if u_rows:
        pushed += push_to_destination(dest, "stack_users", u_rows)

    print(f"[STACK] Sync type: {sync_type}", flush=True)
    print(f"[STACK] Questions: {len(q_rows)}", flush=True)
    print(f"[STACK] Answers: {len(a_rows)}", flush=True)
    print(f"[STACK] Users: {len(u_rows)}", flush=True)
    print(f"[STACK] Rows pushed: {pushed}", flush=True)

    return jsonify({
        "questions": len(q_rows),
        "answers": len(a_rows),
        "users": len(u_rows),
        "rows_pushed": pushed,
        "sync_type": sync_type
    })

# ---------------- STACKOVERFLOW SAVE CONFIG ----------------

@app.route("/connectors/stackoverflow/save_config", methods=["POST"])
def stackoverflow_save_config():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = encrypt_payload(request.get_json())

    api_key = data.get("api_key")

    if not api_key:
        return jsonify({"error": "API key required"}), 400

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, api_key, created_at)
        VALUES (?, 'stackoverflow', ?, datetime('now'))
    """, (uid, api_key))

    con.commit()
    con.close()

    return jsonify({"status": "saved"})

# ---------------- HACKERNEWS ----------------

@app.route("/hackernews/sync")
def hackernews_sync():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    result = sync_hackernews(uid)

    return jsonify(result)

@app.route("/connectors/hackernews/sync")
def hackernews_sync_universal():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # Check connection
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='hackernews'
    """, (uid,))

    row = fetchone_secure(cur)

    if not row or row[0] != 1:
        con.close()
        return jsonify({"error": "not connected"}), 400

    # Get sync type
    cur.execute("""
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source='hackernews'
        LIMIT 1
    """, (uid,))

    job = cur.fetchone()
    sync_type = job[0] if job else "incremental"

    con.close()

    from backend.connectors.hackernews import sync_hackernews

    data = sync_hackernews(uid, sync_type)

    rows = data.get("rows", [])

    # Destination
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT dest_type, host, port, username, password, database_name, format
        FROM destination_configs
        WHERE uid=? AND source=? AND is_active=1
        LIMIT 1
    """, (uid, "hackernews"))

    dest_row = fetchone_secure(cur)
    con.close()

    if not dest_row:
        return jsonify({
            "stories": len(rows),
            "rows_pushed": 0,
            "sync_type": sync_type
        })

    dest = {
        "type": dest_row[0],
        "host": dest_row[1],
        "port": dest_row[2],
        "username": dest_row[3],
        "password": dest_row[4],
        "database_name": dest_row[5]
    }

    from backend.destinations.destination_router import push_to_destination

    pushed = 0

    if rows:
        pushed = push_to_destination(dest, "hackernews_stories", rows)

    print(f"[HN] Sync type: {sync_type}", flush=True)
    print(f"[HN] Stories: {len(rows)}", flush=True)
    print(f"[HN] Rows pushed: {pushed}", flush=True)

    return jsonify({
        "stories": len(rows),
        "rows_pushed": pushed,
        "sync_type": sync_type
    })


@app.route("/connectors/hackernews/connect")
def hackernews_connect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO google_connections
        (uid, source, enabled)
        VALUES (?, 'hackernews', 1)
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "connected"})

@app.route("/connectors/hackernews/disconnect")
def hackernews_disconnect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source='hackernews'
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "disconnected"})

@app.route("/connectors/hackernews/job/get")
def hackernews_job_get():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='hackernews'
        LIMIT 1
    """, (uid,))

    row = fetchone_secure(cur)
    con.close()

    if not row:
        return jsonify({})

    return jsonify({
        "sync_type": row[0],
        "schedule_time": row[1]
    })

@app.route("/connectors/hackernews/job/save", methods=["POST"])
def hackernews_job_save():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time)
        VALUES (?, 'hackernews', ?, ?)
    """, (
        uid,
        data.get("sync_type"),
        data.get("schedule_time")
    ))

    con.commit()
    con.close()

    return jsonify({"status": "saved"})

# ---------------- PRODUCTHUNT ----------------

@app.route("/connectors/producthunt/connect")
def producthunt_connect():

    uid=get_uid()

    con=get_db()
    cur=con.cursor()

    cur.execute("""
    SELECT 1 FROM connector_configs
    WHERE uid=? AND connector='producthunt'
    """,(uid,))

    if not cur.fetchone():
        return jsonify({"error":"config missing"}),400

    cur.execute("""
    INSERT OR REPLACE INTO google_connections
    (uid,source,enabled)
    VALUES (?,?,1)
    """,(uid,"producthunt"))

    con.commit()
    con.close()

    return jsonify({"status":"connected"})

@app.route("/connectors/producthunt/save_config", methods=["POST"])
def producthunt_save_config():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = encrypt_payload(request.get_json()) or {}

    token = data.get("api_token")

    if not token:
        return jsonify({"error":"token required"}),400

    con=get_db()
    cur=con.cursor()

    cur.execute("""
    INSERT OR REPLACE INTO connector_configs
    (uid,connector,config_json,created_at)
    VALUES (?,?,?,datetime('now'))
    """,(
        uid,
        "producthunt",
        json.dumps({"api_token":token})
    ))

    con.commit()
    con.close()

    return jsonify({"status":"saved"})

@app.route("/connectors/producthunt/disconnect")
def producthunt_disconnect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source='producthunt'
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "disconnected"})

@app.route("/api/status/producthunt")
def producthunt_status():

    uid=get_uid()
    con=get_db()
    cur=con.cursor()

    cur.execute("""
    SELECT enabled
    FROM google_connections
    WHERE uid=? AND source='producthunt'
    """,(uid,))

    connected=bool(
        (r:=cur.fetchone()) and r[0]==1
    )

    cur.execute("""
    SELECT 1 FROM connector_configs
    WHERE uid=? AND connector='producthunt'
    """,(uid,))

    has_credentials=bool(cur.fetchone())

    con.close()

    return jsonify({
        "connected":connected,
        "has_credentials":has_credentials
    })

@app.route("/connectors/producthunt/sync")
def producthunt_sync():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # Check enabled
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='producthunt'
    """, (uid,))

    row = fetchone_secure(cur)

    if not row or row[0] != 1:
        con.close()
        return jsonify({"error": "Product Hunt not connected"}), 400

    # Get job config
    cur.execute("""
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source='producthunt'
    """, (uid,))

    job = cur.fetchone()
    sync_type = job[0] if job else "incremental"

    con.close()

    # Run connector
    result = sync_producthunt(
        uid=uid,
        sync_type=sync_type
    )

    rows = result.get("rows", [])

    # Destination
    con = get_db()
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
        SELECT *
        FROM destination_configs
        WHERE uid=? AND source='producthunt'
        ORDER BY id DESC
        LIMIT 1
    """, (uid,))

    dest = cur.fetchone()
    con.close()

    if dest and rows:

        dest_cfg = dict(dest)
        dest_cfg["type"] = dest_cfg["dest_type"]

        inserted = push_to_destination(
            dest_cfg,
            "producthunt_data",
            rows
        )

        return jsonify({
            "status": "pushed_to_destination",
            "rows": inserted
        })

    return jsonify({
        "status": "stored_locally",
        "posts": result.get("posts", 0),
        "topics": result.get("topics", 0)
    })

# ---------------- PRODUCTHUNT DATA ----------------

@app.route("/producthunt/data/posts")
def producthunt_data_posts():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
    SELECT *
    FROM producthunt_posts
    WHERE uid=?
    ORDER BY created_at DESC
    """, (uid,))

    data = [dict(r) for r in cur.fetchall()]

    con.close()

    return jsonify(data)


@app.route("/producthunt/data/topics")
def producthunt_data_topics():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
    SELECT *
    FROM producthunt_topics
    WHERE uid=?
    ORDER BY followers DESC
    """, (uid,))

    data = [dict(r) for r in cur.fetchall()]

    con.close()

    return jsonify(data)

# ---------------- WIKIPEDIA ----------------

@app.route("/connectors/wikipedia/connect")
def wikipedia_connect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO google_connections
        (uid,source,enabled)
        VALUES (?,?,1)
    """,(uid,"wikipedia"))

    con.commit()
    con.close()

    return jsonify({"status":"connected"})

@app.route("/connectors/wikipedia/disconnect")
def wikipedia_disconnect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source='wikipedia'
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "disconnected"})

@app.route("/api/status/wikipedia")
def wikipedia_status():

    uid=get_uid()

    con=get_db()
    cur=con.cursor()

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='wikipedia'
    """,(uid,))

    connected=bool(
        (r:=cur.fetchone()) and r[0]==1
    )

    con.close()

    return jsonify({
        "connected":connected,
        "has_credentials":True
    })

@app.route("/connectors/wikipedia/sync")
def wikipedia_sync():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # Check enabled
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='wikipedia'
    """, (uid,))

    row = fetchone_secure(cur)

    if not row or row[0] != 1:
        con.close()
        return jsonify({"error": "Wikipedia not connected"}), 400

    # Get job config
    cur.execute("""
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source='wikipedia'
    """, (uid,))

    job = cur.fetchone()
    sync_type = job[0] if job else "incremental"

    con.close()

    # Run connector
    result = sync_wikipedia(
        uid=uid,
        sync_type=sync_type
    )

    rows = result.get("rows", [])

    # Destination
    con = get_db()
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
        SELECT *
        FROM destination_configs
        WHERE uid=? AND source='wikipedia'
        ORDER BY id DESC
        LIMIT 1
    """, (uid,))

    dest = cur.fetchone()
    con.close()

    if dest and rows:

        dest_cfg = dict(dest)
        dest_cfg["type"] = dest_cfg["dest_type"]

        inserted = push_to_destination(
            dest_cfg,
            "wikipedia_data",
            rows
        )

        return jsonify({
            "status": "pushed_to_destination",
            "rows": inserted
        })

    return jsonify({
        "status": "stored_locally",
        "recent_changes": result.get("recent_changes"),
        "new_pages": result.get("new_pages"),
        "most_viewed": result.get("most_viewed")
    })

# ---------------- PEERTUBE ----------------

@app.route("/connectors/peertube/sync")
def peertube_sync():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='peertube'
    """, (uid,))
    row = fetchone_secure(cur)

    if not row or row[0] != 1:
        con.close()
        return jsonify({"error": "PeerTube not connected"}), 400

    sync_type = get_connector_sync_type(uid, "peertube")

    con.close()

    result = sync_peertube(uid, sync_type=sync_type)

    rows = result.get("rows", [])

    # ---- Destination ----
    con = get_db()
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
        SELECT *
        FROM destination_configs
        WHERE uid=? AND source='peertube'
        ORDER BY id DESC
        LIMIT 1
    """, (uid,))

    row = fetchone_secure(cur)
    con.close()

    dest = None

    if row:
        dest = dict(row)
        if "dest_type" in dest:
            dest["type"] = dest["dest_type"]

    if dest and rows:
        inserted = push_to_destination(dest, "peertube_videos", rows)

        return jsonify({
            "status": "pushed_to_destination",
            "rows": inserted
        })

    return jsonify({
        "status": "stored_locally",
        "rows": len(rows)
    })

@app.route("/connectors/peertube/connect")
def peertube_connect():

    uid=get_uid()

    con=get_db()
    cur=con.cursor()

    cur.execute("""
    SELECT 1 FROM connector_configs
    WHERE uid=? AND connector='peertube'
    """,(uid,))

    if not cur.fetchone():
        return jsonify({"error":"config missing"}),400

    cur.execute("""
    INSERT OR REPLACE INTO google_connections
    (uid,source,enabled)
    VALUES (?,?,1)
    """,(uid,"peertube"))

    con.commit()
    con.close()

    return jsonify({"status":"connected"})

@app.route("/connectors/peertube/disconnect")
def peertube_disconnect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source='peertube'
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "disconnected"})

@app.route("/connectors/peertube/save_config", methods=["POST"])
def peertube_save_config():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = encrypt_payload(request.get_json()) or {}

    instance = data.get("instance")

    if not instance:
        return jsonify({"error":"instance required"}),400

    instance = instance.strip().rstrip("/")

    con=get_db()
    cur=con.cursor()

    cur.execute("""
    INSERT OR REPLACE INTO connector_configs
    (uid,connector,config_json,created_at)
    VALUES (?,?,?,datetime('now'))
    """,(
        uid,
        "peertube",
        json.dumps({"instance":instance})
    ))

    con.commit()
    con.close()

    return jsonify({"status":"saved"})

@app.route("/api/status/peertube")
def peertube_status():

    uid=get_uid()
    con=get_db()
    cur=con.cursor()

    cur.execute("""
    SELECT enabled
    FROM google_connections
    WHERE uid=? AND source='peertube'
    """,(uid,))

    connected=bool(
        (r:=cur.fetchone()) and r[0]==1
    )

    cur.execute("""
    SELECT 1
    FROM connector_configs
    WHERE uid=? AND connector='peertube'
    """,(uid,))

    has_credentials=bool(cur.fetchone())

    con.close()

    return jsonify({
        "connected":connected,
        "has_credentials":has_credentials
    })
# ---------------- MASTODON ----------------

@app.route("/connectors/mastodon/sync")
def mastodon_sync_universal():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # Check enabled
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='mastodon'
    """, (uid,))
    row = fetchone_secure(cur)

    if not row or row[0] != 1:
        con.close()
        return jsonify({"error": "not connected"}), 400

    # Get sync type
    cur.execute("""
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source='mastodon'
        LIMIT 1
    """, (uid,))
    job = cur.fetchone()
    sync_type = job[0] if job else "historical"

    # Get instance
    cur.execute("""
        SELECT state_json
        FROM connector_state
        WHERE uid=? AND source='mastodon'
    """, (uid,))
    state_row = fetchone_secure(cur)

    instance = "https://mastodon.social"

    if state_row:
        state = json.loads(state_row[0])
        instance = state.get("instance", instance)

    con.close()

    from backend.connectors.mastodon import sync_mastodon

    result = sync_mastodon(uid, instance, sync_type)

    rows = result.get("rows", [])

    # Destination lookup
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT dest_type, host, port, username, password, database_name, format
        FROM destination_configs
        WHERE uid=? AND source=? AND is_active=1
        LIMIT 1
    """, (uid, "mastodon"))

    dest_row = fetchone_secure(cur)
    con.close()

    pushed = 0

    if dest_row and rows:
        dest = {
            "type": dest_row[0],
            "host": dest_row[1],
            "port": dest_row[2],
            "username": dest_row[3],
            "password": dest_row[4],
            "database_name": dest_row[5]
        }

        from backend.destinations.destination_router import push_to_destination
        pushed = push_to_destination(dest, "mastodon_statuses_data", rows)

    return jsonify({
        "statuses": result.get("count", 0),
        "rows_pushed": pushed,
        "sync_type": sync_type
    })

@app.route("/connectors/mastodon/connect")
def mastodon_connect():

    uid=get_uid()

    con=get_db()
    cur=con.cursor()

    cur.execute("""
        SELECT config_json
        FROM connector_configs
        WHERE uid=? AND connector='mastodon'
        LIMIT 1
    """,(uid,))

    row=cur.fetchone()

    if not row:
        con.close()
        return jsonify({"error":"config missing"}),400

    state=json.loads(row[0])
    instance=state.get("instance")

    cur.execute("""
        INSERT OR REPLACE INTO connector_state
        (uid,source,state_json,updated_at)
        VALUES(?,?,?,?)
    """,(
        uid,
        "mastodon",
        json.dumps({"instance":instance}),
        datetime.datetime.now(datetime.UTC).isoformat()
    ))

    cur.execute("""
        INSERT OR REPLACE INTO google_connections
        (uid,source,enabled)
        VALUES(?, 'mastodon',1)
    """,(uid,))

    con.commit()
    con.close()

    return jsonify({"status":"connected"})

@app.route("/api/status/mastodon")
def mastodon_status():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # connection
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='mastodon'
        LIMIT 1
    """,(uid,))
    row = fetchone_secure(cur)

    connected = bool(row and row[0] == 1)

    # credentials
    cur.execute("""
        SELECT config_json
        FROM connector_configs
        WHERE uid=? AND connector='mastodon'
        LIMIT 1
    """,(uid,))
    cfg = cur.fetchone()

    has_credentials = bool(cfg)

    con.close()

    return jsonify({
        "connected": connected,
        "has_credentials": has_credentials
    })

@app.route("/connectors/mastodon/disconnect")
def mastodon_disconnect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source='mastodon'
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "disconnected"})

# ---------------- MASTODON SAVE CONFIG ----------------

@app.route("/connectors/mastodon/save_config", methods=["POST"])
def mastodon_save_config():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = encrypt_payload(request.get_json()) or {}

    instance = data.get("instance")

    if not instance:
        return jsonify({"error":"instance required"}),400

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, config_json, created_at)
        VALUES (?, 'mastodon', ?, datetime('now'))
    """,(
        uid,
        json.dumps({"instance":instance})
    ))

    con.commit()
    con.close()

    return jsonify({"status":"saved"})

# ---------------- DISCOURSE ----------------

@app.route("/connectors/discourse/connect")
def discourse_connect():

    uid=get_uid()

    con=get_db()
    cur=con.cursor()

    cur.execute("""
    SELECT 1 FROM connector_configs
    WHERE uid=? AND connector='discourse'
    """,(uid,))

    if not cur.fetchone():
        return jsonify({"error":"config missing"}),400

    cur.execute("""
    INSERT OR REPLACE INTO google_connections
    VALUES (?,?,1)
    """,(uid,"discourse"))

    con.commit()
    con.close()

    return jsonify({"status":"connected"})

@app.route("/connectors/discourse/save_config",methods=["POST"])
def discourse_save():

    uid=get_uid()
    data = encrypt_payload(request.get_json()) or {}

    forum=data.get("forum")
    api_key=data.get("api_key")
    api_user=data.get("api_user","system")

    if not forum:
        return jsonify({"error":"forum required"}),400

    con=get_db()
    cur=con.cursor()

    cur.execute("""
    INSERT OR REPLACE INTO connector_configs
    (uid,connector,config_json,created_at)
    VALUES (?,?,?,datetime('now'))
    """,(
        uid,
        "discourse",
        json.dumps({
            "forum":forum,
            "api_key":api_key,
            "api_user":api_user
        })
    ))

    con.commit()
    con.close()

    return jsonify({"status":"saved"})

@app.route("/connectors/discourse/disconnect")
def discourse_disconnect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source='discourse'
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "disconnected"})

@app.route("/api/status/discourse")
def discourse_status():

    uid=get_uid()

    con=get_db()
    cur=con.cursor()

    cur.execute("""
    SELECT enabled
    FROM google_connections
    WHERE uid=? AND source='discourse'
    """,(uid,))

    connected=bool(
        (r:=cur.fetchone()) and r[0]==1
    )

    cur.execute("""
    SELECT 1 FROM connector_configs
    WHERE uid=? AND connector='discourse'
    """,(uid,))

    has_credentials=bool(cur.fetchone())

    con.close()

    return jsonify({
        "connected":connected,
        "has_credentials":has_credentials
    })

@app.route("/connectors/discourse/sync")
def discourse_sync():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # Check enabled
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='discourse'
    """, (uid,))

    row = fetchone_secure(cur)

    if not row or row[0] != 1:
        con.close()
        return jsonify({"error": "Discourse not connected"}), 400

    # Get job config
    cur.execute("""
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source='discourse'
    """, (uid,))

    job = cur.fetchone()
    sync_type = job[0] if job else "incremental"

    con.close()

    # Run connector
    result = sync_discourse(
        uid=uid,
        sync_type=sync_type
    )

    rows = result.get("rows", [])

    # Destination
    con = get_db()
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
        SELECT *
        FROM destination_configs
        WHERE uid=? AND source='discourse'
        ORDER BY id DESC
        LIMIT 1
    """, (uid,))

    dest = cur.fetchone()
    con.close()

    if dest and rows:

        dest_cfg = dict(dest)
        dest_cfg["type"] = dest_cfg["dest_type"]

        inserted = push_to_destination(
            dest_cfg,
            "discourse_topics_data",
            rows
        )

        return jsonify({
            "status": "pushed_to_destination",
            "rows": inserted
        })

    return jsonify({
        "status": "stored_locally",
        "new_topics": result.get("new_topics", 0),
        "categories": result.get("categories", 0),
        "users": result.get("users", 0)
    })

# ---------------- DISCOURSE DATA ----------------

@app.route("/discourse/data/topics")
def discourse_topics():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = sqlite3.connect("identity.db")
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
        SELECT *
        FROM discourse_topics
        WHERE uid=?
        ORDER BY created_at DESC
        LIMIT 200
    """, (uid,))

    rows = fetchall_secure(cur)

    con.close()

    return jsonify([dict(r) for r in rows])


@app.route("/discourse/data/categories")
def discourse_categories():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = sqlite3.connect("identity.db")
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
        SELECT *
        FROM discourse_categories
        WHERE uid=?
    """, (uid,))

    rows = fetchall_secure(cur)

    con.close()

    return jsonify([dict(r) for r in rows])

# ---------------- LEMMY ----------------

@app.route("/connectors/lemmy/connect")
def lemmy_connect():

    uid=get_uid()

    con=get_db()
    cur=con.cursor()

    cur.execute("""
        SELECT config_json
        FROM connector_configs
        WHERE uid=? AND connector='lemmy'
        LIMIT 1
    """,(uid,))

    row=cur.fetchone()

    if not row:
        con.close()
        return jsonify({"error":"config missing"}),400

    state=json.loads(row[0])

    cur.execute("""
        INSERT OR REPLACE INTO connector_state
        (uid,source,state_json,updated_at)
        VALUES(?,?,?,?)
    """,(
        uid,
        "lemmy",
        json.dumps(state),
        datetime.datetime.now(datetime.UTC).isoformat()
    ))

    cur.execute("""
        INSERT OR REPLACE INTO google_connections
        (uid,source,enabled)
        VALUES(?, 'lemmy',1)
    """,(uid,))

    con.commit()
    con.close()

    return jsonify({"status":"connected"})

@app.route("/connectors/lemmy/disconnect")
def lemmy_disconnect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source='lemmy'
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "disconnected"})

@app.route("/connectors/lemmy/sync")
def lemmy_sync_universal():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # Check enabled
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='lemmy'
    """, (uid,))
    row = fetchone_secure(cur)

    if not row or row[0] != 1:
        con.close()
        return jsonify({"error": "not connected"}), 400

    # Get sync type
    cur.execute("""
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source='lemmy'
        LIMIT 1
    """, (uid,))
    job = cur.fetchone()
    sync_type = job[0] if job else "incremental"

    from backend.connectors.lemmy import sync_lemmy

    result = sync_lemmy(uid)

    # Fetch newly inserted posts
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
        SELECT *
        FROM lemmy_posts
        WHERE uid=?
        ORDER BY fetched_at DESC
        LIMIT ?
    """, (uid, result.get("new_posts", 0)))

    rows = []
    for r in cur.fetchall():
        d = dict(r)
        d.pop("id", None)
        d.pop("uid", None)
        d.pop("raw_json", None)
        d.pop("fetched_at", None)
        rows.append(d)

    # Destination lookup
    cur.execute("""
        SELECT dest_type, host, port, username, password, database_name, format
        FROM destination_configs
        WHERE uid=? AND source='lemmy' AND is_active=1
        LIMIT 1
    """, (uid,))

    dest_row = fetchone_secure(cur)
    con.close()

    if not dest_row:
        return jsonify({
            "posts": result.get("new_posts", 0),
            "rows_pushed": 0,
            "sync_type": sync_type
        })

    dest = {
        "type": dest_row[0],
        "host": dest_row[1],
        "port": dest_row[2],
        "username": dest_row[3],
        "password": dest_row[4],
        "database_name": dest_row[5]
    }

    from backend.destinations.destination_router import push_to_destination

    pushed = 0
    if rows:
        pushed = push_to_destination(dest, "lemmy_posts_data", rows)

    return jsonify({
        "posts": result.get("new_posts", 0),
        "rows_pushed": pushed,
        "sync_type": sync_type
    })

# ---------------- LEMMY SAVE CONFIG ----------------

@app.route("/connectors/lemmy/save_config", methods=["POST"])
def lemmy_save_config():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = encrypt_payload(request.get_json()) or {}

    instance = data.get("instance")

    if not instance:
        return jsonify({"error":"instance required"}),400

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, config_json, created_at)
        VALUES (?, 'lemmy', ?, datetime('now'))
    """,(
        uid,
        json.dumps({"instance":instance})
    ))

    con.commit()
    con.close()

    return jsonify({"status":"saved"})

@app.route("/api/status/lemmy")
def lemmy_status():

    uid=get_uid()

    con=get_db()
    cur=con.cursor()

    # connected
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='lemmy'
        LIMIT 1
    """,(uid,))
    row=cur.fetchone()

    connected = bool(row and row[0]==1)

    # credentials
    cur.execute("""
        SELECT config_json
        FROM connector_configs
        WHERE uid=? AND connector='lemmy'
        LIMIT 1
    """,(uid,))
    cfg=cur.fetchone()

    has_credentials = bool(cfg)

    con.close()

    return jsonify({
        "connected":connected,
        "has_credentials":has_credentials
    })

# ---------------- OPENSTREETMAP ----------------

@app.route("/connectors/openstreetmap/connect")
def osm_connect():

    uid=get_uid()

    con=get_db()
    cur=con.cursor()

    cur.execute("""
    INSERT OR REPLACE INTO google_connections
    (uid,source,enabled)
    VALUES (?,?,1)
    """,(uid,"openstreetmap"))

    con.commit()
    con.close()

    return jsonify({"status":"connected"})

@app.route("/connectors/openstreetmap/disconnect")
def osm_disconnect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source='openstreetmap'
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "disconnected"})

@app.route("/connectors/openstreetmap/sync")
def osm_sync():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # Check enabled
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='openstreetmap'
    """, (uid,))

    row = fetchone_secure(cur)

    if not row or row[0] != 1:
        con.close()
        return jsonify({"error": "OpenStreetMap not connected"}), 400

    # Get job config
    cur.execute("""
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source='openstreetmap'
    """, (uid,))

    job = cur.fetchone()
    sync_type = job[0] if job else "incremental"

    con.close()

    # Run connector
    result = sync_openstreetmap(
        uid=uid,
        sync_type=sync_type
    )

    rows = result.get("rows", [])

    # Destination lookup
    con = get_db()
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
        SELECT *
        FROM destination_configs
        WHERE uid=? AND source='openstreetmap'
        ORDER BY id DESC
        LIMIT 1
    """, (uid,))

    dest = cur.fetchone()
    con.close()

    if dest and rows:

        dest_cfg = dict(dest)
        dest_cfg["type"] = dest_cfg["dest_type"]

        inserted = push_to_destination(
            dest_cfg,
            "openstreetmap_data",
            rows
        )

        return jsonify({
            "status": "pushed_to_destination",
            "rows": inserted
        })

    return jsonify({
        "status": "stored_locally",
        "new_changesets": result.get("new_changesets", 0),
        "new_notes": result.get("new_notes", 0)
    })

@app.route("/api/status/openstreetmap")
def osm_status():

    uid=get_uid()
    con=get_db()
    cur=con.cursor()

    cur.execute("""
    SELECT enabled
    FROM google_connections
    WHERE uid=? AND source='openstreetmap'
    """,(uid,))

    connected=bool(
        (r:=cur.fetchone()) and r[0]==1
    )

    con.close()

    return jsonify({
        "connected":connected,
        "has_credentials":True
    })

# ---------------- NVD ----------------

@app.route("/connectors/nvd/sync")
def nvd_sync_universal():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # Check connection
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='nvd'
    """, (uid,))

    row = fetchone_secure(cur)

    if not row or row[0] != 1:
        con.close()
        return jsonify({"error": "not connected"}), 400

    # Get sync type
    cur.execute("""
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source='nvd'
        LIMIT 1
    """, (uid,))

    job = cur.fetchone()
    sync_type = job[0] if job else "incremental"

    con.close()

    from backend.connectors.nvd import sync_nvd

    result = sync_nvd(uid, sync_type)

    rows = result.get("rows", [])

    # -------- Destination ----------
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT dest_type, host, port, username, password, database_name, format
        FROM destination_configs
        WHERE uid=? AND source=? AND is_active=1
        LIMIT 1
    """, (uid, "nvd"))

    dest_row = fetchone_secure(cur)
    con.close()

    if not dest_row:
        return jsonify({
            "cves": len(rows),
            "rows_pushed": 0,
            "sync_type": sync_type
        })

    dest = {
        "type": dest_row[0],
        "host": dest_row[1],
        "port": dest_row[2],
        "username": dest_row[3],
        "password": dest_row[4],
        "database_name": dest_row[5]
    }

    from backend.destinations.destination_router import push_to_destination

    pushed = 0

    if rows:
        pushed += push_to_destination(dest, "nvd_cves", rows)

    print(f"[NVD] Sync type: {sync_type}", flush=True)
    print(f"[NVD] CVEs: {len(rows)}", flush=True)
    print(f"[NVD] Rows pushed: {pushed}", flush=True)

    return jsonify({
        "cves": len(rows),
        "rows_pushed": pushed,
        "sync_type": sync_type
    })

@app.route("/connectors/nvd/connect")
def nvd_connect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO google_connections
        (uid, source, enabled)
        VALUES (?, 'nvd', 1)
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "connected"})

@app.route("/connectors/nvd/disconnect")
def nvd_disconnect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source='nvd'
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "disconnected"})

@app.route("/api/status/nvd")
def nvd_status():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # credentials
    cur.execute("""
        SELECT api_key
        FROM connector_configs
        WHERE uid=? AND connector='nvd'
        LIMIT 1
    """, (uid,))
    creds = cur.fetchone()

    # connection
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='nvd'
        LIMIT 1
    """, (uid,))
    row = fetchone_secure(cur)

    con.close()

    return jsonify({
        "has_credentials": bool(creds and creds[0]),
        "connected": bool(row and row[0] == 1)
    })

@app.route("/connectors/nvd/job/save", methods=["POST"])
def nvd_job_save():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time)
        VALUES (?, 'nvd', ?, ?)
    """, (
        uid,
        data.get("sync_type"),
        data.get("schedule_time")
    ))

    con.commit()
    con.close()

    return jsonify({"status": "saved"})

# ---------------- NVD SAVE CONFIG ----------------

@app.route("/connectors/nvd/save_config", methods=["POST"])
def nvd_save_config():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = encrypt_payload(request.get_json())

    api_key = data.get("api_key")
    keywords = data.get("keywords", [])

    if not api_key:
        return jsonify({"error": "API key required"}), 400

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_configs
        (
            uid,
            connector,
            api_key,
            config_json,
            created_at
        )
        VALUES (?, 'nvd', ?, ?, datetime('now'))
    """, (
        uid,
        api_key,
        json.dumps({
            "keywords": keywords
        })
    ))

    con.commit()
    con.close()

    return jsonify({"status": "saved"})

# ---------------- PINTEREST ----------------

@app.route("/connectors/pinterest/connect")
def pinterest_connect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    auth_url = pinterest_get_auth_url(uid)

    if not auth_url:
        return jsonify({"error":"credentials missing"}),400

    return redirect(auth_url)
    
@app.route("/connectors/pinterest/disconnect")
def pinterest_disconnect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # Remove token
    cur.execute("DELETE FROM pinterest_tokens WHERE uid=?", (uid,))

    # Disable connector
    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source='pinterest'
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "disconnected"})

@app.route("/pinterest/callback")
def pinterest_callback():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    code = request.args.get("code")

    token_data = pinterest_exchange_code(uid, code)

    if not token_data:
        return "Token exchange failed",400
    
@app.route("/connectors/pinterest/sync")
def pinterest_sync_universal():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # Check enabled
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='pinterest'
    """, (uid,))
    row = fetchone_secure(cur)

    if not row or row[0] != 1:
        con.close()
        return jsonify({"error": "not connected"}), 400

    # Run sync
    result = sync_pinterest(uid)

    # Fetch newly inserted pins
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
        SELECT *
        FROM pinterest_pins
        WHERE uid=?
        ORDER BY fetched_at DESC
        LIMIT ?
    """, (uid, result.get("pins", 0)))

    rows = []
    for r in cur.fetchall():
        d = dict(r)
        d.pop("id", None)
        d.pop("uid", None)
        d.pop("raw_json", None)
        d.pop("fetched_at", None)
        rows.append(d)

    # Destination lookup
    cur.execute("""
        SELECT dest_type, host, port, username, password, database_name, format
        FROM destination_configs
        WHERE uid=? AND source='pinterest' AND is_active=1
        LIMIT 1
    """, (uid,))

    dest_row = fetchone_secure(cur)
    con.close()

    if not dest_row:
        return jsonify({
            "pins": result.get("pins", 0),
            "rows_pushed": 0
        })

    dest = {
        "type": dest_row[0],
        "host": dest_row[1],
        "port": dest_row[2],
        "username": dest_row[3],
        "password": dest_row[4],
        "database_name": dest_row[5]
    }

    from backend.destinations.destination_router import push_to_destination

    pushed = 0
    if rows:
        pushed = push_to_destination(dest, "pinterest_pins_data", rows)

    return jsonify({
        "pins": result.get("pins", 0),
        "rows_pushed": pushed
    })

# ---------------- PINTEREST SAVE CONFIG ----------------

@app.route("/connectors/pinterest/save_config", methods=["POST"])
def pinterest_save_config():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = encrypt_payload(request.get_json) or {}

    client_id = data.get("client_id")
    client_secret = data.get("client_secret")

    if not client_id or not client_secret:
        return jsonify({"error": "missing credentials"}), 400

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, config_json, created_at)
        VALUES (?, ?, ?, datetime('now'))
    """, (
        uid,
        "pinterest",
        json.dumps({
            "client_id": client_id,
            "client_secret": client_secret
        })
    ))

    con.commit()
    con.close()

    return jsonify({"status": "saved"})    

@app.route("/api/status/pinterest")
def pinterest_status():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='pinterest'
    """,(uid,))

    connected = bool(
        (row := cur.fetchone()) and row[0] == 1
    )

    cur.execute("""
        SELECT 1
        FROM connector_configs
        WHERE uid=? AND connector='pinterest'
    """,(uid,))

    has_credentials = bool(cur.fetchone())

    con.close()

    return jsonify({
        "connected": connected,
        "has_credentials": has_credentials
    })

# ---------------- FACEBOOK PAGES SAVE APP CREDENTIALS ----------------

@app.route("/connectors/facebook/save_app", methods=["POST"])
def facebook_save_app():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    print("SAVE UID:", uid, flush=True)
    data = request.get_json()

    app_id = data.get("app_id")
    app_secret = data.get("app_secret")

    if not app_id or not app_secret:
        return jsonify({"error": "App ID and App Secret required"}), 400

    redirect_uri = request.host_url.rstrip("/") + "/connectors/facebook/callback"

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO facebook_app_credentials
        (uid, app_id, app_secret, redirect_uri, created_at)
        VALUES (?, ?, ?, ?, ?)
    """, (
        uid,
        app_id,
        app_secret,
        redirect_uri,
        datetime.datetime.now(datetime.UTC).isoformat()
    ))

    con.commit()
    con.close()

    ensure_connector_initialized(uid, "facebook")
    return jsonify({"status": "saved"})

#-------------- Temporary route to test saving Facebook credentials without going through the UI --------------
@app.route("/connectors/facebook/test_save", methods=["GET"])
def facebook_test_save():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO facebook_app_credentials
        (uid, app_id, app_secret, redirect_uri, created_at)
        VALUES (?, ?, ?, ?, ?)
    """, (
        uid,
        "TEST_APP_ID",
        "TEST_SECRET",
        request.host_url.rstrip("/") + "/connectors/facebook/callback",
        datetime.datetime.now(datetime.UTC).isoformat()
    ))

    con.commit()
    con.close()

    return "Test credentials saved"

# ---------------- FACEBOOK PAGES CONNECT ----------------

@app.route("/connectors/facebook/connect", methods=["GET"])
def facebook_connect():
    print("CONNECT UID:", uid, flush=True)
    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT app_id, redirect_uri
        FROM facebook_app_credentials
        WHERE uid=?
    """, (uid,))

    row = fetchone_secure(cur)
    con.close()

    if not row:
        return "App credentials not saved", 400

    app_id, redirect_uri = row

    params = {
        "client_id": app_id,
        "redirect_uri": redirect_uri,
        "scope": "pages_show_list,pages_read_engagement,pages_read_user_content,read_insights",
        "response_type": "code"
    }

    auth_url = "https://www.facebook.com/v19.0/dialog/oauth?" + urlencode(params)

    return redirect(auth_url)

# ---------------- FACEBOOK PAGES CALLBACK ----------------

@app.route("/connectors/facebook/callback", methods=["GET"])
def facebook_callback():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    code = request.args.get("code")

    if not code:
        return "Authorization failed: No code received", 400

    # Get user app credentials
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT app_id, app_secret, redirect_uri
        FROM facebook_app_credentials
        WHERE uid=?
    """, (uid,))

    row = fetchone_secure(cur)

    if not row:
        con.close()
        return "App credentials not found", 400

    app_id, app_secret, redirect_uri = row
    con.close()

    # Exchange code for user access token
    token_res = requests.get(
        "https://graph.facebook.com/v19.0/oauth/access_token",
        params={
            "client_id": app_id,
            "redirect_uri": redirect_uri,
            "client_secret": app_secret,
            "code": code
        },
        timeout=30
    )

    token_data = token_res.json()

    user_token = token_data.get("access_token")

    if not user_token:
        return jsonify({"error": "Token exchange failed", "details": token_data}), 400

    # Get managed pages
    pages_res = requests.get(
        "https://graph.facebook.com/v19.0/me/accounts",
        params={"access_token": user_token},
        timeout=30
    )

    pages_data = pages_res.json()
    pages = pages_data.get("data", [])

    if not pages:
        return jsonify({"error": "No pages found"}), 400

    # For now select first page (we can improve later)
    page = pages[0]

    # Store page connection
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO facebook_connections
        (uid, page_id, page_name, page_access_token, connected_at)
        VALUES (?, ?, ?, ?, ?)
    """, (
        uid,
        page.get("id"),
        page.get("name"),
        page.get("access_token"),
        datetime.datetime.now(datetime.UTC).isoformat()
    ))

    # Enable connector
    cur.execute("""
        INSERT OR REPLACE INTO google_connections
        (uid, source, enabled)
        VALUES (?, 'facebook', 1)
    """, (uid,))

    con.commit()
    con.close()

    return "Facebook Page Connected Successfully"

# ---------------- FACEBOOK PAGES DISCONNECT ----------------

@app.route("/connectors/facebook/disconnect", methods=["GET"])
def facebook_disconnect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # Remove stored page token
    cur.execute("DELETE FROM facebook_connections WHERE uid=?", (uid,))

    # Disable connector
    cur.execute("""
        UPDATE google_connections
        SET enabled = 0
        WHERE uid=? AND source='facebook'
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "facebook disconnected"})

# ---------------- FACEBOOK PAGES SYNC ----------------

@app.route("/connectors/facebook/sync")
def facebook_sync():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    # Check enabled
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='facebook'
    """, (uid,))

    row = fetchone_secure(cur)
    con.close()

    if not row or row[0] != 1:
        return jsonify({"error": "Facebook not connected"}), 400

    sync_type = "historical"

    job_res = get_connector_job("facebook")
    try:
        job_data = job_res.get_json()
        if job_data.get("exists"):
            sync_type = job_data.get("sync_type", "historical")
    except:
        import traceback; traceback.print_exc()
        print('Exception caught', flush=True)

    result = sync_facebook_pages(uid, sync_type)

    # Destination push
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT *
        FROM destination_configs
        WHERE uid=? AND source='facebook'
        ORDER BY id DESC
        LIMIT 1
    """, (uid,))

    dest = cur.fetchone()
    con.close()

    if dest and result.get("rows"):
        inserted = push_to_destination(dest, "facebook_data", result["rows"])
        return jsonify({
            "status": "pushed_to_destination",
            "rows": inserted
        })

    return jsonify({
        "status": "stored_locally",
        "posts": result.get("posts", 0),
        "insights": result.get("insights", 0)
    })

@app.route("/connectors/facebook/job/get")
def facebook_job_get():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='facebook'
    """, (uid,))

    row = fetchone_secure(cur)
    con.close()

    if not row:
        return jsonify({"exists": False})

    return jsonify({
        "exists": True,
        "sync_type": row[0],
        "schedule_time": row[1]
    })

# ---------------- FACEBOOK PAGES STATUS ----------------

@app.route("/api/status/facebook")
def facebook_status():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # Check saved app credentials
    cur.execute("""
        SELECT 1 FROM facebook_app_credentials
        WHERE uid=?
        LIMIT 1
    """, (uid,))
    creds = cur.fetchone()

    # Check page connection
    cur.execute("""
        SELECT 1 FROM facebook_connections
        WHERE uid=?
        LIMIT 1
    """, (uid,))
    conn = cur.fetchone()

    con.close()

    return jsonify({
        "connected": bool(conn),
        "has_credentials": bool(creds)
    })

@app.route("/connectors/<source>/disconnect")
def connector_disconnect(source):

    print("DISCONNECT CALLED:", source, flush=True)

    con = sqlite3.connect(DB)
    cur = con.cursor()

    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE source=?
    """, (source,))

    con.commit()
    con.close()

    return jsonify({"status": "disconnected"})

# ---------------- FACEBOOK ADS CONNECT ----------------

@app.route("/connectors/facebook_ads/connect", methods=["GET"])
def facebook_ads_connect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    print("ADS CONNECT UID:", uid, flush=True)

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT app_id, redirect_uri
        FROM facebook_app_credentials
        WHERE uid=?
    """, (uid,))

    row = fetchone_secure(cur)
    con.close()

    if not row:
        return "App credentials not saved", 400

    app_id, redirect_uri = row

    # IMPORTANT: different scope for Ads
    params = {
        "client_id": app_id,
        "redirect_uri": redirect_uri,
        "scope": "ads_read,ads_management",
        "response_type": "code"
    }

    auth_url = "https://www.facebook.com/v19.0/dialog/oauth?" + urlencode(params)

    return redirect(auth_url)

# ---------------- FACEBOOK ADS CALLBACK ----------------

@app.route("/connectors/facebook_ads/callback", methods=["GET"])
def facebook_ads_callback():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    code = request.args.get("code")

    if not code:
        return "Authorization failed: No code received", 400

    # Get app credentials
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT app_id, app_secret, redirect_uri
        FROM facebook_app_credentials
        WHERE uid=?
    """, (uid,))

    row = fetchone_secure(cur)

    if not row:
        con.close()
        return "App credentials not found", 400

    app_id, app_secret, redirect_uri = row
    con.close()

    # Exchange code for token
    token_res = requests.get(
        "https://graph.facebook.com/v19.0/oauth/access_token",
        params={
            "client_id": app_id,
            "redirect_uri": redirect_uri,
            "client_secret": app_secret,
            "code": code
        },
        timeout=30
    )

    token_data = token_res.json()
    user_token = token_data.get("access_token")

    if not user_token:
        return jsonify({"error": "Token exchange failed", "details": token_data}), 400

    # Fetch Ad Accounts
    accounts_res = requests.get(
        "https://graph.facebook.com/v19.0/me/adaccounts",
        params={
            "access_token": user_token,
            "fields": "id,name,account_status,currency,timezone_name"
        },
        timeout=30
    )

    accounts_data = accounts_res.json()
    accounts = accounts_data.get("data", [])

    if not accounts:
        return jsonify({"error": "No Ad Accounts found"}), 400

    # Select first ad account for now
    account = accounts[0]

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO facebook_ads_connections
        (uid, ad_account_id, ad_account_name, access_token, connected_at)
        VALUES (?, ?, ?, ?, ?)
    """, (
        uid,
        account.get("id"),
        account.get("name"),
        user_token,
        datetime.datetime.now(datetime.UTC).isoformat()
    ))

    # Enable connector
    cur.execute("""
        INSERT OR REPLACE INTO google_connections
        (uid, source, enabled)
        VALUES (?, 'facebook_ads', 1)
    """, (uid,))

    con.commit()
    con.close()

    return "Facebook Ads Connected Successfully"

# ---------------- FACEBOOK ADS STATUS ----------------

@app.route("/api/status/facebook_ads")
def facebook_ads_status():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # Check saved app credentials
    cur.execute("""
        SELECT 1 FROM facebook_app_credentials
        WHERE uid=?
        LIMIT 1
    """, (uid,))
    creds = cur.fetchone()

    # Check ads connection
    cur.execute("""
        SELECT 1 FROM facebook_ads_connections
        WHERE uid=?
        LIMIT 1
    """, (uid,))
    conn = cur.fetchone()

    con.close()

    return jsonify({
        "connected": bool(conn),
        "has_credentials": bool(creds)
    })

# ---------------- FACEBOOK ADS DISCONNECT ----------------

@app.route("/connectors/facebook_ads/disconnect", methods=["GET"])
def facebook_ads_disconnect():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # Remove stored ad account connection
    cur.execute("DELETE FROM facebook_ads_connections WHERE uid=?", (uid,))

    # Disable connector
    cur.execute("""
        UPDATE google_connections
        SET enabled = 0
        WHERE uid=? AND source='facebook_ads'
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "facebook_ads disconnected"})

# ---------------- FACEBOOK ADS JOB GET ----------------

@app.route("/connectors/facebook_ads/job/get")
def facebook_ads_job_get():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='facebook_ads'
    """, (uid,))

    row = fetchone_secure(cur)
    con.close()

    if not row:
        return jsonify({"exists": False})

    return jsonify({
        "exists": True,
        "sync_type": row[0],
        "schedule_time": row[1]
    })

# ---------------- FACEBOOK ADS JOB SAVE ----------------

@app.route("/connectors/facebook_ads/job/save", methods=["POST"])
def facebook_ads_job_save():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = request.get_json()

    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time)
        VALUES (?, 'facebook_ads', ?, ?)
    """, (uid, sync_type, schedule_time))

    con.commit()
    con.close()

    return jsonify({"status": "job_saved"})

# ---------------- FACEBOOK ADS SYNC ----------------

from backend.connectors.facebook_ads import sync_facebook_ads

@app.route("/connectors/facebook_ads/sync")
def facebook_ads_sync():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    # Check enabled
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='facebook_ads'
    """, (uid,))

    row = fetchone_secure(cur)
    con.close()

    if not row or row[0] != 1:
        return jsonify({"error": "Facebook Ads not connected"}), 400

    sync_type = "historical"

    job_res = facebook_ads_job_get()
    try:
        job_data = job_res.get_json()
        if job_data.get("exists"):
            sync_type = job_data.get("sync_type", "historical")
    except:
        import traceback; traceback.print_exc()
        print('Exception caught', flush=True)

    result = sync_facebook_ads(uid, sync_type)

    # Destination push
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT *
        FROM destination_configs
        WHERE uid=? AND source='facebook_ads'
        ORDER BY id DESC
        LIMIT 1
    """, (uid,))

    dest = cur.fetchone()
    con.close()

    if dest and result.get("rows"):
        inserted = push_to_destination(dest, "facebook_ads_data", result["rows"])
        return jsonify({
            "status": "pushed_to_destination",
            "rows": inserted
        })

    return jsonify({
        "status": "stored_locally",
        "campaigns": result.get("campaigns", 0),
        "adsets": result.get("adsets", 0),
        "ads": result.get("ads", 0),
        "insights": result.get("insights", 0)
    })

# ---------------- FACEBOOK ADS SAVE APP CREDENTIALS ----------------

@app.route("/connectors/facebook_ads/save_app", methods=["POST"])
def facebook_ads_save_app():

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = request.get_json()

    app_id = data.get("app_id")
    app_secret = data.get("app_secret")

    if not app_id or not app_secret:
        return jsonify({"error": "App ID and App Secret required"}), 400

    redirect_uri = request.host_url.rstrip("/") + "/connectors/facebook_ads/callback"

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO facebook_app_credentials
        (uid, app_id, app_secret, redirect_uri, created_at)
        VALUES (?, ?, ?, ?, ?)
    """, (
        uid,
        app_id,
        app_secret,
        redirect_uri,
        datetime.datetime.now(datetime.UTC).isoformat()
    ))

    con.commit()
    con.close()

    ensure_connector_initialized(uid, "facebook_ads")
    return jsonify({"status": "saved"})

@app.route("/connectors/<source>/connect")
def connector_connect(source):

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    state = _resolve_connector_contract(uid, source)
    if state["connected"]:
        return jsonify({"connected": True})

    if source in OAUTH_SOURCES:
        if not _has_connector_config(uid, source):
            return jsonify({"connected": False, "error": "missing credentials"}), 400

        auth_url = _oauth_redirect_for(source)
        if not auth_url:
            return jsonify({"connected": False, "error": "missing credentials"}), 400

        if _is_internal_ai_request():
            return jsonify({
                "connected": False,
                "auth_required": True,
                "redirect": auth_url,
            })
        return redirect(auth_url)

    if not _has_connector_config(uid, source):
        return jsonify({"connected": False, "error": "missing credentials"}), 400

    result = _run_connector_connect_probe(uid, source)
    if isinstance(result, dict) and (
        result.get("connected") is True
        or result.get("status") in ("success", "connected")
    ):
        return jsonify({"connected": True})

    error = _normalize_probe_error(result or {})
    return jsonify({"connected": False, "error": error}), 400

@app.route("/connectors/<source>/disconnect")
def disconnect_connector(source):

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # Disable connection
    cur.execute("""
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source=?
    """, (uid, source))

    # Disable scheduled jobs
    cur.execute("""
        UPDATE connector_jobs
        SET enabled=0
        WHERE uid=? AND source=?
    """, (uid, source))

    con.commit()
    con.close()

    return jsonify({
        "status": "success",
        "message": f"{source} disconnected"
    })

#sdk_route

@app.route("/static/sdk/<path:filename>")
def sdk_file(filename):
    return send_from_directory("static/sdk", filename)

# ---------------- DOMAINS API ----------------

@app.route("/api/domains")
def get_domains():
    con = get_db()
    cur = con.cursor()
    cur.execute("""
        SELECT DISTINCT domain
        FROM visits
        WHERE domain IS NOT NULL
          AND domain != ''
        ORDER BY domain
    """)
    rows = fetchall_secure(cur)  # List of dicts
    domains = [r["domain"] for r in rows]  # Use key instead of r[0]
    con.close()
    return jsonify(domains)

@app.route("/api/dashboard")
def api_dashboard():

    domain = request.args.get("domain")

    if not domain:
        return jsonify({"error": "domain required"}), 400


    con = get_db()
    cur = con.cursor()


    # ---------------- VISITS (FULL) ----------------

    cur.execute("""
        SELECT
        uid,domain,browser,os,device,ip,
        screen,language,timezone,
        referrer,page_url,user_agent,
        name,age,gender,city,country,profession,
        ts
        FROM visits
        WHERE domain=?
        ORDER BY ts DESC
        LIMIT 300
    """,(domain,))

    visits = cur.fetchall()


    # ---------------- EVENTS (FULL) ----------------

    cur.execute("""
        SELECT
        uid,domain,event,
        device_id,session_id,
        meta,ts
        FROM web_events
        WHERE domain=?
        ORDER BY ts DESC
        LIMIT 500
    """,(domain,))

    events = cur.fetchall()


    # ---------------- IDENTITIES ----------------

    uids = list(set([v[0] for v in visits]))

    identities = []

    if uids:

        placeholders = ",".join("?" for _ in uids)

        cur.execute(f"""
            SELECT
            uid,email,device_id,
            session_id,external_id,
            created_at
            FROM identity_map
            WHERE uid IN ({placeholders})
            ORDER BY created_at DESC
        """, uids)

        identities = cur.fetchall()


    con.close()


    return jsonify({
        "domain": domain,
        "visits": visits,
        "events": events,
        "identities": identities
    })

@app.route("/google/job/save/<source>", methods=["POST"])
def save_connector_job(source):

    data = request.json

    sync_type = data.get("sync_type")
    schedule_time = data.get("schedule_time")

    uid = getattr(g, "user_id", None)

    if not sync_type or not schedule_time:
        return jsonify({
            "status": "error",
            "message": "Missing fields"
        }), 400


    con = get_db()
    cur = con.cursor()

    try:

        # Remove old job
        cur.execute("""
            DELETE FROM connector_jobs
            WHERE uid=? AND source=?
        """, (uid, source))


        # Insert new job
        cur.execute("""
            INSERT INTO connector_jobs
            (uid, source, sync_type, schedule_time, enabled, created_at)
            VALUES (?, ?, ?, ?, 1, ?)
        """, (
            uid,
            source,
            sync_type,
            schedule_time,
            datetime.datetime.now(IST).isoformat()
        ))


        con.commit()

    finally:
        con.close()


    return jsonify({"status": "ok"})

@app.route("/google/job/get/<source>")
def get_connector_job(source):
    uid = getattr(g, "user_id", None)
    con = get_db()
    cur = con.cursor()
    cur.execute("""
        SELECT sync_type, schedule_time, enabled
        FROM connector_jobs
        WHERE uid=? AND source=?
        LIMIT 1
    """, (uid, source))
    row = fetchone_secure(cur)  # Dict
    con.close()
    if not row:
        return jsonify({"exists": False})
    return jsonify({
        "exists": True,
        "sync_type": row["sync_type"],  # Use key
        "schedule_time": row["schedule_time"],
        "enabled": row["enabled"]
    })

def get_connector_sync_type(uid, source):
    con = get_db()
    cur = con.cursor()
    cur.execute("""
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source=? AND enabled=1
        LIMIT 1
    """, (uid, source))
    row = fetchone_secure(cur)  # Dict
    con.close()
    if not row:
        return "historical"
    return row["sync_type"]  # Use key

@app.route("/destination/save", methods=["POST"])
def save_destination():

    data = encrypt_payload(request.json)

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    
    source = data.get("source")

    dest_type = data.get("type")

    host = data.get("host")
    port = data.get("port")

    username = data.get("username")
    password = data.get("password")
    database = data.get("database")
    # -------- FORMAT CONTROL --------
    format_value = data.get("format")

    # allow format ONLY for object-storage destinations
    if dest_type not in ["s3", "bigquery", "azure_datalake"]:
        format_value = None

    # ---------------- Validation ---------------- #

    if not source or not dest_type:
        return jsonify({
            "status": "error",
            "msg": "Missing source or destination type"
        }), 400


    # ---------- MySQL / Postgres Validation ----------

    if dest_type in ["mysql", "postgres"]:

        if not host or not username or not database:
            return jsonify({
                "status": "error",
                "msg": "Missing database credentials"
            }), 400

    # ---------- Redshift Validation ----------

    if dest_type == "redshift":

        if not host or not username or not password or not database:
            return jsonify({
                "status": "error",
                "msg": "Missing Redshift credentials"
            }), 400


    # ---------- BigQuery Validation ----------

    if dest_type == "bigquery":

        if not host or not password or not database:
            return jsonify({
                "status": "error",
                "msg": "Missing BigQuery credentials"
            }), 400

    # ---------- Azure Data Lake Validation ----------

    if dest_type == "azure_datalake":

        if not host or not port or not password:
            return jsonify({
                "status": "error",
                "msg": "Missing Azure Data Lake credentials"
            }), 400

    # ---------- Databricks Validation ----------

    if dest_type == "databricks":

        if not host or not port or not password:
            return jsonify({
                "status": "error",
                "msg": "Missing Databricks credentials"
            }), 400


    con = get_db()
    cur = con.cursor()

    try:

        # ---------------- Deactivate Old ----------------

        cur.execute("""
            UPDATE destination_configs
            SET is_active = 0
            WHERE uid=? AND source=?
        """, (uid, source))


        # ---------------- Insert New ----------------

        cur.execute("""
            INSERT INTO destination_configs
            (
                uid, source, dest_type,
                host, port,
                username, password,
                database_name,
                is_active,
                created_at,
                format
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            uid,
            source,
            dest_type,
            host,
            port,
            username,
            password,
            database,
            1,   # active
            datetime.datetime.now(datetime.UTC).isoformat(),
            format_value
        ))


        con.commit()


    except Exception as e:

        con.rollback()
        con.close()

        return jsonify({
            "status": "error",
            "msg": str(e)
        }), 500


    finally:
        con.close()


    return jsonify({
        "status": "ok",
        "message": "Destination saved and activated"
    })
    
@app.route("/destination/list/<source>")
def list_destinations(source):

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT
            id,
            dest_type,
            host,
            port,
            username,
            database_name,
            is_active,
            created_at,
            format
        FROM destination_configs
        WHERE uid=? AND source=?
        ORDER BY created_at DESC
    """, (uid, source))

    rows = fetchall_secure(cur)
    con.close()

    result = []

    for r in rows:
        result.append({
            "id": r["id"],
            "type": r["dest_type"],
            "host": r["host"],
            "port": r["port"],
            "username": r["username"],
            "database": r["database_name"],
            "active": bool(r["is_active"]),
            "created_at": r["created_at"]
        })

    return jsonify({
        "status": "ok",
        "destinations": result
    })

@app.route("/destination/activate", methods=["POST"])
def activate_destination():

    data = request.get_json()

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    
    source = data.get("source")
    dest_id = data.get("dest_id")

    if not source or not dest_id:
        return jsonify({
            "status": "error",
            "msg": "Missing fields"
        }), 400


    con = get_db()
    cur = con.cursor()

    try:

        # Disable all
        cur.execute("""
            UPDATE destination_configs
            SET is_active=0
            WHERE uid=? AND source=?
        """, (uid, source))


        # Enable selected
        cur.execute("""
            UPDATE destination_configs
            SET is_active=1
            WHERE id=? AND uid=? AND source=?
        """, (dest_id, uid, source))


        con.commit()

    except Exception as e:

        con.rollback()
        con.close()

        return jsonify({
            "status": "error",
            "msg": str(e)
        }), 500


    con.close()

    return jsonify({
        "status": "ok",
        "message": "Destination activated"
    })

@app.route("/destination/delete", methods=["POST"])
def delete_destination():

    data = request.json

    dest_id = data.get("id")

    uid = getattr(g, "user_id", None)

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    if not dest_id:
        return jsonify({"error": "Missing id"}), 400


    con = get_db()
    cur = con.cursor()

    cur.execute("""
        DELETE FROM destination_configs
        WHERE id=? AND uid=?
    """, (dest_id, uid))

    con.commit()
    con.close()

    return jsonify({"status": "ok"})

#============ Helper Function ==============
def get_connector_state(uid, source):
    con = get_db()
    cur = con.cursor()
    cur.execute("""
        SELECT state_json
        FROM connector_state
        WHERE uid=? AND source=?
    """, (uid, source))
    row = fetchone_secure(cur)  # Dict
    con.close()
    if not row:
        return None
    return json.loads(row["state_json"])  # Use key

def save_connector_state(uid, source, state_dict):

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_state
        (uid, source, state_json, updated_at)
        VALUES (?, ?, ?, ?)
    """, (
        uid,
        source,
        json.dumps(state_dict),
        datetime.datetime.now(IST).isoformat()
    ))

    con.commit()
    con.close()

def get_destination(uid, source):
    con = get_db()
    cur = con.cursor()
    cur.execute("""
        SELECT dest_type, host, port,
               username, password, database_name, format
        FROM destination_configs
        WHERE uid=? AND source=?
        ORDER BY id DESC
        LIMIT 1
    """, (uid, source))
    row = fetchone_secure(cur)  # Dict
    con.close()
    if not row:
        return None
    return {
        "type": row["dest_type"],  # Use keys
        "host": row["host"],
        "port": row["port"],
        "username": row["username"],
        "password": row["password"],
        "database_name": row["database_name"]
    }

def get_active_destination(uid, source):
    con = get_db()
    # con.row_factory = sqlite3.Row  # Remove this; auto_decrypt_row handles dict conversion
    cur = con.cursor()
    cur.execute("""
        SELECT *
        FROM destination_configs
        WHERE uid=? AND source=? AND is_active=1
        LIMIT 1
    """, (uid, source))
    row = fetchone_secure(cur)  # Already dict
    con.close()
    if not row:
        return None
    return row  # No need for dict(row); it's already a dict

# ---------- GA4 STATUS (FINAL) ----------

@app.route("/api/status/ga4")
def ga4_status():
    uid = getattr(g, "user_id", None)
    con = get_db()
    cur = con.cursor()
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='ga4'
        LIMIT 1
    """, (uid,))
    row = fetchone_secure(cur)  # Dict
    con.close()
    return jsonify({
        "connected": bool(row and row["enabled"] == 1)  # Use key
    })

@app.route("/connectors/<source>/job/save", methods=["POST"])
def universal_job_save(source):

    uid = get_uid()

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json()

    schedule_time = data.get("schedule_time")
    sync_type = data.get("sync_type", "incremental")

    if not schedule_time:
        return jsonify({"error": "Schedule time required"}), 400

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time, enabled)
        VALUES (?, ?, ?, ?, 1)
    """, (
        uid,
        source,
        sync_type,
        schedule_time
    ))

    con.commit()
    con.close()

    return jsonify({"status": "saved"})

@app.route("/connectors/<source>/job/get", methods=["GET"])
def universal_job_get(source):

    uid = get_uid()

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type,
               schedule_time,
               enabled
        FROM connector_jobs
        WHERE uid=? AND source=?
        LIMIT 1
    """, (uid, source))

    row = cur.fetchone()
    con.close()

    if not row:
        return jsonify({
            "status": "not_configured"
        })

    return jsonify({
        "status": "configured",
        "sync_type": row[0],
        "schedule_time": row[1],
        "enabled": bool(row[2])
    })

# ---------------- UNIVERSAL SYNC ENGINE ----------------

@app.route("/connectors/<source>/sync")
def universal_sync(source):

    uid = get_uid()

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    # execution mode (manual / scheduled)
    mode = request.args.get("mode", "manual")

    if mode not in ["manual", "scheduled"]:
        mode = "manual"

    run_id = log_sync_start(uid, source, mode)

    try:

        # Try dynamic function resolution
        possible_names = [
            f"sync_{source}",
            f"sync_{source}_files",
            f"sync_{source}_data"
        ]

        sync_func = None

        for name in possible_names:
            if name in globals():
                sync_func = globals()[name]
                break

        if not sync_func:
            raise Exception(f"No sync function found for source: {source}")

        print(f"[UNIVERSAL SYNC] Running {source} ({mode})", flush=True)

        # CALL WITHOUT ARGUMENTS
        result = sync_func()

        rows_synced = result if isinstance(result, int) else 0

        log_sync_finish(run_id, rows_synced, "success")

        return jsonify({
            "status": "success",
            "rows_synced": rows_synced
        })

    except Exception as e:

        print("[SYNC ERROR]", str(e), flush=True)

        log_sync_finish(run_id, 0, "failed", str(e))

        return jsonify({
            "status": "failed",
            "error": str(e)
        }), 500

# ---------------- WHATSAPP ----------------

from backend.connectors.whatsapp import (
    sync_whatsapp,
    disconnect_whatsapp
)

@app.route("/connectors/whatsapp/save_app", methods=["POST"])
def whatsapp_save_app():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
        
    data = request.get_json() or {}
    access_token = data.get("access_token")
    waba_id = data.get("waba_id")
    phone_number_id = data.get("phone_number_id")
    
    if not access_token or not waba_id or not phone_number_id:
        return jsonify({"error": "access_token, waba_id, and phone_number_id are required"}), 400
        
    # Validate credentials with Meta Graph API
    url = f"https://graph.facebook.com/v18.0/{waba_id}"

    response = requests.get(
        url,
        headers={"Authorization": f"Bearer {access_token}"}
    )

    if response.status_code != 200:
        return jsonify({
            "error": "Invalid WhatsApp credentials",
            "details": response.json()
        }), 403
        
    con = get_db()
    cur = con.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO whatsapp_connections
        (uid, access_token_encrypted, waba_id, phone_number_id, connected_at)
        VALUES (?, ?, ?, ?, datetime('now'))
    """, (
        uid,
        encrypt_value(access_token),
        waba_id,
        phone_number_id
    ))
    con.commit()
    con.close()
    
    ensure_connector_initialized(uid, "whatsapp")

    con = get_db()
    cur = con.cursor()

    cur.execute("""
    INSERT OR REPLACE INTO google_connections
    (uid, source, enabled)
    VALUES (?, 'whatsapp', 1)
    """, (uid,))

    con.commit()
    con.close()
    
    # Initialize connector state
    from backend.connectors.whatsapp import save_state
    save_state(uid, {"last_sync_date": None})
    
    return jsonify({"status": "saved"}), 200

@app.route("/api/status/whatsapp")
def whatsapp_status():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT 1 FROM whatsapp_connections WHERE uid=?", (uid,))
    row = cur.fetchone()
    con.close()
    
    if row:
        return jsonify({"connected": True, "has_credentials": True})
    return jsonify({"connected": False, "has_credentials": False})

@app.route("/connectors/whatsapp/sync")
def whatsapp_sync_route():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
        
    con = get_db()
    cur = con.cursor()
    cur.execute("""
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source='whatsapp'
        LIMIT 1
    """, (uid,))
    row = fetchone_secure(cur)
    con.close()
    
    sync_type = row["sync_type"] if row and row.get("sync_type") else "historical"
    
    try:
        rows = sync_whatsapp(uid, sync_type)
        return jsonify({"status": "success", "rows_synced": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route("/connectors/whatsapp/disconnect")
def whatsapp_disconnect_route():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
        
    disconnect_whatsapp(uid)
    return jsonify({"status": "disconnected"})

@app.route("/connectors/whatsapp/job/get")
def whatsapp_job_get():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
        
    con = get_db()
    cur = con.cursor()
    cur.execute("""
        SELECT sync_type, schedule_time, enabled
        FROM connector_jobs
        WHERE uid=? AND source='whatsapp'
        LIMIT 1
    """, (uid,))
    row = fetchone_secure(cur)
    con.close()
    
    if row:
        return jsonify({
            "status": "configured",
            "sync_type": row["sync_type"],
            "schedule_time": row["schedule_time"],
            "enabled": bool(row["enabled"])
        })
    return jsonify({"status": "not_configured", "sync_type": "historical", "schedule_time": "00:00"})

@app.route("/connectors/whatsapp/job/save", methods=["POST"])
def whatsapp_job_save():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
        
    data = request.get_json() or {}
    sync_type = data.get("sync_type", "historical")
    schedule_time = data.get("schedule_time", "00:00")
    
    con = get_db()
    cur = con.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time, enabled, created_at)
        VALUES (?, 'whatsapp', ?, ?, 1, datetime('now'))
    """, (uid, sync_type, schedule_time))
    con.commit()
    con.close()
    
    return jsonify({"status": "job_saved"})

# UNIVERSAL SYNC LOGGER (SAFE VERSION)
# NOTE:
# Logging is handled centrally via Flask middleware.
# This prevents scheduler argument conflicts.

def wrap_sync_function(func_name, func):

    def wrapper(*args, **kwargs):

        uid = get_uid()
        source = func_name.replace("sync_", "")

        run_id = None

        try:
            if uid:
                mode = request.args.get(
                    "mode",
                    request.headers.get(
                        "X-Sync-Mode",
                        "manual"
                    )
                )

                run_id = log_sync_start(uid, source, mode)

            print(f"[USAGE] Sync started → {source}", flush=True)

            # SAFE CALL
            result = func()

            rows = result if isinstance(result, int) else 0

            if run_id:
                log_sync_finish(run_id, rows, "success")

            return result

        except Exception as e:

            if run_id:
                log_sync_finish(run_id, 0, "failed", str(e))

            raise e

    return wrapper

def auto_wrap_all_syncs():
    """
    Disabled auto wrapping.
    Middleware logging replaces this safely.
    """
    print("[USAGE] Sync auto-wrapper disabled (middleware active)", flush=True)

# DO NOT WRAP FUNCTIONS ANYMORE
auto_wrap_all_syncs()

# USAGE ANALYTICS API (FIXED & PRODUCTION READY)

@app.route("/api/usage")
def usage_analytics():

    uid = get_uid()

    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    # ---------------- ACCOUNT OVERVIEW ----------------

    cur.execute("""
        SELECT created_at, company_name, is_individual
        FROM users
        WHERE id=?
    """, (uid,))
    user_row = cur.fetchone()

    account_created = user_row[0] if user_row else None
    company_name = user_row[1] if user_row else ""
    is_individual = bool(user_row[2]) if user_row else True

    cur.execute("""
        SELECT COUNT(*)
        FROM user_sessions
        WHERE user_id=?
    """, (uid,))
    total_sessions = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*)
        FROM api_usage_logs
        WHERE uid=?
    """, (uid,))
    total_api_calls = cur.fetchone()[0]

    # ---------------- CONNECTOR METRICS ----------------

    TOTAL_CONNECTORS = 48

    cur.execute("""
        SELECT COUNT(*)
        FROM google_connections
        WHERE uid=? AND enabled=1
    """, (uid,))
    connected_connectors = cur.fetchone()[0]

    disconnected_connectors = TOTAL_CONNECTORS - connected_connectors

    cur.execute("""
        SELECT MIN(started_at)
        FROM sync_runs
        WHERE uid=?
    """, (uid,))
    first_connected_date = cur.fetchone()[0]

    cur.execute("""
        SELECT MAX(finished_at)
        FROM sync_runs
        WHERE uid=? AND status='success'
    """, (uid,))
    last_sync_time = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*)
        FROM sync_runs
        WHERE uid=?
    """, (uid,))
    total_sync_runs = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*)
        FROM sync_runs
        WHERE uid=? AND status='failed'
    """, (uid,))
    failed_sync_runs = cur.fetchone()[0]

    cur.execute("""
        SELECT sync_type, COUNT(*)
        FROM sync_runs
        WHERE uid=?
        GROUP BY sync_type
    """, (uid,))
    sync_type_breakdown = dict(cur.fetchall())

    # ---------------- DATA VOLUME (REAL SOURCE) ----------------
    # IMPORTANT: Now reading from destination_push_logs

    cur.execute("""
        SELECT COALESCE(SUM(rows_pushed),0)
        FROM destination_push_logs
        WHERE uid=?
    """, (uid,))
    total_records_synced = cur.fetchone()[0]

    cur.execute("""
        SELECT COALESCE(SUM(rows_pushed),0)
        FROM destination_push_logs
        WHERE uid=?
        AND date(pushed_at)=date('now')
    """, (uid,))
    records_today = cur.fetchone()[0]

    cur.execute("""
        SELECT COALESCE(SUM(rows_pushed),0)
        FROM destination_push_logs
        WHERE uid=?
        AND strftime('%Y-%m', pushed_at)=strftime('%Y-%m','now')
    """, (uid,))
    records_this_month = cur.fetchone()[0]

    cur.execute("""
        SELECT source, SUM(rows_pushed)
        FROM destination_push_logs
        WHERE uid=?
        GROUP BY source
    """, (uid,))
    records_per_connector = cur.fetchall()

    largest_connector = None
    if records_per_connector:
        largest_connector = max(
            records_per_connector,
            key=lambda x: x[1]
        )

    # ---------------- DESTINATION METRICS ----------------

    cur.execute("""
        SELECT COUNT(*)
        FROM destination_configs
        WHERE uid=?
    """, (uid,))
    total_destinations = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*)
        FROM destination_configs
        WHERE uid=? AND is_active=1
    """, (uid,))
    active_destinations = cur.fetchone()[0]

    cur.execute("""
        SELECT destination_type, SUM(rows_pushed)
        FROM destination_push_logs
        WHERE uid=?
        GROUP BY destination_type
    """, (uid,))
    rows_per_destination = cur.fetchall()

    cur.execute("""
        SELECT MAX(pushed_at)
        FROM destination_push_logs
        WHERE uid=?
    """, (uid,))
    last_push_time = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*)
        FROM destination_push_logs
        WHERE uid=? AND status='failed'
    """, (uid,))
    push_failures = cur.fetchone()[0]

    # ---------------- SCHEDULER ----------------

    cur.execute("""
        SELECT COUNT(*)
        FROM connector_jobs
        WHERE uid=? AND enabled=1
    """, (uid,))
    scheduled_jobs = cur.fetchone()[0]

    # ---------------- DAILY USAGE (last 14 days) ----------------

    cur.execute("""
        SELECT
            DATE(pushed_at) as date,
            SUM(rows_pushed) as rows
        FROM destination_push_logs
        WHERE uid=?
        GROUP BY DATE(pushed_at)
        ORDER BY DATE(pushed_at) DESC
        LIMIT 14
    """, (uid,))
    raw_daily = {row[0]: row[1] for row in cur.fetchall()}

    daily_usage = []
    for offset in range(13, -1, -1):
        day_str = (datetime.date.today() - datetime.timedelta(days=offset)).isoformat()
        daily_usage.append({
            "date": day_str,
            "rows": raw_daily.get(day_str, 0)
        })

    # ---------------- TOP CONNECTORS ----------------

    cur.execute("""
        SELECT
            source,
            SUM(rows_pushed) as rows
        FROM destination_push_logs
        WHERE uid=?
        GROUP BY source
        ORDER BY rows DESC
        LIMIT 10
    """, (uid,))
    top_connectors = [
        {"source": row[0], "rows": row[1]}
        for row in cur.fetchall()
    ]

    con.close()

    # ---------------- HEALTH CALCULATION ----------------

    success_rate = 0
    if total_sync_runs > 0:
        success_rate = round(
            ((total_sync_runs - failed_sync_runs)
             / total_sync_runs) * 100,
            2
        )

    return jsonify({

        "account": {
            "created_at": account_created,
            "company_name": company_name,
            "is_individual": is_individual,
            "total_sessions": total_sessions,
            "total_api_calls": total_api_calls
        },

        "connectors": {
            "total_available": TOTAL_CONNECTORS,
            "connected": connected_connectors,
            "disconnected": disconnected_connectors,
            "first_connected": first_connected_date,
            "last_sync": last_sync_time,
            "total_sync_runs": total_sync_runs,
            "failed_sync_runs": failed_sync_runs,
            "sync_type_breakdown": sync_type_breakdown
        },

        "data_volume": {
            "total_records_synced": total_records_synced,
            "records_today": records_today,
            "records_this_month": records_this_month,
            "records_per_connector": records_per_connector,
            "largest_connector": largest_connector
        },

        "destinations": {
            "total": total_destinations,
            "active": active_destinations,
            "rows_per_destination": rows_per_destination,
            "last_push_time": last_push_time,
            "push_failures": push_failures
        },

        "scheduler": {
            "scheduled_jobs": scheduled_jobs
        },

        "health": {
            "sync_success_rate": success_rate
        },

        "daily_usage":    daily_usage,
        "top_connectors": top_connectors,
        "last_sync":      last_sync_time,
        "stats": {
            "connectedConnectors": connected_connectors,
            "totalRecords":        total_records_synced,
            "activeDestinations":  active_destinations,
            "successRate":         success_rate
        }
    })

# ---------------- RUN ----------------

def seed_test_user():
    con = get_db()
    cur = con.cursor()
    try:
        cur.execute("SELECT id FROM users WHERE email='test@example.com'")
        if cur.fetchone():
            return
            
        import uuid
        from werkzeug.security import generate_password_hash
        import datetime
        
        user_id = str(uuid.uuid4())
        password_hash = generate_password_hash("testpassword")
        secured = encrypt_payload({"password": password_hash})
        
        cur.execute("""
            INSERT INTO users(
                id,email,password,
                first_name,last_name,
                company_name,company_size,
                country,phone,
                is_individual,
                created_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?)
        """, (
            user_id,
            "test@example.com",
            secured["password"],
            "Test",
            "User",
            "Segmento Test",
            "1-50",
            "Testland",
            "1234567890",
            1,
            datetime.datetime.utcnow().isoformat()
        ))
        con.commit()
        print("Seeded test user: test@example.com / testpassword", flush=True)
    except Exception as e:
        print("Failed to seed test user:", e, flush=True)
    finally:
        con.close()

# -------- CHARTBEAT --------

@app.route("/connectors/chartbeat/save_app", methods=["POST"])
def chartbeat_save_app():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    data     = request.get_json() or {}
    api_key  = data.get("api_key", "").strip()
    host     = data.get("host", "").strip()
    query_id = data.get("query_id", "").strip() or None

    if not api_key or not host:
        return jsonify({"error": "api_key and host are required"}), 400

    from backend.connectors.chartbeat import save_credentials
    save_credentials(uid, api_key, host, query_id)

    ensure_connector_initialized(uid, "chartbeat")
    return jsonify({"status": "saved"})


@app.route("/connectors/chartbeat/connect")
def chartbeat_connect():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    result = connect_chartbeat(uid)
    if result.get("status") != "success":
        return jsonify(result), 400
    return jsonify(result)


@app.route("/connectors/chartbeat/disconnect")
def chartbeat_disconnect():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    disconnect_chartbeat(uid)
    return jsonify({"status": "disconnected"})


@app.route("/connectors/chartbeat/sync")
def chartbeat_sync_route():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()
    cur.execute("""
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source='chartbeat'
        LIMIT 1
    """, (uid,))
    row = fetchone_secure(cur)
    con.close()

    sync_type = row["sync_type"] if row and row.get("sync_type") else "historical"
    return jsonify(sync_chartbeat(uid, sync_type=sync_type))


@app.route("/api/status/chartbeat")
def chartbeat_status():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT 1
        FROM connector_configs
        WHERE uid=? AND connector='chartbeat'
        LIMIT 1
    """, (uid,))
    creds = fetchone_secure(cur)

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='chartbeat'
        LIMIT 1
    """, (uid,))
    conn = fetchone_secure(cur)

    cur.execute("""
        SELECT host
        FROM chartbeat_connections
        WHERE uid=?
        LIMIT 1
    """, (uid,))
    cb = fetchone_secure(cur)

    con.close()

    return jsonify({
        "has_credentials": bool(creds),
        "connected":       bool(conn and conn["enabled"] == 1),
        "host":            cb["host"] if cb else None,
    })


@app.route("/connectors/chartbeat/job/get")
def chartbeat_job_get():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()
    try:
        cur.execute("""
            SELECT sync_type, schedule_time
            FROM connector_jobs
            WHERE uid=? AND source='chartbeat'
        """, (uid,))
        row = fetchone_secure(cur)
    finally:
        con.close()

    if not row:
        return jsonify({"exists": False})

    return jsonify({
        "exists":        True,
        "sync_type":     row["sync_type"],
        "schedule_time": row["schedule_time"],
    })


@app.route("/connectors/chartbeat/job/save", methods=["POST"])
def chartbeat_job_save():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    data          = request.get_json() or {}
    sync_type     = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")

    con = get_db()
    cur = con.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time)
        VALUES (?, 'chartbeat', ?, ?)
    """, (uid, sync_type, schedule_time))
    con.commit()
    con.close()

    return jsonify({"status": "job_saved"})


# -------- SLACK --------

@app.route("/connectors/slack/save_app", methods=["POST"])
def api_slack_save_config():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json or {}
    bot_token = data.get("bot_token")

    if not bot_token:
        return jsonify({"error": "Missing bot token"}), 400

    save_slack_config(uid, bot_token)
    ensure_connector_initialized(uid, "slack")
    return jsonify({"status": "saved"})


@app.route("/connectors/slack/connect")
def api_slack_connect():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
        
    result = connect_slack(uid)
    if result.get("status") != "success":
        return jsonify(result), 400
    return jsonify(result)


@app.route("/connectors/slack/sync")
def api_slack_sync():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    
    con = get_db()
    cur = con.cursor()
    cur.execute("""
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source='slack'
        LIMIT 1
    """, (uid,))
    row = fetchone_secure(cur)
    con.close()

    sync_type = row["sync_type"] if row and row.get("sync_type") else "incremental"
    return jsonify(sync_slack(uid, sync_type=sync_type))


@app.route("/connectors/slack/disconnect")
def api_slack_disconnect():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    return jsonify(disconnect_slack(uid))

@app.route("/api/status/slack")
def slack_status():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT 1
        FROM connector_configs
        WHERE uid=? AND connector='slack'
        LIMIT 1
    """, (uid,))
    creds = fetchone_secure(cur)

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='slack'
        LIMIT 1
    """, (uid,))
    conn = fetchone_secure(cur)

    con.close()

    return jsonify({
        "has_credentials": bool(creds),
        "connected": bool(conn and conn["enabled"] == 1)
    })

@app.route("/connectors/slack/job/get")
def slack_job_get():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='slack'
    """, (uid,))

    row = fetchone_secure(cur)
    con.close()

    if not row:
        return jsonify({"exists": False})

    return jsonify({
        "exists": True,
        "sync_type": row["sync_type"],
        "schedule_time": row["schedule_time"],
    })

@app.route("/connectors/slack/job/save", methods=["POST"])
def slack_job_save():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json() or {}

    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time)
        VALUES (?, 'slack', ?, ?)
    """, (uid, sync_type, schedule_time))

    con.commit()
    con.close()

    return jsonify({"status": "job_saved"})

# -------- STRIPE --------

@app.route("/connectors/stripe/save_app", methods=["POST"])
def stripe_save_app():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json() or {}
    secret_key = (data.get("secret_key") or "").strip()

    if not secret_key:
        return jsonify({"error": "secret_key is required"}), 400

    save_stripe_credentials(uid, secret_key)
    ensure_connector_initialized(uid, "stripe")
    return jsonify({"status": "saved"})


@app.route("/connectors/stripe/connect")
def stripe_connect():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    result = connect_stripe(uid)
    if result.get("status") != "success":
        return jsonify(result), 400
    return jsonify(result)


@app.route("/connectors/stripe/disconnect")
def stripe_disconnect():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    disconnect_stripe(uid)
    return jsonify({"status": "disconnected"})


@app.route("/connectors/stripe/sync")
def stripe_sync_route():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()
    cur.execute("""
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source='stripe'
        LIMIT 1
    """, (uid,))
    row = fetchone_secure(cur)
    con.close()

    sync_type = row["sync_type"] if row and row.get("sync_type") else "historical"
    return jsonify(sync_stripe(uid, sync_type=sync_type))


@app.route("/api/status/stripe")
def stripe_status():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT 1
        FROM connector_configs
        WHERE uid=? AND connector='stripe'
        LIMIT 1
    """, (uid,))
    creds = fetchone_secure(cur)

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='stripe'
        LIMIT 1
    """, (uid,))
    conn = fetchone_secure(cur)

    cur.execute("""
        SELECT account_id, display_name
        FROM stripe_connections
        WHERE uid=?
        LIMIT 1
    """, (uid,))
    stripe_conn = fetchone_secure(cur)

    con.close()

    return jsonify({
        "has_credentials": bool(creds),
        "connected": bool(conn and conn["enabled"] == 1),
        "account_id": stripe_conn["account_id"] if stripe_conn else None,
        "display_name": stripe_conn["display_name"] if stripe_conn else None,
    })


@app.route("/connectors/stripe/job/get")
def stripe_job_get():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()
    try:
        cur.execute("""
            SELECT sync_type, schedule_time
            FROM connector_jobs
            WHERE uid=? AND source='stripe'
        """, (uid,))
        row = fetchone_secure(cur)
    finally:
        con.close()

    if not row:
        return jsonify({"exists": False})

    return jsonify({
        "exists": True,
        "sync_type": row["sync_type"],
        "schedule_time": row["schedule_time"],
    })


@app.route("/connectors/stripe/job/save", methods=["POST"])
def stripe_job_save():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json() or {}
    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")

    con = get_db()
    cur = con.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time)
        VALUES (?, 'stripe', ?, ?)
    """, (uid, sync_type, schedule_time))
    con.commit()
    con.close()

    return jsonify({"status": "job_saved"})


# -------- SOCIAL INSIDER --------

@app.route("/connectors/socialinsider/save_app", methods=["POST"])
def socialinsider_save_app():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    data     = request.get_json() or {}
    api_key  = data.get("api_key", "").strip()
    platform = data.get("platform", "").strip()
    handle   = data.get("handle", "").strip()

    if not api_key or not platform or not handle:
        return jsonify({"error": "api_key, platform, and handle are required"}), 400

    from backend.connectors.socialinsider import save_credentials
    save_credentials(uid, api_key, platform, handle)

    ensure_connector_initialized(uid, "socialinsider")
    return jsonify({"status": "saved"})


@app.route("/connectors/socialinsider/connect")
def socialinsider_connect():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    result = connect_socialinsider(uid)
    if result.get("status") != "success":
        return jsonify(result), 400
    return jsonify(result)


@app.route("/connectors/socialinsider/disconnect")
def socialinsider_disconnect():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    disconnect_socialinsider(uid)
    return jsonify({"status": "disconnected"})


@app.route("/connectors/socialinsider/sync")
def socialinsider_sync_route():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()
    cur.execute("""
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source='socialinsider'
        LIMIT 1
    """, (uid,))
    row = fetchone_secure(cur)
    con.close()

    sync_type = row["sync_type"] if row and row.get("sync_type") else "historical"
    return jsonify(sync_socialinsider(uid, sync_type=sync_type))


@app.route("/api/status/socialinsider")
def socialinsider_status():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT 1
        FROM connector_configs
        WHERE uid=? AND connector='socialinsider'
        LIMIT 1
    """, (uid,))
    creds = fetchone_secure(cur)

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='socialinsider'
        LIMIT 1
    """, (uid,))
    conn = fetchone_secure(cur)

    cur.execute("""
        SELECT platform, handle
        FROM socialinsider_connections
        WHERE uid=?
        LIMIT 1
    """, (uid,))
    si = fetchone_secure(cur)

    con.close()

    return jsonify({
        "has_credentials": bool(creds),
        "connected":       bool(conn and conn["enabled"] == 1),
        "platform":        si["platform"] if si else None,
        "handle":          si["handle"] if si else None,
    })


@app.route("/connectors/socialinsider/job/get")
def socialinsider_job_get():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()
    try:
        cur.execute("""
            SELECT sync_type, schedule_time
            FROM connector_jobs
            WHERE uid=? AND source='socialinsider'
        """, (uid,))
        row = fetchone_secure(cur)
    finally:
        con.close()

    if not row:
        return jsonify({"exists": False})

    return jsonify({
        "exists":        True,
        "sync_type":     row["sync_type"],
        "schedule_time": row["schedule_time"],
    })


@app.route("/connectors/socialinsider/job/save", methods=["POST"])
def socialinsider_job_save():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    data          = request.get_json() or {}
    sync_type     = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")

    con = get_db()
    cur = con.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time)
        VALUES (?, 'socialinsider', ?, ?)
    """, (uid, sync_type, schedule_time))
    con.commit()
    con.close()

    return jsonify({"status": "job_saved"})

# -------- AWS RDS --------

@app.route("/connectors/aws_rds/save_app", methods=["POST"])
def aws_rds_save_app():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json() or {}

    engine   = data.get("engine", "").strip()
    host     = data.get("host", "").strip()
    port     = data.get("port")
    database = data.get("database", "").strip()
    username = data.get("username", "").strip()
    password = data.get("password", "")

    if not engine or not host or not port or not database or not username or not password:
        return jsonify({"error": "All fields are required: engine, host, port, database, username, password"}), 400

    try:
        port = int(port)
    except (TypeError, ValueError):
        return jsonify({"error": "Port must be a valid integer"}), 400

    save_rds_config(uid, engine, host, port, database, username, password)
    ensure_connector_initialized(uid, "aws_rds")
    return jsonify({"status": "saved"})


@app.route("/connectors/aws_rds/connect")
def aws_rds_connect():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    result = connect_rds(uid)
    if result.get("status") != "success":
        return jsonify(result), 400
    return jsonify(result)


@app.route("/connectors/aws_rds/disconnect")
def aws_rds_disconnect():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    disconnect_rds(uid)
    return jsonify({"status": "disconnected"})


@app.route("/connectors/aws_rds/sync")
def aws_rds_sync_route():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source='aws_rds'
        LIMIT 1
        """,
        (uid,),
    )
    row = fetchone_secure(cur)
    con.close()

    sync_type = row["sync_type"] if row and row.get("sync_type") else "historical"
    return jsonify(sync_rds(uid, sync_type=sync_type))


@app.route("/api/status/aws_rds")
def aws_rds_status():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute(
        """
        SELECT config_json
        FROM connector_configs
        WHERE uid=? AND connector='aws_rds'
        LIMIT 1
        """,
        (uid,),
    )
    cfg_row = fetchone_secure(cur)

    cur.execute(
        """
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='aws_rds'
        LIMIT 1
        """,
        (uid,),
    )
    conn_row = fetchone_secure(cur)
    con.close()

    # Decode config to surface engine/host in status response
    engine = None
    host   = None
    if cfg_row and cfg_row.get("config_json"):
        try:
            cfg    = json.loads(cfg_row["config_json"])
            engine = cfg.get("engine")
            host   = cfg.get("host")
        except Exception:
            import traceback; traceback.print_exc()
            print('Exception caught', flush=True)

    return jsonify({
        "has_credentials": bool(cfg_row),
        "connected":       bool(conn_row and conn_row["enabled"] == 1),
        "engine":          engine,
        "host":            host,
    })


@app.route("/connectors/aws_rds/job/get")
def aws_rds_job_get():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()
    try:
        cur.execute(
            """
            SELECT sync_type, schedule_time
            FROM connector_jobs
            WHERE uid=? AND source='aws_rds'
            """,
            (uid,),
        )
        row = fetchone_secure(cur)
    finally:
        con.close()

    if not row:
        return jsonify({"exists": False})

    return jsonify({
        "exists":        True,
        "sync_type":     row["sync_type"],
        "schedule_time": row["schedule_time"],
    })


@app.route("/connectors/aws_rds/job/save", methods=["POST"])
def aws_rds_job_save():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    data          = request.get_json() or {}
    sync_type     = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")

    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time)
        VALUES (?, 'aws_rds', ?, ?)
        """,
        (uid, sync_type, schedule_time),
    )
    con.commit()
    con.close()

    return jsonify({"status": "job_saved"})

# -------- AWS DYNAMODB --------

@app.route("/connectors/dynamodb/save_app", methods=["POST"])
def dynamodb_save_app():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json() or {}
    access_key = data.get("access_key", "").strip()
    secret_key = data.get("secret_key", "")
    region = data.get("region", "").strip()

    if not access_key or not secret_key or not region:
        return jsonify({"error": "All fields are required: access_key, secret_key, region"}), 400

    save_dynamodb_config(uid, access_key, secret_key, region)
    ensure_connector_initialized(uid, "dynamodb")
    return jsonify({"status": "saved"})


@app.route("/connectors/dynamodb/connect")
def dynamodb_connect():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    result = connect_dynamodb(uid)
    if result.get("status") != "success":
        return jsonify(result), 400
    return jsonify(result)


@app.route("/connectors/dynamodb/disconnect")
def dynamodb_disconnect():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    disconnect_dynamodb(uid)
    return jsonify({"status": "disconnected"})


@app.route("/connectors/dynamodb/sync")
def dynamodb_sync_route():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source='dynamodb'
        LIMIT 1
        """,
        (uid,),
    )
    row = fetchone_secure(cur)
    con.close()

    sync_type = row["sync_type"] if row and row.get("sync_type") else "historical"
    return jsonify(sync_dynamodb(uid, sync_type=sync_type))


@app.route("/api/status/dynamodb")
def dynamodb_status():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute(
        """
        SELECT config_json
        FROM connector_configs
        WHERE uid=? AND connector='dynamodb'
        LIMIT 1
        """,
        (uid,),
    )
    cfg_row = fetchone_secure(cur)

    cur.execute(
        """
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='dynamodb'
        LIMIT 1
        """,
        (uid,),
    )
    conn_row = fetchone_secure(cur)
    con.close()

    region = None
    access_key = None
    if cfg_row and cfg_row.get("config_json"):
        try:
            cfg = json.loads(cfg_row["config_json"])
            region = cfg.get("region")
            raw_access_key = cfg.get("access_key")
            if raw_access_key:
                access_key = f"{raw_access_key[:4]}{'*' * max(len(raw_access_key) - 8, 4)}{raw_access_key[-4:]}"
        except Exception:
            import traceback; traceback.print_exc()
            print('Exception caught', flush=True)

    return jsonify({
        "has_credentials": bool(cfg_row),
        "connected": bool(conn_row and conn_row["enabled"] == 1),
        "region": region,
        "access_key": access_key,
    })


@app.route("/connectors/dynamodb/job/get")
def dynamodb_job_get():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    con = get_db()
    cur = con.cursor()
    try:
        cur.execute(
            """
            SELECT sync_type, schedule_time
            FROM connector_jobs
            WHERE uid=? AND source='dynamodb'
            """,
            (uid,),
        )
        row = fetchone_secure(cur)
    finally:
        con.close()

    if not row:
        return jsonify({"exists": False})

    return jsonify({
        "exists": True,
        "sync_type": row["sync_type"],
        "schedule_time": row["schedule_time"],
    })


@app.route("/connectors/dynamodb/job/save", methods=["POST"])
def dynamodb_job_save():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json() or {}
    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")

    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time)
        VALUES (?, 'dynamodb', ?, ?)
        """,
        (uid, sync_type, schedule_time),
    )
    con.commit()
    con.close()

    return jsonify({"status": "job_saved"})

# ================= NOTION ========================

@app.route("/connectors/notion/save_app", methods=["POST"])
def _notion_save_config():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json() or {}
    access_token = (data.get("access_token") or "").strip()

    if not access_token:
        return jsonify({"error": "missing token"}), 400

    save_notion_config(uid, access_token)
    ensure_connector_initialized(uid, "notion")
    return jsonify({"status": "saved"})


@app.route("/connectors/notion/connect")
def _notion_connect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    res = connect_notion(uid)
    if res.get("status") != "success":
        return jsonify(res), 400
    return jsonify(res)


@app.route("/connectors/notion/disconnect")
def _notion_disconnect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    res = disconnect_notion(uid)
    return jsonify(res)


@app.route("/connectors/notion/sync")
def _notion_sync():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401

    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source='notion'
        LIMIT 1
        """,
        (uid,),
    )
    row = fetchone_secure(cur)
    con.close()

    sync_type = row["sync_type"] if row and row.get("sync_type") else "historical"
    return jsonify(sync_notion(uid, sync_type=sync_type))


@app.route("/api/status/notion")
def _notion_status():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401

    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT config_json
        FROM connector_configs
        WHERE uid=? AND connector='notion'
        LIMIT 1
        """,
        (uid,),
    )
    cfg_row = fetchone_secure(cur)

    cur.execute(
        """
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='notion'
        LIMIT 1
        """,
        (uid,),
    )
    conn_row = fetchone_secure(cur)
    con.close()

    access_token = None
    if cfg_row and cfg_row.get("config_json"):
        try:
            cfg = json.loads(cfg_row["config_json"])
            raw_token = cfg.get("access_token")
            if raw_token:
                access_token = f"{raw_token[:4]}{'*' * max(len(raw_token) - 8, 4)}{raw_token[-4:]}"
        except Exception:
            import traceback; traceback.print_exc()
            print('Exception caught', flush=True)

    return jsonify(
        {
            "has_credentials": bool(cfg_row),
            "connected": bool(conn_row and conn_row.get("enabled") == 1),
            "access_token": access_token,
        }
    )

@app.route("/connectors/notion/job/get")
def get_notion_job():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source=?
        """,
        (uid, "notion"),
    )
    row = fetchone_secure(cur)
    con.close()

    if not row:
        return jsonify({"exists": False})

    return jsonify(
        {
            "exists": True,
            "sync_type": row["sync_type"],
            "schedule_time": row["schedule_time"],
        }
    )

@app.route("/connectors/notion/job/save", methods=["POST"])
def save_notion_job():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    
    data = request.get_json() or {}
    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")

    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time)
        VALUES (?, ?, ?, ?)
        """,
        (uid, "notion", sync_type, schedule_time),
    )
    con.commit()
    con.close()

    return jsonify({"status": "job_saved"})


# ================= HUBSPOT ========================

@app.route("/connectors/hubspot/save_app", methods=["POST"])
def hubspot_save_config():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json() or {}
    access_token = (data.get("access_token") or "").strip()

    if not access_token:
        return jsonify({"error": "missing access_token"}), 400

    save_hubspot_config(uid, access_token)
    ensure_connector_initialized(uid, "hubspot")
    return jsonify({"status": "saved"})


@app.route("/connectors/hubspot/connect")
def hubspot_connect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401

    res = connect_hubspot(uid)
    if res.get("status") != "success":
        return jsonify(res), 400
    return jsonify(res)


@app.route("/connectors/hubspot/disconnect")
def hubspot_disconnect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401

    return jsonify(disconnect_hubspot(uid))


@app.route("/connectors/hubspot/sync")
def hubspot_sync():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401

    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source='hubspot'
        LIMIT 1
        """,
        (uid,),
    )
    row = fetchone_secure(cur)
    con.close()

    sync_type = row["sync_type"] if row and row.get("sync_type") else "historical"
    return jsonify(sync_hubspot(uid, sync_type=sync_type))


@app.route("/api/status/hubspot")
def hubspot_status():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401

    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT config_json
        FROM connector_configs
        WHERE uid=? AND connector='hubspot'
        LIMIT 1
        """,
        (uid,),
    )
    cfg_row = fetchone_secure(cur)

    cur.execute(
        """
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='hubspot'
        LIMIT 1
        """,
        (uid,),
    )
    conn_row = fetchone_secure(cur)
    con.close()

    access_token = None
    if cfg_row and cfg_row.get("config_json"):
        try:
            cfg = json.loads(cfg_row["config_json"])
            raw_token = cfg.get("access_token")
            if raw_token:
                access_token = f"{raw_token[:4]}{'*' * max(len(raw_token) - 8, 4)}{raw_token[-4:]}"
        except Exception:
            import traceback; traceback.print_exc()
            print('Exception caught', flush=True)

    return jsonify(
        {
            "has_credentials": bool(cfg_row),
            "connected": bool(conn_row and conn_row.get("enabled") == 1),
            "access_token": access_token,
        }
    )


@app.route("/connectors/hubspot/job/get")
def hubspot_job_get():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401

    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source=?
        """,
        (uid, "hubspot"),
    )
    row = fetchone_secure(cur)
    con.close()

    if not row:
        return jsonify({"exists": False})

    return jsonify(
        {
            "exists": True,
            "sync_type": row["sync_type"],
            "schedule_time": row["schedule_time"],
        }
    )


@app.route("/connectors/hubspot/job/save", methods=["POST"])
def hubspot_job_save():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json() or {}
    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")

    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time)
        VALUES (?, ?, ?, ?)
        """,
        (uid, "hubspot", sync_type, schedule_time),
    )
    con.commit()
    con.close()

    return jsonify({"status": "job_saved"})


# ================= ZENDESK ========================

@app.route("/connectors/zendesk/save_app", methods=["POST"])
def _zendesk_save_config():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json() or {}
    subdomain = data.get("subdomain")
    email = data.get("email")
    api_token = data.get("api_token")
    if not subdomain or not email or not api_token:
        return jsonify({"error": "missing subdomain, email or api_token"}), 400
    save_zendesk_config(uid, subdomain, email, api_token)
    ensure_connector_initialized(uid, "zendesk")
    return jsonify({"status": "saved"})

@app.route("/connectors/zendesk/connect")
def _zendesk_connect():
    uid = getattr(g, "user_id", None)
    if not uid: return jsonify({"error": "unauthorized"}), 401
    res = connect_zendesk(uid)
    return jsonify(res)

@app.route("/connectors/zendesk/disconnect")
def _zendesk_disconnect():
    uid = getattr(g, "user_id", None)
    if not uid: return jsonify({"error": "unauthorized"}), 401
    return jsonify(disconnect_zendesk(uid))

@app.route("/connectors/zendesk/sync")
def _zendesk_sync():
    uid = getattr(g, "user_id", None)
    if not uid: return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT sync_type FROM connector_jobs WHERE uid=? AND source='zendesk' LIMIT 1", (uid,))
    row = fetchone_secure(cur)
    con.close()
    sync_type = row["sync_type"] if row and row.get("sync_type") else "historical"
    return jsonify(sync_zendesk(uid, sync_type=sync_type))

@app.route("/api/status/zendesk")
def _zendesk_status():
    uid = getattr(g, "user_id", None)
    if not uid: return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT 1 FROM connector_configs WHERE uid=? AND connector='zendesk'", (uid,))
    creds = fetchone_secure(cur)
    cur.execute("SELECT enabled FROM google_connections WHERE uid=? AND source='zendesk'", (uid,))
    conn_row = fetchone_secure(cur)
    con.close()
    return jsonify({"has_credentials": bool(creds), "connected": bool(conn_row and conn_row.get("enabled") == 1)})

@app.route("/connectors/zendesk/job/get")
def _zendesk_job_get():
    uid = getattr(g, "user_id", None)
    if not uid: return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT sync_type, schedule_time FROM connector_jobs WHERE uid=? AND source='zendesk'", (uid,))
    row = fetchone_secure(cur)
    con.close()
    if not row: return jsonify({"exists": False})
    return jsonify({"exists": True, "sync_type": row["sync_type"], "schedule_time": row["schedule_time"]})

@app.route("/connectors/zendesk/job/save", methods=["POST"])
def _zendesk_job_save():
    uid = getattr(g, "user_id", None)
    if not uid: return jsonify({"error": "unauthorized"}), 401
    data = request.get_json() or {}
    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")
    con = get_db()
    cur = con.cursor()
    cur.execute("INSERT OR REPLACE INTO connector_jobs (uid, source, sync_type, schedule_time) VALUES (?, 'zendesk', ?, ?)", (uid, sync_type, schedule_time))
    con.commit()
    con.close()
    return jsonify({"status": "job_saved"})


# ================= INTERCOM ========================

@app.route("/connectors/intercom/save_app", methods=["POST"])
def _intercom_save_config():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json() or {}
    access_token = data.get("access_token")
    if not access_token:
        return jsonify({"error": "missing access_token"}), 400
    save_intercom_config(uid, access_token)
    ensure_connector_initialized(uid, "intercom")
    return jsonify({"status": "saved"})

@app.route("/connectors/intercom/connect")
def _intercom_connect():
    uid = getattr(g, "user_id", None)
    if not uid: return jsonify({"error": "unauthorized"}), 401
    return jsonify(connect_intercom(uid))

@app.route("/connectors/intercom/disconnect")
def _intercom_disconnect():
    uid = getattr(g, "user_id", None)
    if not uid: return jsonify({"error": "unauthorized"}), 401
    return jsonify(disconnect_intercom(uid))

@app.route("/connectors/intercom/sync")
def _intercom_sync():
    uid = getattr(g, "user_id", None)
    if not uid: return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT sync_type FROM connector_jobs WHERE uid=? AND source='intercom' LIMIT 1", (uid,))
    row = fetchone_secure(cur)
    con.close()
    sync_type = row["sync_type"] if row and row.get("sync_type") else "historical"
    return jsonify(sync_intercom(uid, sync_type=sync_type))

@app.route("/api/status/intercom")
def _intercom_status():
    uid = getattr(g, "user_id", None)
    if not uid: return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT 1 FROM connector_configs WHERE uid=? AND connector='intercom'", (uid,))
    creds = fetchone_secure(cur)
    cur.execute("SELECT enabled FROM google_connections WHERE uid=? AND source='intercom'", (uid,))
    conn_row = fetchone_secure(cur)
    con.close()
    return jsonify({"has_credentials": bool(creds), "connected": bool(conn_row and conn_row.get("enabled") == 1)})

@app.route("/connectors/intercom/job/get")
def _intercom_job_get():
    uid = getattr(g, "user_id", None)
    if not uid: return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT sync_type, schedule_time FROM connector_jobs WHERE uid=? AND source='intercom'", (uid,))
    row = fetchone_secure(cur)
    con.close()
    if not row: return jsonify({"exists": False})
    return jsonify({"exists": True, "sync_type": row["sync_type"], "schedule_time": row["schedule_time"]})

@app.route("/connectors/intercom/job/save", methods=["POST"])
def _intercom_job_save():
    uid = getattr(g, "user_id", None)
    if not uid: return jsonify({"error": "unauthorized"}), 401
    data = request.get_json() or {}
    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")
    con = get_db()
    cur = con.cursor()
    cur.execute("INSERT OR REPLACE INTO connector_jobs (uid, source, sync_type, schedule_time) VALUES (?, 'intercom', ?, ?)", (uid, sync_type, schedule_time))
    con.commit()
    con.close()
    return jsonify({"status": "job_saved"})


# ================= SHOPIFY ========================

@app.route("/connectors/shopify/save_app", methods=["POST"])
def _shopify_save_config():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json() or {}
    shop_domain = (data.get("shopDomain") or "").strip()
    access_token = (data.get("accessToken") or "").strip()

    if not shop_domain or not access_token:
        return jsonify({"error": "missing shopDomain or accessToken"}), 400

    save_shopify_config(uid, shop_domain, access_token)
    ensure_connector_initialized(uid, "shopify")
    return jsonify({"status": "saved"})


@app.route("/connectors/shopify/connect")
def _shopify_connect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    res = connect_shopify(uid)
    if res.get("status") != "success":
        return jsonify(res), 400
    return jsonify(res)


@app.route("/connectors/shopify/disconnect")
def _shopify_disconnect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    res = disconnect_shopify(uid)
    return jsonify(res)


@app.route("/connectors/shopify/sync")
def _shopify_sync():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401

    con = get_db()
    cur = con.cursor()
    cur.execute("""
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source='shopify'
        LIMIT 1
    """, (uid,))
    row = fetchone_secure(cur)
    con.close()

    sync_type = row["sync_type"] if row and row.get("sync_type") else "historical"
    return jsonify(sync_shopify(uid, sync_type=sync_type))


@app.route("/api/status/shopify")
def _shopify_status():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT 1
        FROM connector_configs
        WHERE uid=? AND connector='shopify'
        LIMIT 1
    """, (uid,))
    creds = fetchone_secure(cur)

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='shopify'
        LIMIT 1
    """, (uid,))
    conn = fetchone_secure(cur)

    con.close()

    return jsonify({
        "has_credentials": bool(creds),
        "connected": bool(conn and conn["enabled"] == 1)
    })


@app.route("/connectors/shopify/job/get")
def _shopify_job_get():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401

    con = get_db()
    cur = con.cursor()
    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='shopify'
    """, (uid,))
    row = fetchone_secure(cur)
    con.close()

    if not row:
        return jsonify({"exists": False})

    return jsonify({
        "exists": True,
        "sync_type": row["sync_type"],
        "schedule_time": row["schedule_time"],
    })


@app.route("/connectors/shopify/job/save", methods=["POST"])
def _shopify_job_save():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json() or {}
    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")

    con = get_db()
    cur = con.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time)
        VALUES (?, 'shopify', ?, ?)
    """, (uid, sync_type, schedule_time))
    con.commit()
    con.close()

    return jsonify({"status": "job_saved"})


# ================= MAILCHIMP ========================

@app.route("/connectors/mailchimp/save_app", methods=["POST"])
def _mailchimp_save_config():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json() or {}
    api_key = data.get("api_key")
    if not api_key:
        return jsonify({"error": "missing api_key"}), 400
    save_mailchimp_config(uid, api_key)
    ensure_connector_initialized(uid, "mailchimp")
    return jsonify({"status": "saved"})

@app.route("/connectors/mailchimp/connect")
def _mailchimp_connect():
    uid = getattr(g, "user_id", None)
    if not uid: return jsonify({"error": "unauthorized"}), 401
    return jsonify(connect_mailchimp(uid))

@app.route("/connectors/mailchimp/disconnect")
def _mailchimp_disconnect():
    uid = getattr(g, "user_id", None)
    if not uid: return jsonify({"error": "unauthorized"}), 401
    return jsonify(disconnect_mailchimp(uid))

@app.route("/connectors/mailchimp/sync")
def _mailchimp_sync():
    uid = getattr(g, "user_id", None)
    if not uid: return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT sync_type FROM connector_jobs WHERE uid=? AND source='mailchimp' LIMIT 1", (uid,))
    row = fetchone_secure(cur)
    con.close()
    sync_type = row["sync_type"] if row and row.get("sync_type") else "historical"
    return jsonify(sync_mailchimp(uid, sync_type=sync_type))

@app.route("/api/status/mailchimp")
def _mailchimp_status():
    uid = getattr(g, "user_id", None)
    if not uid: return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT 1 FROM connector_configs WHERE uid=? AND connector='mailchimp'", (uid,))
    creds = fetchone_secure(cur)
    cur.execute("SELECT enabled FROM google_connections WHERE uid=? AND source='mailchimp'", (uid,))
    conn_row = fetchone_secure(cur)
    con.close()
    return jsonify({"has_credentials": bool(creds), "connected": bool(conn_row and conn_row.get("enabled") == 1)})

@app.route("/connectors/mailchimp/job/get")
def _mailchimp_job_get():
    uid = getattr(g, "user_id", None)
    if not uid: return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT sync_type, schedule_time FROM connector_jobs WHERE uid=? AND source='mailchimp'", (uid,))
    row = fetchone_secure(cur)
    con.close()
    if not row: return jsonify({"exists": False})
    return jsonify({"exists": True, "sync_type": row["sync_type"], "schedule_time": row["schedule_time"]})

@app.route("/connectors/mailchimp/job/save", methods=["POST"])
def _mailchimp_job_save():
    uid = getattr(g, "user_id", None)
    if not uid: return jsonify({"error": "unauthorized"}), 401
    data = request.get_json() or {}
    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")
    con = get_db()
    cur = con.cursor()
    cur.execute("INSERT OR REPLACE INTO connector_jobs (uid, source, sync_type, schedule_time) VALUES (?, 'mailchimp', ?, ?)", (uid, sync_type, schedule_time))
    con.commit()
    con.close()
    return jsonify({"status": "job_saved"})


# ================= TWILIO ========================

@app.route("/connectors/twilio/save_app", methods=["POST"])
def _twilio_save_config():
    uid = getattr(g, "user_id", None)
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json() or {}
    account_sid = data.get("account_sid")
    auth_token = data.get("auth_token")
    if not account_sid or not auth_token:
        return jsonify({"error": "missing account_sid or auth_token"}), 400
    save_twilio_config(uid, account_sid, auth_token)
    ensure_connector_initialized(uid, "twilio")
    return jsonify({"status": "saved"})

@app.route("/connectors/twilio/connect")
def _twilio_connect():
    uid = getattr(g, "user_id", None)
    if not uid: return jsonify({"error": "unauthorized"}), 401
    return jsonify(connect_twilio(uid))

@app.route("/connectors/twilio/disconnect")
def _twilio_disconnect():
    uid = getattr(g, "user_id", None)
    if not uid: return jsonify({"error": "unauthorized"}), 401
    return jsonify(disconnect_twilio(uid))

@app.route("/connectors/twilio/sync")
def _twilio_sync():
    uid = getattr(g, "user_id", None)
    if not uid: return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT sync_type FROM connector_jobs WHERE uid=? AND source='twilio' LIMIT 1", (uid,))
    row = fetchone_secure(cur)
    con.close()
    sync_type = row["sync_type"] if row and row.get("sync_type") else "historical"
    return jsonify(sync_twilio(uid, sync_type=sync_type))

@app.route("/api/status/twilio")
def _twilio_status():
    uid = getattr(g, "user_id", None)
    if not uid: return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT 1 FROM connector_configs WHERE uid=? AND connector='twilio'", (uid,))
    creds = fetchone_secure(cur)
    cur.execute("SELECT enabled FROM google_connections WHERE uid=? AND source='twilio'", (uid,))
    conn_row = fetchone_secure(cur)
    con.close()
    return jsonify({"has_credentials": bool(creds), "connected": bool(conn_row and conn_row.get("enabled") == 1)})

@app.route("/connectors/twilio/job/get")
def _twilio_job_get():
    uid = getattr(g, "user_id", None)
    if not uid: return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT sync_type, schedule_time FROM connector_jobs WHERE uid=? AND source='twilio'", (uid,))
    row = fetchone_secure(cur)
    con.close()
    if not row: return jsonify({"exists": False})
    return jsonify({"exists": True, "sync_type": row["sync_type"], "schedule_time": row["schedule_time"]})

@app.route("/connectors/twilio/job/save", methods=["POST"])
def _twilio_job_save():
    uid = getattr(g, "user_id", None)
    if not uid: return jsonify({"error": "unauthorized"}), 401
    data = request.get_json() or {}
    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")
    con = get_db()
    cur = con.cursor()
    cur.execute("INSERT OR REPLACE INTO connector_jobs (uid, source, sync_type, schedule_time) VALUES (?, 'twilio', ?, ?)", (uid, sync_type, schedule_time))
    con.commit()
    con.close()
    return jsonify({"status": "job_saved"})


# ================= AIRTABLE ========================

@app.route("/connectors/airtable/save_app", methods=["POST"])
def airtable_save_config():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json() or {}
    access_token = (data.get("access_token") or "").strip()
    base_id = (data.get("base_id") or "").strip()
    table_name = (data.get("table_name") or "").strip()

    if not access_token or not base_id or not table_name:
        return jsonify({"error": "missing fields: access_token, base_id, table_name"}), 400

    save_airtable_config(uid, access_token, base_id, table_name)
    ensure_connector_initialized(uid, "airtable")
    return jsonify({"status": "saved"})


@app.route("/connectors/airtable/connect")
def airtable_connect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    res = connect_airtable(uid)
    if res.get("status") != "success":
        return jsonify(res), 400
    return jsonify(res)


@app.route("/connectors/airtable/disconnect")
def airtable_disconnect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    res = disconnect_airtable(uid)
    return jsonify(res)


@app.route("/connectors/airtable/sync")
def airtable_sync():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401

    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source='airtable'
        LIMIT 1
        """,
        (uid,),
    )
    row = fetchone_secure(cur)
    con.close()

    sync_type = row["sync_type"] if row and row.get("sync_type") else "historical"
    return jsonify(sync_airtable(uid, sync_type=sync_type))


@app.route("/api/status/airtable")
def airtable_status():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401

    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT config_json
        FROM connector_configs
        WHERE uid=? AND connector='airtable'
        LIMIT 1
        """,
        (uid,),
    )
    cfg_row = fetchone_secure(cur)

    cur.execute(
        """
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='airtable'
        LIMIT 1
        """,
        (uid,),
    )
    conn_row = fetchone_secure(cur)
    con.close()

    access_token = None
    base_id = None
    table_name = None
    if cfg_row and cfg_row.get("config_json"):
        try:
            cfg = json.loads(cfg_row["config_json"])
            base_id = cfg.get("base_id")
            table_name = cfg.get("table_name")
            raw_token = cfg.get("access_token")
            if raw_token:
                access_token = f"{raw_token[:4]}{'*' * max(len(raw_token) - 8, 4)}{raw_token[-4:]}"
        except Exception:
            import traceback; traceback.print_exc()
            print('Exception caught', flush=True)

    return jsonify(
        {
            "has_credentials": bool(cfg_row),
            "connected": bool(conn_row and conn_row.get("enabled") == 1),
            "access_token": access_token,
            "base_id": base_id,
            "table_name": table_name,
        }
    )


@app.route("/connectors/airtable/job/get")
def airtable_job_get():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401

    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source=?
        """,
        (uid, "airtable"),
    )
    row = fetchone_secure(cur)
    con.close()

    if not row:
        return jsonify({"exists": False})

    return jsonify(
        {
            "exists": True,
            "sync_type": row["sync_type"],
            "schedule_time": row["schedule_time"],
        }
    )


@app.route("/connectors/airtable/job/save", methods=["POST"])
def airtable_job_save():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json() or {}
    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")

    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time)
        VALUES (?, ?, ?, ?)
        """,
        (uid, "airtable", sync_type, schedule_time),
    )
    con.commit()
    con.close()

    return jsonify({"status": "job_saved"})

# ---------------- PIPEDRIVE ----------------
 
@app.route("/connectors/pipedrive/save_app", methods=["POST"])
def pipedrive_save_config():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
 
    data = request.get_json() or {}
    api_token = (data.get("api_token") or "").strip()
 
    if not api_token:
        return jsonify({"error": "missing field: api_token"}), 400
 
    save_pipedrive_config(uid, api_token)
    ensure_connector_initialized(uid, "pipedrive")
    return jsonify({"status": "saved"})
 
 
@app.route("/connectors/pipedrive/connect")
def pipedrive_connect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    res = connect_pipedrive(uid)
    if res.get("status") != "success":
        return jsonify(res), 400
    return jsonify(res)
 
 
@app.route("/connectors/pipedrive/disconnect")
def pipedrive_disconnect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    res = disconnect_pipedrive(uid)
    return jsonify(res)
 
 
@app.route("/connectors/pipedrive/sync")
def pipedrive_sync():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
 
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source='pipedrive'
        LIMIT 1
        """,
        (uid,),
    )
    row = fetchone_secure(cur)
    con.close()
 
    sync_type = row["sync_type"] if row and row.get("sync_type") else "historical"
    return jsonify(sync_pipedrive(uid, sync_type=sync_type))
 
 
@app.route("/api/status/pipedrive")
def pipedrive_status():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
 
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT config_json
        FROM connector_configs
        WHERE uid=? AND connector='pipedrive'
        LIMIT 1
        """,
        (uid,),
    )
    cfg_row = fetchone_secure(cur)
 
    cur.execute(
        """
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='pipedrive'
        LIMIT 1
        """,
        (uid,),
    )
    conn_row = fetchone_secure(cur)
    con.close()
 
    api_token = None
    if cfg_row and cfg_row.get("config_json"):
        try:
            cfg = json.loads(cfg_row["config_json"])
            raw_token = cfg.get("api_token")
            if raw_token:
                api_token = f"{raw_token[:4]}{'*' * max(len(raw_token) - 8, 4)}{raw_token[-4:]}"
        except Exception:
            import traceback; traceback.print_exc()
            print('Exception caught', flush=True)
 
    return jsonify(
        {
            "has_credentials": bool(cfg_row),
            "connected": bool(conn_row and conn_row.get("enabled") == 1),
            "api_token": api_token,
        }
    )
 
 
@app.route("/connectors/pipedrive/job/get")
def pipedrive_job_get():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
 
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source=?
        """,
        (uid, "pipedrive"),
    )
    row = fetchone_secure(cur)
    con.close()
 
    if not row:
        return jsonify({"exists": False})
 
    return jsonify(
        {
            "exists": True,
            "sync_type": row["sync_type"],
            "schedule_time": row["schedule_time"],
        }
    )
 
 
@app.route("/connectors/pipedrive/job/save", methods=["POST"])
def pipedrive_job_save():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
 
    data = request.get_json() or {}
    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")
 
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time)
        VALUES (?, ?, ?, ?)
        """,
        (uid, "pipedrive", sync_type, schedule_time),
    )
    con.commit()
    con.close()
 
    return jsonify({"status": "job_saved"})
 
 
# ---------------- FRESHDESK ----------------
 
@app.route("/connectors/freshdesk/save_app", methods=["POST"])
def freshdesk_save_config():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
 
    data = request.get_json() or {}
    domain = (data.get("domain") or "").strip()
    api_key = (data.get("api_key") or "").strip()
 
    if not domain or not api_key:
        return jsonify({"error": "missing fields: domain, api_key"}), 400
 
    save_freshdesk_config(uid, domain, api_key)
    ensure_connector_initialized(uid, "freshdesk")
    return jsonify({"status": "saved"})
 
 
@app.route("/connectors/freshdesk/connect")
def freshdesk_connect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    res = connect_freshdesk(uid)
    if res.get("status") != "success":
        return jsonify(res), 400
    return jsonify(res)
 
 
@app.route("/connectors/freshdesk/disconnect")
def freshdesk_disconnect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    res = disconnect_freshdesk(uid)
    return jsonify(res)
 
 
@app.route("/connectors/freshdesk/sync")
def freshdesk_sync():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
 
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source='freshdesk'
        LIMIT 1
        """,
        (uid,),
    )
    row = fetchone_secure(cur)
    con.close()
 
    sync_type = row["sync_type"] if row and row.get("sync_type") else "historical"
    return jsonify(sync_freshdesk(uid, sync_type=sync_type))
 
 
@app.route("/api/status/freshdesk")
def freshdesk_status():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
 
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT config_json
        FROM connector_configs
        WHERE uid=? AND connector='freshdesk'
        LIMIT 1
        """,
        (uid,),
    )
    cfg_row = fetchone_secure(cur)
 
    cur.execute(
        """
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='freshdesk'
        LIMIT 1
        """,
        (uid,),
    )
    conn_row = fetchone_secure(cur)
    con.close()
 
    api_key = None
    domain = None
    if cfg_row and cfg_row.get("config_json"):
        try:
            cfg = json.loads(cfg_row["config_json"])
            domain = cfg.get("domain")
            raw_key = cfg.get("api_key")
            if raw_key:
                api_key = f"{raw_key[:4]}{'*' * max(len(raw_key) - 8, 4)}{raw_key[-4:]}"
        except Exception:
            import traceback; traceback.print_exc()
            print('Exception caught', flush=True)
 
    return jsonify(
        {
            "has_credentials": bool(cfg_row),
            "connected": bool(conn_row and conn_row.get("enabled") == 1),
            "api_key": api_key,
            "domain": domain,
        }
    )
 
 
@app.route("/connectors/freshdesk/job/get")
def freshdesk_job_get():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
 
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source=?
        """,
        (uid, "freshdesk"),
    )
    row = fetchone_secure(cur)
    con.close()
 
    if not row:
        return jsonify({"exists": False})
 
    return jsonify(
        {
            "exists": True,
            "sync_type": row["sync_type"],
            "schedule_time": row["schedule_time"],
        }
    )
 
 
@app.route("/connectors/freshdesk/job/save", methods=["POST"])
def freshdesk_job_save():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
 
    data = request.get_json() or {}
    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")
 
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time)
        VALUES (?, ?, ?, ?)
        """,
        (uid, "freshdesk", sync_type, schedule_time),
    )
    con.commit()
    con.close()
 
    return jsonify({"status": "job_saved"})
 
 
# ---------------- KLAVIYO ----------------
 
@app.route("/connectors/klaviyo/save_app", methods=["POST"])
def klaviyo_save_config():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
 
    data = request.get_json() or {}
    api_key = (data.get("api_key") or "").strip()
 
    if not api_key:
        return jsonify({"error": "missing field: api_key"}), 400
 
    save_klaviyo_config(uid, api_key)
    ensure_connector_initialized(uid, "klaviyo")
    return jsonify({"status": "saved"})
 
 
@app.route("/connectors/klaviyo/connect")
def klaviyo_connect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    res = connect_klaviyo(uid)
    if res.get("status") != "success":
        return jsonify(res), 400
    return jsonify(res)
 
 
@app.route("/connectors/klaviyo/disconnect")
def klaviyo_disconnect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    res = disconnect_klaviyo(uid)
    return jsonify(res)
 
 
@app.route("/connectors/klaviyo/sync")
def klaviyo_sync():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
 
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source='klaviyo'
        LIMIT 1
        """,
        (uid,),
    )
    row = fetchone_secure(cur)
    con.close()
 
    sync_type = row["sync_type"] if row and row.get("sync_type") else "historical"
    return jsonify(sync_klaviyo(uid, sync_type=sync_type))
 
 
@app.route("/api/status/klaviyo")
def klaviyo_status():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
 
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT config_json
        FROM connector_configs
        WHERE uid=? AND connector='klaviyo'
        LIMIT 1
        """,
        (uid,),
    )
    cfg_row = fetchone_secure(cur)
 
    cur.execute(
        """
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='klaviyo'
        LIMIT 1
        """,
        (uid,),
    )
    conn_row = fetchone_secure(cur)
    con.close()
 
    api_key = None
    if cfg_row and cfg_row.get("config_json"):
        try:
            cfg = json.loads(cfg_row["config_json"])
            raw_key = cfg.get("api_key")
            if raw_key:
                api_key = f"{raw_key[:4]}{'*' * max(len(raw_key) - 8, 4)}{raw_key[-4:]}"
        except Exception:
            import traceback; traceback.print_exc()
            print('Exception caught', flush=True)
 
    return jsonify(
        {
            "has_credentials": bool(cfg_row),
            "connected": bool(conn_row and conn_row.get("enabled") == 1),
            "api_key": api_key,
        }
    )
 
 
@app.route("/connectors/klaviyo/job/get")
def klaviyo_job_get():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
 
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source=?
        """,
        (uid, "klaviyo"),
    )
    row = fetchone_secure(cur)
    con.close()
 
    if not row:
        return jsonify({"exists": False})
 
    return jsonify(
        {
            "exists": True,
            "sync_type": row["sync_type"],
            "schedule_time": row["schedule_time"],
        }
    )
 
 
@app.route("/connectors/klaviyo/job/save", methods=["POST"])
def klaviyo_job_save():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
 
    data = request.get_json() or {}
    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")
 
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time)
        VALUES (?, ?, ?, ?)
        """,
        (uid, "klaviyo", sync_type, schedule_time),
    )
    con.commit()
    con.close()
 
    return jsonify({"status": "job_saved"})
 
 
# ---------------- AMPLITUDE ----------------
 
@app.route("/connectors/amplitude/save_app", methods=["POST"])
def amplitude_save_config():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
 
    data = request.get_json() or {}
    api_key = (data.get("api_key") or "").strip()
    secret_key = (data.get("secret_key") or "").strip()
 
    if not api_key or not secret_key:
        return jsonify({"error": "missing fields: api_key, secret_key"}), 400
 
    save_amplitude_config(uid, api_key, secret_key)
    ensure_connector_initialized(uid, "amplitude")
    return jsonify({"status": "saved"})
 
 
@app.route("/connectors/amplitude/connect")
def amplitude_connect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    res = connect_amplitude(uid)
    if res.get("status") != "success":
        return jsonify(res), 400
    return jsonify(res)
 
 
@app.route("/connectors/amplitude/disconnect")
def amplitude_disconnect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    res = disconnect_amplitude(uid)
    return jsonify(res)
 
 
@app.route("/connectors/amplitude/sync")
def amplitude_sync():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
 
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source='amplitude'
        LIMIT 1
        """,
        (uid,),
    )
    row = fetchone_secure(cur)
    con.close()
 
    sync_type = row["sync_type"] if row and row.get("sync_type") else "historical"
    return jsonify(sync_amplitude(uid, sync_type=sync_type))
 
 
@app.route("/api/status/amplitude")
def amplitude_status():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
 
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT config_json
        FROM connector_configs
        WHERE uid=? AND connector='amplitude'
        LIMIT 1
        """,
        (uid,),
    )
    cfg_row = fetchone_secure(cur)
 
    cur.execute(
        """
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='amplitude'
        LIMIT 1
        """,
        (uid,),
    )
    conn_row = fetchone_secure(cur)
    con.close()
 
    api_key = None
    secret_key = None
    if cfg_row and cfg_row.get("config_json"):
        try:
            cfg = json.loads(cfg_row["config_json"])
            raw_api_key = cfg.get("api_key")
            raw_secret_key = cfg.get("secret_key")
            if raw_api_key:
                api_key = f"{raw_api_key[:4]}{'*' * max(len(raw_api_key) - 8, 4)}{raw_api_key[-4:]}"
            if raw_secret_key:
                secret_key = f"{raw_secret_key[:4]}{'*' * max(len(raw_secret_key) - 8, 4)}{raw_secret_key[-4:]}"
        except Exception:
            import traceback; traceback.print_exc()
            print('Exception caught', flush=True)
 
    return jsonify(
        {
            "has_credentials": bool(cfg_row),
            "connected": bool(conn_row and conn_row.get("enabled") == 1),
            "api_key": api_key,
            "secret_key": secret_key,
        }
    )
 
 
@app.route("/connectors/amplitude/job/get")
def amplitude_job_get():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
 
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source=?
        """,
        (uid, "amplitude"),
    )
    row = fetchone_secure(cur)
    con.close()
 
    if not row:
        return jsonify({"exists": False})
 
    return jsonify(
        {
            "exists": True,
            "sync_type": row["sync_type"],
            "schedule_time": row["schedule_time"],
        }
    )
 
 
@app.route("/connectors/amplitude/job/save", methods=["POST"])
def amplitude_job_save():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
 
    data = request.get_json() or {}
    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")
 
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO connector_jobs
        (uid, source, sync_type, schedule_time)
        VALUES (?, ?, ?, ?)
        """,
        (uid, "amplitude", sync_type, schedule_time),
    )
    con.commit()
    con.close()
 
    return jsonify({"status": "job_saved"})

# SALESFORCE
@app.route("/connectors/salesforce/save_app", methods=["POST"])
def salesforce_save_app():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.json
    save_salesforce_config(uid, client_id=data.get("client_id", ""), 
                          client_secret=data.get("client_secret", ""),
                          instance_url=data.get("instance_url", ""))
    return jsonify({"status": "saved"})

@app.route("/connectors/salesforce/connect")
def salesforce_connect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(connect_salesforce(uid))

@app.route("/connectors/salesforce/disconnect")
def salesforce_disconnect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(disconnect_salesforce(uid))

@app.route("/connectors/salesforce/sync")
def salesforce_sync():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    sync_type = request.args.get("type", "incremental")
    return jsonify(sync_salesforce(uid, sync_type))

@app.route("/api/status/salesforce")
def status_salesforce():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT config_json FROM connector_configs WHERE uid=? AND connector='salesforce' LIMIT 1", (uid,))
    cfg_row = fetchone_secure(cur)
    cur.execute("SELECT enabled FROM google_connections WHERE uid=? AND source='salesforce' LIMIT 1", (uid,))
    conn_row = fetchone_secure(cur)
    con.close()
    return jsonify({"has_credentials": bool(cfg_row), "connected": bool(conn_row and conn_row.get("enabled") == 1)})

@app.route("/connectors/salesforce/job/get")
def salesforce_job_get():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT schedule_time, sync_type FROM connector_jobs WHERE uid=? AND source='salesforce' LIMIT 1", (uid,))
    row = fetchone_secure(cur)
    con.close()
    if not row:
        return jsonify({"exists": False})
    return jsonify({"exists": True, "schedule_time": row.get("schedule_time"), "sync_type": row.get("sync_type", "incremental")})

@app.route("/connectors/salesforce/job/save", methods=["POST"])
def salesforce_job_save():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.json
    con = get_db()
    cur = con.cursor()
    cur.execute("INSERT OR REPLACE INTO connector_jobs (uid, source, schedule_time, sync_type, updated_at) VALUES (?, 'salesforce', ?, ?, ?)",
                (uid, data.get("schedule_time"), data.get("sync_type", "incremental"), datetime.now().isoformat()))
    con.commit()
    con.close()
    return jsonify({"status": "saved"})


# JIRA
@app.route("/connectors/jira/save_app", methods=["POST"])
def jira_save_app():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.json
    save_jira_config(uid, email=data.get("email", ""), 
                    api_token=data.get("api_token", ""),
                    domain=data.get("domain", ""))
    return jsonify({"status": "saved"})

@app.route("/connectors/jira/connect")
def jira_connect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(connect_jira(uid))

@app.route("/connectors/jira/disconnect")
def jira_disconnect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(disconnect_jira(uid))

@app.route("/connectors/jira/sync")
def jira_sync():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    sync_type = request.args.get("type", "incremental")
    return jsonify(sync_jira(uid, sync_type))

@app.route("/api/status/jira")
def status_jira():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT config_json FROM connector_configs WHERE uid=? AND connector='jira' LIMIT 1", (uid,))
    cfg_row = fetchone_secure(cur)
    cur.execute("SELECT enabled FROM google_connections WHERE uid=? AND source='jira' LIMIT 1", (uid,))
    conn_row = fetchone_secure(cur)
    con.close()
    return jsonify({"has_credentials": bool(cfg_row), "connected": bool(conn_row and conn_row.get("enabled") == 1)})

@app.route("/connectors/jira/job/get")
def jira_job_get():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT schedule_time, sync_type FROM connector_jobs WHERE uid=? AND source='jira' LIMIT 1", (uid,))
    row = fetchone_secure(cur)
    con.close()
    if not row:
        return jsonify({"exists": False})
    return jsonify({"exists": True, "schedule_time": row.get("schedule_time"), "sync_type": row.get("sync_type", "incremental")})

@app.route("/connectors/jira/job/save", methods=["POST"])
def jira_job_save():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.json
    con = get_db()
    cur = con.cursor()
    cur.execute("INSERT OR REPLACE INTO connector_jobs (uid, source, schedule_time, sync_type, updated_at) VALUES (?, 'jira', ?, ?, ?)",
                (uid, data.get("schedule_time"), data.get("sync_type", "incremental"), datetime.now().isoformat()))
    con.commit()
    con.close()
    return jsonify({"status": "saved"})


# ZOHO CRM
@app.route("/connectors/zoho_crm/save_app", methods=["POST"])
def zoho_crm_save_app():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.json
    save_zoho_crm_config(uid, client_id=data.get("client_id", ""),
                        client_secret=data.get("client_secret", ""),
                        refresh_token=data.get("refresh_token", ""))
    return jsonify({"status": "saved"})

@app.route("/connectors/zoho_crm/connect")
def zoho_crm_connect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(connect_zoho_crm(uid))

@app.route("/connectors/zoho_crm/disconnect")
def zoho_crm_disconnect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(disconnect_zoho_crm(uid))

@app.route("/connectors/zoho_crm/sync")
def zoho_crm_sync():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    sync_type = request.args.get("type", "incremental")
    return jsonify(sync_zoho_crm(uid, sync_type))

@app.route("/api/status/zoho_crm")
def status_zoho_crm():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT config_json FROM connector_configs WHERE uid=? AND connector='zoho_crm' LIMIT 1", (uid,))
    cfg_row = fetchone_secure(cur)
    cur.execute("SELECT enabled FROM google_connections WHERE uid=? AND source='zoho_crm' LIMIT 1", (uid,))
    conn_row = fetchone_secure(cur)
    con.close()
    return jsonify({"has_credentials": bool(cfg_row), "connected": bool(conn_row and conn_row.get("enabled") == 1)})

@app.route("/connectors/zoho_crm/job/get")
def zoho_crm_job_get():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT schedule_time, sync_type FROM connector_jobs WHERE uid=? AND source='zoho_crm' LIMIT 1", (uid,))
    row = fetchone_secure(cur)
    con.close()
    if not row:
        return jsonify({"exists": False})
    return jsonify({"exists": True, "schedule_time": row.get("schedule_time"), "sync_type": row.get("sync_type", "incremental")})

@app.route("/connectors/zoho_crm/job/save", methods=["POST"])
def zoho_crm_job_save():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.json
    con = get_db()
    cur = con.cursor()
    cur.execute("INSERT OR REPLACE INTO connector_jobs (uid, source, schedule_time, sync_type, updated_at) VALUES (?, 'zoho_crm', ?, ?, ?)",
                (uid, data.get("schedule_time"), data.get("sync_type", "incremental"), datetime.now().isoformat()))
    con.commit()
    con.close()
    return jsonify({"status": "saved"})


# PAYPAL
@app.route("/connectors/paypal/save_app", methods=["POST"])
def paypal_save_app():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.json
    save_paypal_config(uid, client_id=data.get("client_id", ""),
                      client_secret=data.get("client_secret", ""),
                      use_sandbox=data.get("use_sandbox", False))
    return jsonify({"status": "saved"})

@app.route("/connectors/paypal/connect")
def paypal_connect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(connect_paypal(uid))

@app.route("/connectors/paypal/disconnect")
def paypal_disconnect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(disconnect_paypal(uid))

@app.route("/connectors/paypal/sync")
def paypal_sync():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    sync_type = request.args.get("type", "incremental")
    return jsonify(sync_paypal(uid, sync_type))

@app.route("/api/status/paypal")
def status_paypal():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT config_json FROM connector_configs WHERE uid=? AND connector='paypal' LIMIT 1", (uid,))
    cfg_row = fetchone_secure(cur)
    cur.execute("SELECT enabled FROM google_connections WHERE uid=? AND source='paypal' LIMIT 1", (uid,))
    conn_row = fetchone_secure(cur)
    con.close()
    return jsonify({"has_credentials": bool(cfg_row), "connected": bool(conn_row and conn_row.get("enabled") == 1)})

@app.route("/connectors/paypal/job/get")
def paypal_job_get():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT schedule_time, sync_type FROM connector_jobs WHERE uid=? AND source='paypal' LIMIT 1", (uid,))
    row = fetchone_secure(cur)
    con.close()
    if not row:
        return jsonify({"exists": False})
    return jsonify({"exists": True, "schedule_time": row.get("schedule_time"), "sync_type": row.get("sync_type", "incremental")})

@app.route("/connectors/paypal/job/save", methods=["POST"])
def paypal_job_save():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.json
    con = get_db()
    cur = con.cursor()
    cur.execute("INSERT OR REPLACE INTO connector_jobs (uid, source, schedule_time, sync_type, updated_at) VALUES (?, 'paypal', ?, ?, ?)",
                (uid, data.get("schedule_time"), data.get("sync_type", "incremental"), datetime.now().isoformat()))
    con.commit()
    con.close()
    return jsonify({"status": "saved"})

# ASANA
@app.route("/connectors/asana/save_app", methods=["POST"])
def asana_save_app():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.json
    save_asana_config(uid, access_token=data.get("access_token", ""))
    return jsonify({"status": "saved"})
 
@app.route("/connectors/asana/connect")
def asana_connect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(connect_asana(uid))
 
@app.route("/connectors/asana/disconnect")
def asana_disconnect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(disconnect_asana(uid))
 
@app.route("/connectors/asana/sync")
def asana_sync():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    sync_type = request.args.get("type", "incremental")
    return jsonify(sync_asana(uid, sync_type))
 
@app.route("/api/status/asana")
def status_asana():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT config_json FROM connector_configs WHERE uid=? AND connector='asana' LIMIT 1", (uid,))
    cfg_row = fetchone_secure(cur)
    cur.execute("SELECT enabled FROM google_connections WHERE uid=? AND source='asana' LIMIT 1", (uid,))
    conn_row = fetchone_secure(cur)
    con.close()
    return jsonify({"has_credentials": bool(cfg_row), "connected": bool(conn_row and conn_row.get("enabled") == 1)})
 
@app.route("/connectors/asana/job/get")
def asana_job_get():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT schedule_time, sync_type FROM connector_jobs WHERE uid=? AND source='asana' LIMIT 1", (uid,))
    row = fetchone_secure(cur)
    con.close()
    if not row:
        return jsonify({"exists": False})
    return jsonify({"exists": True, "schedule_time": row.get("schedule_time"), "sync_type": row.get("sync_type", "incremental")})
 
@app.route("/connectors/asana/job/save", methods=["POST"])
def asana_job_save():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.json
    con = get_db()
    cur = con.cursor()
    cur.execute("INSERT OR REPLACE INTO connector_jobs (uid, source, schedule_time, sync_type, updated_at) VALUES (?, 'asana', ?, ?, ?)",
                (uid, data.get("schedule_time"), data.get("sync_type", "incremental"), datetime.now().isoformat()))
    con.commit()
    con.close()
    return jsonify({"status": "saved"})
 
 
# SENDGRID
@app.route("/connectors/sendgrid/save_app", methods=["POST"])
def sendgrid_save_app():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.json
    save_sendgrid_config(uid, api_key=data.get("api_key", ""))
    return jsonify({"status": "saved"})
 
@app.route("/connectors/sendgrid/connect")
def sendgrid_connect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(connect_sendgrid(uid))
 
@app.route("/connectors/sendgrid/disconnect")
def sendgrid_disconnect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(disconnect_sendgrid(uid))
 
@app.route("/connectors/sendgrid/sync")
def sendgrid_sync():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    sync_type = request.args.get("type", "incremental")
    return jsonify(sync_sendgrid(uid, sync_type))
 
@app.route("/api/status/sendgrid")
def status_sendgrid():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT config_json FROM connector_configs WHERE uid=? AND connector='sendgrid' LIMIT 1", (uid,))
    cfg_row = fetchone_secure(cur)
    cur.execute("SELECT enabled FROM google_connections WHERE uid=? AND source='sendgrid' LIMIT 1", (uid,))
    conn_row = fetchone_secure(cur)
    con.close()
    return jsonify({"has_credentials": bool(cfg_row), "connected": bool(conn_row and conn_row.get("enabled") == 1)})
 
@app.route("/connectors/sendgrid/job/get")
def sendgrid_job_get():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT schedule_time, sync_type FROM connector_jobs WHERE uid=? AND source='sendgrid' LIMIT 1", (uid,))
    row = fetchone_secure(cur)
    con.close()
    if not row:
        return jsonify({"exists": False})
    return jsonify({"exists": True, "schedule_time": row.get("schedule_time"), "sync_type": row.get("sync_type", "incremental")})
 
@app.route("/connectors/sendgrid/job/save", methods=["POST"])
def sendgrid_job_save():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.json
    con = get_db()
    cur = con.cursor()
    cur.execute("INSERT OR REPLACE INTO connector_jobs (uid, source, schedule_time, sync_type, updated_at) VALUES (?, 'sendgrid', ?, ?, ?)",
                (uid, data.get("schedule_time"), data.get("sync_type", "incremental"), datetime.now().isoformat()))
    con.commit()
    con.close()
    return jsonify({"status": "saved"})


# TABLEAU
@app.route("/connectors/tableau/save_app", methods=["POST"])
def tableau_save_app():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    save_tableau_config(uid, request.json or {})
    ensure_connector_initialized(uid, "tableau")
    return jsonify({"status": "saved"})


@app.route("/connectors/tableau/connect")
def tableau_connect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(connect_tableau(uid))


@app.route("/connectors/tableau/disconnect")
def tableau_disconnect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(disconnect_tableau(uid))


@app.route("/connectors/tableau/sync")
def tableau_sync():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    sync_type = request.args.get("type", "incremental")
    return jsonify(sync_tableau(uid, sync_type))


@app.route("/api/status/tableau")
def status_tableau():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT config_json FROM connector_configs WHERE uid=? AND connector='tableau' LIMIT 1", (uid,))
    cfg_row = fetchone_secure(cur)
    cur.execute("SELECT enabled FROM google_connections WHERE uid=? AND source='tableau' LIMIT 1", (uid,))
    conn_row = fetchone_secure(cur)
    con.close()
    return jsonify({"has_credentials": bool(cfg_row), "connected": bool(conn_row and conn_row.get("enabled") == 1)})


@app.route("/connectors/tableau/job/get")
def tableau_job_get():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT schedule_time, sync_type FROM connector_jobs WHERE uid=? AND source='tableau' LIMIT 1", (uid,))
    row = fetchone_secure(cur)
    con.close()
    if not row:
        return jsonify({"exists": False})
    return jsonify({"exists": True, "schedule_time": row.get("schedule_time"), "sync_type": row.get("sync_type", "incremental")})


@app.route("/connectors/tableau/job/save", methods=["POST"])
def tableau_job_save():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.json or {}
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO connector_jobs (uid, source, schedule_time, sync_type, updated_at) VALUES (?, 'tableau', ?, ?, ?)",
        (uid, data.get("schedule_time"), data.get("sync_type", "incremental"), datetime.datetime.now(datetime.UTC).isoformat()),
    )
    con.commit()
    con.close()
    return jsonify({"status": "saved"})


# POWER BI
@app.route("/connectors/power_bi/save_app", methods=["POST"])
def power_bi_save_app():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    save_power_bi_config(uid, request.json or {})
    ensure_connector_initialized(uid, "power_bi")
    return jsonify({"status": "saved"})


@app.route("/connectors/power_bi/connect")
def power_bi_connect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(connect_power_bi(uid))


@app.route("/connectors/power_bi/disconnect")
def power_bi_disconnect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(disconnect_power_bi(uid))


@app.route("/connectors/power_bi/sync")
def power_bi_sync():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    sync_type = request.args.get("type", "incremental")
    return jsonify(sync_power_bi(uid, sync_type))


@app.route("/api/status/power_bi")
def status_power_bi():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT config_json FROM connector_configs WHERE uid=? AND connector='power_bi' LIMIT 1", (uid,))
    cfg_row = fetchone_secure(cur)
    cur.execute("SELECT enabled FROM google_connections WHERE uid=? AND source='power_bi' LIMIT 1", (uid,))
    conn_row = fetchone_secure(cur)
    con.close()
    return jsonify({"has_credentials": bool(cfg_row), "connected": bool(conn_row and conn_row.get("enabled") == 1)})


@app.route("/connectors/power_bi/job/get")
def power_bi_job_get():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT schedule_time, sync_type FROM connector_jobs WHERE uid=? AND source='power_bi' LIMIT 1", (uid,))
    row = fetchone_secure(cur)
    con.close()
    if not row:
        return jsonify({"exists": False})
    return jsonify({"exists": True, "schedule_time": row.get("schedule_time"), "sync_type": row.get("sync_type", "incremental")})


@app.route("/connectors/power_bi/job/save", methods=["POST"])
def power_bi_job_save():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.json or {}
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO connector_jobs (uid, source, schedule_time, sync_type, updated_at) VALUES (?, 'power_bi', ?, ?, ?)",
        (uid, data.get("schedule_time"), data.get("sync_type", "incremental"), datetime.datetime.now(datetime.UTC).isoformat()),
    )
    con.commit()
    con.close()
    return jsonify({"status": "saved"})


# WORKDAY
@app.route("/connectors/workday/save_app", methods=["POST"])
def workday_save_app():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    save_workday_config(uid, request.json or {})
    ensure_connector_initialized(uid, "workday")
    return jsonify({"status": "saved"})


@app.route("/connectors/workday/connect")
def workday_connect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(connect_workday(uid))


@app.route("/connectors/workday/disconnect")
def workday_disconnect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(disconnect_workday(uid))


@app.route("/connectors/workday/sync")
def workday_sync():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    sync_type = request.args.get("type", "incremental")
    return jsonify(sync_workday(uid, sync_type))


@app.route("/api/status/workday")
def status_workday():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT config_json FROM connector_configs WHERE uid=? AND connector='workday' LIMIT 1", (uid,))
    cfg_row = fetchone_secure(cur)
    cur.execute("SELECT enabled FROM google_connections WHERE uid=? AND source='workday' LIMIT 1", (uid,))
    conn_row = fetchone_secure(cur)
    con.close()
    return jsonify({"has_credentials": bool(cfg_row), "connected": bool(conn_row and conn_row.get("enabled") == 1)})


@app.route("/connectors/workday/job/get")
def workday_job_get():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT schedule_time, sync_type FROM connector_jobs WHERE uid=? AND source='workday' LIMIT 1", (uid,))
    row = fetchone_secure(cur)
    con.close()
    if not row:
        return jsonify({"exists": False})
    return jsonify({"exists": True, "schedule_time": row.get("schedule_time"), "sync_type": row.get("sync_type", "incremental")})


@app.route("/connectors/workday/job/save", methods=["POST"])
def workday_job_save():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.json or {}
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO connector_jobs (uid, source, schedule_time, sync_type, updated_at) VALUES (?, 'workday', ?, ?, ?)",
        (uid, data.get("schedule_time"), data.get("sync_type", "incremental"), datetime.datetime.now(datetime.UTC).isoformat()),
    )
    con.commit()
    con.close()
    return jsonify({"status": "saved"})


# EBAY
@app.route("/connectors/ebay/save_app", methods=["POST"])
def ebay_save_app():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    save_ebay_config(uid, request.json or {})
    ensure_connector_initialized(uid, "ebay")
    return jsonify({"status": "saved"})


@app.route("/connectors/ebay/connect")
def ebay_connect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(connect_ebay(uid))


@app.route("/connectors/ebay/disconnect")
def ebay_disconnect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(disconnect_ebay(uid))


@app.route("/connectors/ebay/sync")
def ebay_sync():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    sync_type = request.args.get("type", "incremental")
    return jsonify(sync_ebay(uid, sync_type))


@app.route("/api/status/ebay")
def status_ebay():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT config_json FROM connector_configs WHERE uid=? AND connector='ebay' LIMIT 1", (uid,))
    cfg_row = fetchone_secure(cur)
    cur.execute("SELECT enabled FROM google_connections WHERE uid=? AND source='ebay' LIMIT 1", (uid,))
    conn_row = fetchone_secure(cur)
    con.close()
    return jsonify({"has_credentials": bool(cfg_row), "connected": bool(conn_row and conn_row.get("enabled") == 1)})


@app.route("/connectors/ebay/job/get")
def ebay_job_get():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT schedule_time, sync_type FROM connector_jobs WHERE uid=? AND source='ebay' LIMIT 1", (uid,))
    row = fetchone_secure(cur)
    con.close()
    if not row:
        return jsonify({"exists": False})
    return jsonify({"exists": True, "schedule_time": row.get("schedule_time"), "sync_type": row.get("sync_type", "incremental")})


@app.route("/connectors/ebay/job/save", methods=["POST"])
def ebay_job_save():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.json or {}
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO connector_jobs (uid, source, schedule_time, sync_type, updated_at) VALUES (?, 'ebay', ?, ?, ?)",
        (uid, data.get("schedule_time"), data.get("sync_type", "incremental"), datetime.datetime.now(datetime.UTC).isoformat()),
    )
    con.commit()
    con.close()
    return jsonify({"status": "saved"})
 
 
# MIXPANEL
@app.route("/connectors/mixpanel/save_app", methods=["POST"])
def mixpanel_save_app():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.json
    save_mixpanel_config(uid, api_secret=data.get("api_secret", ""))
    return jsonify({"status": "saved"})
 
@app.route("/connectors/mixpanel/connect")
def mixpanel_connect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(connect_mixpanel(uid))
 
@app.route("/connectors/mixpanel/disconnect")
def mixpanel_disconnect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(disconnect_mixpanel(uid))
 
@app.route("/connectors/mixpanel/sync")
def mixpanel_sync():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    sync_type = request.args.get("type", "incremental")
    return jsonify(sync_mixpanel(uid, sync_type))
 
@app.route("/api/status/mixpanel")
def status_mixpanel():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT config_json FROM connector_configs WHERE uid=? AND connector='mixpanel' LIMIT 1", (uid,))
    cfg_row = fetchone_secure(cur)
    cur.execute("SELECT enabled FROM google_connections WHERE uid=? AND source='mixpanel' LIMIT 1", (uid,))
    conn_row = fetchone_secure(cur)
    con.close()
    return jsonify({"has_credentials": bool(cfg_row), "connected": bool(conn_row and conn_row.get("enabled") == 1)})
 
@app.route("/connectors/mixpanel/job/get")
def mixpanel_job_get():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT schedule_time, sync_type FROM connector_jobs WHERE uid=? AND source='mixpanel' LIMIT 1", (uid,))
    row = fetchone_secure(cur)
    con.close()
    if not row:
        return jsonify({"exists": False})
    return jsonify({"exists": True, "schedule_time": row.get("schedule_time"), "sync_type": row.get("sync_type", "incremental")})
 
@app.route("/connectors/mixpanel/job/save", methods=["POST"])
def mixpanel_job_save():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.json
    con = get_db()
    cur = con.cursor()
    cur.execute("INSERT OR REPLACE INTO connector_jobs (uid, source, schedule_time, sync_type, updated_at) VALUES (?, 'mixpanel', ?, ?, ?)",
                (uid, data.get("schedule_time"), data.get("sync_type", "incremental"), datetime.now().isoformat()))
    con.commit()
    con.close()
    return jsonify({"status": "saved"})

# MONDAY.COM
@app.route("/connectors/monday/save_app", methods=["POST"])
def monday_save_app():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.json
    save_monday_config(uid, api_token=data.get("api_token", ""))
    return jsonify({"status": "saved"})
 
@app.route("/connectors/monday/connect")
def monday_connect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(connect_monday(uid))
 
@app.route("/connectors/monday/disconnect")
def monday_disconnect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(disconnect_monday(uid))
 
@app.route("/connectors/monday/sync")
def monday_sync():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    sync_type = request.args.get("type", "incremental")
    return jsonify(sync_monday(uid, sync_type))
 
@app.route("/api/status/monday")
def status_monday():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT config_json FROM connector_configs WHERE uid=? AND connector='monday' LIMIT 1", (uid,))
    cfg_row = fetchone_secure(cur)
    cur.execute("SELECT enabled FROM google_connections WHERE uid=? AND source='monday' LIMIT 1", (uid,))
    conn_row = fetchone_secure(cur)
    con.close()
    return jsonify({"has_credentials": bool(cfg_row), "connected": bool(conn_row and conn_row.get("enabled") == 1)})
 
@app.route("/connectors/monday/job/get")
def monday_job_get():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT schedule_time, sync_type FROM connector_jobs WHERE uid=? AND source='monday' LIMIT 1", (uid,))
    row = fetchone_secure(cur)
    con.close()
    if not row:
        return jsonify({"exists": False})
    return jsonify({"exists": True, "schedule_time": row.get("schedule_time"), "sync_type": row.get("sync_type", "incremental")})
 
@app.route("/connectors/monday/job/save", methods=["POST"])
def monday_job_save():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.json
    con = get_db()
    cur = con.cursor()
    cur.execute("INSERT OR REPLACE INTO connector_jobs (uid, source, schedule_time, sync_type, updated_at) VALUES (?, 'monday', ?, ?, ?)",
                (uid, data.get("schedule_time"), data.get("sync_type", "incremental"), datetime.now().isoformat()))
    con.commit()
    con.close()
    return jsonify({"status": "saved"})
 
 
# CLICKUP
@app.route("/connectors/clickup/save_app", methods=["POST"])
def clickup_save_app():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.json
    save_clickup_config(uid, api_token=data.get("api_token", ""))
    return jsonify({"status": "saved"})
 
@app.route("/connectors/clickup/connect")
def clickup_connect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(connect_clickup(uid))
 
@app.route("/connectors/clickup/disconnect")
def clickup_disconnect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(disconnect_clickup(uid))
 
@app.route("/connectors/clickup/sync")
def clickup_sync():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    sync_type = request.args.get("type", "incremental")
    return jsonify(sync_clickup(uid, sync_type))
 
@app.route("/api/status/clickup")
def status_clickup():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT config_json FROM connector_configs WHERE uid=? AND connector='clickup' LIMIT 1", (uid,))
    cfg_row = fetchone_secure(cur)
    cur.execute("SELECT enabled FROM google_connections WHERE uid=? AND source='clickup' LIMIT 1", (uid,))
    conn_row = fetchone_secure(cur)
    con.close()
    return jsonify({"has_credentials": bool(cfg_row), "connected": bool(conn_row and conn_row.get("enabled") == 1)})
 
@app.route("/connectors/clickup/job/get")
def clickup_job_get():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT schedule_time, sync_type FROM connector_jobs WHERE uid=? AND source='clickup' LIMIT 1", (uid,))
    row = fetchone_secure(cur)
    con.close()
    if not row:
        return jsonify({"exists": False})
    return jsonify({"exists": True, "schedule_time": row.get("schedule_time"), "sync_type": row.get("sync_type", "incremental")})
 
@app.route("/connectors/clickup/job/save", methods=["POST"])
def clickup_job_save():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.json
    con = get_db()
    cur = con.cursor()
    cur.execute("INSERT OR REPLACE INTO connector_jobs (uid, source, schedule_time, sync_type, updated_at) VALUES (?, 'clickup', ?, ?, ?)",
                (uid, data.get("schedule_time"), data.get("sync_type", "incremental"), datetime.now().isoformat()))
    con.commit()
    con.close()
    return jsonify({"status": "saved"})
 
 
# HELPSCOUT
@app.route("/connectors/helpscout/save_app", methods=["POST"])
def helpscout_save_app():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.json
    save_helpscout_config(uid, api_key=data.get("api_key", ""))
    return jsonify({"status": "saved"})
 
@app.route("/connectors/helpscout/connect")
def helpscout_connect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(connect_helpscout(uid))
 
@app.route("/connectors/helpscout/disconnect")
def helpscout_disconnect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(disconnect_helpscout(uid))
 
@app.route("/connectors/helpscout/sync")
def helpscout_sync():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    sync_type = request.args.get("type", "incremental")
    return jsonify(sync_helpscout(uid, sync_type))
 
@app.route("/api/status/helpscout")
def status_helpscout():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT config_json FROM connector_configs WHERE uid=? AND connector='helpscout' LIMIT 1", (uid,))
    cfg_row = fetchone_secure(cur)
    cur.execute("SELECT enabled FROM google_connections WHERE uid=? AND source='helpscout' LIMIT 1", (uid,))
    conn_row = fetchone_secure(cur)
    con.close()
    return jsonify({"has_credentials": bool(cfg_row), "connected": bool(conn_row and conn_row.get("enabled") == 1)})
 
@app.route("/connectors/helpscout/job/get")
def helpscout_job_get():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT schedule_time, sync_type FROM connector_jobs WHERE uid=? AND source='helpscout' LIMIT 1", (uid,))
    row = fetchone_secure(cur)
    con.close()
    if not row:
        return jsonify({"exists": False})
    return jsonify({"exists": True, "schedule_time": row.get("schedule_time"), "sync_type": row.get("sync_type", "incremental")})
 
@app.route("/connectors/helpscout/job/save", methods=["POST"])
def helpscout_job_save():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.json
    con = get_db()
    cur = con.cursor()
    cur.execute("INSERT OR REPLACE INTO connector_jobs (uid, source, schedule_time, sync_type, updated_at) VALUES (?, 'helpscout', ?, ?, ?)",
                (uid, data.get("schedule_time"), data.get("sync_type", "incremental"), datetime.now().isoformat()))

    con.commit()
    con.close()
    return jsonify({"status": "saved"})

# ================= LOOKER =================

@app.route("/connectors/looker/connect")
def looker_connect_api():
    uid = get_uid()
    if not uid: return jsonify({"error":"unauthorized"}), 401
    return jsonify(connect_looker(uid))

@app.route("/connectors/looker/disconnect")
def looker_disconnect_api():
    uid = get_uid()
    if not uid: return jsonify({"error":"unauthorized"}), 401
    return jsonify(disconnect_looker(uid))

@app.route("/connectors/looker/sync")
def looker_sync_api():
    uid = get_uid()
    if not uid: return jsonify({"error":"unauthorized"}), 401
    sync_type = request.args.get("type", "incremental")
    return jsonify(sync_looker(uid, sync_type))

@app.route("/connectors/looker/save_app", methods=["POST"])
def looker_save_app_api():
    uid = get_uid()
    if not uid: return jsonify({"error":"unauthorized"}), 401
    save_looker_config(uid, request.json)
    return jsonify({"status":"success"})


# ================= SUPERSET =================

@app.route("/connectors/superset/connect")
def superset_connect_api():
    uid = get_uid()
    if not uid: return jsonify({"error":"unauthorized"}), 401
    return jsonify(connect_superset(uid))

@app.route("/connectors/superset/disconnect")
def superset_disconnect_api():
    uid = get_uid()
    if not uid: return jsonify({"error":"unauthorized"}), 401
    return jsonify(disconnect_superset(uid))

@app.route("/connectors/superset/sync")
def superset_sync_api():
    uid = get_uid()
    if not uid: return jsonify({"error":"unauthorized"}), 401
    sync_type = request.args.get("type", "incremental")
    return jsonify(sync_superset(uid, sync_type))

@app.route("/connectors/superset/save_app", methods=["POST"])
def superset_save_app_api():
    uid = get_uid()
    if not uid: return jsonify({"error":"unauthorized"}), 401
    save_superset_config(uid, request.json)
    return jsonify({"status":"success"})


# ================= AZURE_BLOB =================

@app.route("/connectors/azure_blob/connect")
def azure_blob_connect_api():
    uid = get_uid()
    if not uid: return jsonify({"error":"unauthorized"}), 401
    return jsonify(connect_azure_blob(uid))

@app.route("/connectors/azure_blob/disconnect")
def azure_blob_disconnect_api():
    uid = get_uid()
    if not uid: return jsonify({"error":"unauthorized"}), 401
    return jsonify(disconnect_azure_blob(uid))

@app.route("/connectors/azure_blob/sync")
def azure_blob_sync_api():
    uid = get_uid()
    if not uid: return jsonify({"error":"unauthorized"}), 401
    sync_type = request.args.get("type", "incremental")
    return jsonify(sync_azure_blob(uid, sync_type))

@app.route("/connectors/azure_blob/save_app", methods=["POST"])
def azure_blob_save_app_api():
    uid = get_uid()
    if not uid: return jsonify({"error":"unauthorized"}), 401
    save_azure_blob_config(uid, request.json)
    return jsonify({"status":"success"})


# ================= DATADOG =================

@app.route("/connectors/datadog/connect")
def datadog_connect_api():
    uid = get_uid()
    if not uid: return jsonify({"error":"unauthorized"}), 401
    return jsonify(connect_datadog(uid))

@app.route("/connectors/datadog/disconnect")
def datadog_disconnect_api():
    uid = get_uid()
    if not uid: return jsonify({"error":"unauthorized"}), 401
    return jsonify(disconnect_datadog(uid))

@app.route("/connectors/datadog/sync")
def datadog_sync_api():
    uid = get_uid()
    if not uid: return jsonify({"error":"unauthorized"}), 401
    sync_type = request.args.get("type", "incremental")
    return jsonify(sync_datadog(uid, sync_type))

@app.route("/connectors/datadog/save_app", methods=["POST"])
def datadog_save_app_api():
    uid = get_uid()
    if not uid: return jsonify({"error":"unauthorized"}), 401
    save_datadog_config(uid, request.json)
    return jsonify({"status":"success"})


# ================= OKTA =================

@app.route("/connectors/okta/connect")
def okta_connect_api():
    uid = get_uid()
    if not uid: return jsonify({"error":"unauthorized"}), 401
    return jsonify(connect_okta(uid))

@app.route("/connectors/okta/disconnect")
def okta_disconnect_api():
    uid = get_uid()
    if not uid: return jsonify({"error":"unauthorized"}), 401
    return jsonify(disconnect_okta(uid))

@app.route("/connectors/okta/sync")
def okta_sync_api():
    uid = get_uid()
    if not uid: return jsonify({"error":"unauthorized"}), 401
    sync_type = request.args.get("type", "incremental")
    return jsonify(sync_okta(uid, sync_type))

@app.route("/connectors/okta/save_app", methods=["POST"])
def okta_save_app_api():
    uid = get_uid()
    if not uid: return jsonify({"error":"unauthorized"}), 401
    save_okta_config(uid, request.json)
    return jsonify({"status":"success"})

@app.route("/api/status/okta")
def okta_status_api():
    uid = get_uid()
    if not uid: return jsonify({"error":"unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT status, config_json FROM connector_configs WHERE uid=? AND connector='okta' LIMIT 1", (uid,))
    row = cur.fetchone()
    con.close()
    if not row:
        return jsonify({"connected": False, "has_credentials": False})
    status, config_json = row
    return jsonify({"connected": status == "connected", "has_credentials": bool(config_json), "status": status})


# ================= AUTH0 =================

@app.route("/connectors/auth0/connect")
def auth0_connect_api():
    uid = get_uid()
    if not uid: return jsonify({"error":"unauthorized"}), 401
    return jsonify(connect_auth0(uid))

@app.route("/connectors/auth0/disconnect")
def auth0_disconnect_api():
    uid = get_uid()
    if not uid: return jsonify({"error":"unauthorized"}), 401
    return jsonify(disconnect_auth0(uid))

@app.route("/connectors/auth0/sync")
def auth0_sync_api():
    uid = get_uid()
    if not uid: return jsonify({"error":"unauthorized"}), 401
    sync_type = request.args.get("type", "incremental")
    return jsonify(sync_auth0(uid, sync_type))

@app.route("/connectors/auth0/save_app", methods=["POST"])
def auth0_save_app_api():
    uid = get_uid()
    if not uid: return jsonify({"error":"unauthorized"}), 401
    save_auth0_config(uid, request.json)
    return jsonify({"status":"success"})

@app.route("/api/status/auth0")
def auth0_status_api():
    uid = get_uid()
    if not uid: return jsonify({"error":"unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT status, config_json FROM connector_configs WHERE uid=? AND connector='auth0' LIMIT 1", (uid,))
    row = cur.fetchone()
    con.close()
    if not row:
        return jsonify({"connected": False, "has_credentials": False})
    status, config_json = row
    return jsonify({"connected": status == "connected", "has_credentials": bool(config_json), "status": status})


# ================= CLOUDFLARE =================

@app.route("/connectors/cloudflare/connect")
def cloudflare_connect_api():
    uid = get_uid()
    if not uid: return jsonify({"error":"unauthorized"}), 401
    return jsonify(connect_cloudflare(uid))

@app.route("/connectors/cloudflare/disconnect")
def cloudflare_disconnect_api():
    uid = get_uid()
    if not uid: return jsonify({"error":"unauthorized"}), 401
    return jsonify(disconnect_cloudflare(uid))

@app.route("/connectors/cloudflare/sync")
def cloudflare_sync_api():
    uid = get_uid()
    if not uid: return jsonify({"error":"unauthorized"}), 401
    sync_type = request.args.get("type", "incremental")
    return jsonify(sync_cloudflare(uid, sync_type))

@app.route("/connectors/cloudflare/save_app", methods=["POST"])
def cloudflare_save_app_api():
    uid = get_uid()
    if not uid: return jsonify({"error":"unauthorized"}), 401
    save_cloudflare_config(uid, request.json)
    return jsonify({"status":"success"})

@app.route("/api/status/cloudflare")
def cloudflare_status_api():
    uid = get_uid()
    if not uid: return jsonify({"error":"unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT status, config_json FROM connector_configs WHERE uid=? AND connector='cloudflare' LIMIT 1", (uid,))
    row = cur.fetchone()
    con.close()
    if not row:
        return jsonify({"connected": False, "has_credentials": False})
    status, config_json = row
    return jsonify({"connected": status == "connected", "has_credentials": bool(config_json), "status": status})


# ================= SENTRY =================

@app.route("/connectors/sentry/connect")
def sentry_connect_api():
    uid = get_uid()
    if not uid: return jsonify({"error":"unauthorized"}), 401
    return jsonify(connect_sentry(uid))

@app.route("/connectors/sentry/disconnect")
def sentry_disconnect_api():
    uid = get_uid()
    if not uid: return jsonify({"error":"unauthorized"}), 401
    return jsonify(disconnect_sentry(uid))

@app.route("/connectors/sentry/sync")
def sentry_sync_api():
    uid = get_uid()
    if not uid: return jsonify({"error":"unauthorized"}), 401
    sync_type = request.args.get("type", "incremental")
    return jsonify(sync_sentry(uid, sync_type))

@app.route("/connectors/sentry/save_app", methods=["POST"])
def sentry_save_app_api():
    uid = get_uid()
    if not uid: return jsonify({"error":"unauthorized"}), 401
    save_sentry_config(uid, request.json)
    return jsonify({"status":"success"})

@app.route("/api/status/sentry")
def sentry_status_api():
    uid = get_uid()
    if not uid: return jsonify({"error":"unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT status, config_json FROM connector_configs WHERE uid=? AND connector='sentry' LIMIT 1", (uid,))
    row = cur.fetchone()
    con.close()
    if not row:
        return jsonify({"connected": False, "has_credentials": False})
    status, config_json = row
    return jsonify({"connected": status == "connected", "has_credentials": bool(config_json), "status": status})

# ---------------- QUICKBOOKS ----------------
@app.route("/connectors/quickbooks/save_app", methods=["POST"])
def qb_save():
    data = request.json
    return jsonify(quickbooks.save_app_quickbooks(data.get("client_id"), data.get("client_secret")))

@app.route("/connectors/quickbooks/connect")
def qb_connect():
    return quickbooks.connect_quickbooks()

@app.route("/connectors/quickbooks/callback")
def qb_callback():
    return quickbooks.callback_quickbooks()

@app.route("/connectors/quickbooks/sync")
def qb_sync():
    return jsonify(quickbooks.sync_quickbooks())

@app.route("/connectors/quickbooks/disconnect")
def qb_disconnect():
    return jsonify(quickbooks.disconnect_quickbooks())

@app.route("/api/status/quickbooks")
def qb_status():
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT 1 FROM quickbooks_config LIMIT 1")
    has_creds = bool(cur.fetchone())
    cur.execute("SELECT 1 FROM quickbooks_auth LIMIT 1")
    connected = bool(cur.fetchone())
    con.close()
    return jsonify({"has_credentials": has_creds, "connected": connected})

# ---------------- XERO ----------------
@app.route("/connectors/xero/save_app", methods=["POST"])
def xero_save():
    data = request.json
    return jsonify(xero.save_app_xero(data.get("client_id"), data.get("client_secret")))

@app.route("/connectors/xero/connect")
def xero_connect():
    return xero.connect_xero()

@app.route("/connectors/xero/callback")
def xero_callback():
    return xero.callback_xero()

@app.route("/connectors/xero/sync")
def xero_sync():
    return jsonify(xero.sync_xero())

@app.route("/connectors/xero/disconnect")
def xero_disconnect():
    return jsonify(xero.disconnect_xero())

@app.route("/api/status/xero")
def xero_status_route():
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT 1 FROM xero_config LIMIT 1")
    has_creds = bool(cur.fetchone())
    cur.execute("SELECT tenant_name FROM xero_auth LIMIT 1")
    row = cur.fetchone()
    con.close()
    return jsonify({"has_credentials": has_creds, "connected": bool(row), "tenant_name": row[0] if row else None})

@app.route("/api/status/google_gmail")
@app.route("/api/status/gmail")
def gmail_status():
    uid = get_uid()
    if not uid: return jsonify({"error":"unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    # Check if we have an account for this user and source with BOTH tokens
    cur.execute("""
        SELECT 1 FROM google_accounts 
        WHERE uid=? AND source='gmail' 
        AND access_token IS NOT NULL AND access_token != ''
        AND refresh_token IS NOT NULL AND refresh_token != ''
        LIMIT 1
    """, (uid,))
    row = cur.fetchone()
    con.close()
    return jsonify({"connected": bool(row)})

# ---------------- AMAZON SELLER ----------------
@app.route("/connectors/amazon_seller/save_app", methods=["POST"])
def amz_save():
    data = request.json
    return jsonify(amazon_seller.save_app_amazon_seller(data.get("client_id"), data.get("client_secret"), data.get("seller_id"), data.get("region")))

@app.route("/connectors/amazon_seller/connect")
def amz_connect():
    return amazon_seller.connect_amazon_seller()

@app.route("/connectors/amazon_seller/callback")
def amz_callback():
    return amazon_seller.callback_amazon_seller()

@app.route("/connectors/amazon_seller/sync")
def amz_sync():
    return jsonify(amazon_seller.sync_amazon_seller())

@app.route("/connectors/amazon_seller/disconnect")
def amz_disconnect():
    return jsonify(amazon_seller.disconnect_amazon_seller())

@app.route("/api/status/amazon_seller")
def amz_status():
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT 1 FROM amazon_seller_config LIMIT 1")
    has_creds = bool(cur.fetchone())
    cur.execute("SELECT 1 FROM amazon_seller_auth LIMIT 1")
    connected = bool(cur.fetchone())
    con.close()
    return jsonify({"has_credentials": has_creds, "connected": connected})

# ---------------- NEW RELIC ----------------
@app.route("/connectors/newrelic/save_app", methods=["POST"])
def nr_save():
    data = request.json
    return jsonify(newrelic.save_app_newrelic(data.get("api_key"), data.get("account_id"), data.get("region")))

@app.route("/connectors/newrelic/sync")
def nr_sync():
    return jsonify(newrelic.sync_newrelic())

@app.route("/connectors/newrelic/disconnect")
def nr_disconnect():
    return jsonify(newrelic.disconnect_newrelic())

@app.route("/api/status/newrelic")
def nr_status():
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT 1 FROM newrelic_auth LIMIT 1")
    connected = bool(cur.fetchone())
    con.close()
    return jsonify({"has_credentials": connected, "connected": connected})

# OPENAI
@app.route("/connectors/openai/save_app", methods=["POST"])
def openai_save_app():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.json
    save_openai_config(uid, api_key=data.get("api_key", ""))
    return jsonify({"status": "saved"})
 
@app.route("/connectors/openai/connect")
def openai_connect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(connect_openai(uid))
 
@app.route("/connectors/openai/disconnect")
def openai_disconnect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(disconnect_openai(uid))
 
@app.route("/connectors/openai/sync")
def openai_sync():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    sync_type = request.args.get("type", "incremental")
    return jsonify(sync_openai(uid, sync_type))
 
@app.route("/api/status/openai")
def status_openai():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT config_json FROM connector_configs WHERE uid=? AND connector='openai' LIMIT 1", (uid,))
    cfg_row = fetchone_secure(cur)
    cur.execute("SELECT enabled FROM google_connections WHERE uid=? AND source='openai' LIMIT 1", (uid,))
    conn_row = fetchone_secure(cur)
    con.close()
    return jsonify({"has_credentials": bool(cfg_row), "connected": bool(conn_row and conn_row.get("enabled") == 1)})
 
@app.route("/connectors/openai/job/get")
def openai_job_get():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT schedule_time, sync_type FROM connector_jobs WHERE uid=? AND source='openai' LIMIT 1", (uid,))
    row = fetchone_secure(cur)
    con.close()
    if not row:
        return jsonify({"exists": False})
    return jsonify({"exists": True, "schedule_time": row.get("schedule_time"), "sync_type": row.get("sync_type", "incremental")})
 
@app.route("/connectors/openai/job/save", methods=["POST"])
def openai_job_save():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.json
    con = get_db()
    cur = con.cursor()
    cur.execute("INSERT OR REPLACE INTO connector_jobs (uid, source, schedule_time, sync_type, updated_at) VALUES (?, 'openai', ?, ?, ?)",
                (uid, data.get("schedule_time"), data.get("sync_type", "incremental"), datetime.now().isoformat()))
    con.commit()
    con.close()
    return jsonify({"status": "saved"})
 
 
# HUGGINGFACE
@app.route("/connectors/huggingface/save_app", methods=["POST"])
def huggingface_save_app():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.json
    save_huggingface_config(uid, access_token=data.get("access_token", ""))
    return jsonify({"status": "saved"})
 
@app.route("/connectors/huggingface/connect")
def huggingface_connect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(connect_huggingface(uid))
 
@app.route("/connectors/huggingface/disconnect")
def huggingface_disconnect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(disconnect_huggingface(uid))
 
@app.route("/connectors/huggingface/sync")
def huggingface_sync():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    sync_type = request.args.get("type", "incremental")
    return jsonify(sync_huggingface(uid, sync_type))
 
@app.route("/api/status/huggingface")
def status_huggingface():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT config_json FROM connector_configs WHERE uid=? AND connector='huggingface' LIMIT 1", (uid,))
    cfg_row = fetchone_secure(cur)
    cur.execute("SELECT enabled FROM google_connections WHERE uid=? AND source='huggingface' LIMIT 1", (uid,))
    conn_row = fetchone_secure(cur)
    con.close()
    return jsonify({"has_credentials": bool(cfg_row), "connected": bool(conn_row and conn_row.get("enabled") == 1)})
 
@app.route("/connectors/huggingface/job/get")
def huggingface_job_get():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT schedule_time, sync_type FROM connector_jobs WHERE uid=? AND source='huggingface' LIMIT 1", (uid,))
    row = fetchone_secure(cur)
    con.close()
    if not row:
        return jsonify({"exists": False})
    return jsonify({"exists": True, "schedule_time": row.get("schedule_time"), "sync_type": row.get("sync_type", "incremental")})
 
@app.route("/connectors/huggingface/job/save", methods=["POST"])
def huggingface_job_save():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.json
    con = get_db()
    cur = con.cursor()
    cur.execute("INSERT OR REPLACE INTO connector_jobs (uid, source, schedule_time, sync_type, updated_at) VALUES (?, 'huggingface', ?, ?, ?)",
                (uid, data.get("schedule_time"), data.get("sync_type", "incremental"), datetime.now().isoformat()))
    con.commit()
    con.close()
    return jsonify({"status": "saved"})
 
 
# AIRFLOW
@app.route("/connectors/airflow/save_app", methods=["POST"])
def airflow_save_app():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.json
    save_airflow_config(uid, base_url=data.get("base_url", ""), username=data.get("username", ""), password=data.get("password", ""))
    return jsonify({"status": "saved"})
 
@app.route("/connectors/airflow/connect")
def airflow_connect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(connect_airflow(uid))
 
@app.route("/connectors/airflow/disconnect")
def airflow_disconnect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(disconnect_airflow(uid))
 
@app.route("/connectors/airflow/sync")
def airflow_sync():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    sync_type = request.args.get("type", "incremental")
    return jsonify(sync_airflow(uid, sync_type))
 
@app.route("/api/status/airflow")
def status_airflow():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT config_json FROM connector_configs WHERE uid=? AND connector='airflow' LIMIT 1", (uid,))
    cfg_row = fetchone_secure(cur)
    cur.execute("SELECT enabled FROM google_connections WHERE uid=? AND source='airflow' LIMIT 1", (uid,))
    conn_row = fetchone_secure(cur)
    con.close()
    return jsonify({"has_credentials": bool(cfg_row), "connected": bool(conn_row and conn_row.get("enabled") == 1)})
 
@app.route("/connectors/airflow/job/get")
def airflow_job_get():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT schedule_time, sync_type FROM connector_jobs WHERE uid=? AND source='airflow' LIMIT 1", (uid,))
    row = fetchone_secure(cur)
    con.close()
    if not row:
        return jsonify({"exists": False})
    return jsonify({"exists": True, "schedule_time": row.get("schedule_time"), "sync_type": row.get("sync_type", "incremental")})
 
@app.route("/connectors/airflow/job/save", methods=["POST"])
def airflow_job_save():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.json
    con = get_db()
    cur = con.cursor()
    cur.execute("INSERT OR REPLACE INTO connector_jobs (uid, source, schedule_time, sync_type, updated_at) VALUES (?, 'airflow', ?, ?, ?)",
                (uid, data.get("schedule_time"), data.get("sync_type", "incremental"), datetime.now().isoformat()))
    con.commit()
    con.close()
    return jsonify({"status": "saved"})
 
 
# KAFKA
@app.route("/connectors/kafka/save_app", methods=["POST"])
def kafka_save_app():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.json
    save_kafka_config(uid, bootstrap_servers=data.get("bootstrap_servers", ""))
    return jsonify({"status": "saved"})
 
@app.route("/connectors/kafka/connect")
def kafka_connect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(connect_kafka(uid))
 
@app.route("/connectors/kafka/disconnect")
def kafka_disconnect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(disconnect_kafka(uid))
 
@app.route("/connectors/kafka/sync")
def kafka_sync():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    sync_type = request.args.get("type", "incremental")
    return jsonify(sync_kafka(uid, sync_type))
 
@app.route("/api/status/kafka")
def status_kafka():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT config_json FROM connector_configs WHERE uid=? AND connector='kafka' LIMIT 1", (uid,))
    cfg_row = fetchone_secure(cur)
    cur.execute("SELECT enabled FROM google_connections WHERE uid=? AND source='kafka' LIMIT 1", (uid,))
    conn_row = fetchone_secure(cur)
    con.close()
    return jsonify({"has_credentials": bool(cfg_row), "connected": bool(conn_row and conn_row.get("enabled") == 1)})
 
@app.route("/connectors/kafka/job/get")
def kafka_job_get():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT schedule_time, sync_type FROM connector_jobs WHERE uid=? AND source='kafka' LIMIT 1", (uid,))
    row = fetchone_secure(cur)
    con.close()
    if not row:
        return jsonify({"exists": False})
    return jsonify({"exists": True, "schedule_time": row.get("schedule_time"), "sync_type": row.get("sync_type", "incremental")})
 
@app.route("/connectors/kafka/job/save", methods=["POST"])
def kafka_job_save():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.json
    con = get_db()
    cur = con.cursor()
    cur.execute("INSERT OR REPLACE INTO connector_jobs (uid, source, schedule_time, sync_type, updated_at) VALUES (?, 'kafka', ?, ?, ?)",
                (uid, data.get("schedule_time"), data.get("sync_type", "incremental"), datetime.now().isoformat()))
    con.commit()
    con.close()
    return jsonify({"status": "saved"})
    
# ================= DBT ========================
 
@app.route("/connectors/dbt/save_app", methods=["POST"])
def _dbt_save_config():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json() or {}
    api_token  = (data.get("api_token") or "").strip()
    account_id = (data.get("account_id") or "").strip()
    if not api_token or not account_id:
        return jsonify({"error": "missing api_token or account_id"}), 400
    save_dbt_config(uid, api_token, account_id)
    ensure_connector_initialized(uid, "dbt")
    return jsonify({"status": "saved"})
 
 
@app.route("/connectors/dbt/connect")
def _dbt_connect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    res = connect_dbt(uid)
    if res.get("status") != "success":
        return jsonify(res), 400
    return jsonify(res)
 
 
@app.route("/connectors/dbt/disconnect")
def _dbt_disconnect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(disconnect_dbt(uid))
 
 
@app.route("/connectors/dbt/sync")
def _dbt_sync():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "SELECT sync_type FROM connector_jobs WHERE uid=? AND source='dbt' LIMIT 1",
        (uid,),
    )
    row = fetchone_secure(cur)
    con.close()
    sync_type = row["sync_type"] if row and row.get("sync_type") else "historical"
    return jsonify(sync_dbt(uid, sync_type=sync_type))
 
 
@app.route("/api/status/dbt")
def _dbt_status():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "SELECT config_json FROM connector_configs WHERE uid=? AND connector='dbt' LIMIT 1",
        (uid,),
    )
    cfg_row = fetchone_secure(cur)
    cur.execute(
        "SELECT enabled FROM google_connections WHERE uid=? AND source='dbt' LIMIT 1",
        (uid,),
    )
    conn_row = fetchone_secure(cur)
    con.close()
    return jsonify({
        "has_credentials": bool(cfg_row),
        "connected": bool(conn_row and conn_row.get("enabled") == 1),
    })
 
 
@app.route("/connectors/dbt/job/get")
def _dbt_job_get():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "SELECT sync_type, schedule_time FROM connector_jobs WHERE uid=? AND source='dbt'",
        (uid,),
    )
    row = fetchone_secure(cur)
    con.close()
    if not row:
        return jsonify({"exists": False})
    return jsonify({"exists": True, "sync_type": row["sync_type"], "schedule_time": row["schedule_time"]})
 
 
@app.route("/connectors/dbt/job/save", methods=["POST"])
def _dbt_job_save():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json() or {}
    sync_type     = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO connector_jobs (uid, source, sync_type, schedule_time) VALUES (?, 'dbt', ?, ?)",
        (uid, sync_type, schedule_time),
    )
    con.commit()
    con.close()
    return jsonify({"status": "job_saved"})
 
 
# ================= TYPEFORM ========================
 
@app.route("/connectors/typeform/save_app", methods=["POST"])
def _typeform_save_config():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json() or {}
    access_token = (data.get("access_token") or "").strip()
    if not access_token:
        return jsonify({"error": "missing access_token"}), 400
    save_typeform_config(uid, access_token)
    ensure_connector_initialized(uid, "typeform")
    return jsonify({"status": "saved"})
 
 
@app.route("/connectors/typeform/connect")
def _typeform_connect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    res = connect_typeform(uid)
    if res.get("status") != "success":
        return jsonify(res), 400
    return jsonify(res)
 
 
@app.route("/connectors/typeform/disconnect")
def _typeform_disconnect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(disconnect_typeform(uid))
 
 
@app.route("/connectors/typeform/sync")
def _typeform_sync():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "SELECT sync_type FROM connector_jobs WHERE uid=? AND source='typeform' LIMIT 1",
        (uid,),
    )
    row = fetchone_secure(cur)
    con.close()
    sync_type = row["sync_type"] if row and row.get("sync_type") else "historical"
    return jsonify(sync_typeform(uid, sync_type=sync_type))
 
 
@app.route("/api/status/typeform")
def _typeform_status():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "SELECT config_json FROM connector_configs WHERE uid=? AND connector='typeform' LIMIT 1",
        (uid,),
    )
    cfg_row = fetchone_secure(cur)
    cur.execute(
        "SELECT enabled FROM google_connections WHERE uid=? AND source='typeform' LIMIT 1",
        (uid,),
    )
    conn_row = fetchone_secure(cur)
    con.close()
    return jsonify({
        "has_credentials": bool(cfg_row),
        "connected": bool(conn_row and conn_row.get("enabled") == 1),
    })
 
 
@app.route("/connectors/typeform/job/get")
def _typeform_job_get():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "SELECT sync_type, schedule_time FROM connector_jobs WHERE uid=? AND source='typeform'",
        (uid,),
    )
    row = fetchone_secure(cur)
    con.close()
    if not row:
        return jsonify({"exists": False})
    return jsonify({"exists": True, "sync_type": row["sync_type"], "schedule_time": row["schedule_time"]})
 
 
@app.route("/connectors/typeform/job/save", methods=["POST"])
def _typeform_job_save():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json() or {}
    sync_type     = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO connector_jobs (uid, source, sync_type, schedule_time) VALUES (?, 'typeform', ?, ?)",
        (uid, sync_type, schedule_time),
    )
    con.commit()
    con.close()
    return jsonify({"status": "job_saved"})
 
 
# ================= SURVEYMONKEY ========================
 
@app.route("/connectors/surveymonkey/save_app", methods=["POST"])
def _surveymonkey_save_config():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json() or {}
    access_token = (data.get("access_token") or "").strip()
    if not access_token:
        return jsonify({"error": "missing access_token"}), 400
    save_surveymonkey_config(uid, access_token)
    ensure_connector_initialized(uid, "surveymonkey")
    return jsonify({"status": "saved"})
 
 
@app.route("/connectors/surveymonkey/connect")
def _surveymonkey_connect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    res = connect_surveymonkey(uid)
    if res.get("status") != "success":
        return jsonify(res), 400
    return jsonify(res)
 
 
@app.route("/connectors/surveymonkey/disconnect")
def _surveymonkey_disconnect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(disconnect_surveymonkey(uid))
 
 
@app.route("/connectors/surveymonkey/sync")
def _surveymonkey_sync():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "SELECT sync_type FROM connector_jobs WHERE uid=? AND source='surveymonkey' LIMIT 1",
        (uid,),
    )
    row = fetchone_secure(cur)
    con.close()
    sync_type = row["sync_type"] if row and row.get("sync_type") else "historical"
    return jsonify(sync_surveymonkey(uid, sync_type=sync_type))
 
 
@app.route("/api/status/surveymonkey")
def _surveymonkey_status():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "SELECT config_json FROM connector_configs WHERE uid=? AND connector='surveymonkey' LIMIT 1",
        (uid,),
    )
    cfg_row = fetchone_secure(cur)
    cur.execute(
        "SELECT enabled FROM google_connections WHERE uid=? AND source='surveymonkey' LIMIT 1",
        (uid,),
    )
    conn_row = fetchone_secure(cur)
    con.close()
    return jsonify({
        "has_credentials": bool(cfg_row),
        "connected": bool(conn_row and conn_row.get("enabled") == 1),
    })
 
 
@app.route("/connectors/surveymonkey/job/get")
def _surveymonkey_job_get():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "SELECT sync_type, schedule_time FROM connector_jobs WHERE uid=? AND source='surveymonkey'",
        (uid,),
    )
    row = fetchone_secure(cur)
    con.close()
    if not row:
        return jsonify({"exists": False})
    return jsonify({"exists": True, "sync_type": row["sync_type"], "schedule_time": row["schedule_time"]})
 
 
@app.route("/connectors/surveymonkey/job/save", methods=["POST"])
def _surveymonkey_job_save():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json() or {}
    sync_type     = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO connector_jobs (uid, source, sync_type, schedule_time) VALUES (?, 'surveymonkey', ?, ?)",
        (uid, sync_type, schedule_time),
    )
    con.commit()
    con.close()
    return jsonify({"status": "job_saved"})
 
# ================= PINECONE ========================
 
@app.route("/connectors/pinecone/save_app", methods=["POST"])
def _pinecone_save_config():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json() or {}
    api_key     = (data.get("api_key") or "").strip()
    environment = (data.get("environment") or "").strip()
    if not api_key or not environment:
        return jsonify({"error": "missing api_key or environment"}), 400
    save_pinecone_config(uid, api_key, environment)
    ensure_connector_initialized(uid, "pinecone")
    return jsonify({"status": "saved"})
 
 
@app.route("/connectors/pinecone/connect")
def _pinecone_connect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    res = connect_pinecone(uid)
    if res.get("status") != "success":
        return jsonify(res), 400
    return jsonify(res)
 
 
@app.route("/connectors/pinecone/disconnect")
def _pinecone_disconnect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(disconnect_pinecone(uid))
 
 
@app.route("/connectors/pinecone/sync")
def _pinecone_sync():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "SELECT sync_type FROM connector_jobs WHERE uid=? AND source='pinecone' LIMIT 1",
        (uid,),
    )
    row = fetchone_secure(cur)
    con.close()
    sync_type = row["sync_type"] if row and row.get("sync_type") else "historical"
    return jsonify(sync_pinecone(uid, sync_type=sync_type))
 
 
@app.route("/api/status/pinecone")
def _pinecone_status():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "SELECT config_json FROM connector_configs WHERE uid=? AND connector='pinecone' LIMIT 1",
        (uid,),
    )
    cfg_row = fetchone_secure(cur)
    cur.execute(
        "SELECT enabled FROM google_connections WHERE uid=? AND source='pinecone' LIMIT 1",
        (uid,),
    )
    conn_row = fetchone_secure(cur)
    con.close()
    return jsonify({
        "has_credentials": bool(cfg_row),
        "connected": bool(conn_row and conn_row.get("enabled") == 1),
    })
 
 
@app.route("/connectors/pinecone/job/get")
def _pinecone_job_get():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "SELECT sync_type, schedule_time FROM connector_jobs WHERE uid=? AND source='pinecone'",
        (uid,),
    )
    row = fetchone_secure(cur)
    con.close()
    if not row:
        return jsonify({"exists": False})
    return jsonify({"exists": True, "sync_type": row["sync_type"], "schedule_time": row["schedule_time"]})
 
 
@app.route("/connectors/pinecone/job/save", methods=["POST"])
def _pinecone_job_save():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json() or {}
    sync_type     = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO connector_jobs (uid, source, sync_type, schedule_time) VALUES (?, 'pinecone', ?, ?)",
        (uid, sync_type, schedule_time),
    )
    con.commit()
    con.close()
    return jsonify({"status": "job_saved"})

# ================= BITBUCKET ========================

@app.route("/connectors/bitbucket/save_app", methods=["POST"])
def _bitbucket_save_config():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json() or {}
    username = (data.get("username") or "").strip()
    api_token = (data.get("api_token") or "").strip()
    if not username or not api_token:
        return jsonify({"error": "missing username or api_token"}), 400
    save_bitbucket_config(uid, username, api_token)
    ensure_connector_initialized(uid, "bitbucket")
    return jsonify({"status": "saved"})

@app.route("/connectors/bitbucket/connect")
def _bitbucket_connect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    res = connect_bitbucket(uid)
    if res.get("status") != "success":
        return jsonify(res), 400
    return jsonify(res)


@app.route("/connectors/bitbucket/disconnect")
def _bitbucket_disconnect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(disconnect_bitbucket(uid))


@app.route("/connectors/bitbucket/sync")
def _bitbucket_sync():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "SELECT sync_type FROM connector_jobs WHERE uid=? AND source='bitbucket' LIMIT 1",
        (uid,),
    )
    row = fetchone_secure(cur)
    con.close()
    sync_type = row["sync_type"] if row and row.get("sync_type") else "historical"
    return jsonify(sync_bitbucket(uid, sync_type=sync_type))


@app.route("/api/status/bitbucket")
def _bitbucket_status():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "SELECT config_json FROM connector_configs WHERE uid=? AND connector='bitbucket' LIMIT 1",
        (uid,),
    )
    cfg_row = fetchone_secure(cur)
    cur.execute(
        "SELECT enabled FROM google_connections WHERE uid=? AND source='bitbucket' LIMIT 1",
        (uid,),
    )
    conn_row = fetchone_secure(cur)
    con.close()
    return jsonify({
        "has_credentials": bool(cfg_row),
        "connected": bool(conn_row and conn_row.get("enabled") == 1),
    })


@app.route("/connectors/bitbucket/job/get")
def _bitbucket_job_get():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "SELECT sync_type, schedule_time FROM connector_jobs WHERE uid=? AND source='bitbucket'",
        (uid,),
    )
    row = fetchone_secure(cur)
    con.close()
    if not row:
        return jsonify({"exists": False})
    return jsonify({"exists": True, "sync_type": row["sync_type"], "schedule_time": row["schedule_time"]})


@app.route("/connectors/bitbucket/job/save", methods=["POST"])
def _bitbucket_job_save():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json() or {}
    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO connector_jobs (uid, source, sync_type, schedule_time) VALUES (?, 'bitbucket', ?, ?)",
        (uid, sync_type, schedule_time),
    )
    con.commit()
    con.close()
    return jsonify({"status": "job_saved"})

# ================= VERCEL ========================

@app.route("/connectors/vercel/save_app", methods=["POST"])
def _vercel_save_config():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json() or {}
    api_key = (data.get("api_key") or "").strip()
    if not api_key:
        return jsonify({"error": "missing api_key"}), 400
    save_vercel_config(uid, api_key)
    ensure_connector_initialized(uid, "vercel")
    return jsonify({"status": "saved"})


@app.route("/connectors/vercel/connect")
def _vercel_connect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    res = connect_vercel(uid)
    if res.get("status") != "success":
        return jsonify(res), 400
    return jsonify(res)


@app.route("/connectors/vercel/disconnect")
def _vercel_disconnect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(disconnect_vercel(uid))


@app.route("/connectors/vercel/sync")
def _vercel_sync():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "SELECT sync_type FROM connector_jobs WHERE uid=? AND source='vercel' LIMIT 1",
        (uid,),
    )
    row = fetchone_secure(cur)
    con.close()
    sync_type = row["sync_type"] if row and row.get("sync_type") else "historical"
    return jsonify(sync_vercel(uid, sync_type=sync_type))


@app.route("/api/status/vercel")
def _vercel_status():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "SELECT config_json FROM connector_configs WHERE uid=? AND connector='vercel' LIMIT 1",
        (uid,),
    )
    cfg_row = fetchone_secure(cur)
    cur.execute(
        "SELECT enabled FROM google_connections WHERE uid=? AND source='vercel' LIMIT 1",
        (uid,),
    )
    conn_row = fetchone_secure(cur)
    con.close()
    return jsonify({
        "has_credentials": bool(cfg_row),
        "connected": bool(conn_row and conn_row.get("enabled") == 1),
    })


@app.route("/connectors/vercel/job/get")
def _vercel_job_get():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "SELECT sync_type, schedule_time FROM connector_jobs WHERE uid=? AND source='vercel'",
        (uid,),
    )
    row = fetchone_secure(cur)
    con.close()
    if not row:
        return jsonify({"exists": False})
    return jsonify({
        "exists": True,
        "sync_type": row["sync_type"],
        "schedule_time": row["schedule_time"]
    })


@app.route("/connectors/vercel/job/save", methods=["POST"])
def _vercel_job_save():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json() or {}
    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO connector_jobs (uid, source, sync_type, schedule_time) VALUES (?, 'vercel', ?, ?)",
        (uid, sync_type, schedule_time),
    )
    con.commit()
    con.close()
    return jsonify({"status": "job_saved"})

# ================= NETLIFY ========================

@app.route("/connectors/netlify/save_app", methods=["POST"])
def _netlify_save_config():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json() or {}
    api_key = (data.get("api_key") or "").strip()
    if not api_key:
        return jsonify({"error": "missing api_key"}), 400
    save_netlify_config(uid, api_key)
    ensure_connector_initialized(uid, "netlify")
    return jsonify({"status": "saved"})


@app.route("/connectors/netlify/connect")
def _netlify_connect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    res = connect_netlify(uid)
    if res.get("status") != "success":
        return jsonify(res), 400
    return jsonify(res)


@app.route("/connectors/netlify/disconnect")
def _netlify_disconnect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(disconnect_netlify(uid))


@app.route("/connectors/netlify/sync")
def _netlify_sync():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "SELECT sync_type FROM connector_jobs WHERE uid=? AND source='netlify' LIMIT 1",
        (uid,),
    )
    row = fetchone_secure(cur)
    con.close()
    sync_type = row["sync_type"] if row and row.get("sync_type") else "historical"
    return jsonify(sync_netlify(uid, sync_type=sync_type))


@app.route("/api/status/netlify")
def _netlify_status():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "SELECT config_json FROM connector_configs WHERE uid=? AND connector='netlify' LIMIT 1",
        (uid,),
    )
    cfg_row = fetchone_secure(cur)
    cur.execute(
        "SELECT enabled FROM google_connections WHERE uid=? AND source='netlify' LIMIT 1",
        (uid,),
    )
    conn_row = fetchone_secure(cur)
    con.close()
    return jsonify({
        "has_credentials": bool(cfg_row),
        "connected": bool(conn_row and conn_row.get("enabled") == 1),
    })


@app.route("/connectors/netlify/job/get")
def _netlify_job_get():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "SELECT sync_type, schedule_time FROM connector_jobs WHERE uid=? AND source='netlify'",
        (uid,),
    )
    row = fetchone_secure(cur)
    con.close()
    if not row:
        return jsonify({"exists": False})
    return jsonify({
        "exists": True,
        "sync_type": row["sync_type"],
        "schedule_time": row["schedule_time"]
    })


@app.route("/connectors/netlify/job/save", methods=["POST"])
def _netlify_job_save():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json() or {}
    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO connector_jobs (uid, source, sync_type, schedule_time) VALUES (?, 'netlify', ?, ?)",
        (uid, sync_type, schedule_time),
    )
    con.commit()
    con.close()
    return jsonify({"status": "job_saved"})

# ================= LINEAR ========================

@app.route("/connectors/linear/save_app", methods=["POST"])
def _linear_save_config():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json() or {}
    api_key = (data.get("api_key") or "").strip()
    if not api_key:
        return jsonify({"error": "missing api_key"}), 400
    save_linear_config(uid, api_key)
    ensure_connector_initialized(uid, "linear")
    return jsonify({"status": "saved"})


@app.route("/connectors/linear/connect")
def _linear_connect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    res = connect_linear(uid)
    if res.get("status") != "success":
        return jsonify(res), 400
    return jsonify(res)


@app.route("/connectors/linear/disconnect")
def _linear_disconnect():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(disconnect_linear(uid))


@app.route("/connectors/linear/sync")
def _linear_sync():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "SELECT sync_type FROM connector_jobs WHERE uid=? AND source='linear' LIMIT 1",
        (uid,),
    )
    row = fetchone_secure(cur)
    con.close()
    sync_type = row["sync_type"] if row and row.get("sync_type") else "historical"
    return jsonify(sync_linear(uid, sync_type=sync_type))


@app.route("/api/status/linear")
def _linear_status():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "SELECT config_json FROM connector_configs WHERE uid=? AND connector='linear' LIMIT 1",
        (uid,),
    )
    cfg_row = fetchone_secure(cur)
    cur.execute(
        "SELECT enabled FROM google_connections WHERE uid=? AND source='linear' LIMIT 1",
        (uid,),
    )
    conn_row = fetchone_secure(cur)
    con.close()
    return jsonify({
        "has_credentials": bool(cfg_row),
        "connected": bool(conn_row and conn_row.get("enabled") == 1),
    })


@app.route("/connectors/linear/job/get")
def _linear_job_get():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "SELECT sync_type, schedule_time FROM connector_jobs WHERE uid=? AND source='linear'",
        (uid,),
    )
    row = fetchone_secure(cur)
    con.close()
    if not row:
        return jsonify({"exists": False})
    return jsonify({
        "exists": True,
        "sync_type": row["sync_type"],
        "schedule_time": row["schedule_time"]
    })

@app.route("/connectors/linear/job/save", methods=["POST"])
def _linear_job_save():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json() or {}
    sync_type = data.get("sync_type", "incremental")
    schedule_time = data.get("schedule_time")
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO connector_jobs (uid, source, sync_type, schedule_time) VALUES (?, 'linear', ?, ?)",
        (uid, sync_type, schedule_time),
    )
    con.commit()
    con.close()
    return jsonify({"status": "job_saved"})

# ================= AI COMPANION HELPERS =================

def create_chat(user_id):
    chat_id = "chat_" + secrets.token_hex(8)
    title = "New Conversation"
    created_at = datetime.datetime.now(datetime.UTC).isoformat()
    con = get_db()
    cur = con.cursor()
    cur.execute("INSERT INTO ai_chats (id, user_id, title, created_at) VALUES (?, ?, ?, ?)",
                (chat_id, user_id, title, created_at))
    con.commit()
    con.close()
    return {"id": chat_id, "user_id": user_id, "title": title, "created_at": created_at}

def save_message(chat_id, role, content):
    msg_id = "msg_" + secrets.token_hex(8)
    created_at = datetime.datetime.now(datetime.UTC).isoformat()
    con = get_db()
    cur = con.cursor()
    cur.execute("INSERT INTO ai_messages (id, chat_id, role, content, created_at) VALUES (?, ?, ?, ?, ?)",
                (msg_id, chat_id, role, content, created_at))
    con.commit()
    con.close()
    return {"id": msg_id, "chat_id": chat_id, "role": role, "content": content, "created_at": created_at}

def get_chats(user_id):
    con = get_db()
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("SELECT * FROM ai_chats WHERE user_id=? ORDER BY created_at DESC", (user_id,))
    rows = cur.fetchall()
    con.close()
    return [dict(r) for r in rows]

def get_messages(chat_id):
    con = get_db()
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("SELECT * FROM ai_messages WHERE chat_id=? ORDER BY created_at ASC", (chat_id,))
    rows = cur.fetchall()
    con.close()
    return [dict(r) for r in rows]

# ================= AI COMPANION ROUTES =================

@app.route("/ai/chats", methods=["GET"])
def ai_chats():
    uid = get_uid()
    if not uid:
        return jsonify({"chats": []}), 200
    chats = get_chats(uid)
    return jsonify({"chats": chats}), 200

@app.route("/ai/chat/<chat_id>", methods=["GET"])
def ai_chat_history(chat_id):
    uid = get_uid()
    if not uid:
        return jsonify({"messages": []}), 200
        
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT user_id FROM ai_chats WHERE id=?", (chat_id,))
    row = cur.fetchone()
    con.close()
    
    if not row or row[0] != uid:
        return jsonify({"error": "unauthorized"}), 403
        
    messages = get_messages(chat_id)
    return jsonify({"messages": messages}), 200

def get_ai_state(chat_id):
    con = get_db()
    cur = con.cursor()
    try:
        cur.execute("SELECT state_json FROM ai_state WHERE chat_id=?", (chat_id,))
        row = cur.fetchone()
        con.close()
        if not row or not row[0]: return None
        return json.loads(row[0])
    except:
        con.close()
        return None

def save_ai_state(chat_id, uid, state):
    con = get_db()
    cur = con.cursor()
    if not state:
        cur.execute("DELETE FROM ai_state WHERE chat_id=?", (chat_id,))
    else:
        cur.execute("""
            INSERT OR REPLACE INTO ai_state 
            (chat_id, uid, state_json, updated_at)
            VALUES (?, ?, ?, ?)
        """, (
            chat_id,
            uid,
            json.dumps(state),
            datetime.datetime.now(datetime.UTC).isoformat()
        ))
    con.commit()
    con.close()

@app.route("/ai/chat", methods=["POST"])
def ai_chat_message():
    uid = get_uid()
    if not uid:
        return jsonify({"error": "unauthorized"}), 401
 
    data    = request.get_json(silent=True) or {}
    message = (data.get("message") or "").strip()
    chat_id = data.get("chat_id")
 
    if not message:
        return jsonify({"error": "message required"}), 400
 
    # ── Resolve / create chat ──────────────────────────────────
    if not chat_id:
        chat_res = create_chat(uid)
        chat_id  = chat_res["id"]
        title    = message[:40] + ("..." if len(message) > 40 else "")
        con = get_db()
        cur = con.cursor()
        cur.execute("UPDATE ai_chats SET title=? WHERE id=?", (title, chat_id))
        con.commit()
        con.close()
    else:
        con = get_db()
        cur = con.cursor()
        cur.execute("SELECT user_id FROM ai_chats WHERE id=?", (chat_id,))
        row = cur.fetchone()
        con.close()
        if not row or row[0] != uid:
            return jsonify({"error": "unauthorized"}), 403
 
    # ── Persist user message ───────────────────────────────────
    save_message(chat_id, "user", message)
 
    # ── Orchestration ──────────────────────────────────────────
    try:
        # Load state
        state = data.get("state") or get_ai_state(chat_id)
        
        intent = resolve_intent(message)
        print(f"[AI] Intent -> {intent}", flush=True)
        result = orchestrate(intent, uid, chat_id, message, state)
        
        # Save state
        save_ai_state(chat_id, uid, result.get("state"))
        
    except Exception as exc:
        print(f"[AI] Error in orchestration: {exc}", flush=True)
        import traceback
        traceback.print_exc()
        result = {
            "type":       "error",
            "message":    "Something went wrong while processing your request. Please try again.",
            "connectors": [],
            "links":      [],
            "data":       None,
        }
 
    # ── Persist AI reply ──────────────────────────────────────
    save_message(chat_id, "ai", result["message"])
 
    # ── Return result ──
    return jsonify({
        "message":    result["message"],
        "chat_id":    chat_id,
        "type":       result["type"],
        "connectors": result.get("connectors", []),
        "links":      result.get("links", []),
        "data":       result.get("data"),
        "state":      result.get("state")
    }), 200

init_db()
seed_test_user()

if __name__=="__main__":
    app.run(port=4000,debug=True,host="0.0.0.0",use_reloader=False)
