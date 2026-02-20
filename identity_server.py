import os
os.environ["OAUTHLIB_RELAX_TOKEN_SCOPE"] = "1"
import time
from flask import send_from_directory
from flask import Flask,request,redirect,make_response,jsonify,render_template_string
import sqlite3,uuid,datetime,os,json
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
from destinations.destination_router import push_to_destination
# Google OAuth
from dotenv import load_dotenv
from google_auth_oauthlib.flow import Flow

# Connectors
from connectors.pinterest import (
    pinterest_get_auth_url,
    pinterest_exchange_code,
    pinterest_save_token,
    sync_pinterest
)
from connectors.nvd import sync_nvd
from connectors.openstreetmap import sync_openstreetmap
from connectors.lemmy import sync_lemmy
from connectors.discourse import sync_discourse
from connectors.mastodon import sync_mastodon
from connectors.peertube import sync_peertube
from connectors.wikipedia import sync_wikipedia
from connectors.producthunt import sync_producthunt
from connectors.hackernews import sync_hackernews
from connectors.google_youtube import sync_youtube
from connectors.google_webfonts import sync_webfonts
from connectors.google_gcs import sync_gcs
from connectors.google_contacts import sync_contacts
from connectors.google_tasks import sync_tasks
from connectors.classroom import sync_classroom
from connectors.google_gmail import sync_gmail
from connectors.google_calendar import sync_calendar_files
from connectors.google_pagespeed import sync_pagespeed
from connectors.google_search_console import sync_search_console
from connectors.google_drive import sync_drive_files
from connectors.google_forms import sync_forms
from connectors.google_sheets import sync_sheets_files
from connectors.google_ga4 import sync_ga4
from connectors.facebook_pages import sync_facebook_pages

# ---------------- CONFIG ----------------
load_dotenv()

IST = zoneinfo.ZoneInfo("Asia/Kolkata")

app = Flask(__name__)
CORS(app, supports_credentials=True)

@app.route("/__ping")
def ping():
    return "IDENTITY OK"

UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

DB = "identity.db"


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

def get_uid():
    return request.cookies.get("uid") or "demo_user"

# ---------------- DB INIT ----------------

def init_db():

    con = get_db()
    cur = con.cursor()

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

        created_at TEXT
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

    # ---------------- FACEBOOK PAGES ----------------

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

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source=?
        LIMIT 1
    """, (uid, source))

    row = cur.fetchone()
    con.close()

    return jsonify({
        "connected": True if row and row[0] == 1 else False
    })

# ---------------- GOOGLE OAUTH ----------------

@app.route("/google/connect")
def google_connect():

    uid = get_uid()
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

    row = cur.fetchone()
    con.close()

    if not row:
        return "Google App credentials not saved for this connector", 400

    client_id, client_secret = row

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
    uid = get_uid()

    # Fetch Google App Credentials from DB
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT client_id, client_secret
        FROM connector_configs
        WHERE uid=? AND connector=?
    """, (uid, source))

    row = cur.fetchone()

    if not row:
        con.close()
        return "Google App credentials not found", 400

    client_id, client_secret = row

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
            datetime.datetime.utcnow().isoformat()
        ))

        # Enable connector
        cur.execute("""
            INSERT OR REPLACE INTO google_connections
            (uid, source, enabled)
            VALUES (?, ?, 1)
        """, (uid, source))

        con.commit()

    finally:
        con.close()

    return redirect(
        f"http://localhost:3000/connectors/{source}"
    )

# ---------------- DRIVE ----------------

@app.route("/google/sync/drive")
def sync_drive():

    from connectors.google_drive import sync_drive_files

    return jsonify(sync_drive_files())

# ---------------- DRIVE SAVE APP CREDENTIALS ----------------

@app.route("/connectors/drive/save_app", methods=["POST"])
def drive_save_app():

    uid = get_uid()
    data = request.get_json()

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
        datetime.datetime.utcnow().isoformat()
    ))

    con.commit()
    con.close()

    return jsonify({"status": "saved"})

# ---------------- SHEETS ----------------

@app.route("/google/sync/sheets")
def sync_sheets():
    return jsonify(sync_sheets_files())

# ---------------- SHEETS SAVE APP ----------------

@app.route("/connectors/sheets/save_app", methods=["POST"])
def sheets_save_app():

    uid = get_uid()
    data = request.get_json()

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
        datetime.datetime.utcnow().isoformat()
    ))

    con.commit()
    con.close()

    return jsonify({"status": "saved"})

# ---------------- SHEETS DISCONNECT ----------------

@app.route("/google/disconnect/sheets")
def disconnect_sheets():

    uid = get_uid()

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

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='sheets'
    """, (uid,))

    row = cur.fetchone()
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

    uid = get_uid()
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
from connectors.google_ga4 import sync_ga4 as run_ga4_sync


@app.route("/google/sync/ga4")
def google_ga4_sync():

    try:
        result = sync_ga4()

        # Always return JSON
        return jsonify(result), 200

    except Exception as e:

        print("[GA4 SYNC ERROR]", str(e))

        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

# ---------------- GA4 SAVE APP ----------------

@app.route("/connectors/ga4/save_app", methods=["POST"])
def ga4_save_app():

    uid = get_uid()
    data = request.get_json()

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
        datetime.datetime.utcnow().isoformat()
    ))

    con.commit()
    con.close()

    return jsonify({"status": "saved"})

# ---------------- GA4 JOB GET ----------------

@app.route("/connectors/ga4/job/get")
def ga4_job_get():

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='ga4'
    """, (uid,))

    row = cur.fetchone()
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

    uid = get_uid()
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

    uid = get_uid()
    data = request.get_json()

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
        datetime.datetime.utcnow().isoformat()
    ))

    con.commit()
    con.close()

    return jsonify({"status": "saved"})

# ---------------- SEARCH CONSOLE JOB GET ----------------

@app.route("/connectors/search-console/job/get")
def gsc_job_get():

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='search-console'
    """, (uid,))

    row = cur.fetchone()
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

    uid = get_uid()
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

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='search-console'
    """, (uid,))

    row = cur.fetchone()
    con.close()

    return jsonify({
        "connected": True if row and row[0] == 1 else False
    })

@app.route("/google/disconnect/search-console")
def disconnect_search_console():

    uid = get_uid()

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

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    # ✅ ensure API key exists first
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

    uid = get_uid()

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

    uid = get_uid()
    data = request.get_json()

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

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    # ---------- connection ----------
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='pagespeed'
        LIMIT 1
    """, (uid,))
    row = cur.fetchone()

    connected = bool(row and row[0] == 1)

    # ---------- api key ----------
    cur.execute("""
        SELECT api_key
        FROM connector_configs
        WHERE uid=? AND connector='pagespeed'
        LIMIT 1
    """, (uid,))
    key_row = cur.fetchone()

    api_key_saved = key_row is not None

    con.close()

    return jsonify({
        "connected": connected,
        "api_key_saved": api_key_saved
    })

@app.route("/connectors/pagespeed/job/get")
def pagespeed_job_get():

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='pagespeed'
    """, (uid,))

    row = cur.fetchone()
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

    uid = get_uid()
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

    uid = get_uid()
    data = request.get_json()

    client_id = data.get("client_id")
    client_secret = data.get("client_secret")

    if not client_id or not client_secret:
        return jsonify({"error": "Client ID and Secret required"}), 400

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, client_id, client_secret, config_json, created_at)
        VALUES (?, 'forms', ?, ?, ?)
    """, (
        uid,
        client_id,
        client_secret,
        datetime.datetime.utcnow().isoformat()
    ))

    con.commit()
    con.close()

    return jsonify({"status": "saved"})

# ---------------- FORMS DISCONNECT ----------------

@app.route("/google/disconnect/forms")
def disconnect_forms():

    uid = get_uid()

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

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='forms'
    """, (uid,))

    row = cur.fetchone()
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

    uid = get_uid()
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

    from connectors.google_calendar import sync_calendar_files

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

# ---------------- CALENDAR SAVE APP ----------------

@app.route("/connectors/calendar/save_app", methods=["POST"])
def calendar_save_app():

    uid = get_uid()
    data = request.get_json()

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
        datetime.datetime.utcnow().isoformat()
    ))

    con.commit()
    con.close()

    return jsonify({"status": "saved"})

@app.route("/google/disconnect/gmail")
def google_disconnect_gmail():

    uid = request.cookies.get("uid") or "demo_user"
    source = "gmail"

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

    uid = get_uid()
    data = request.get_json()

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
        datetime.datetime.utcnow().isoformat()
    ))

    con.commit()
    con.close()

    return jsonify({"status": "saved"})

@app.route("/google/disconnect/drive")
def disconnect_drive():

    uid = get_uid()

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

    uid = get_uid()

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

    uid = get_uid()

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

    uid = get_uid()

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

    print("DISCONNECT CALLED:", source)

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

    uid = get_uid()
    data = request.get_json()

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
        datetime.datetime.utcnow().isoformat()
    ))

    con.commit()
    con.close()

    return jsonify({"status": "saved"})

@app.route("/google/disconnect/classroom")
def disconnect_classroom():

    uid = get_uid()

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

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='classroom'
    """, (uid,))

    row = cur.fetchone()
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

    uid = get_uid()
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

    uid = get_uid()
    data = request.get_json()

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
        datetime.datetime.utcnow().isoformat()
    ))

    con.commit()
    con.close()

    return jsonify({"status": "saved"})

# ---------------- TASKS JOB GET ----------------

@app.route("/connectors/tasks/job/get")
def tasks_job_get():

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='tasks'
    """, (uid,))

    row = cur.fetchone()
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

    uid = get_uid()
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
    print("[SERVER] Triggering contacts sync")
    return jsonify(sync_contacts())

# ---------------- CONTACTS SAVE APP ----------------

@app.route("/connectors/contacts/save_app", methods=["POST"])
def contacts_save_app():

    uid = get_uid()
    data = request.get_json()

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
        datetime.datetime.utcnow().isoformat()
    ))

    con.commit()
    con.close()

    return jsonify({"status": "saved"})

# ---------------- CONTACTS DISCONNECT ----------------

@app.route("/google/disconnect/contacts")
def disconnect_contacts():

    uid = get_uid()

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

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='contacts'
    """, (uid,))

    row = cur.fetchone()
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

    uid = get_uid()
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
        from connectors.google_gcs import sync_gcs

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

    uid = get_uid()

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

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='gcs'
    """, (uid,))

    row = cur.fetchone()
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

    uid = get_uid()
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

    uid = get_uid()
    data = request.get_json()

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
        datetime.datetime.utcnow().isoformat()
    ))

    con.commit()
    con.close()

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
    
from connectors.google_webfonts import sync_webfonts

@app.route("/connectors/webfonts/sync")
def webfonts_sync_route():

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='webfonts'
    """, (uid,))

    row = cur.fetchone()
    con.close()

    if not row or row[0] != 1:
        return jsonify({"error": "WebFonts not connected"}), 400

    result = sync_webfonts()

    return jsonify(result)

# ---------------- GOOGLE WEBFONTS CONNECT ----------------

@app.route("/connectors/webfonts/connect")
def webfonts_connect():

    uid = get_uid()

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

    uid = get_uid()

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

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    # ---------- connection ----------
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='webfonts'
        LIMIT 1
    """, (uid,))
    row = cur.fetchone()

    connected = bool(row and row[0] == 1)

    # ---------- API KEY ----------
    cur.execute("""
        SELECT api_key
        FROM connector_configs
        WHERE uid=? AND connector='webfonts'
        LIMIT 1
    """, (uid,))

    key_row = cur.fetchone()

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

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='webfonts'
    """, (uid,))

    row = cur.fetchone()
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

    uid = get_uid()
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

    uid = get_uid()
    data = request.get_json()

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

    uid = get_uid()
    data = request.get_json()

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
        datetime.datetime.utcnow().isoformat()
    ))

    con.commit()
    con.close()

    return jsonify({"status": "saved"})

@app.route("/google/disconnect/youtube")
def disconnect_youtube():

    uid = get_uid()

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

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='youtube'
    """, (uid,))

    row = cur.fetchone()
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

    uid = get_uid()
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

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT client_id, client_secret, config_json
        FROM connector_configs
        WHERE uid=? AND connector='reddit'
        LIMIT 1
    """, (uid,))

    row = cur.fetchone()

    if not row:
        return jsonify({"error":"Config missing"}),400

    client_id, client_secret, cfg = row
    cfg=json.loads(cfg)

    from connectors.reddit import connect_reddit

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

    uid = get_uid()

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

    uid = get_uid()
    data = request.json

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

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='reddit'
        LIMIT 1
    """, (uid,))

    row = cur.fetchone()
    con.close()

    if not row:
        return jsonify({})

    return jsonify({
        "sync_type": row[0],
        "schedule_time": row[1]
    })

@app.route("/connectors/reddit/job/save", methods=["POST"])
def reddit_job_save():

    uid = get_uid()
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

    uid = get_uid()

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

    row = cur.fetchone()

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
    from connectors.reddit import (
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
        SELECT dest_type, host, port, username, password, database_name
        FROM destination_configs
        WHERE uid=? AND source=? AND is_active=1
        LIMIT 1
    """, (uid, "reddit"))

    dest_row = cur.fetchone()
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

    from destinations.destination_router import push_to_destination

    pushed = 0

    if post_rows:
        pushed += push_to_destination(dest, "reddit_posts", post_rows)

    print(f"[REDDIT] Sync type: {sync_type}")
    print(f"[REDDIT] Posts fetched: {len(post_rows)}")
    print(f"[REDDIT] Rows pushed: {pushed}")

    return jsonify({
        "posts": len(post_rows),
        "rows_pushed": pushed,
        "sync_type": sync_type
    })

@app.route("/reddit/sync/profile")
def reddit_profile():

    uid = request.args.get("uid")

    from connectors.reddit import sync_profile

    return sync_profile(uid)

@app.route("/reddit/sync/posts")
def reddit_posts():

    uid = request.args.get("uid")
    q = request.args.get("q", "python")

    from connectors.reddit import sync_posts

    return sync_posts(uid, q)

@app.route("/reddit/sync/messages")
def reddit_messages():

    uid = request.args.get("uid")

    from connectors.reddit import sync_messages

    return sync_messages(uid)

# ---------------- TELEGRAM ----------------

@app.route("/connectors/telegram/sync")
def telegram_sync_universal():

    uid = get_uid()

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

    from connectors.telegram import sync_messages

    res = sync_messages(uid, sync_type)

    total_messages = res["messages"]
    rows = res["rows"]

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT dest_type, host, port, username, password, database_name
        FROM destination_configs
        WHERE uid=? AND source='telegram' AND is_active=1
        LIMIT 1
    """, (uid,))

    dest_row = cur.fetchone()
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

    from destinations.destination_router import push_to_destination

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

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    # read saved token
    cur.execute("""
        SELECT api_key
        FROM connector_configs
        WHERE uid=? AND connector='telegram'
        LIMIT 1
    """,(uid,))

    row = cur.fetchone()

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
        datetime.datetime.utcnow().isoformat()
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

    uid = get_uid()

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

    uid = get_uid()

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

    uid = get_uid()
    data = request.json

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

    uid = request.cookies.get("uid") or "demo_user"
    username = request.args.get("username")

    from connectors.medium import sync_user

    return jsonify(sync_user(uid, username))

@app.route("/connectors/medium/connect")
def medium_connect():

    uid = get_uid()

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

    uid = get_uid()
    data = request.get_json()

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

    row = cur.fetchone()

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

    uid = get_uid()

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

    uid = get_uid()

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

    uid = get_uid()

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

    from connectors.medium import sync_medium

    res = sync_medium(uid, sync_type)

    total = res["posts"]
    rows = res["rows"]

    # Destination
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT dest_type, host, port, username, password, database_name
        FROM destination_configs
        WHERE uid=? AND source='medium' AND is_active=1
        LIMIT 1
    """, (uid,))

    dest_row = cur.fetchone()
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

    from destinations.destination_router import push_to_destination

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

    from connectors.tumblr import sync_blog

    return jsonify(sync_blog(uid, blog))


@app.route("/tumblr/sync/posts")
def tumblr_sync_posts():

    uid = request.cookies.get("uid")
    blog = request.args.get("blog")

    from connectors.tumblr import sync_posts

    return jsonify(sync_posts(uid, blog))

# ---------------- TWITCH ----------------

@app.route("/connectors/twitch/connect", methods=["POST"])
def twitch_connect():

    uid = get_uid()
    data = request.get_json()

    if not data:
        return jsonify({"status": "error", "message": "No data provided"}), 400

    username = data.get("username")

    if not username:
        return jsonify({"status": "error", "message": "Username required"}), 400

    con = get_db()
    cur = con.cursor()

    now = datetime.datetime.utcnow().isoformat()

    # Save username inside connector_state
    cur.execute("""
        INSERT OR REPLACE INTO connector_state
        (uid, source, state_json, updated_at)
        VALUES (?, 'twitch', ?, ?)
    """, (
        uid,
        json.dumps({"username": username}),
        now
    ))

    # Enable connector
    cur.execute("""
        INSERT OR REPLACE INTO google_connections
        (uid, source, enabled)
        VALUES (?, 'twitch', 1)
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "connected"})

@app.route("/connectors/twitch/disconnect")
def twitch_disconnect():

    uid = get_uid()

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

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    # Check if enabled
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='twitch'
    """, (uid,))
    row = cur.fetchone()

    if not row or row[0] != 1:
        con.close()
        return jsonify({"error": "Twitch not connected"}), 400

    # Get username from connector_state
    cur.execute("""
        SELECT state_json
        FROM connector_state
        WHERE uid=? AND source='twitch'
    """, (uid,))
    row = cur.fetchone()

    if not row:
        con.close()
        return jsonify({"error": "Username missing"}), 400

    state = json.loads(row[0])
    username = state.get("username")

    if not username:
        con.close()
        return jsonify({"error": "Username missing"}), 400

    # Get sync type from job
    sync_type = get_connector_sync_type(uid, "twitch")

    con.close()

    # Run connector
    from connectors.twitch import sync_videos

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

    row = cur.fetchone()
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

#------------------- Tumblr --------------------------

@app.route("/connectors/tumblr/save_config", methods=["POST"])
def tumblr_save_config():

    uid = get_uid()
    data = request.get_json()

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

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    # check config exists
    cur.execute("""
        SELECT api_key
        FROM connector_configs
        WHERE uid=? AND connector='tumblr'
    """,(uid,))

    row = cur.fetchone()

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

    from connectors.tumblr import sync_posts

    result=sync_posts(uid,sync_type)

    rows=result.get("rows",[])

    return jsonify({
        "posts":len(rows),
        "sync_type":sync_type
    })

@app.route("/api/status/tumblr")
def tumblr_status():

    uid = get_uid()

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

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT api_key
        FROM connector_configs
        WHERE uid=? AND connector='discord'
        LIMIT 1
    """, (uid,))

    row = cur.fetchone()

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
        datetime.datetime.utcnow().isoformat()
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

    uid = get_uid()

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

    uid = get_uid()
    print("SYNC UID:", uid)

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='discord'
    """, (uid,))
    row = cur.fetchone()

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

    from connectors.discord import sync_guilds, sync_channels, sync_messages

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
                print("Channel error:", channel_id, str(e))
                continue

        if total_messages >= max_global_messages:
            break

    cur.execute("""
        SELECT dest_type, host, port, username, password, database_name
        FROM destination_configs
        WHERE uid=? AND source=? AND is_active=1
        LIMIT 1
    """, (uid, "discord"))

    dest_row = cur.fetchone()
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

    from destinations.destination_router import push_to_destination

    pushed = 0

    if all_rows:
        pushed = push_to_destination(dest, "discord_messages", all_rows)

    print(f"[DISCORD] Sync type: {sync_type}")
    print(f"[DISCORD] Messages inserted: {total_messages}")
    print(f"[DISCORD] Rows pushed: {pushed}")

    return jsonify({
        "messages": total_messages,
        "rows_pushed": pushed,
        "sync_type": sync_type
    })

@app.route("/api/status/discord")
def discord_status():

    uid = get_uid()

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

    uid = get_uid()
    data = request.json

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

from connectors.googlebooks import sync_books


@app.route("/connectors/books/connect")
def connect_books():

    uid = get_uid()

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

    uid = get_uid()

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

    uid = get_uid()
    query = request.args.get("query")
    sync_type = request.args.get("sync_type", "incremental")

    if not query:
        return jsonify({"status": "error", "message": "query required"})

    return jsonify(sync_books(query, sync_type))


@app.route("/connectors/books/job/get")
def get_books_job():

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='books'
    """, (uid,))

    row = cur.fetchone()
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

    uid = get_uid()
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

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='books'
        LIMIT 1
    """, (uid,))

    row = cur.fetchone()
    con.close()

    return jsonify({
        "connected": bool(row and row[0] == 1)
    })

# ---------------- GOOGLE FACT CHECK ----------------

from connectors.googlefactcheck import sync_factcheck

@app.route("/connectors/factcheck/sync")
def factcheck_sync():

    uid = get_uid()
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

    uid = get_uid()
    data = request.get_json()

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

    uid = get_uid()

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

    uid = get_uid()

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

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    # connection
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='factcheck'
        LIMIT 1
    """, (uid,))
    row = cur.fetchone()

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

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='factcheck'
        LIMIT 1
    """, (uid,))

    row = cur.fetchone()
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

    uid = get_uid()
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

    uid = get_uid()
    query = request.args.get("q")
    limit = int(request.args.get("limit", 100))

    from connectors.googlenews import sync_articles

    return jsonify(sync_articles(uid, query, limit))

from connectors.googlenews import sync_news

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

    uid = get_uid()

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

    uid = get_uid()

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

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='news'
        LIMIT 1
    """, (uid,))

    row = cur.fetchone()
    con.close()

    return jsonify({
        "connected": bool(row and row[0] == 1)
    })

# ---------------- GOOGLE NEWS JOB GET ----------------

@app.route("/connectors/news/job/get")
def news_job_get():

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='news'
    """, (uid,))

    row = cur.fetchone()
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

    uid = get_uid()
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

    uid = get_uid()
    keyword = request.args.get("q")

    from connectors.googletrends import sync_interest

    return jsonify(sync_interest(uid, keyword))


@app.route("/googletrends/sync/related")
def googletrends_sync_related():

    uid = get_uid()
    keyword = request.args.get("q")

    from connectors.googletrends import sync_related

    return jsonify(sync_related(uid, keyword))

from connectors.googletrends import sync_trends


@app.route("/connectors/trends/sync", methods=["GET"])
def trends_sync():

    uid = get_uid()
    keyword = request.args.get("keyword")
    sync_type = request.args.get("sync_type", "incremental")

    if not keyword:
        return jsonify({"status": "error", "message": "Keyword required"})

    print("CALLING SYNC TRENDS NOW")
    result = sync_trends(uid, keyword, sync_type)

    return jsonify(result)

@app.route("/connectors/trends/connect")
def trends_connect():

    uid = get_uid()

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

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='trends'
        LIMIT 1
    """, (uid,))

    row = cur.fetchone()
    con.close()

    return jsonify({
        "connected": bool(row and row[0] == 1)
    })

# ---------------- TRENDS DISCONNECT ----------------

@app.route("/google/disconnect/trends")
def disconnect_trends():

    uid = get_uid()

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

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='trends'
    """, (uid,))

    row = cur.fetchone()
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

    uid = get_uid()
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

    uid = get_uid()

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

    uid = get_uid()

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

    row = cur.fetchone()
    con.close()

    return jsonify({
        "has_credentials": has_credentials,
        "connected": bool(row and row[0] == 1)
    })

@app.route("/connectors/devto/disconnect")
def devto_disconnect():

    uid = get_uid()

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

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='devto'
    """, (uid,))

    row = cur.fetchone()
    con.close()

    if not row:
        return jsonify({})

    return jsonify({
        "sync_type": row[0],
        "schedule_time": row[1]
    })


@app.route("/connectors/devto/job/save", methods=["POST"])
def devto_job_save():

    uid = get_uid()
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

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    # ---- Check connection ----
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='devto'
    """, (uid,))

    row = cur.fetchone()

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
    from connectors.devto import sync_articles, sync_tags

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
            database_name
        FROM destination_configs
        WHERE uid=? AND source=? AND is_active=1
        LIMIT 1
    """, (uid, "devto"))

    dest_row = cur.fetchone()
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
    from destinations.destination_router import push_to_destination

    pushed = 0

    if article_rows:
        pushed += push_to_destination(dest, "devto_articles", article_rows)

    if tag_rows:
        pushed += push_to_destination(dest, "devto_tags", tag_rows)

    print(f"[DEVTO] Sync type: {sync_type}")
    print(f"[DEVTO] Articles found: {len(article_rows)}")
    print(f"[DEVTO] Tags found: {len(tag_rows)}")
    print(f"[DEVTO] Rows pushed: {pushed}")

    return jsonify({
        "articles": len(article_rows),
        "tags": len(tag_rows),
        "rows_pushed": pushed,
        "sync_type": sync_type
    })

# ---------------- GITHUB ----------------

from connectors.github import (
    get_auth_url,
    exchange_code,
    save_token,
    sync_github,
    disable_connection
)

@app.route("/github/connect")
def github_connect():
    uid = get_uid()
    return redirect(get_auth_url(uid))

@app.route("/github/callback")
def github_callback():

    code = request.args.get("code")
    if not code:
        return "Authorization failed", 400

    uid = get_uid()

    token = exchange_code(uid, code)

    if not token.get("access_token"):
        return "Token exchange failed", 400

    save_token(uid, token)

    return redirect("http://localhost:3000/connectors/github")


@app.route("/connectors/github/disconnect")
def github_disconnect():

    uid = get_uid()

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
    uid = get_uid()
    return jsonify(sync_github(uid))

@app.route("/connectors/github/save_app", methods=["POST"])
def github_save_app():

    uid = get_uid()
    data = request.get_json()

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

    return jsonify({"status": "saved"})

@app.route("/api/status/github")
def github_status():

    uid = request.cookies.get("uid") or "demo_user"

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
    row = cur.fetchone()

    conn.close()

    return jsonify({
        "has_credentials": bool(creds),
        "connected": bool(row and row[0] == 1)
    })

@app.route("/connectors/github/job/get")
def github_job_get():

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='github'
    """, (uid,))

    row = cur.fetchone()
    con.close()

    if not row:
        return jsonify({"exists": False})

    return jsonify({
        "exists": True,
        "sync_type": row[0],
        "schedule_time": row[1]
    })

@app.route("/connectors/github/job/save", methods=["POST"])
def github_job_save():

    uid = get_uid()
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

# ---------------- GITLAB ----------------

@app.route("/gitlab/connect")
def gitlab_connect():

    from connectors.gitlab import get_auth_url

    uid = get_uid()
    return redirect(get_auth_url(uid))

# ---------------- GITLAB SAVE CONFIG ----------------

@app.route("/connectors/gitlab/save_app", methods=["POST"])
def gitlab_save_app():

    uid = get_uid()
    data = request.get_json()

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

    return jsonify({"status": "saved"})

@app.route("/connectors/gitlab/disconnect")
def gitlab_disconnect():

    uid = get_uid()

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

    uid = get_uid()
    code = request.args.get("code")

    from connectors.gitlab import exchange_code, save_token

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

    uid = get_uid()

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
    row = cur.fetchone()

    con.close()

    return jsonify({
        "has_credentials": bool(creds),
        "connected": bool(row and row[0] == 1)
    })

@app.route("/connectors/gitlab/job/get")
def gitlab_job_get():

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='gitlab'
    """, (uid,))

    row = cur.fetchone()
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

    uid = get_uid()
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

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='gitlab'
    """, (uid,))

    row = cur.fetchone()

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

    from connectors.gitlab import (
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

    from destinations.destination_router import push_to_destination

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT dest_type, host, port, username, password, database_name
        FROM destination_configs
        WHERE uid=? AND source='gitlab' AND is_active=1
        LIMIT 1
    """, (uid,))

    dest_row = cur.fetchone()
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

    print(f"[GITLAB] Sync type: {sync_type}")
    print(f"[GITLAB] New rows found: {len(new_rows)}")
    print(f"[GITLAB] Rows pushed: {pushed}")

    return jsonify({
        "projects": len(project_ids),
        "rows_pushed": pushed,
        "rows_found": len(new_rows),
        "sync_type": sync_type
    })

# ---------------- STACKOVERFLOW ----------------

@app.route("/connectors/stackoverflow/connect")
def stackoverflow_connect():

    uid = get_uid()

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

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    # connection
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='stackoverflow'
        LIMIT 1
    """, (uid,))
    row = cur.fetchone()

    connected = bool(row and row[0] == 1)

    # api key
    cur.execute("""
        SELECT api_key
        FROM connector_configs
        WHERE uid=? AND connector='stackoverflow'
        LIMIT 1
    """, (uid,))

    key_row = cur.fetchone()

    api_key_saved = bool(key_row and key_row[0])

    con.close()

    return jsonify({
        "has_credentials": api_key_saved,
        "connected": connected
    })

@app.route("/connectors/stackoverflow/disconnect")
def stackoverflow_disconnect():

    uid = get_uid()

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

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='stackoverflow'
        LIMIT 1
    """, (uid,))

    row = cur.fetchone()
    con.close()

    if not row:
        return jsonify({})

    return jsonify({
        "sync_type": row[0],
        "schedule_time": row[1]
    })


@app.route("/connectors/stackoverflow/job/save", methods=["POST"])
def stackoverflow_job_save():

    uid = get_uid()
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

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    # Check connection
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='stackoverflow'
    """, (uid,))

    row = cur.fetchone()

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

    from connectors.stackoverflow import (
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
        SELECT dest_type, host, port, username, password, database_name
        FROM destination_configs
        WHERE uid=? AND source=? AND is_active=1
        LIMIT 1
    """, (uid, "stackoverflow"))

    dest_row = cur.fetchone()
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

    from destinations.destination_router import push_to_destination

    pushed = 0

    if q_rows:
        pushed += push_to_destination(dest, "stack_questions", q_rows)

    if a_rows:
        pushed += push_to_destination(dest, "stack_answers", a_rows)

    if u_rows:
        pushed += push_to_destination(dest, "stack_users", u_rows)

    print(f"[STACK] Sync type: {sync_type}")
    print(f"[STACK] Questions: {len(q_rows)}")
    print(f"[STACK] Answers: {len(a_rows)}")
    print(f"[STACK] Users: {len(u_rows)}")
    print(f"[STACK] Rows pushed: {pushed}")

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

    uid = get_uid()
    data = request.get_json()

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

    uid = get_uid()

    result = sync_hackernews(uid)

    return jsonify(result)

@app.route("/connectors/hackernews/sync")
def hackernews_sync_universal():

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    # Check connection
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='hackernews'
    """, (uid,))

    row = cur.fetchone()

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

    from connectors.hackernews import sync_hackernews

    data = sync_hackernews(uid, sync_type)

    rows = data.get("rows", [])

    # Destination
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT dest_type, host, port, username, password, database_name
        FROM destination_configs
        WHERE uid=? AND source=? AND is_active=1
        LIMIT 1
    """, (uid, "hackernews"))

    dest_row = cur.fetchone()
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

    from destinations.destination_router import push_to_destination

    pushed = 0

    if rows:
        pushed = push_to_destination(dest, "hackernews_stories", rows)

    print(f"[HN] Sync type: {sync_type}")
    print(f"[HN] Stories: {len(rows)}")
    print(f"[HN] Rows pushed: {pushed}")

    return jsonify({
        "stories": len(rows),
        "rows_pushed": pushed,
        "sync_type": sync_type
    })


@app.route("/connectors/hackernews/connect")
def hackernews_connect():

    uid = get_uid()

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

    uid = get_uid()

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

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='hackernews'
        LIMIT 1
    """, (uid,))

    row = cur.fetchone()
    con.close()

    if not row:
        return jsonify({})

    return jsonify({
        "sync_type": row[0],
        "schedule_time": row[1]
    })

@app.route("/connectors/hackernews/job/save", methods=["POST"])
def hackernews_job_save():

    uid = get_uid()
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

@app.route("/connectors/producthunt/connect", methods=["POST"])
def producthunt_connect():

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO google_connections
        (uid, source, enabled)
        VALUES (?, 'producthunt', 1)
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "connected"})

@app.route("/connectors/producthunt/disconnect")
def producthunt_disconnect():

    uid = get_uid()

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

@app.route("/connectors/producthunt/sync")
def producthunt_sync():

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    # Check enabled
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='producthunt'
    """, (uid,))

    row = cur.fetchone()

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

    uid = get_uid()

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

    uid = get_uid()

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

@app.route("/connectors/wikipedia/connect", methods=["POST"])
def wikipedia_connect():

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO google_connections
        (uid, source, enabled)
        VALUES (?, 'wikipedia', 1)
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "connected"})

@app.route("/connectors/wikipedia/disconnect")
def wikipedia_disconnect():

    uid = get_uid()

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

@app.route("/connectors/wikipedia/sync")
def wikipedia_sync():

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    # Check enabled
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='wikipedia'
    """, (uid,))

    row = cur.fetchone()

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

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='peertube'
    """, (uid,))
    row = cur.fetchone()

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

    row = cur.fetchone()
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

@app.route("/connectors/peertube/connect", methods=["POST"])
def peertube_connect():

    uid = get_uid()
    data = request.get_json()

    if not data:
        return jsonify({"status": "error", "message": "No data provided"}), 400

    instance = data.get("instance")

    if not instance:
        return jsonify({"status": "error", "message": "Instance required"}), 400

    instance = instance.strip().rstrip("/")

    con = get_db()
    cur = con.cursor()

    now = datetime.datetime.utcnow().isoformat()

    # Save instance inside connector_state
    cur.execute("""
        INSERT OR REPLACE INTO connector_state
        (uid, source, state_json, updated_at)
        VALUES (?, 'peertube', ?, ?)
    """, (
        uid,
        json.dumps({"instance": instance}),
        now
    ))

    # Enable connector
    cur.execute("""
        INSERT OR REPLACE INTO google_connections
        (uid, source, enabled)
        VALUES (?, 'peertube', 1)
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "connected"})

@app.route("/connectors/peertube/disconnect")
def peertube_disconnect():

    uid = get_uid()

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

# ---------------- MASTODON ----------------

@app.route("/connectors/mastodon/sync")
def mastodon_sync_universal():

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    # Check enabled
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='mastodon'
    """, (uid,))
    row = cur.fetchone()

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
    state_row = cur.fetchone()

    instance = "https://mastodon.social"

    if state_row:
        state = json.loads(state_row[0])
        instance = state.get("instance", instance)

    con.close()

    from connectors.mastodon import sync_mastodon

    result = sync_mastodon(uid, instance, sync_type)

    rows = result.get("rows", [])

    # Destination lookup
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT dest_type, host, port, username, password, database_name
        FROM destination_configs
        WHERE uid=? AND source=? AND is_active=1
        LIMIT 1
    """, (uid, "mastodon"))

    dest_row = cur.fetchone()
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

        from destinations.destination_router import push_to_destination
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
        datetime.datetime.utcnow().isoformat()
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

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    # connection
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='mastodon'
        LIMIT 1
    """,(uid,))
    row = cur.fetchone()

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

    uid = get_uid()

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

    uid = get_uid()
    data = request.json or {}

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

@app.route("/connectors/discourse/connect", methods=["POST"])
def discourse_connect():

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO google_connections
        (uid, source, enabled)
        VALUES (?, 'discourse', 1)
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "connected"})

@app.route("/connectors/discourse/disconnect")
def discourse_disconnect():

    uid = get_uid()

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

@app.route("/connectors/discourse/sync")
def discourse_sync():

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    # Check enabled
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='discourse'
    """, (uid,))

    row = cur.fetchone()

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

    uid = get_uid()

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

    rows = cur.fetchall()

    con.close()

    return jsonify([dict(r) for r in rows])


@app.route("/discourse/data/categories")
def discourse_categories():

    uid = get_uid()

    con = sqlite3.connect("identity.db")
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
        SELECT *
        FROM discourse_categories
        WHERE uid=?
    """, (uid,))

    rows = cur.fetchall()

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
        datetime.datetime.utcnow().isoformat()
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

    uid = get_uid()

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

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    # Check enabled
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='lemmy'
    """, (uid,))
    row = cur.fetchone()

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

    from connectors.lemmy import sync_lemmy

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
        SELECT dest_type, host, port, username, password, database_name
        FROM destination_configs
        WHERE uid=? AND source='lemmy' AND is_active=1
        LIMIT 1
    """, (uid,))

    dest_row = cur.fetchone()
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

    from destinations.destination_router import push_to_destination

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

    uid = get_uid()
    data = request.json or {}

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

@app.route("/connectors/openstreetmap/connect", methods=["POST"])
def osm_connect():

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    now = datetime.datetime.utcnow().isoformat()

    # Mark connector enabled
    cur.execute("""
        INSERT OR REPLACE INTO google_connections
        (uid, source, enabled)
        VALUES (?, 'openstreetmap', 1)
    """, (uid,))

    con.commit()
    con.close()

    return jsonify({"status": "connected"})

@app.route("/connectors/openstreetmap/disconnect")
def osm_disconnect():

    uid = get_uid()

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

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    # Check enabled
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='openstreetmap'
    """, (uid,))

    row = cur.fetchone()

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

# ---------------- NVD ----------------

@app.route("/connectors/nvd/sync")
def nvd_sync_universal():

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    # Check connection
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='nvd'
    """, (uid,))

    row = cur.fetchone()

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

    from connectors.nvd import sync_nvd

    result = sync_nvd(uid, sync_type)

    rows = result.get("rows", [])

    # -------- Destination ----------
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT dest_type, host, port, username, password, database_name
        FROM destination_configs
        WHERE uid=? AND source=? AND is_active=1
        LIMIT 1
    """, (uid, "nvd"))

    dest_row = cur.fetchone()
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

    from destinations.destination_router import push_to_destination

    pushed = 0

    if rows:
        pushed += push_to_destination(dest, "nvd_cves", rows)

    print(f"[NVD] Sync type: {sync_type}")
    print(f"[NVD] CVEs: {len(rows)}")
    print(f"[NVD] Rows pushed: {pushed}")

    return jsonify({
        "cves": len(rows),
        "rows_pushed": pushed,
        "sync_type": sync_type
    })

@app.route("/connectors/nvd/connect")
def nvd_connect():

    uid = get_uid()

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

    uid = get_uid()

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

    uid = get_uid()

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
    row = cur.fetchone()

    con.close()

    return jsonify({
        "has_credentials": bool(creds and creds[0]),
        "connected": bool(row and row[0] == 1)
    })

@app.route("/connectors/nvd/job/save", methods=["POST"])
def nvd_job_save():

    uid = get_uid()
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

    uid = get_uid()
    data = request.get_json()

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

    auth_url = pinterest_get_auth_url() # this uses CLIENT_ID + REDIRECT_URI

    return redirect(auth_url)

@app.route("/connectors/pinterest/disconnect")
def pinterest_disconnect():

    uid = get_uid()

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

    print("PINTEREST CALLBACK HIT")

    uid = get_uid()
    code = request.args.get("code")

    if not code:
        return "Authorization failed", 400

    token_data = pinterest_exchange_code(code)

    if not token_data:
        return "Token exchange failed", 400

    access_token = token_data.get("access_token")
    refresh_token = token_data.get("refresh_token")
    expires_in = token_data.get("expires_in")

    # Save token
    pinterest_save_token(uid, access_token, refresh_token, expires_in)

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO google_connections
        (uid, source, enabled)
        VALUES (?, 'pinterest', 1)
    """, (uid,))

    con.commit()
    con.close()

    # IMPORTANT: redirect to UI (3000)
    return redirect("http://localhost:3000/connectors/pinterest")

@app.route("/connectors/pinterest/sync")
def pinterest_sync_universal():

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    # Check enabled
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='pinterest'
    """, (uid,))
    row = cur.fetchone()

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
        SELECT dest_type, host, port, username, password, database_name
        FROM destination_configs
        WHERE uid=? AND source='pinterest' AND is_active=1
        LIMIT 1
    """, (uid,))

    dest_row = cur.fetchone()
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

    from destinations.destination_router import push_to_destination

    pushed = 0
    if rows:
        pushed = push_to_destination(dest, "pinterest_pins_data", rows)

    return jsonify({
        "pins": result.get("pins", 0),
        "rows_pushed": pushed
    })

# ---------------- FACEBOOK PAGES SAVE APP CREDENTIALS ----------------

@app.route("/connectors/facebook/save_app", methods=["POST"])
def facebook_save_app():

    uid = get_uid()
    print("SAVE UID:", uid)
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
        datetime.datetime.utcnow().isoformat()
    ))

    con.commit()
    con.close()

    return jsonify({"status": "saved"})

#-------------- Temporary route to test saving Facebook credentials without going through the UI --------------
@app.route("/connectors/facebook/test_save", methods=["GET"])
def facebook_test_save():

    uid = get_uid()

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
        datetime.datetime.utcnow().isoformat()
    ))

    con.commit()
    con.close()

    return "Test credentials saved"

# ---------------- FACEBOOK PAGES CONNECT ----------------

@app.route("/connectors/facebook/connect", methods=["GET"])
def facebook_connect():
    print("CONNECT UID:", uid)
    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT app_id, redirect_uri
        FROM facebook_app_credentials
        WHERE uid=?
    """, (uid,))

    row = cur.fetchone()
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

    uid = get_uid()
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

    row = cur.fetchone()

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
        datetime.datetime.utcnow().isoformat()
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

    uid = get_uid()

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

    uid = get_uid()

    # Check enabled
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='facebook'
    """, (uid,))

    row = cur.fetchone()
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
        pass

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

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='facebook'
    """, (uid,))

    row = cur.fetchone()
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

    uid = get_uid()

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

    print("DISCONNECT CALLED:", source)

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

    uid = get_uid()
    print("ADS CONNECT UID:", uid)

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT app_id, redirect_uri
        FROM facebook_app_credentials
        WHERE uid=?
    """, (uid,))

    row = cur.fetchone()
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

    uid = get_uid()
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

    row = cur.fetchone()

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
        datetime.datetime.utcnow().isoformat()
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

    uid = get_uid()

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

    uid = get_uid()

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

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time
        FROM connector_jobs
        WHERE uid=? AND source='facebook_ads'
    """, (uid,))

    row = cur.fetchone()
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

    uid = get_uid()
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

from connectors.facebook_ads import sync_facebook_ads

@app.route("/connectors/facebook_ads/sync")
def facebook_ads_sync():

    uid = get_uid()

    # Check enabled
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='facebook_ads'
    """, (uid,))

    row = cur.fetchone()
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
        pass

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

    uid = get_uid()
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
        datetime.datetime.utcnow().isoformat()
    ))

    con.commit()
    con.close()

    return jsonify({"status": "saved"})

@app.route("/connectors/<source>/connect")
def connector_connect(source):

    uid = get_uid()

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO google_connections (uid, source, enabled)
        VALUES (?, ?, 1)
    """, (uid, source))

    con.commit()
    con.close()

    return jsonify({"status": "connected"})

@app.route("/connectors/<source>/disconnect")
def disconnect_connector(source):

    uid = get_uid()

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

    rows = cur.fetchall()

    domains = [r[0] for r in rows]

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

    uid = request.cookies.get("uid") or "demo_user"

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

    uid = request.cookies.get("uid") or "demo_user"

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type, schedule_time, enabled
        FROM connector_jobs
        WHERE uid=? AND source=?
        LIMIT 1
    """, (uid, source))

    row = cur.fetchone()

    con.close()


    if not row:
        return jsonify({"exists": False})


    return jsonify({
        "exists": True,
        "sync_type": row[0],
        "schedule_time": row[1],
        "enabled": row[2]
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

    row = cur.fetchone()
    con.close()

    if not row:
        return "historical"

    return row[0]

@app.route("/destination/save", methods=["POST"])
def save_destination():

    data = request.json

    uid = data.get("uid", "demo_user")
    source = data.get("source")

    dest_type = data.get("type")

    host = data.get("host")
    port = data.get("port")

    username = data.get("username")
    password = data.get("password")

    database = data.get("database")

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


    # ---------- BigQuery Validation ----------

    if dest_type == "bigquery":

        if not host or not password or not database:
            return jsonify({
                "status": "error",
                "msg": "Missing BigQuery credentials"
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
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            datetime.datetime.utcnow().isoformat()
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

    uid = request.args.get("uid", "demo_user")

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
            created_at
        FROM destination_configs
        WHERE uid=? AND source=?
        ORDER BY created_at DESC
    """, (uid, source))

    rows = cur.fetchall()
    con.close()

    result = []

    for r in rows:
        result.append({
            "id": r[0],
            "type": r[1],
            "host": r[2],
            "port": r[3],
            "username": r[4],
            "database": r[5],
            "active": bool(r[6]),
            "created_at": r[7]
        })

    return jsonify({
        "status": "ok",
        "destinations": result
    })

@app.route("/destination/activate", methods=["POST"])
def activate_destination():

    data = request.get_json()

    uid = data.get("uid", "demo_user")
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

    uid = request.cookies.get("uid") or "demo_user"

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

    row = cur.fetchone()
    con.close()

    if not row:
        return None

    return json.loads(row[0])



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
               username, password, database_name
        FROM destination_configs
        WHERE uid=? AND source=?
        ORDER BY id DESC
        LIMIT 1
    """, (uid, source))

    row = cur.fetchone()
    con.close()

    if not row:
        return None


    return {
        "type": row[0],
        "host": row[1],
        "port": row[2],
        "username": row[3],
        "password": row[4],
        "database_name": row[5]
    }

def get_active_destination(uid, source):

    con = get_db()
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
        SELECT *
        FROM destination_configs
        WHERE uid=? AND source=? AND is_active=1
        LIMIT 1
    """, (uid, source))

    row = cur.fetchone()
    con.close()

    if not row:
        return None

    return dict(row)

# ---------- GA4 STATUS (FINAL) ----------

@app.route("/api/status/ga4")
def ga4_status():

    uid = request.cookies.get("uid") or "demo_user"

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='ga4'
        LIMIT 1
    """, (uid,))

    row = cur.fetchone()
    con.close()

    return jsonify({
        "connected": bool(row and row[0] == 1)
    })

# ---------------- RUN ----------------

if __name__=="__main__":

    app.run(port=4000,debug=True,host="0.0.0.0",use_reloader=False)        