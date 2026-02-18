import sqlite3
import json
import datetime
import time

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from destinations.destination_router import push_to_destination


DB = "identity.db"
SOURCE = "youtube"


# ---------------- DB ---------------- #

def get_db():
    con = sqlite3.connect(
        DB,
        timeout=60,
        isolation_level=None,
        check_same_thread=False
    )
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con


# ---------------- STATE ---------------- #

def get_state(uid):
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT state_json
        FROM connector_state
        WHERE uid=? AND source=?
    """, (uid, SOURCE))

    row = cur.fetchone()
    con.close()

    return json.loads(row[0]) if row else None


def save_state(uid, state):
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_state
        (uid, source, state_json, updated_at)
        VALUES (?, ?, ?, ?)
    """, (
        uid,
        SOURCE,
        json.dumps(state),
        datetime.datetime.utcnow().isoformat()
    ))

    con.commit()
    con.close()


# ---------------- DESTINATION ---------------- #

def get_active_destination(uid):
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT dest_type, host, port, username, password, database_name
        FROM destination_configs
        WHERE uid=? AND source=?
        ORDER BY id DESC
        LIMIT 1
    """, (uid, SOURCE))

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


# ---------------- AUTH ---------------- #

# ---------------- AUTH ---------------- #

def get_creds():

    con = get_db()
    cur = con.cursor()

    # Get enabled connection
    cur.execute("""
        SELECT uid
        FROM google_connections
        WHERE source=? AND enabled=1
        LIMIT 1
    """, (SOURCE,))

    row = cur.fetchone()

    if not row:
        con.close()
        return None, None

    uid = row[0]

    # Get token
    cur.execute("""
        SELECT access_token, refresh_token, scopes
        FROM google_accounts
        WHERE uid=? AND source=?
        ORDER BY id DESC
        LIMIT 1
    """, (uid, SOURCE))

    token_row = cur.fetchone()

    if not token_row:
        con.close()
        return None, None

    access_token, refresh_token, scopes = token_row

    # Get client credentials
    cur.execute("""
        SELECT client_id, client_secret
        FROM connector_configs
        WHERE uid=? AND connector=?
        LIMIT 1
    """, (uid, SOURCE))

    cfg = cur.fetchone()
    con.close()

    if not cfg:
        return None, None

    client_id, client_secret = cfg

    creds = Credentials(
        token=access_token,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=scopes.split(",")
    )

    return uid, creds

# ---------------- FETCH ---------------- #

def fetch_youtube_data(service, since=None):

    channels = service.channels().list(
        part="snippet,statistics,contentDetails",
        mine=True
    ).execute()

    items = channels.get("items", [])
    if not items:
        return [], []

    channel = items[0]
    channel_id = channel["id"]

    channel_row = {
        "channel_id": channel_id,
        "title": channel.get("snippet", {}).get("title"),
        "description": channel.get("snippet", {}).get("description"),
        "subscribers": int(channel.get("statistics", {}).get("subscriberCount", 0)),
        "views": int(channel.get("statistics", {}).get("viewCount", 0)),
        "videos": int(channel.get("statistics", {}).get("videoCount", 0)),
        "raw_json": json.dumps(channel),
        "fetched_at": datetime.datetime.utcnow().isoformat()
    }

    uploads = (
        channel
        .get("contentDetails", {})
        .get("relatedPlaylists", {})
        .get("uploads")
    )

    if not uploads:
        return [channel_row], []

    video_rows = []
    next_page = None

    while True:
        playlist = service.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=uploads,
            maxResults=50,
            pageToken=next_page
        ).execute()

        for item in playlist.get("items", []):
            video_id = item["contentDetails"]["videoId"]
            published = item["snippet"].get("publishedAt")

            if since and published:
                published_date = datetime.datetime.fromisoformat(
                    published.replace("Z", "+00:00")
                )
                if published_date <= since:
                    continue

            video = service.videos().list(
                part="snippet,statistics",
                id=video_id
            ).execute()

            if not video.get("items"):
                continue

            v = video["items"][0]

            video_rows.append({
                "channel_id": channel_id,
                "video_id": video_id,
                "title": v.get("snippet", {}).get("title"),
                "description": v.get("snippet", {}).get("description"),
                "published_at": published,
                "views": int(v.get("statistics", {}).get("viewCount", 0)),
                "likes": int(v.get("statistics", {}).get("likeCount", 0)),
                "comments": int(v.get("statistics", {}).get("commentCount", 0)),
                "raw_json": json.dumps(v),
                "fetched_at": datetime.datetime.utcnow().isoformat()
            })

        next_page = playlist.get("nextPageToken")
        if not next_page:
            break

        time.sleep(0.2)

    return [channel_row], video_rows


# ---------------- MAIN SYNC ---------------- #

def sync_youtube(sync_type="incremental"):

    print("[YT] Sync started...")

    uid, creds = get_creds()

    if not creds:
        return {"status": "error", "message": "Not connected"}

    service = build("youtube", "v3", credentials=creds)

    state = get_state(uid)

    if sync_type == "full":
        since = None
    else:
        if state and state.get("last_sync"):
            since = datetime.datetime.fromisoformat(state["last_sync"])
        else:
            since = None

    channels, videos = fetch_youtube_data(service, since)

    all_rows = {
        "channels": channels,
        "videos": videos
    }

    dest = get_active_destination(uid)

    if dest:
        push_to_destination(dest, SOURCE + "_channels", channels)
        push_to_destination(dest, SOURCE + "_videos", videos)

    save_state(uid, {
        "last_sync": datetime.datetime.utcnow().isoformat()
    })

    print("[YT] Done:", len(videos))

    return {
        "status": "success",
        "channels": len(channels),
        "videos": len(videos)
    }