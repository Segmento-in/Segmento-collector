import sqlite3
import json
import datetime
import time
from destinations.mysql_writer import push_to_mysql
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

DB = "identity.db"

def get_state(uid, source):

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


def save_state(uid, source, state):

    con = get_db()
    cur = con.cursor()

    for i in range(5):

        try:

            cur.execute("""
                INSERT OR REPLACE INTO connector_state
                (uid, source, state_json, updated_at)
                VALUES (?, ?, ?, ?)
            """, (
                uid,
                source,
                json.dumps(state),
                datetime.datetime.utcnow().isoformat()
            ))

            con.commit()
            con.close()
            return

        except sqlite3.OperationalError as e:

            if "locked" in str(e).lower():
                time.sleep(2 * (i + 1))
                continue

            raise e


    con.close()
    raise Exception("State DB locked")


# ---------------- DB ---------------- #

def get_db():

    con = sqlite3.connect(
        DB,
        timeout=60,
        isolation_level=None,
        check_same_thread=False
    )

    con.text_factory = str

    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    con.execute("PRAGMA synchronous=NORMAL;")

    return con

# ---------------- AUTH ---------------- #

def get_creds():

    con = get_db()
    cur = con.cursor()

    # Get enabled Gmail connection for current user
    cur.execute("""
        SELECT uid, access_token, refresh_token, scopes
        FROM google_accounts
        WHERE source='gmail'
        ORDER BY id DESC
        LIMIT 1
    """)

    row = cur.fetchone()

    if not row:
        con.close()
        return None, None

    uid, access, refresh, scopes = row

    if not access or not refresh:
        con.close()
        return None, None

    # Fetch Google App credentials (Client ID + Secret)
    cur.execute("""
        SELECT client_id, client_secret
        FROM connector_configs
        WHERE uid=? AND connector='gmail'
        LIMIT 1
    """, (uid,))

    cfg = cur.fetchone()
    con.close()

    if not cfg:
        return None, None

    client_id, client_secret = cfg

    creds = Credentials(
        token=access,
        refresh_token=refresh,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=scopes.split(",")
    )

    return uid, creds

# ---------------- PAGINATION ---------------- #

def fetch_all(func, limit=200, **kwargs):

    rows = []
    token = None

    while True:

        res = func(
            pageToken=token,
            maxResults=100,
            **kwargs
        ).execute()

        rows.extend(res.get("messages", []))

        if len(rows) >= limit:
            return rows[:limit]

        token = res.get("nextPageToken")

        if not token:
            break

        time.sleep(0.2)

    return rows


# ---------------- HEADERS ---------------- #

def parse_headers(headers):

    data = {}

    for h in headers:
        data[h.get("name", "").lower()] = h.get("value", "")

    return data


# ---------------- SAFE EXEC ---------------- #

def safe_execute(cur, query, data, retries=5):

    for i in range(retries):

        try:
            cur.execute(query, data)
            return

        except sqlite3.OperationalError as e:

            if "locked" in str(e).lower():
                time.sleep(2 * (i + 1))
                continue

            raise e

    raise Exception("DB locked after retries")

def fetch_gmail_history(service, start_id):

    all_changes = []

    page_token = None

    while True:

        res = service.users().history().list(
            userId="me",
            startHistoryId=start_id,
            pageToken=page_token
        ).execute()

        all_changes.extend(res.get("history", []))

        page_token = res.get("nextPageToken")

        if not page_token:
            break


    return all_changes

# ---------------- MAIN ---------------- #
def sync_gmail():

    uid, creds = get_creds()

    if not creds:
        return {
            "status": "error",
            "message": "Gmail not connected"
        }


    service = build(
        "gmail",
        "v1",
        credentials=creds,
        cache_discovery=False
    )

    con = get_db()
    cur = con.cursor()

    now = datetime.datetime.utcnow().isoformat()

    total = 0
    rows = []


    # ---------------- Load State ---------------- #

    state = get_state(uid, "gmail")

    last_history_id = None

    if state:
        last_history_id = state.get("last_history_id")


    try:

        # ============ PROFILE (always refresh) ============ #

        profile = service.users().getProfile(
            userId="me"
        ).execute()

        safe_execute(cur, """
            INSERT OR REPLACE INTO google_gmail_profile
            VALUES (NULL,?,?,?,?,?,?)
        """, (
            uid,
            profile.get("emailAddress"),
            profile.get("messagesTotal"),
            profile.get("threadsTotal"),
            json.dumps(profile),
            now
        ))


        # ============ LABELS (always refresh) ============ #

        labels = service.users().labels().list(
            userId="me"
        ).execute()

        for l in labels.get("labels", []):

            safe_execute(cur, """
                INSERT OR IGNORE INTO google_gmail_labels
                VALUES (NULL,?,?,?,?,?,?)
            """, (
                uid,
                l.get("id"),
                l.get("name"),
                l.get("type"),
                json.dumps(l),
                now
            ))


        # ================================================= #
        # ============ FIRST RUN (FULL SYNC) ============== #
        # ================================================= #

        if not last_history_id:

            print("[GMAIL] First sync (full)")

            messages = fetch_all(
                service.users().messages().list,
                limit=200,
                userId="me",
                includeSpamTrash=False
            )


        # ================================================= #
        # ============ INCREMENTAL SYNC =================== #
        # ================================================= #
        else:

            print("[GMAIL] Incremental from", last_history_id)

            messages = []

            try:

                page_token = None
                history_items = []

                while True:

                    res = service.users().history().list(
                        userId="me",
                        startHistoryId=last_history_id,
                        pageToken=page_token,
                        historyTypes=["messageAdded"]
                    ).execute()

                    history_items.extend(res.get("history", []))

                    page_token = res.get("nextPageToken")

                    if not page_token:
                        break


                for h in history_items:

                    for m in h.get("messagesAdded", []):

                        messages.append({
                            "id": m["message"]["id"]
                        })


                print(f"[GMAIL] Found {len(messages)} new messages")


            except Exception as e:

                # History expired / invalid → fallback
                print("[GMAIL] History failed:", e)
                print("[GMAIL] Falling back to full sync")

                messages = fetch_all(
                    service.users().messages().list,
                    limit=200,
                    userId="me",
                    includeSpamTrash=False
                )

                last_history_id = None



        # ============ FETCH DETAILS ============ #
        for m in messages:

            try:

                d = service.users().messages().get(
                    userId="me",
                    id=m["id"],
                    format="full"
                ).execute()

                rows.append({
                    "message_id": d.get("id"),
                    "thread_id": d.get("threadId"),
                    "snippet": d.get("snippet"),
                    "internal_date": d.get("internalDate"),
                    "raw_json": json.dumps(d)
                })


            except Exception as e:

                print("[GMAIL] Skipping missing message:", m["id"])
                continue


            headers = parse_headers(
                d.get("payload", {}).get("headers", [])
            )



            safe_execute(cur, """
                INSERT OR IGNORE INTO google_gmail_message_details
                VALUES (NULL,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                uid,
                d.get("id"),
                d.get("threadId"),
                headers.get("subject"),
                headers.get("from"),
                headers.get("to"),
                d.get("snippet"),
                d.get("internalDate"),
                json.dumps(d.get("payload")),
                json.dumps(d),
                now
            ))


            safe_execute(cur, """
                INSERT OR IGNORE INTO google_gmail_messages
                VALUES (NULL,?,?,?,?,?,?,?)
            """, (
                uid,
                d.get("id"),
                d.get("threadId"),
                d.get("snippet"),
                d.get("internalDate"),
                json.dumps(d),
                now
            ))


            total += 1


        # ============ SAVE STATE ============ #

        profile = service.users().getProfile(
            userId="me"
        ).execute()

        new_history_id = profile.get("historyId")

        if new_history_id:

            save_state(uid, "gmail", {
                "last_history_id": new_history_id
            })

        # ============ PUSH TO DESTINATION ============ #

        from destinations.destination_router import push_to_destination


        cur.execute("""
            SELECT dest_type, host, port, username, password, database_name
            FROM destination_configs
            WHERE uid=? AND source='gmail' AND is_active=1
            LIMIT 1
        """, (uid,))

        dest = cur.fetchone()

        if dest:

            dest_cfg = {
                "type": dest[0],
                "host": dest[1],
                "port": dest[2],
                "username": dest[3],
                "password": dest[4],
                "database_name": dest[5]
            }

            pushed = push_to_destination(dest_cfg, "gmail", rows)

            print(f"[DEST] Pushed {pushed} rows to {dest_cfg['type'].upper()}")


    except Exception as e:

        print("[GMAIL] FATAL ERROR:", str(e))
        raise


    finally:

        con.close()

    return {
        "status": "ok",
        "messages": total,
        "mode": "incremental" if last_history_id else "full"
    }