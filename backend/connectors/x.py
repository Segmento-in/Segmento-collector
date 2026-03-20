import base64
import datetime
import json
import sqlite3
import time
from urllib.parse import urlencode

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure


DB = "identity.db"
SOURCE = "x"
AUTH_URL = "https://twitter.com/i/oauth2/authorize"
TOKEN_URL = "https://api.x.com/2/oauth2/token"
API_BASE = "https://api.x.com/2"
SCOPES = ["tweet.read", "users.read", "follows.read", "offline.access"]


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(msg):
    print(f"[X] {msg}", flush=True)


def _parse_dt(value):
    if not value:
        return None
    try:
        dt = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.UTC)
        return dt.astimezone(datetime.UTC)
    except Exception:
        return None


def _to_iso_z(dt):
    if not dt:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.UTC)
    return dt.astimezone(datetime.UTC).isoformat().replace("+00:00", "Z")


def _token_expired(expires_at):
    dt = _parse_dt(expires_at)
    if not dt:
        return True
    return dt <= (datetime.datetime.now(datetime.UTC) + datetime.timedelta(minutes=2))


def _get_config(uid):
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT client_id, client_secret, scopes, access_token, refresh_token
        FROM connector_configs
        WHERE uid=? AND connector=?
        LIMIT 1
        """,
        (uid, SOURCE),
    )
    row = fetchone_secure(cur)
    con.close()
    return row


def _get_connection(uid):
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT x_user_id, username, access_token, refresh_token, expires_at
        FROM x_connections
        WHERE uid=?
        LIMIT 1
        """,
        (uid,),
    )
    row = fetchone_secure(cur)
    con.close()
    return row


def get_x_auth_url(uid):
    cfg = _get_config(uid)
    if not cfg:
        raise Exception("X app not configured")

    client_id = cfg.get("client_id")
    redirect_uri = cfg.get("scopes")
    if not client_id or not redirect_uri:
        raise Exception("Missing client_id or redirect_uri")

    params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "scope": " ".join(SCOPES),
        "state": f"x-{uid}",
        "code_challenge": "segmentoxchallenge",
        "code_challenge_method": "plain",
    }
    return AUTH_URL + "?" + urlencode(params)


def _basic_auth_header(client_id, client_secret):
    token = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    return {"Authorization": f"Basic {token}"}


def _exchange_code_for_token(uid, code):
    cfg = _get_config(uid)
    if not cfg:
        raise Exception("X app not configured")

    client_id = cfg.get("client_id")
    client_secret = cfg.get("client_secret")
    redirect_uri = cfg.get("scopes")
    if not client_id or not client_secret or not redirect_uri:
        raise Exception("Missing OAuth app settings")

    headers = {
        **_basic_auth_header(client_id, client_secret),
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "authorization_code",
        "client_id": client_id,
        "code": code,
        "redirect_uri": redirect_uri,
        "code_verifier": "segmentoxchallenge",
    }

    res = requests.post(TOKEN_URL, headers=headers, data=data, timeout=30)
    if res.status_code != 200:
        raise Exception(f"X token exchange failed ({res.status_code}): {res.text[:300]}")

    token_data = res.json()
    if not token_data.get("access_token"):
        raise Exception(f"X token exchange error: {token_data}")
    return token_data


def _save_connection(uid, x_user_id, username, access_token, refresh_token, expires_at):
    con = get_db()
    cur = con.cursor()

    enc_access = encrypt_value(access_token) if access_token else None
    enc_refresh = encrypt_value(refresh_token) if refresh_token else None

    cur.execute(
        """
        INSERT OR REPLACE INTO x_connections
        (uid, x_user_id, username, access_token, refresh_token, expires_at, connected_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            uid,
            x_user_id,
            username,
            enc_access,
            enc_refresh,
            expires_at,
            datetime.datetime.now(datetime.UTC).isoformat(),
        ),
    )

    cur.execute(
        """
        INSERT OR REPLACE INTO google_connections
        (uid, source, enabled)
        VALUES (?, ?, 1)
        """,
        (uid, SOURCE),
    )

    cur.execute(
        """
        UPDATE connector_configs
        SET access_token=?, refresh_token=?, status='connected'
        WHERE uid=? AND connector=?
        """,
        (enc_access, enc_refresh, uid, SOURCE),
    )

    con.commit()
    con.close()


def _update_error_status(uid, message):
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        UPDATE connector_configs
        SET status='error'
        WHERE uid=? AND connector=?
        """,
        (uid, SOURCE),
    )
    con.commit()
    con.close()
    _log(f"status set to error: {message}")


def _refresh_access_token(uid, refresh_token):
    cfg = _get_config(uid)
    if not cfg:
        raise Exception("X app not configured")

    client_id = cfg.get("client_id")
    client_secret = cfg.get("client_secret")
    if not client_id or not client_secret:
        raise Exception("X app missing client credentials")

    headers = {
        **_basic_auth_header(client_id, client_secret),
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "refresh_token",
        "client_id": client_id,
        "refresh_token": refresh_token,
    }

    res = requests.post(TOKEN_URL, headers=headers, data=data, timeout=30)
    if res.status_code != 200:
        raise Exception(f"X token refresh failed ({res.status_code}): {res.text[:300]}")

    token_data = res.json()
    access_token = token_data.get("access_token")
    next_refresh = token_data.get("refresh_token") or refresh_token
    expires_in = int(token_data.get("expires_in") or 7200)
    expires_at = (
        datetime.datetime.now(datetime.UTC) + datetime.timedelta(seconds=expires_in)
    ).isoformat()

    if not access_token:
        raise Exception(f"X token refresh error: {token_data}")

    con = get_db()
    cur = con.cursor()
    enc_access = encrypt_value(access_token)
    enc_refresh = encrypt_value(next_refresh) if next_refresh else None

    cur.execute(
        """
        UPDATE connector_configs
        SET access_token=?, refresh_token=?, status='connected'
        WHERE uid=? AND connector=?
        """,
        (enc_access, enc_refresh, uid, SOURCE),
    )

    cur.execute(
        """
        UPDATE x_connections
        SET access_token=?, refresh_token=?, expires_at=?
        WHERE uid=?
        """,
        (enc_access, enc_refresh, expires_at, uid),
    )

    con.commit()
    con.close()

    _log("access token refreshed")
    return access_token, next_refresh, expires_at


def _ensure_valid_token(uid):
    conn = _get_connection(uid)
    if not conn:
        return None, None, None

    x_user_id = conn.get("x_user_id")
    username = conn.get("username")
    access_token = conn.get("access_token")
    refresh_token = conn.get("refresh_token")
    expires_at = conn.get("expires_at")

    if access_token and not _token_expired(expires_at):
        return x_user_id, username, access_token

    if not refresh_token:
        return x_user_id, username, access_token

    new_access, _, _ = _refresh_access_token(uid, refresh_token)
    return x_user_id, username, new_access


def _request_with_retry(method, url, headers=None, params=None, retries=5, timeout=30):
    headers = headers or {}
    params = params or {}

    for attempt in range(retries):
        try:
            res = requests.request(
                method,
                url,
                headers=headers,
                params=params,
                timeout=timeout,
            )
        except requests.RequestException as e:
            if attempt == retries - 1:
                raise
            wait_s = min(2 ** attempt, 30)
            _log(f"network error (attempt {attempt + 1}): {e}; sleeping {wait_s}s")
            time.sleep(wait_s)
            continue

        remaining = res.headers.get("x-rate-limit-remaining")
        reset_raw = res.headers.get("x-rate-limit-reset")
        if remaining is not None:
            _log(f"rate remaining={remaining}, reset={reset_raw}")

        if res.status_code == 429:
            reset_wait = 0
            if reset_raw:
                try:
                    reset_epoch = int(reset_raw)
                    reset_wait = max(reset_epoch - int(time.time()) + 1, 1)
                except Exception:
                    reset_wait = 0
            exp_wait = min(2 ** attempt, 60)
            wait_s = max(reset_wait, exp_wait)
            if attempt == retries - 1:
                return res
            _log(f"rate limited; sleeping {wait_s}s")
            time.sleep(wait_s)
            continue

        if res.status_code in (500, 502, 503, 504):
            if attempt == retries - 1:
                return res
            wait_s = min(2 ** attempt, 30)
            _log(f"server error {res.status_code}; sleeping {wait_s}s")
            time.sleep(wait_s)
            continue

        return res

    return res


def _api_get(access_token, path, params=None):
    res = _request_with_retry(
        "GET",
        f"{API_BASE}{path}",
        headers={"Authorization": f"Bearer {access_token}"},
        params=params or {},
    )

    if res.status_code == 401:
        raise PermissionError("X API 401 unauthorized")
    if res.status_code == 403:
        raise PermissionError("X API 403 forbidden")
    if res.status_code == 429:
        raise Exception("X API rate limit exceeded after retries")
    if res.status_code >= 500:
        raise Exception(f"X API server error ({res.status_code})")
    if res.status_code < 200 or res.status_code >= 300:
        raise Exception(f"X API error ({res.status_code}): {res.text[:300]}")

    return res.json()


def _upsert_user(cur, uid, user_obj, fetched_at):
    metrics = user_obj.get("public_metrics") or {}
    cur.execute(
        """
        INSERT OR REPLACE INTO x_users
        (uid, user_id, username, display_name, bio, location,
         followers_count, following_count, tweet_count, profile_image_url,
         raw_json, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            uid,
            user_obj.get("id"),
            user_obj.get("username"),
            user_obj.get("name"),
            user_obj.get("description"),
            user_obj.get("location"),
            metrics.get("followers_count"),
            metrics.get("following_count"),
            metrics.get("tweet_count"),
            user_obj.get("profile_image_url"),
            json.dumps(user_obj),
            fetched_at,
        ),
    )


def _upsert_tweet(cur, uid, tweet_obj, fetched_at):
    metrics = tweet_obj.get("public_metrics") or {}
    media_keys = (tweet_obj.get("attachments") or {}).get("media_keys") or []
    created_at = tweet_obj.get("created_at")

    cur.execute(
        """
        INSERT OR REPLACE INTO x_tweets
        (uid, tweet_id, author_id, text, like_count, retweet_count, reply_count,
         media_ids, created_at, raw_json, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            uid,
            tweet_obj.get("id"),
            tweet_obj.get("author_id"),
            tweet_obj.get("text"),
            metrics.get("like_count"),
            metrics.get("retweet_count"),
            metrics.get("reply_count"),
            ",".join(media_keys) if media_keys else None,
            created_at,
            json.dumps(tweet_obj),
            fetched_at,
        ),
    )
    return created_at


def get_state(uid):
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT state_json
        FROM connector_state
        WHERE uid=? AND source=?
        LIMIT 1
        """,
        (uid, SOURCE),
    )
    row = fetchone_secure(cur)
    con.close()

    if not row:
        return {"last_tweet_timestamp": None}

    try:
        return json.loads(row["state_json"])
    except Exception:
        return {"last_tweet_timestamp": None}


def save_state(uid, state):
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO connector_state
        (uid, source, state_json, updated_at)
        VALUES (?, ?, ?, ?)
        """,
        (
            uid,
            SOURCE,
            json.dumps(state),
            datetime.datetime.now(datetime.UTC).isoformat(),
        ),
    )
    con.commit()
    con.close()


def get_active_destination(uid):
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT dest_type, host, port, username, password, database_name
        FROM destination_configs
        WHERE uid=? AND source=? AND is_active=1
        ORDER BY id DESC
        LIMIT 1
        """,
        (uid, SOURCE),
    )
    row = fetchone_secure(cur)
    con.close()
    if not row:
        return None
    return {
        "type": row["dest_type"],
        "host": row["host"],
        "port": row["port"],
        "username": row["username"],
        "password": row["password"],
        "database_name": row["database_name"],
    }


def handle_x_oauth_callback(uid, code):
    token_data = _exchange_code_for_token(uid, code)
    access_token = token_data.get("access_token")
    refresh_token = token_data.get("refresh_token")
    expires_in = int(token_data.get("expires_in") or 7200)
    expires_at = (
        datetime.datetime.now(datetime.UTC) + datetime.timedelta(seconds=expires_in)
    ).isoformat()

    me = _api_get(
        access_token,
        "/users/me",
        params={"user.fields": "id,username,name"},
    ).get("data")

    if not me:
        raise Exception("Unable to fetch authenticated X user profile")

    _save_connection(
        uid,
        me.get("id"),
        me.get("username"),
        access_token,
        refresh_token,
        expires_at,
    )
    return {"status": "success", "x_user_id": me.get("id"), "username": me.get("username")}


def sync_x(uid, sync_type="historical"):
    try:
        x_user_id, username, access_token = _ensure_valid_token(uid)
        if not access_token:
            return {"status": "error", "message": "X not connected"}

        fetched_at = datetime.datetime.now(datetime.UTC).isoformat()
        state = get_state(uid)
        last_ts_raw = state.get("last_tweet_timestamp")
        last_ts = _parse_dt(last_ts_raw)
        now_utc = datetime.datetime.now(datetime.UTC)
        min_recent = now_utc - datetime.timedelta(days=7)

        me_data = _api_get(
            access_token,
            "/users/me",
            params={
                "user.fields": "id,name,username,description,location,profile_image_url,public_metrics,created_at"
            },
        ).get("data")

        if not me_data:
            return {"status": "error", "message": "Could not fetch X profile"}

        x_user_id = me_data.get("id")
        username = me_data.get("username")

        con = get_db()
        cur = con.cursor()

        user_rows_for_push = []
        tweet_rows_for_push = []

        _upsert_user(cur, uid, me_data, fetched_at)
        user_rows_for_push.append(
            {
                "entity": "profile",
                "uid": uid,
                "user_id": me_data.get("id"),
                "username": me_data.get("username"),
                "display_name": me_data.get("name"),
            }
        )

        followers_count = 0
        followers_next = None
        while True:
            follower_data = _api_get(
                access_token,
                f"/users/{x_user_id}/followers",
                params={
                    "max_results": 1000,
                    "pagination_token": followers_next,
                    "user.fields": "id,name,username,description,location,profile_image_url,public_metrics,created_at",
                },
            )
            batch = follower_data.get("data") or []
            for follower in batch:
                _upsert_user(cur, uid, follower, fetched_at)
                followers_count += 1
                user_rows_for_push.append(
                    {
                        "entity": "follower",
                        "uid": uid,
                        "user_id": follower.get("id"),
                        "username": follower.get("username"),
                        "display_name": follower.get("name"),
                    }
                )

            followers_next = (follower_data.get("meta") or {}).get("next_token")
            if not followers_next:
                break

        query = f"from:{username} -is:retweet"
        if sync_type == "incremental" and last_ts:
            start_time = max(last_ts, min_recent)
        else:
            start_time = min_recent

        newest_ts = last_ts
        next_token = None
        tweets_count = 0

        while True:
            tweets_data = _api_get(
                access_token,
                "/tweets/search/recent",
                params={
                    "query": query,
                    "max_results": 100,
                    "start_time": _to_iso_z(start_time),
                    "tweet.fields": "id,text,author_id,created_at,public_metrics,attachments",
                    "pagination_token": next_token,
                },
            )

            tweets = tweets_data.get("data") or []
            for tweet in tweets:
                created_at_raw = _upsert_tweet(cur, uid, tweet, fetched_at)
                tweets_count += 1
                created_dt = _parse_dt(created_at_raw)
                if created_dt and (not newest_ts or created_dt > newest_ts):
                    newest_ts = created_dt

                tweet_rows_for_push.append(
                    {
                        "entity": "tweet",
                        "uid": uid,
                        "tweet_id": tweet.get("id"),
                        "author_id": tweet.get("author_id"),
                        "text": tweet.get("text"),
                        "created_at": tweet.get("created_at"),
                    }
                )

            next_token = (tweets_data.get("meta") or {}).get("next_token")
            if not next_token:
                break

        con.commit()
        con.close()

        if newest_ts and (not last_ts or newest_ts > last_ts):
            save_state(uid, {"last_tweet_timestamp": _to_iso_z(newest_ts)})

        rows = user_rows_for_push + tweet_rows_for_push
        dest_cfg = get_active_destination(uid)
        if not dest_cfg:
            return {
                "status": "success",
                "users_synced": len(user_rows_for_push),
                "followers_synced": followers_count,
                "tweets_synced": tweets_count,
                "rows_found": len(rows),
                "rows_pushed": 0,
                "sync_type": sync_type,
                "message": "No active destination",
            }

        pushed = push_to_destination(dest_cfg, SOURCE, rows) if rows else 0
        return {
            "status": "success",
            "users_synced": len(user_rows_for_push),
            "followers_synced": followers_count,
            "tweets_synced": tweets_count,
            "rows_found": len(rows),
            "rows_pushed": pushed,
            "sync_type": sync_type,
        }

    except PermissionError as e:
        _update_error_status(uid, str(e))
        return {"status": "error", "message": str(e)}
    except Exception as e:
        _log(f"sync failed: {e}")
        return {"status": "error", "message": str(e)}


def disconnect_x(uid):
    con = get_db()
    cur = con.cursor()

    cur.execute(
        """
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source=?
        """,
        (uid, SOURCE),
    )

    cur.execute("DELETE FROM x_connections WHERE uid=?", (uid,))

    cur.execute(
        """
        UPDATE connector_configs
        SET status='disconnected'
        WHERE uid=? AND connector=?
        """,
        (uid, SOURCE),
    )

    con.commit()
    con.close()
