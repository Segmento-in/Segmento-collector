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
SOURCE = "tiktok"
AUTH_URL = "https://www.tiktok.com/v2/auth/authorize/"
TOKEN_URL = "https://open.tiktokapis.com/v2/oauth/token/"
BUSINESS_BASE = "https://business-api.tiktok.com/open_api/v1.3"
DEFAULT_SCOPES = "user.info.basic,video.list,ads.read"


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(msg):
    print(f"[TIKTOK] {msg}", flush=True)


def _request_with_retry(method, url, retries=3, timeout=30, **kwargs):
    for attempt in range(retries):
        try:
            res = requests.request(method, url, timeout=timeout, **kwargs)
        except requests.RequestException as e:
            if attempt == retries - 1:
                raise
            _log(f"request failed (attempt {attempt + 1}): {e}")
            time.sleep(min(2 ** attempt, 8))
            continue

        if res.status_code in (429, 500, 502, 503, 504):
            if attempt == retries - 1:
                return res
            time.sleep(min(2 ** attempt, 8))
            continue

        return res

    return res


def _token_expired(expires_at):
    if not expires_at:
        return True
    try:
        dt = datetime.datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.UTC)
        return dt <= (datetime.datetime.now(datetime.UTC) + datetime.timedelta(minutes=2))
    except Exception:
        return True


def _get_tiktok_config(uid):
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT client_id, client_secret, scopes, api_key, access_token, refresh_token
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
        SELECT advertiser_id, access_token, refresh_token, expires_at
        FROM tiktok_connections
        WHERE uid=?
        LIMIT 1
        """,
        (uid,),
    )
    row = fetchone_secure(cur)
    con.close()
    return row


def get_tiktok_auth_url(uid):
    cfg = _get_tiktok_config(uid)
    if not cfg:
        raise Exception("TikTok app not configured")

    redirect_uri = cfg.get("scopes")
    scopes = DEFAULT_SCOPES

    params = {
        "client_key": cfg["client_id"],
        "scope": scopes,
        "response_type": "code",
        "redirect_uri": redirect_uri,
    }
    return AUTH_URL + "?" + urlencode(params)


def _exchange_code_for_token(uid, code):
    cfg = _get_tiktok_config(uid)
    if not cfg:
        raise Exception("TikTok app not configured")

    payload = {
        "client_key": cfg["client_id"],
        "client_secret": cfg["client_secret"],
        "code": code,
        "grant_type": "authorization_code",
        "redirect_uri": cfg.get("scopes"),
    }

    res = _request_with_retry(
        "POST",
        TOKEN_URL,
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    return res


def _refresh_access_token(uid, refresh_token):
    cfg = _get_tiktok_config(uid)
    if not cfg:
        raise Exception("TikTok app not configured")

    payload = {
        "client_key": cfg["client_id"],
        "client_secret": cfg["client_secret"],
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }

    res = _request_with_retry(
        "POST",
        TOKEN_URL,
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )

    if res.status_code != 200:
        raise Exception(f"TikTok token refresh failed: {res.text[:300]}")

    data = res.json()
    body = data.get("data") if isinstance(data.get("data"), dict) else data
    if data.get("error") or body.get("error"):
        raise Exception(
            f"TikTok token refresh error: {body.get('error_description') or body.get('error') or data.get('error')}"
        )

    access_token = body.get("access_token")
    next_refresh_token = body.get("refresh_token") or refresh_token
    expires_in = int(body.get("expires_in") or 86400)
    expires_at = (datetime.datetime.now(datetime.UTC) + datetime.timedelta(seconds=expires_in)).isoformat()

    if not access_token:
        raise Exception("TikTok token refresh did not return access_token")

    con = get_db()
    cur = con.cursor()
    enc_access = encrypt_value(access_token)
    enc_refresh = encrypt_value(next_refresh_token) if next_refresh_token else None

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
        UPDATE tiktok_connections
        SET access_token=?, refresh_token=?, expires_at=?
        WHERE uid=?
        """,
        (enc_access, enc_refresh, expires_at, uid),
    )

    con.commit()
    con.close()

    _log("access token refreshed")
    return access_token, next_refresh_token, expires_at


def _ensure_valid_token(uid):
    conn = _get_connection(uid)
    if not conn:
        return None, None

    advertiser_id = conn.get("advertiser_id")
    access_token = conn.get("access_token")
    refresh_token = conn.get("refresh_token")
    expires_at = conn.get("expires_at")

    if access_token and not _token_expired(expires_at):
        return advertiser_id, access_token

    if not refresh_token:
        return advertiser_id, access_token

    new_access, _, _ = _refresh_access_token(uid, refresh_token)
    return advertiser_id, new_access


def _save_connection(uid, advertiser_id, access_token, refresh_token, expires_at):
    con = get_db()
    cur = con.cursor()

    enc_access = encrypt_value(access_token) if access_token else None
    enc_refresh = encrypt_value(refresh_token) if refresh_token else None

    cur.execute(
        """
        INSERT OR REPLACE INTO tiktok_connections
        (uid, advertiser_id, access_token, refresh_token, expires_at, connected_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            uid,
            advertiser_id,
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


def _business_post(access_token, path, payload, retries=3):
    res = _request_with_retry(
        "POST",
        f"{BUSINESS_BASE}{path}",
        retries=retries,
        headers={
            "Content-Type": "application/json",
            "Access-Token": access_token,
            "Authorization": f"Bearer {access_token}",
        },
        json=payload,
    )
    if res.status_code != 200:
        raise Exception(f"TikTok API HTTP {res.status_code}: {res.text[:300]}")

    body = res.json()
    code = body.get("code")
    if code not in (0, "0"):
        msg = body.get("message") or body.get("msg") or "TikTok API error"
        raise Exception(f"TikTok API error ({code}): {msg}")

    return body.get("data") or {}


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
        return {"last_sync_date": None}

    try:
        return json.loads(row["state_json"])
    except Exception:
        return {"last_sync_date": None}


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


def _extract_list(data):
    return (
        data.get("list")
        or data.get("campaigns")
        or data.get("ads")
        or data.get("data")
        or []
    )


def sync_tiktok(uid, sync_type="historical"):
    try:
        advertiser_id, access_token = _ensure_valid_token(uid)
        if not advertiser_id or not access_token:
            return {"status": "error", "message": "TikTok not connected"}

        state = get_state(uid)
        today = datetime.date.today()
        today_str = today.isoformat()
        if sync_type == "incremental" and state.get("last_sync_date"):
            start_date = state.get("last_sync_date")
        else:
            start_date = (today - datetime.timedelta(days=30)).isoformat()

        con = get_db()
        cur = con.cursor()
        now = datetime.datetime.now(datetime.UTC).isoformat()
        rows = []

        # Campaigns
        page = 1
        campaign_count = 0
        while True:
            data = _business_post(
                access_token,
                "/campaign/get/",
                {
                    "advertiser_id": advertiser_id,
                    "page": page,
                    "page_size": 50,
                },
            )
            campaigns = _extract_list(data)
            if not campaigns:
                break

            for item in campaigns:
                cid = str(item.get("campaign_id") or item.get("id") or "")
                if not cid:
                    continue
                campaign_count += 1
                cur.execute(
                    """
                    INSERT OR REPLACE INTO tiktok_campaigns
                    (uid, advertiser_id, campaign_id, campaign_name, objective, status, budget, budget_type, raw_json, fetched_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        uid,
                        str(advertiser_id),
                        cid,
                        item.get("campaign_name") or item.get("name"),
                        item.get("objective"),
                        item.get("status"),
                        str(item.get("budget")) if item.get("budget") is not None else None,
                        item.get("budget_type"),
                        json.dumps(item),
                        now,
                    ),
                )
                rows.append(
                    {
                        "entity": "campaign",
                        "uid": uid,
                        "advertiser_id": str(advertiser_id),
                        "campaign_id": cid,
                        "campaign_name": item.get("campaign_name") or item.get("name"),
                        "objective": item.get("objective"),
                        "status": item.get("status"),
                    }
                )

            if len(campaigns) < 50:
                break
            page += 1

        # Ads
        page = 1
        ad_count = 0
        while True:
            data = _business_post(
                access_token,
                "/ad/get/",
                {
                    "advertiser_id": advertiser_id,
                    "page": page,
                    "page_size": 50,
                },
            )
            ads = _extract_list(data)
            if not ads:
                break

            for item in ads:
                ad_id = str(item.get("ad_id") or item.get("id") or "")
                if not ad_id:
                    continue
                ad_count += 1
                cur.execute(
                    """
                    INSERT OR REPLACE INTO tiktok_ads
                    (uid, advertiser_id, ad_id, campaign_id, adgroup_id, ad_name, status, raw_json, fetched_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        uid,
                        str(advertiser_id),
                        ad_id,
                        str(item.get("campaign_id")) if item.get("campaign_id") is not None else None,
                        str(item.get("adgroup_id")) if item.get("adgroup_id") is not None else None,
                        item.get("ad_name") or item.get("name"),
                        item.get("status"),
                        json.dumps(item),
                        now,
                    ),
                )
                rows.append(
                    {
                        "entity": "ad",
                        "uid": uid,
                        "advertiser_id": str(advertiser_id),
                        "ad_id": ad_id,
                        "campaign_id": item.get("campaign_id"),
                        "adgroup_id": item.get("adgroup_id"),
                        "ad_name": item.get("ad_name") or item.get("name"),
                        "status": item.get("status"),
                    }
                )

            if len(ads) < 50:
                break
            page += 1

        # Integrated reports
        report_count = 0
        page = 1
        while True:
            data = _business_post(
                access_token,
                "/report/integrated/get/",
                {
                    "advertiser_id": advertiser_id,
                    "service_type": "AUCTION",
                    "data_level": "AUCTION_AD",
                    "report_type": "BASIC",
                    "dimensions": ["ad_id", "stat_time_day"],
                    "metrics": [
                        "spend",
                        "impressions",
                        "clicks",
                        "cpc",
                        "cpm",
                        "ctr",
                        "conversions",
                    ],
                    "start_date": start_date,
                    "end_date": today_str,
                    "page": page,
                    "page_size": 100,
                },
            )
            reports = _extract_list(data)
            if not reports:
                break

            for item in reports:
                dims = item.get("dimensions") or {}
                mets = item.get("metrics") or {}

                ad_id = str(item.get("ad_id") or dims.get("ad_id") or "")
                stat_time_day = str(item.get("stat_time_day") or dims.get("stat_time_day") or "")
                if not ad_id or not stat_time_day:
                    continue

                report_count += 1
                cur.execute(
                    """
                    INSERT OR REPLACE INTO tiktok_reports
                    (uid, advertiser_id, ad_id, campaign_id, adgroup_id, stat_time_day, impressions, clicks, spend, ctr, cpc, cpm, conversions, raw_json, fetched_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        uid,
                        str(advertiser_id),
                        ad_id,
                        str(item.get("campaign_id") or dims.get("campaign_id") or "") or None,
                        str(item.get("adgroup_id") or dims.get("adgroup_id") or "") or None,
                        stat_time_day,
                        str(mets.get("impressions") or item.get("impressions") or ""),
                        str(mets.get("clicks") or item.get("clicks") or ""),
                        str(mets.get("spend") or item.get("spend") or ""),
                        str(mets.get("ctr") or item.get("ctr") or ""),
                        str(mets.get("cpc") or item.get("cpc") or ""),
                        str(mets.get("cpm") or item.get("cpm") or ""),
                        str(mets.get("conversions") or item.get("conversions") or ""),
                        json.dumps(item),
                        now,
                    ),
                )
                rows.append(
                    {
                        "entity": "report",
                        "uid": uid,
                        "advertiser_id": str(advertiser_id),
                        "ad_id": ad_id,
                        "stat_time_day": stat_time_day,
                        "impressions": mets.get("impressions") or item.get("impressions"),
                        "clicks": mets.get("clicks") or item.get("clicks"),
                        "spend": mets.get("spend") or item.get("spend"),
                        "ctr": mets.get("ctr") or item.get("ctr"),
                        "cpc": mets.get("cpc") or item.get("cpc"),
                        "cpm": mets.get("cpm") or item.get("cpm"),
                        "conversions": mets.get("conversions") or item.get("conversions"),
                    }
                )

            if len(reports) < 100:
                break
            page += 1

        con.commit()
        con.close()

        save_state(uid, {"last_sync_date": today_str})

        dest_cfg = get_active_destination(uid)
        if not dest_cfg:
            return {
                "status": "success",
                "campaigns": campaign_count,
                "ads": ad_count,
                "reports": report_count,
                "rows_found": len(rows),
                "rows_pushed": 0,
                "sync_type": sync_type,
                "message": "No active destination",
            }

        pushed = push_to_destination(dest_cfg, SOURCE, rows) if rows else 0
        return {
            "status": "success",
            "campaigns": campaign_count,
            "ads": ad_count,
            "reports": report_count,
            "rows_found": len(rows),
            "rows_pushed": pushed,
            "sync_type": sync_type,
        }

    except Exception as e:
        _log(f"sync failed: {e}")
        return {"status": "error", "message": str(e)}


def handle_tiktok_oauth_callback(uid, code):
    token_res = _exchange_code_for_token(uid, code)
    if token_res.status_code != 200:
        return {
            "status": "error",
            "message": f"Token exchange failed: {token_res.text[:300]}",
        }

    data = token_res.json()
    body = data.get("data") if isinstance(data.get("data"), dict) else data
    if data.get("error") or body.get("error"):
        return {
            "status": "error",
            "message": body.get("error_description") or body.get("error") or data.get("error"),
        }

    access_token = body.get("access_token")
    refresh_token = body.get("refresh_token")
    expires_in = int(body.get("expires_in") or 86400)
    expires_at = (datetime.datetime.now(datetime.UTC) + datetime.timedelta(seconds=expires_in)).isoformat()

    cfg = _get_tiktok_config(uid)
    advertiser_id = cfg.get("api_key") if cfg else None

    if not access_token or not advertiser_id:
        return {
            "status": "error",
            "message": "Missing access token or advertiser_id in saved config",
        }

    _save_connection(uid, advertiser_id, access_token, refresh_token, expires_at)
    return {"status": "success", "advertiser_id": advertiser_id}


def disconnect_tiktok(uid):
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

    cur.execute("DELETE FROM tiktok_connections WHERE uid=?", (uid,))

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
