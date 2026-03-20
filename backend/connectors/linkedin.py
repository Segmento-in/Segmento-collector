import datetime
import json
import sqlite3
import time
from urllib.parse import quote, urlencode

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure


DB = "identity.db"
SOURCE = "linkedin"
AUTH_URL = "https://www.linkedin.com/oauth/v2/authorization"
TOKEN_URL = "https://www.linkedin.com/oauth/v2/accessToken"
API_BASE = "https://api.linkedin.com"
DEFAULT_SCOPES = ["r_ads", "r_ads_reporting", "offline_access"]
DEFAULT_LINKEDIN_VERSION = "202503"


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(message):
    print(f"[LINKEDIN] {message}", flush=True)


def _parse_dt(value):
    if not value:
        return None
    try:
        dt = datetime.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.UTC)
        return dt.astimezone(datetime.UTC)
    except Exception:
        return None


def _token_expired(expires_at):
    dt = _parse_dt(expires_at)
    if not dt:
        return True
    return dt <= (datetime.datetime.now(datetime.UTC) + datetime.timedelta(minutes=2))


def _urn_account(account_id):
    value = str(account_id or "").strip()
    if not value:
        return None
    if value.startswith("urn:li:sponsoredAccount:"):
        return value
    return f"urn:li:sponsoredAccount:{value}"


def _urn_campaign(campaign_id):
    value = str(campaign_id or "").strip()
    if not value:
        return None
    if value.startswith("urn:li:sponsoredCampaign:"):
        return value
    return f"urn:li:sponsoredCampaign:{value}"


def _urn_creative(creative_id):
    value = str(creative_id or "").strip()
    if not value:
        return None
    if value.startswith("urn:li:sponsoredCreative:"):
        return value
    return f"urn:li:sponsoredCreative:{value}"


def _date_parts(yyyy_mm_dd):
    dt = datetime.datetime.strptime(yyyy_mm_dd, "%Y-%m-%d").date()
    return dt.year, dt.month, dt.day


def _get_config(uid):
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
        SELECT linkedin_member_id, access_token, refresh_token, expires_at, linkedin_version
        FROM linkedin_connections
        WHERE uid=?
        LIMIT 1
        """,
        (uid,),
    )
    row = fetchone_secure(cur)
    con.close()
    return row


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


def get_linkedin_auth_url(uid, state):
    cfg = _get_config(uid)
    if not cfg:
        raise Exception("LinkedIn app not configured")

    client_id = cfg.get("client_id")
    redirect_uri = cfg.get("scopes")
    scopes = DEFAULT_SCOPES
    if not client_id or not redirect_uri:
        raise Exception("Missing LinkedIn client_id or redirect_uri")

    params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "scope": " ".join(scopes),
        "state": state,
    }
    return AUTH_URL + "?" + urlencode(params)


def _linkedin_headers(access_token, linkedin_version):
    return {
        "Authorization": f"Bearer {access_token}",
        "X-Restli-Protocol-Version": "2.0.0",
        "Linkedin-Version": linkedin_version,
        "Accept": "application/json",
    }


def _request_with_retry(
    method,
    url,
    retries=5,
    timeout=40,
    linkedin_version=DEFAULT_LINKEDIN_VERSION,
    **kwargs,
):
    for attempt in range(retries):
        try:
            res = requests.request(method, url, timeout=timeout, **kwargs)
        except requests.RequestException as e:
            if attempt == retries - 1:
                raise
            wait_s = min(2 ** attempt, 30)
            _log(f"network error (attempt {attempt + 1}): {e}; sleeping {wait_s}s")
            time.sleep(wait_s)
            continue

        app_remaining = (
            res.headers.get("x-restli-rate-limit-remaining")
            or res.headers.get("x-ratelimit-remaining")
            or res.headers.get("X-Ratelimit-Remaining")
        )
        member_remaining = res.headers.get("x-li-member-limit-remaining")
        if app_remaining is not None or member_remaining is not None:
            _log(
                "quota remaining "
                f"(app={app_remaining if app_remaining is not None else 'n/a'}, "
                f"member={member_remaining if member_remaining is not None else 'n/a'}, "
                f"version={linkedin_version})"
            )

        if res.status_code == 429:
            retry_after = res.headers.get("Retry-After")
            try:
                retry_wait = int(retry_after) if retry_after else 0
            except Exception:
                retry_wait = 0
            exp_wait = min(2 ** attempt, 60)
            wait_s = max(retry_wait, exp_wait)
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


def _api_get(access_token, path, linkedin_version, params=None):
    res = _request_with_retry(
        "GET",
        f"{API_BASE}{path}",
        headers=_linkedin_headers(access_token, linkedin_version),
        params=params or {},
        linkedin_version=linkedin_version,
    )

    if res.status_code == 401:
        raise PermissionError("LinkedIn API 401 unauthorized")
    if res.status_code == 403:
        raise PermissionError("LinkedIn API 403 forbidden")
    if res.status_code == 429:
        raise Exception("LinkedIn API rate limit exceeded after retries")
    if res.status_code >= 500:
        raise Exception(f"LinkedIn API server error ({res.status_code})")
    if res.status_code < 200 or res.status_code >= 300:
        raise Exception(f"LinkedIn API error ({res.status_code}): {res.text[:300]}")

    try:
        return res.json()
    except Exception:
        return {}


def _extract_items(payload):
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    return payload.get("elements") or payload.get("items") or payload.get("data") or []


def _extract_paging(payload):
    if not isinstance(payload, dict):
        return {}
    return payload.get("paging") or {}


def _fetch_paginated(access_token, path, linkedin_version, base_params=None, count=100):
    params = dict(base_params or {})
    all_items = []

    if "start" not in params:
        params["start"] = 0
    if "count" not in params:
        params["count"] = count

    page_token = None
    for _ in range(1000):
        req_params = dict(params)
        if page_token:
            req_params.pop("start", None)
            req_params.pop("count", None)
            req_params["pageToken"] = page_token
            req_params.setdefault("pageSize", count)

        payload = _api_get(access_token, path, linkedin_version, req_params)
        items = _extract_items(payload)
        all_items.extend(items)

        paging = _extract_paging(payload)
        next_page_token = paging.get("pageToken") or payload.get("pageToken")
        if next_page_token:
            page_token = next_page_token
            continue

        page_count = req_params.get("count", count)
        start = int(req_params.get("start", 0))
        total = paging.get("total")
        next_start = start + page_count

        if total is not None and next_start < int(total):
            params["start"] = next_start
            continue

        if len(items) >= page_count:
            params["start"] = next_start
            continue

        break

    return all_items


def _save_connection(
    uid,
    linkedin_member_id,
    access_token,
    refresh_token,
    expires_at,
    linkedin_version,
):
    con = get_db()
    cur = con.cursor()

    enc_access = encrypt_value(access_token) if access_token else None
    enc_refresh = encrypt_value(refresh_token) if refresh_token else None

    cur.execute(
        """
        INSERT OR REPLACE INTO linkedin_connections
        (uid, linkedin_member_id, access_token, refresh_token, expires_at, linkedin_version, connected_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            uid,
            linkedin_member_id,
            enc_access,
            enc_refresh,
            expires_at,
            linkedin_version,
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


def _exchange_code_for_token(uid, code):
    cfg = _get_config(uid)
    if not cfg:
        raise Exception("LinkedIn app not configured")

    client_id = cfg.get("client_id")
    client_secret = cfg.get("client_secret")
    redirect_uri = cfg.get("scopes")
    if not client_id or not client_secret or not redirect_uri:
        raise Exception("LinkedIn app missing client_id, client_secret or redirect_uri")

    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
        "client_id": client_id,
        "client_secret": client_secret,
    }

    res = _request_with_retry(
        "POST",
        TOKEN_URL,
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )

    if res.status_code != 200:
        raise Exception(f"LinkedIn token exchange failed ({res.status_code}): {res.text[:300]}")

    token_data = res.json()
    if not token_data.get("access_token"):
        raise Exception(f"LinkedIn token exchange error: {token_data}")
    return token_data


def _refresh_access_token(uid, refresh_token):
    cfg = _get_config(uid)
    if not cfg:
        raise Exception("LinkedIn app not configured")

    client_id = cfg.get("client_id")
    client_secret = cfg.get("client_secret")
    if not client_id or not client_secret:
        raise Exception("LinkedIn app missing client credentials")

    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
    }

    res = _request_with_retry(
        "POST",
        TOKEN_URL,
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )

    if res.status_code != 200:
        raise Exception(f"LinkedIn token refresh failed ({res.status_code}): {res.text[:300]}")

    token_data = res.json()
    access_token = token_data.get("access_token")
    next_refresh = token_data.get("refresh_token") or refresh_token
    expires_in = int(token_data.get("expires_in") or 5184000)
    expires_at = (
        datetime.datetime.now(datetime.UTC) + datetime.timedelta(seconds=expires_in)
    ).isoformat()

    if not access_token:
        raise Exception(f"LinkedIn token refresh error: {token_data}")

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
        UPDATE linkedin_connections
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

    member_id = conn.get("linkedin_member_id")
    access_token = conn.get("access_token")
    refresh_token = conn.get("refresh_token")
    expires_at = conn.get("expires_at")
    linkedin_version = conn.get("linkedin_version") or DEFAULT_LINKEDIN_VERSION

    if access_token and not _token_expired(expires_at):
        return member_id, access_token, linkedin_version

    if not refresh_token:
        return member_id, access_token, linkedin_version

    new_access, _, _ = _refresh_access_token(uid, refresh_token)
    return member_id, new_access, linkedin_version


def _fetch_member_id(access_token, linkedin_version):
    payload = _api_get(access_token, "/v2/userinfo", linkedin_version, {})
    return payload.get("sub") or payload.get("id")


def handle_linkedin_oauth_callback(uid, code):
    token_data = _exchange_code_for_token(uid, code)
    access_token = token_data.get("access_token")
    refresh_token = token_data.get("refresh_token")
    expires_in = int(token_data.get("expires_in") or 5184000)
    expires_at = (
        datetime.datetime.now(datetime.UTC) + datetime.timedelta(seconds=expires_in)
    ).isoformat()

    cfg = _get_config(uid) or {}
    linkedin_version = cfg.get("api_key") or DEFAULT_LINKEDIN_VERSION
    member_id = _fetch_member_id(access_token, linkedin_version) or ""

    _save_connection(
        uid,
        member_id,
        access_token,
        refresh_token,
        expires_at,
        linkedin_version,
    )
    return {
        "status": "success",
        "linkedin_member_id": member_id,
        "expires_at": expires_at,
    }


def _upsert_account(cur, uid, account, fetched_at):
    account_urn = account.get("id") or account.get("account") or account.get("accountId")
    account_id = str(account_urn or "")
    if account_id.startswith("urn:li:sponsoredAccount:"):
        account_id = account_id.split(":")[-1]
    if not account_id:
        return None

    cur.execute(
        """
        INSERT OR REPLACE INTO linkedin_ad_accounts
        (uid, account_id, name, status, type, currency, test, raw_json, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            uid,
            account_id,
            account.get("name") or account.get("reference"),
            account.get("status"),
            account.get("type"),
            account.get("currency"),
            str(account.get("test")) if account.get("test") is not None else None,
            json.dumps(account),
            fetched_at,
        ),
    )
    return account_id


def _upsert_campaign(cur, uid, account_id, campaign, fetched_at):
    campaign_urn = campaign.get("id") or campaign.get("campaign") or campaign.get("campaignId")
    campaign_id = str(campaign_urn or "")
    if campaign_id.startswith("urn:li:sponsoredCampaign:"):
        campaign_id = campaign_id.split(":")[-1]
    if not campaign_id:
        return None

    cur.execute(
        """
        INSERT OR REPLACE INTO linkedin_campaigns
        (uid, account_id, campaign_id, name, status, daily_budget, objective, start_date, end_date, raw_json, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            uid,
            account_id,
            campaign_id,
            campaign.get("name"),
            campaign.get("status"),
            str(campaign.get("dailyBudget")) if campaign.get("dailyBudget") is not None else None,
            campaign.get("objectiveType") or campaign.get("objective"),
            campaign.get("runSchedule", {}).get("start") if isinstance(campaign.get("runSchedule"), dict) else campaign.get("startDate"),
            campaign.get("runSchedule", {}).get("end") if isinstance(campaign.get("runSchedule"), dict) else campaign.get("endDate"),
            json.dumps(campaign),
            fetched_at,
        ),
    )
    return campaign_id


def _upsert_creative(cur, uid, account_id, creative, fetched_at):
    creative_urn = creative.get("id") or creative.get("creative") or creative.get("creativeId")
    creative_id = str(creative_urn or "")
    if creative_id.startswith("urn:li:sponsoredCreative:"):
        creative_id = creative_id.split(":")[-1]
    if not creative_id:
        return None

    campaign_ref = creative.get("campaign")
    campaign_id = None
    if campaign_ref:
        campaign_id = str(campaign_ref).split(":")[-1]

    cur.execute(
        """
        INSERT OR REPLACE INTO linkedin_creatives
        (uid, account_id, creative_id, campaign_id, intended_status, is_serving, review_status, raw_json, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            uid,
            account_id,
            creative_id,
            campaign_id,
            creative.get("intendedStatus") or creative.get("status"),
            str(creative.get("isServing")) if creative.get("isServing") is not None else None,
            creative.get("reviewStatus"),
            json.dumps(creative),
            fetched_at,
        ),
    )
    return creative_id


def _fetch_analytics_for_account(
    access_token,
    linkedin_version,
    account_id,
    start_date,
    end_date,
    pivot,
):
    sy, sm, sd = _date_parts(start_date)
    ey, em, ed = _date_parts(end_date)

    params = {
        "q": "analytics",
        "pivot": pivot,
        "dateRange.start.year": sy,
        "dateRange.start.month": sm,
        "dateRange.start.day": sd,
        "dateRange.end.year": ey,
        "dateRange.end.month": em,
        "dateRange.end.day": ed,
        "accounts[0]": _urn_account(account_id),
    }

    return _fetch_paginated(
        access_token,
        "/rest/adAnalytics",
        linkedin_version,
        base_params=params,
        count=100,
    )


def _analytics_pivot_value(item, pivot):
    values = item.get("pivotValues")
    if isinstance(values, list) and values:
        return str(values[0])
    if pivot == "ACCOUNT":
        return str(item.get("account") or item.get("pivotValue") or "")
    if pivot == "CAMPAIGN":
        return str(item.get("campaign") or item.get("pivotValue") or "")
    if pivot == "CREATIVE":
        return str(item.get("creative") or item.get("pivotValue") or "")
    return str(item.get("pivotValue") or "")


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


def sync_linkedin(uid, sync_type="historical"):
    try:
        _, access_token, linkedin_version = _ensure_valid_token(uid)
        if not access_token:
            return {"status": "error", "message": "LinkedIn not connected"}

        state = get_state(uid)
        today = datetime.date.today()
        end_date = today.isoformat()
        if sync_type == "incremental" and state.get("last_sync_date"):
            try:
                last_dt = datetime.datetime.strptime(state.get("last_sync_date"), "%Y-%m-%d").date()
                start_date = (last_dt + datetime.timedelta(days=1)).isoformat()
            except Exception:
                start_date = (today - datetime.timedelta(days=30)).isoformat()
            if start_date > end_date:
                start_date = end_date
        else:
            start_date = (today - datetime.timedelta(days=30)).isoformat()

        con = get_db()
        cur = con.cursor()
        fetched_at = datetime.datetime.now(datetime.UTC).isoformat()
        rows = []

        accounts_raw = _fetch_paginated(
            access_token,
            "/rest/adAccounts",
            linkedin_version,
            base_params={"start": 0, "count": 100},
            count=100,
        )

        account_ids = []
        for account in accounts_raw:
            account_id = _upsert_account(cur, uid, account, fetched_at)
            if not account_id:
                continue
            account_ids.append(account_id)
            rows.append(
                {
                    "entity": "account",
                    "uid": uid,
                    "account_id": account_id,
                    "name": account.get("name"),
                    "status": account.get("status"),
                }
            )

        campaign_count = 0
        creative_count = 0
        analytics_count = 0

        for account_id in account_ids:
            account_path_id = quote(str(account_id), safe="")

            campaigns_raw = _fetch_paginated(
                access_token,
                f"/rest/adAccounts/{account_path_id}/adCampaigns",
                linkedin_version,
                base_params={"start": 0, "count": 100},
                count=100,
            )
            for campaign in campaigns_raw:
                campaign_id = _upsert_campaign(cur, uid, account_id, campaign, fetched_at)
                if not campaign_id:
                    continue
                campaign_count += 1
                rows.append(
                    {
                        "entity": "campaign",
                        "uid": uid,
                        "account_id": account_id,
                        "campaign_id": campaign_id,
                        "name": campaign.get("name"),
                        "status": campaign.get("status"),
                    }
                )

            creatives_raw = _fetch_paginated(
                access_token,
                f"/rest/adAccounts/{account_path_id}/creatives",
                linkedin_version,
                base_params={"start": 0, "count": 100},
                count=100,
            )
            for creative in creatives_raw:
                creative_id = _upsert_creative(cur, uid, account_id, creative, fetched_at)
                if not creative_id:
                    continue
                creative_count += 1
                rows.append(
                    {
                        "entity": "creative",
                        "uid": uid,
                        "account_id": account_id,
                        "creative_id": creative_id,
                        "campaign_id": creative.get("campaign"),
                    }
                )

            for pivot in ("ACCOUNT", "CAMPAIGN", "CREATIVE"):
                analytics_raw = _fetch_analytics_for_account(
                    access_token,
                    linkedin_version,
                    account_id,
                    start_date,
                    end_date,
                    pivot,
                )
                for item in analytics_raw:
                    pivot_value = _analytics_pivot_value(item, pivot)
                    if pivot_value.startswith("urn:"):
                        pivot_value = pivot_value.split(":")[-1]

                    impressions = item.get("impressions")
                    clicks = item.get("clicks")
                    cost = item.get("costInLocalCurrency") or item.get("costInUsd")

                    cur.execute(
                        """
                        INSERT OR REPLACE INTO linkedin_ad_analytics
                        (uid, account_id, pivot_type, pivot_value, impressions, clicks, cost_in_local_currency, date_start, date_end, raw_json, fetched_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            uid,
                            account_id,
                            pivot,
                            pivot_value,
                            str(impressions) if impressions is not None else None,
                            str(clicks) if clicks is not None else None,
                            str(cost) if cost is not None else None,
                            start_date,
                            end_date,
                            json.dumps(item),
                            fetched_at,
                        ),
                    )
                    analytics_count += 1
                    rows.append(
                        {
                            "entity": "analytics",
                            "uid": uid,
                            "account_id": account_id,
                            "pivot_type": pivot,
                            "pivot_value": pivot_value,
                            "date_start": start_date,
                            "date_end": end_date,
                            "impressions": impressions,
                            "clicks": clicks,
                            "cost_in_local_currency": cost,
                        }
                    )

        con.commit()
        con.close()

        save_state(uid, {"last_sync_date": end_date})

        dest_cfg = get_active_destination(uid)
        if not dest_cfg:
            return {
                "status": "success",
                "accounts": len(account_ids),
                "campaigns": campaign_count,
                "creatives": creative_count,
                "analytics": analytics_count,
                "rows_found": len(rows),
                "rows_pushed": 0,
                "sync_type": sync_type,
                "message": "No active destination",
            }

        pushed = push_to_destination(dest_cfg, SOURCE, rows) if rows else 0
        return {
            "status": "success",
            "accounts": len(account_ids),
            "campaigns": campaign_count,
            "creatives": creative_count,
            "analytics": analytics_count,
            "rows_found": len(rows),
            "rows_pushed": pushed,
            "sync_type": sync_type,
        }

    except PermissionError as e:
        _log(f"sync permission error: {e}")
        return {"status": "error", "message": str(e)}
    except Exception as e:
        _log(f"sync failed: {e}")
        return {"status": "error", "message": str(e)}


def disconnect_linkedin(uid):
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

    cur.execute("DELETE FROM linkedin_connections WHERE uid=?", (uid,))

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
