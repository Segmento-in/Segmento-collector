import datetime
import json
import sqlite3
import time
from urllib.parse import parse_qs, urlparse

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure


DB = "identity.db"
SOURCE = "taboola"
TOKEN_URL = "https://backstage.taboola.com/backstage/oauth/token"
API_BASE = "https://backstage.taboola.com/backstage/api/1.0"
TOKEN_TTL_SECONDS = 12 * 60 * 60
CAMPAIGN_DIMENSIONS = ("day", "campaign", "site", "country", "platform")
REVENUE_DIMENSIONS = ("day", "site", "country", "platform")


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(message):
    print(f"[TABOOLA] {message}", flush=True)


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


def _as_text(value):
    if value is None:
        return None
    return str(value)


def _get_config(uid):
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT client_id, client_secret, api_key, access_token
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
        SELECT account_id, access_token, expires_at
        FROM taboola_connections
        WHERE uid=?
        LIMIT 1
        """,
        (uid,),
    )
    row = fetchone_secure(cur)
    con.close()
    return row


def _update_error_status(uid):
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


def _request_with_retry(method, url, retries=5, timeout=40, **kwargs):
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

        remaining = (
            res.headers.get("X-RateLimit-Remaining")
            or res.headers.get("x-ratelimit-remaining")
            or res.headers.get("RateLimit-Remaining")
        )
        reset_raw = res.headers.get("Retry-After") or res.headers.get("X-RateLimit-Reset")
        if remaining is not None:
            _log(f"rate remaining={remaining}, reset={reset_raw}")

        if res.status_code == 429:
            retry_wait = 0
            if reset_raw:
                try:
                    retry_wait = int(reset_raw)
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


def _save_connection(uid, account_id, access_token, expires_at):
    con = get_db()
    cur = con.cursor()
    enc_access = encrypt_value(access_token) if access_token else None

    cur.execute(
        """
        INSERT OR REPLACE INTO taboola_connections
        (uid, account_id, access_token, expires_at, connected_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            uid,
            account_id,
            enc_access,
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
        SET access_token=?, status='connected'
        WHERE uid=? AND connector=?
        """,
        (enc_access, uid, SOURCE),
    )

    con.commit()
    con.close()


def _request_access_token(uid):
    cfg = _get_config(uid)
    if not cfg:
        raise Exception("Taboola app not configured")

    client_id = cfg.get("client_id")
    client_secret = cfg.get("client_secret")
    if not client_id or not client_secret:
        raise Exception("Taboola app missing client_id or client_secret")

    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    res = _request_with_retry(
        "POST",
        TOKEN_URL,
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )

    if res.status_code == 401:
        raise PermissionError("Taboola token endpoint returned 401 unauthorized")
    if res.status_code == 403:
        raise PermissionError("Taboola token endpoint returned 403 forbidden")
    if res.status_code == 429:
        raise Exception("Taboola token endpoint rate limited after retries")
    if res.status_code >= 500:
        raise Exception(f"Taboola token endpoint server error ({res.status_code})")
    if res.status_code < 200 or res.status_code >= 300:
        raise Exception(f"Taboola token error ({res.status_code}): {res.text[:300]}")

    body = res.json()
    access_token = body.get("access_token")
    expires_in = int(body.get("expires_in") or TOKEN_TTL_SECONDS)
    expires_at = (
        datetime.datetime.now(datetime.UTC) + datetime.timedelta(seconds=expires_in)
    ).isoformat()

    if not access_token:
        raise Exception(f"Taboola token response missing access_token: {body}")

    return access_token, expires_at


def _ensure_valid_token(uid):
    conn = _get_connection(uid)
    cfg = _get_config(uid) or {}
    preferred_account = cfg.get("api_key")

    if conn:
        account_id = conn.get("account_id") or preferred_account
        access_token = conn.get("access_token")
        expires_at = conn.get("expires_at")

        if access_token and not _token_expired(expires_at):
            return account_id, access_token, expires_at

    access_token, expires_at = _request_access_token(uid)
    account_id = ((conn or {}).get("account_id") or preferred_account) if conn else preferred_account
    _save_connection(uid, account_id, access_token, expires_at)
    return account_id, access_token, expires_at


def _api_get_url(access_token, url, params=None):
    res = _request_with_retry(
        "GET",
        url,
        params=params or {},
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        },
    )

    if res.status_code == 401:
        raise PermissionError("Taboola API 401 unauthorized")
    if res.status_code == 403:
        raise PermissionError("Taboola API 403 forbidden")
    if res.status_code == 429:
        raise Exception("Taboola API rate limited after retries")
    if res.status_code >= 500:
        raise Exception(f"Taboola API server error ({res.status_code})")
    if res.status_code < 200 or res.status_code >= 300:
        raise Exception(f"Taboola API error ({res.status_code}): {res.text[:300]}")

    try:
        return res.json()
    except Exception:
        return {}


def _api_get(access_token, path, params=None):
    p = path.strip("/")
    return _api_get_url(access_token, f"{API_BASE}/{p}", params=params)


def _extract_records(payload):
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []

    for key in ("results", "data", "items", "records", "list"):
        value = payload.get(key)
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            for nested in ("results", "items", "records", "data"):
                nested_value = value.get(nested)
                if isinstance(nested_value, list):
                    return nested_value
    return []


def _next_request(current_url, current_params, payload, records):
    if not isinstance(payload, dict):
        return None, None

    paging = payload.get("paging") if isinstance(payload.get("paging"), dict) else {}
    meta = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}

    next_candidate = (
        paging.get("next")
        or meta.get("next")
        or payload.get("next")
        or payload.get("next_page")
    )

    if isinstance(next_candidate, str) and next_candidate:
        if next_candidate.startswith("http"):
            return next_candidate, None
        if next_candidate.startswith("/"):
            parsed = urlparse(current_url)
            return f"{parsed.scheme}://{parsed.netloc}{next_candidate}", None
        if next_candidate.startswith("?"):
            parsed = urlparse(current_url)
            return f"{parsed.scheme}://{parsed.netloc}{parsed.path}{next_candidate}", None
        next_params = dict(current_params or {})
        next_params["page"] = next_candidate
        return current_url, next_params

    if isinstance(next_candidate, dict):
        next_params = dict(current_params or {})
        next_params.update(next_candidate)
        return current_url, next_params

    token = (
        paging.get("next_token")
        or paging.get("nextToken")
        or paging.get("next_cursor")
        or meta.get("next_token")
        or payload.get("next_token")
    )
    if token:
        next_params = dict(current_params or {})
        next_params["page_token"] = token
        return current_url, next_params

    has_more = (
        payload.get("has_more")
        or payload.get("hasMore")
        or paging.get("has_more")
        or meta.get("has_more")
    )
    if has_more:
        next_params = dict(current_params or {})
        page_num = int(next_params.get("page") or 1)
        next_params["page"] = page_num + 1
        return current_url, next_params

    limit = (
        current_params or {}
    ).get("page_size") or (current_params or {}).get("limit") or 0
    if limit and len(records) >= int(limit):
        next_params = dict(current_params or {})
        page_num = int(next_params.get("page") or 1)
        next_params["page"] = page_num + 1
        return current_url, next_params

    return None, None


def _iter_paginated(access_token, path, params=None):
    next_url = f"{API_BASE}/{path.strip('/')}"
    next_params = dict(params or {})

    for _ in range(500):
        payload = _api_get_url(access_token, next_url, params=next_params)
        records = _extract_records(payload)
        for rec in records:
            yield rec

        next_url, next_params = _next_request(next_url, next_params, payload, records)
        if not next_url:
            break

        # If URL already has query string, absorb into params and normalize URL.
        if "?" in next_url:
            parsed = urlparse(next_url)
            base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            qs = parse_qs(parsed.query)
            merged = dict(next_params or {})
            for key, vals in qs.items():
                merged[key] = vals[-1] if vals else None
            next_url = base_url
            next_params = merged


def _discover_allowed_accounts(access_token):
    payload = _api_get(access_token, "users/current/allowed-accounts/", {})
    records = _extract_records(payload)
    if not records and isinstance(payload, dict):
        # Some responses may return a single object.
        if payload.get("account_id") or payload.get("id"):
            records = [payload]

    result = []
    for item in records:
        account_id = (
            item.get("account_id")
            or item.get("id")
            or item.get("account")
            or item.get("name")
        )
        if account_id:
            result.append(
                {
                    "account_id": str(account_id),
                    "name": item.get("name"),
                    "raw": item,
                }
            )
    return result


def _resolve_target_accounts(uid, discovered_accounts):
    cfg = _get_config(uid) or {}
    preferred = str(cfg.get("api_key") or "").strip()
    discovered_ids = [a["account_id"] for a in discovered_accounts]

    if preferred and preferred in discovered_ids:
        return [preferred]
    if preferred and not discovered_ids:
        return [preferred]
    if discovered_ids:
        return discovered_ids
    return []


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


def connect_taboola(uid):
    try:
        _, access_token, expires_at = _ensure_valid_token(uid)
        allowed_accounts = _discover_allowed_accounts(access_token)
        target_accounts = _resolve_target_accounts(uid, allowed_accounts)
        selected_account = target_accounts[0] if target_accounts else None

        if not selected_account and allowed_accounts:
            selected_account = allowed_accounts[0]["account_id"]

        if not selected_account:
            return {
                "status": "error",
                "message": "No allowed Taboola accounts found for these credentials",
            }

        _save_connection(uid, selected_account, access_token, expires_at)
        return {
            "status": "success",
            "account_id": selected_account,
            "allowed_accounts": [a["account_id"] for a in allowed_accounts],
            "expires_at": expires_at,
        }
    except PermissionError as e:
        _update_error_status(uid)
        return {"status": "error", "message": str(e)}
    except Exception as e:
        _log(f"connect failed: {e}")
        return {"status": "error", "message": str(e)}


def _sync_window(sync_type, state):
    today = datetime.date.today()
    end_date = today.isoformat()
    historical_start = (today - datetime.timedelta(days=30)).isoformat()

    if sync_type != "incremental" or not state.get("last_sync_date"):
        return historical_start, end_date

    try:
        last_dt = datetime.datetime.strptime(state.get("last_sync_date"), "%Y-%m-%d").date()
        since_last = (last_dt + datetime.timedelta(days=1)).isoformat()
    except Exception:
        since_last = historical_start

    first_of_current = today.replace(day=1)
    first_of_previous = (first_of_current - datetime.timedelta(days=1)).replace(day=1)
    freshness_floor = first_of_previous.isoformat()
    start_date = min(since_last, freshness_floor)
    if start_date > end_date:
        start_date = end_date
    return start_date, end_date


def _upsert_campaign_report(cur, uid, account_id, dimension, item, fetched_at):
    date_value = item.get("date") or item.get("day")
    dimension_value = (
        item.get("campaign_name")
        or item.get("campaign_id")
        or item.get("site")
        or item.get("country")
        or item.get("platform")
        or item.get(dimension)
    )
    campaign_id = item.get("campaign_id") or item.get("campaign")
    dimension_value_txt = _as_text(dimension_value) or ""
    campaign_id_txt = _as_text(campaign_id) or ""
    date_txt = _as_text(date_value) or ""

    cur.execute(
        """
        INSERT OR REPLACE INTO taboola_campaign_reports
        (uid, account_id, dimension, dimension_value, campaign_id, campaign_name, impressions, clicks, ctr, spent, cpc, cpm, conversions, conversion_rate, roas, date, raw_json, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            uid,
            account_id,
            dimension,
            dimension_value_txt,
            campaign_id_txt,
            item.get("campaign_name"),
            _as_text(item.get("impressions")),
            _as_text(item.get("clicks")),
            _as_text(item.get("ctr")),
            _as_text(item.get("spent")),
            _as_text(item.get("cpc")),
            _as_text(item.get("cpm")),
            _as_text(item.get("conversions")),
            _as_text(item.get("conversion_rate")),
            _as_text(item.get("roas")),
            date_txt,
            json.dumps(item),
            fetched_at,
        ),
    )

    return {
        "entity": "campaign_report",
        "uid": uid,
        "account_id": account_id,
        "dimension": dimension,
        "dimension_value": dimension_value_txt,
        "campaign_id": campaign_id_txt,
        "campaign_name": item.get("campaign_name"),
        "impressions": _as_text(item.get("impressions")),
        "clicks": _as_text(item.get("clicks")),
        "ctr": _as_text(item.get("ctr")),
        "spent": _as_text(item.get("spent")),
        "date": date_txt,
    }


def _upsert_ad_report(cur, uid, account_id, item, end_date, fetched_at):
    report_date = item.get("date") or item.get("day") or end_date
    campaign_id = item.get("campaign_id") or item.get("campaign")
    item_id = item.get("item_id") or item.get("id")
    campaign_id_txt = _as_text(campaign_id) or ""
    item_id_txt = _as_text(item_id) or ""
    report_date_txt = _as_text(report_date) or ""

    cur.execute(
        """
        INSERT OR REPLACE INTO taboola_ads
        (uid, account_id, campaign_id, campaign_name, item_id, item_name, thumbnail_url, url, impressions, clicks, ctr, cpc, spent, conversions, date, raw_json, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            uid,
            account_id,
            campaign_id_txt,
            item.get("campaign_name"),
            item_id_txt,
            item.get("item_name"),
            item.get("thumbnail_url"),
            item.get("url"),
            _as_text(item.get("impressions")),
            _as_text(item.get("clicks")),
            _as_text(item.get("ctr")),
            _as_text(item.get("cpc")),
            _as_text(item.get("spent")),
            _as_text(item.get("conversions")),
            report_date_txt,
            json.dumps(item),
            fetched_at,
        ),
    )

    return {
        "entity": "ad_report",
        "uid": uid,
        "account_id": account_id,
        "campaign_id": campaign_id_txt,
        "campaign_name": item.get("campaign_name"),
        "item_id": item_id_txt,
        "item_name": item.get("item_name"),
        "impressions": _as_text(item.get("impressions")),
        "clicks": _as_text(item.get("clicks")),
        "ctr": _as_text(item.get("ctr")),
        "spent": _as_text(item.get("spent")),
        "date": report_date_txt,
    }


def _upsert_revenue_report(cur, uid, account_id, dimension, item, fetched_at):
    date_value = item.get("date") or item.get("day")
    dimension_value = (
        item.get("site")
        or item.get("country")
        or item.get("platform")
        or item.get(dimension)
        or item.get("name")
    )
    dimension_value_txt = _as_text(dimension_value) or ""
    date_txt = _as_text(date_value) or ""

    cur.execute(
        """
        INSERT OR REPLACE INTO taboola_publisher_revenue
        (uid, account_id, dimension, dimension_value, page_views, page_views_with_ads_pct, ad_revenue, ad_rpm, ad_cpc, date, raw_json, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            uid,
            account_id,
            dimension,
            dimension_value_txt,
            _as_text(item.get("page_views")),
            _as_text(item.get("page_views_with_ads_pct")),
            _as_text(item.get("ad_revenue")),
            _as_text(item.get("ad_rpm")),
            _as_text(item.get("ad_cpc")),
            date_txt,
            json.dumps(item),
            fetched_at,
        ),
    )

    return {
        "entity": "publisher_revenue",
        "uid": uid,
        "account_id": account_id,
        "dimension": dimension,
        "dimension_value": dimension_value_txt,
        "page_views": _as_text(item.get("page_views")),
        "ad_revenue": _as_text(item.get("ad_revenue")),
        "ad_rpm": _as_text(item.get("ad_rpm")),
        "date": date_txt,
    }


def sync_taboola(uid, sync_type="historical"):
    try:
        account_id, access_token, _ = _ensure_valid_token(uid)
        if not access_token:
            return {"status": "error", "message": "Taboola not connected"}

        discovered_accounts = _discover_allowed_accounts(access_token)
        target_accounts = _resolve_target_accounts(uid, discovered_accounts)
        if account_id and account_id not in target_accounts:
            target_accounts = [account_id] + target_accounts
        if not target_accounts and account_id:
            target_accounts = [account_id]
        if not target_accounts:
            return {"status": "error", "message": "No allowed Taboola accounts discovered"}

        state = get_state(uid)
        start_date, end_date = _sync_window(sync_type, state)
        fetched_at = datetime.datetime.now(datetime.UTC).isoformat()

        con = get_db()
        cur = con.cursor()
        rows = []
        campaign_count = 0
        ads_count = 0
        revenue_count = 0

        for acct in target_accounts:
            for dimension in CAMPAIGN_DIMENSIONS:
                path = f"{acct}/reports/campaign-summary/dimensions/{dimension}"
                params = {"start_date": start_date, "end_date": end_date, "dimension": dimension}
                for item in _iter_paginated(access_token, path, params=params):
                    rows.append(_upsert_campaign_report(cur, uid, acct, dimension, item, fetched_at))
                    campaign_count += 1

            ads_path = f"{acct}/reports/top-campaign-content/dimensions/item_breakdown"
            ads_params = {
                "start_date": start_date,
                "end_date": end_date,
                "dimension": "item_breakdown",
            }
            for item in _iter_paginated(access_token, ads_path, params=ads_params):
                rows.append(_upsert_ad_report(cur, uid, acct, item, end_date, fetched_at))
                ads_count += 1

            for dimension in REVENUE_DIMENSIONS:
                revenue_path = f"{acct}/reports/revenue-summary/dimensions/{dimension}"
                revenue_params = {"start_date": start_date, "end_date": end_date, "dimension": dimension}
                try:
                    for item in _iter_paginated(access_token, revenue_path, params=revenue_params):
                        rows.append(_upsert_revenue_report(cur, uid, acct, dimension, item, fetched_at))
                        revenue_count += 1
                except Exception as e:
                    _log(f"revenue report skipped for account={acct}, dimension={dimension}: {e}")

        con.commit()
        con.close()

        save_state(uid, {"last_sync_date": end_date})

        dest_cfg = get_active_destination(uid)
        if not dest_cfg:
            return {
                "status": "success",
                "accounts": len(target_accounts),
                "campaign_reports": campaign_count,
                "ads": ads_count,
                "publisher_revenue": revenue_count,
                "rows_found": len(rows),
                "rows_pushed": 0,
                "sync_type": sync_type,
                "window_start": start_date,
                "window_end": end_date,
                "message": "No active destination",
            }

        pushed = push_to_destination(dest_cfg, SOURCE, rows) if rows else 0
        return {
            "status": "success",
            "accounts": len(target_accounts),
            "campaign_reports": campaign_count,
            "ads": ads_count,
            "publisher_revenue": revenue_count,
            "rows_found": len(rows),
            "rows_pushed": pushed,
            "sync_type": sync_type,
            "window_start": start_date,
            "window_end": end_date,
        }

    except PermissionError as e:
        _update_error_status(uid)
        return {"status": "error", "message": str(e)}
    except Exception as e:
        _log(f"sync failed: {e}")
        return {"status": "error", "message": str(e)}


def disconnect_taboola(uid):
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

    cur.execute("DELETE FROM taboola_connections WHERE uid=?", (uid,))

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
