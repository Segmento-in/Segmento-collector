import base64
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
SOURCE = "outbrain"
LOGIN_URL = "https://api.outbrain.com/amplify/v0.1/login"
API_BASE = "https://api.outbrain.com/amplify/v0.1"
TOKEN_TTL_SECONDS = 30 * 24 * 60 * 60
PERIODIC_BREAKDOWNS = ("daily", "weekly", "monthly")


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(message):
    print(f"[OUTBRAIN] {message}", flush=True)


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
        SELECT marketer_id, access_token, expires_at
        FROM outbrain_connections
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


def _basic_auth_header(username, password):
    encoded = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("utf-8")
    return {"Authorization": f"Basic {encoded}"}


def _save_connection(uid, marketer_id, access_token, expires_at):
    con = get_db()
    cur = con.cursor()
    enc_access = encrypt_value(access_token) if access_token else None

    cur.execute(
        """
        INSERT OR REPLACE INTO outbrain_connections
        (uid, marketer_id, access_token, expires_at, connected_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            uid,
            marketer_id,
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


def _extract_login_token(payload, headers):
    if isinstance(payload, dict):
        for key in ("OB-TOKEN-V1", "ob-token-v1", "token", "authToken", "obToken"):
            token = payload.get(key)
            if token:
                return str(token)
        metadata = payload.get("metadata")
        if isinstance(metadata, dict):
            token = metadata.get("OB-TOKEN-V1") or metadata.get("token")
            if token:
                return str(token)
    token_header = headers.get("OB-TOKEN-V1") or headers.get("ob-token-v1")
    if token_header:
        return str(token_header)
    return None


def _request_access_token(uid):
    cfg = _get_config(uid)
    if not cfg:
        raise Exception("Outbrain app not configured")

    username = cfg.get("client_id")
    password = cfg.get("client_secret")
    if not username or not password:
        raise Exception("Outbrain app missing username or password")

    res = _request_with_retry(
        "GET",
        LOGIN_URL,
        headers={
            **_basic_auth_header(username, password),
            "Content-Type": "application/json",
        },
    )

    if res.status_code == 401:
        raise PermissionError("Outbrain login returned 401 unauthorized")
    if res.status_code == 403:
        raise PermissionError("Outbrain login returned 403 forbidden")
    if res.status_code == 429:
        raise Exception("Outbrain login rate limited after retries")
    if res.status_code >= 500:
        raise Exception(f"Outbrain login server error ({res.status_code})")
    if res.status_code < 200 or res.status_code >= 300:
        raise Exception(f"Outbrain login error ({res.status_code}): {res.text[:300]}")

    body = {}
    try:
        body = res.json()
    except Exception:
        body = {}

    token = _extract_login_token(body, res.headers)
    if not token:
        raise Exception(f"Outbrain login response missing OB-TOKEN-V1: {body}")

    expires_at = (
        datetime.datetime.now(datetime.UTC) + datetime.timedelta(seconds=TOKEN_TTL_SECONDS)
    ).isoformat()
    return token, expires_at


def _ensure_valid_token(uid):
    conn = _get_connection(uid)
    cfg = _get_config(uid) or {}
    preferred_marketer = _as_text(cfg.get("api_key"))

    if conn:
        marketer_id = _as_text(conn.get("marketer_id")) or preferred_marketer
        token = conn.get("access_token")
        expires_at = conn.get("expires_at")
        if token and not _token_expired(expires_at):
            return marketer_id, token, expires_at

    token, expires_at = _request_access_token(uid)
    marketer_id = ((conn or {}).get("marketer_id") or preferred_marketer) if conn else preferred_marketer
    _save_connection(uid, _as_text(marketer_id), token, expires_at)
    return _as_text(marketer_id), token, expires_at


def _api_get_url(token, url, params=None):
    res = _request_with_retry(
        "GET",
        url,
        params=params or {},
        headers={
            "OB-TOKEN-V1": token,
            "Content-Type": "application/json",
        },
    )

    if res.status_code == 401:
        raise PermissionError("Outbrain API 401 unauthorized")
    if res.status_code == 403:
        raise PermissionError("Outbrain API 403 forbidden")
    if res.status_code == 429:
        raise Exception("Outbrain API rate limited after retries")
    if res.status_code >= 500:
        raise Exception(f"Outbrain API server error ({res.status_code})")
    if res.status_code < 200 or res.status_code >= 300:
        raise Exception(f"Outbrain API error ({res.status_code}): {res.text[:300]}")

    try:
        return res.json()
    except Exception:
        return {}


def _api_get(token, path, params=None):
    return _api_get_url(token, f"{API_BASE}/{path.strip('/')}", params=params)


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


def _iter_paginated(token, path, params=None):
    next_url = f"{API_BASE}/{path.strip('/')}"
    next_params = dict(params or {})

    for _ in range(500):
        payload = _api_get_url(token, next_url, params=next_params)
        records = _extract_records(payload)
        for item in records:
            yield item

        next_url, next_params = _next_request(next_url, next_params, payload, records)
        if not next_url:
            break

        if "?" in next_url:
            parsed = urlparse(next_url)
            base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            qs = parse_qs(parsed.query)
            merged = dict(next_params or {})
            for key, vals in qs.items():
                merged[key] = vals[-1] if vals else None
            next_url = base_url
            next_params = merged


def _sync_window(sync_type, state):
    today = datetime.date.today()
    end_date = today.isoformat()
    default_start = (today - datetime.timedelta(days=30)).isoformat()

    if sync_type != "incremental" or not state.get("last_sync_date"):
        return default_start, end_date

    try:
        last_dt = datetime.datetime.strptime(state.get("last_sync_date"), "%Y-%m-%d").date()
        start_date = (last_dt + datetime.timedelta(days=1)).isoformat()
    except Exception:
        start_date = default_start

    if start_date > end_date:
        start_date = end_date
    return start_date, end_date


def _extract_marketers(payload):
    records = _extract_records(payload)
    if not records and isinstance(payload, dict):
        if payload.get("id") or payload.get("marketerId"):
            records = [payload]
    result = []
    for item in records:
        marketer_id = item.get("marketerId") or item.get("id")
        if not marketer_id:
            continue
        result.append(
            {
                "marketer_id": _as_text(marketer_id),
                "name": item.get("name"),
                "raw": item,
            }
        )
    return result


def _discover_marketers(token):
    payload = _api_get(token, "marketers", {})
    return _extract_marketers(payload)


def _resolve_marketers(uid, discovered):
    cfg = _get_config(uid) or {}
    preferred = _as_text(cfg.get("api_key"))
    discovered_ids = [m["marketer_id"] for m in discovered]

    if preferred and preferred in discovered_ids:
        return [preferred]
    if preferred and not discovered_ids:
        return [preferred]
    return discovered_ids


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


def connect_outbrain(uid):
    try:
        _, token, expires_at = _ensure_valid_token(uid)
        marketers = _discover_marketers(token)
        target_marketers = _resolve_marketers(uid, marketers)
        selected = target_marketers[0] if target_marketers else None
        if not selected and marketers:
            selected = marketers[0]["marketer_id"]
        if not selected:
            return {
                "status": "error",
                "message": "No marketer accounts found for these credentials",
            }

        _save_connection(uid, selected, token, expires_at)
        return {
            "status": "success",
            "marketer_id": selected,
            "marketers": [m["marketer_id"] for m in marketers],
            "expires_at": expires_at,
        }
    except PermissionError as e:
        _update_error_status(uid)
        return {"status": "error", "message": str(e)}
    except Exception as e:
        _log(f"connect failed: {e}")
        return {"status": "error", "message": str(e)}


def _upsert_marketer(cur, uid, marketer, fetched_at):
    cur.execute(
        """
        INSERT OR REPLACE INTO outbrain_marketers
        (uid, marketer_id, name, raw_json, fetched_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            uid,
            marketer["marketer_id"],
            marketer.get("name"),
            json.dumps(marketer.get("raw") or {}),
            fetched_at,
        ),
    )

    return {
        "entity": "marketer",
        "uid": uid,
        "marketer_id": marketer["marketer_id"],
        "name": marketer.get("name"),
    }


def _upsert_campaign_report(cur, uid, marketer_id, item, date_value, breakdown, fetched_at):
    campaign_id = _as_text(item.get("campaignId") or item.get("id")) or ""
    campaign_name = item.get("campaignName") or item.get("name")
    date_txt = _as_text(date_value) or ""

    cur.execute(
        """
        INSERT OR REPLACE INTO outbrain_campaign_reports
        (uid, marketer_id, campaign_id, campaign_name, breakdown, impressions, clicks, ctr, spend, conversions, date, raw_json, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            uid,
            marketer_id,
            campaign_id,
            campaign_name,
            breakdown,
            _as_text(item.get("impressions")),
            _as_text(item.get("clicks")),
            _as_text(item.get("ctr")),
            _as_text(item.get("spend")),
            _as_text(item.get("conversions")),
            date_txt,
            json.dumps(item),
            fetched_at,
        ),
    )

    return {
        "entity": "campaign_report",
        "uid": uid,
        "marketer_id": marketer_id,
        "campaign_id": campaign_id,
        "campaign_name": campaign_name,
        "breakdown": breakdown,
        "impressions": _as_text(item.get("impressions")),
        "clicks": _as_text(item.get("clicks")),
        "ctr": _as_text(item.get("ctr")),
        "spend": _as_text(item.get("spend")),
        "conversions": _as_text(item.get("conversions")),
        "date": date_txt,
    }


def _upsert_ad_report(cur, uid, marketer_id, item, date_value, fetched_at):
    campaign_id = _as_text(item.get("campaignId")) or ""
    promoted_link_id = _as_text(item.get("promotedLinkId") or item.get("id")) or ""
    date_txt = _as_text(date_value) or ""

    cur.execute(
        """
        INSERT OR REPLACE INTO outbrain_ads
        (uid, marketer_id, campaign_id, promoted_link_id, promoted_link_text, impressions, clicks, ctr, spend, conversions, date, raw_json, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            uid,
            marketer_id,
            campaign_id,
            promoted_link_id,
            item.get("promotedLinkText") or item.get("title"),
            _as_text(item.get("impressions")),
            _as_text(item.get("clicks")),
            _as_text(item.get("ctr")),
            _as_text(item.get("spend")),
            _as_text(item.get("conversions")),
            date_txt,
            json.dumps(item),
            fetched_at,
        ),
    )

    return {
        "entity": "ad_report",
        "uid": uid,
        "marketer_id": marketer_id,
        "campaign_id": campaign_id,
        "promoted_link_id": promoted_link_id,
        "promoted_link_text": item.get("promotedLinkText") or item.get("title"),
        "impressions": _as_text(item.get("impressions")),
        "clicks": _as_text(item.get("clicks")),
        "ctr": _as_text(item.get("ctr")),
        "spend": _as_text(item.get("spend")),
        "conversions": _as_text(item.get("conversions")),
        "date": date_txt,
    }


def sync_outbrain(uid, sync_type="historical"):
    try:
        marketer_id, token, _ = _ensure_valid_token(uid)
        if not token:
            return {"status": "error", "message": "Outbrain not connected"}

        marketers = _discover_marketers(token)
        target_marketers = _resolve_marketers(uid, marketers)
        if marketer_id and marketer_id not in target_marketers:
            target_marketers = [marketer_id] + target_marketers
        if not target_marketers and marketer_id:
            target_marketers = [marketer_id]
        if not target_marketers:
            return {"status": "error", "message": "No marketer accounts discovered"}

        state = get_state(uid)
        start_date, end_date = _sync_window(sync_type, state)
        fetched_at = datetime.datetime.now(datetime.UTC).isoformat()

        con = get_db()
        cur = con.cursor()
        rows = []
        marketer_count = 0
        campaign_count = 0
        ad_count = 0
        periodic_count = 0

        marketers_map = {m["marketer_id"]: m for m in marketers}
        for mid in target_marketers:
            marketer_obj = marketers_map.get(mid) or {"marketer_id": mid, "name": None, "raw": {"marketerId": mid}}
            rows.append(_upsert_marketer(cur, uid, marketer_obj, fetched_at))
            marketer_count += 1

            campaign_path = f"reports/marketers/{mid}/campaigns"
            params = {"from": start_date, "to": end_date}
            campaign_rows = []
            for item in _iter_paginated(token, campaign_path, params=params):
                date_value = item.get("date") or item.get("day") or end_date
                rows.append(_upsert_campaign_report(cur, uid, mid, item, date_value, "summary", fetched_at))
                campaign_rows.append(item)
                campaign_count += 1

            ad_path = f"reports/marketers/{mid}/content"
            for item in _iter_paginated(token, ad_path, params=params):
                date_value = item.get("date") or item.get("day") or end_date
                rows.append(_upsert_ad_report(cur, uid, mid, item, date_value, fetched_at))
                ad_count += 1

            campaign_ids = []
            for item in campaign_rows:
                cid = _as_text(item.get("campaignId") or item.get("id"))
                if cid:
                    campaign_ids.append(cid)
            campaign_ids = list(dict.fromkeys(campaign_ids))

            for cid in campaign_ids:
                periodic_path = f"reports/marketers/{mid}/campaigns/{cid}/periodicContent"
                for breakdown in PERIODIC_BREAKDOWNS:
                    periodic_params = {"from": start_date, "to": end_date, "breakdown": breakdown}
                    try:
                        for item in _iter_paginated(token, periodic_path, params=periodic_params):
                            date_value = (
                                item.get("date")
                                or item.get("startDate")
                                or item.get("day")
                                or end_date
                            )
                            item_with_campaign = dict(item)
                            item_with_campaign.setdefault("campaignId", cid)
                            rows.append(
                                _upsert_campaign_report(
                                    cur,
                                    uid,
                                    mid,
                                    item_with_campaign,
                                    date_value,
                                    breakdown,
                                    fetched_at,
                                )
                            )
                            periodic_count += 1
                    except Exception as e:
                        _log(
                            f"periodic report skipped for marketer={mid}, campaign={cid}, "
                            f"breakdown={breakdown}: {e}"
                        )

        con.commit()
        con.close()

        save_state(uid, {"last_sync_date": end_date})

        dest_cfg = get_active_destination(uid)
        if not dest_cfg:
            return {
                "status": "success",
                "marketers": marketer_count,
                "campaign_reports": campaign_count,
                "ads": ad_count,
                "periodic_rows": periodic_count,
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
            "marketers": marketer_count,
            "campaign_reports": campaign_count,
            "ads": ad_count,
            "periodic_rows": periodic_count,
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


def disconnect_outbrain(uid):
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

    cur.execute("DELETE FROM outbrain_connections WHERE uid=?", (uid,))

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
