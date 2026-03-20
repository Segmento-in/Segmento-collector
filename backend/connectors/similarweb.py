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
SOURCE = "similarweb"
API_BASE = "https://api.similarweb.com"
DEFAULT_LIMIT = 100


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(message):
    print(f"[SIMILARWEB] {message}", flush=True)


def _as_text(value):
    if value is None:
        return None
    return str(value)


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


def _api_get(url, params=None):
    res = _request_with_retry("GET", url, params=params or {})

    if res.status_code == 401:
        raise PermissionError("SimilarWeb API 401 unauthorized")
    if res.status_code == 403:
        raise PermissionError("SimilarWeb API 403 forbidden")
    if res.status_code == 429:
        raise Exception("SimilarWeb API rate limited after retries")
    if res.status_code >= 500:
        raise Exception(f"SimilarWeb API server error ({res.status_code})")
    if res.status_code < 200 or res.status_code >= 300:
        raise Exception(f"SimilarWeb API error ({res.status_code}): {res.text[:300]}")

    try:
        return res.json()
    except Exception:
        return {}


def _extract_records(payload):
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []

    for key in ("results", "data", "items", "records", "visits", "social", "keywords"):
        value = payload.get(key)
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            for nested in ("results", "items", "records", "data"):
                nested_value = value.get(nested)
                if isinstance(nested_value, list):
                    return nested_value
    return []


def _extract_single(payload):
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, list) and payload:
        if isinstance(payload[0], dict):
            return payload[0]
    return {}


def _next_request(current_url, current_params, payload, records):
    if not isinstance(payload, dict):
        return None, None

    paging = payload.get("paging") if isinstance(payload.get("paging"), dict) else {}
    meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}

    next_candidate = (
        paging.get("next")
        or payload.get("next")
        or payload.get("next_page")
        or meta.get("next")
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
        next_params["offset"] = next_candidate
        return current_url, next_params

    limit = int((current_params or {}).get("limit") or DEFAULT_LIMIT)
    offset = int((current_params or {}).get("offset") or 0)
    has_more = (
        payload.get("has_more")
        or payload.get("hasMore")
        or paging.get("has_more")
        or meta.get("has_more")
    )
    if has_more:
        next_params = dict(current_params or {})
        next_params["offset"] = offset + limit
        return current_url, next_params

    if records and len(records) >= limit:
        next_params = dict(current_params or {})
        next_params["offset"] = offset + limit
        return current_url, next_params

    return None, None


def _iter_paginated(path, params):
    next_url = f"{API_BASE}/{path.strip('/')}"
    next_params = dict(params or {})
    next_params.setdefault("limit", DEFAULT_LIMIT)
    next_params.setdefault("offset", 0)

    for _ in range(500):
        payload = _api_get(next_url, next_params)
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


def _get_config(uid):
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT api_key, scopes
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
        SELECT domain
        FROM similarweb_connections
        WHERE uid=?
        LIMIT 1
        """,
        (uid,),
    )
    row = fetchone_secure(cur)
    con.close()
    return row


def _get_api_key_and_domain(uid):
    cfg = _get_config(uid)
    if not cfg:
        return None, None
    return cfg.get("api_key"), cfg.get("scopes")


def _save_connection(uid, domain):
    con = get_db()
    cur = con.cursor()

    cur.execute(
        """
        INSERT OR REPLACE INTO similarweb_connections
        (uid, domain, connected_at)
        VALUES (?, ?, ?)
        """,
        (uid, domain, datetime.datetime.now(datetime.UTC).isoformat()),
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
        SET status='connected'
        WHERE uid=? AND connector=?
        """,
        (uid, SOURCE),
    )

    con.commit()
    con.close()


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


def _build_params(api_key, start_date, end_date, **extra):
    params = {
        "api_key": api_key,
        "start_date": start_date,
        "end_date": end_date,
        "limit": extra.pop("limit", DEFAULT_LIMIT),
        "offset": extra.pop("offset", 0),
        "sort": extra.pop("sort", "date"),
        "asc": extra.pop("asc", "false"),
    }
    params.update(extra)
    return params


def _extract_metric_value(payload, key):
    if not isinstance(payload, dict):
        return None
    if key in payload:
        return payload.get(key)
    data = payload.get("data")
    if isinstance(data, dict) and key in data:
        return data.get(key)
    if isinstance(data, list) and data:
        first = data[0]
        if isinstance(first, dict):
            return first.get(key)
    return None


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


def _validate_api_key(api_key, domain):
    if not api_key or not domain:
        return False
    test_path = f"v1/website/{domain}/total-traffic-and-engagement/visits"
    payload = _api_get(
        f"{API_BASE}/{test_path}",
        {"api_key": api_key, "limit": 1, "offset": 0},
    )
    if isinstance(payload, dict) and payload.get("error"):
        return False
    return True


def connect_similarweb(uid):
    try:
        api_key, domain = _get_api_key_and_domain(uid)
        if not api_key or not domain:
            return {"status": "error", "message": "SimilarWeb API key and domain are required"}

        if not _validate_api_key(api_key, domain):
            return {"status": "error", "message": "SimilarWeb API key validation failed"}

        _save_connection(uid, domain)
        return {"status": "success", "domain": domain}
    except PermissionError as e:
        _update_error_status(uid)
        return {"status": "error", "message": str(e)}
    except Exception as e:
        _log(f"connect failed: {e}")
        return {"status": "error", "message": str(e)}


def sync_similarweb(uid, sync_type="historical"):
    try:
        api_key, domain = _get_api_key_and_domain(uid)
        if not api_key or not domain:
            return {"status": "error", "message": "SimilarWeb API key and domain are required"}
        if not _validate_api_key(api_key, domain):
            return {"status": "error", "message": "SimilarWeb API key validation failed"}

        state = get_state(uid)
        start_date, end_date = _sync_window(sync_type, state)
        fetched_at = datetime.datetime.now(datetime.UTC).isoformat()

        params_base = _build_params(api_key, start_date, end_date)

        # 1) Domain overview
        visits_payload = _api_get(
            f"{API_BASE}/v1/website/{domain}/total-traffic-and-engagement/visits",
            params=params_base,
        )
        overview = _extract_single(visits_payload)

        # 2) Traffic sources
        traffic_payload = _api_get(
            f"{API_BASE}/v1/website/{domain}/traffic-sources",
            params=params_base,
        )
        traffic = _extract_single(traffic_payload)

        con = get_db()
        cur = con.cursor()
        rows = []

        cur.execute(
            """
            INSERT OR REPLACE INTO similarweb_domain_overview
            (uid, domain, date, visits, desktop_share, mobile_share, pages_per_visit, visit_duration, bounce_rate, raw_json, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                uid,
                domain,
                end_date,
                _as_text(_extract_metric_value(visits_payload, "visits") or overview.get("visits")),
                _as_text(_extract_metric_value(visits_payload, "desktop_share") or overview.get("desktop_share")),
                _as_text(_extract_metric_value(visits_payload, "mobile_share") or overview.get("mobile_share")),
                _as_text(_extract_metric_value(visits_payload, "pages_per_visit") or overview.get("pages_per_visit")),
                _as_text(_extract_metric_value(visits_payload, "visit_duration") or overview.get("visit_duration")),
                _as_text(_extract_metric_value(visits_payload, "bounce_rate") or overview.get("bounce_rate")),
                json.dumps(visits_payload),
                fetched_at,
            ),
        )
        rows.append(
            {
                "entity": "domain_overview",
                "uid": uid,
                "domain": domain,
                "date": end_date,
                "visits": _as_text(_extract_metric_value(visits_payload, "visits") or overview.get("visits")),
            }
        )

        cur.execute(
            """
            INSERT OR REPLACE INTO similarweb_traffic_sources
            (uid, domain, date, direct_share, referral_share, organic_search_share, paid_search_share, social_share, mail_share, display_share, raw_json, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                uid,
                domain,
                end_date,
                _as_text(_extract_metric_value(traffic_payload, "direct_share") or traffic.get("direct_share")),
                _as_text(_extract_metric_value(traffic_payload, "referral_share") or traffic.get("referral_share")),
                _as_text(_extract_metric_value(traffic_payload, "organic_search_share") or traffic.get("organic_search_share")),
                _as_text(_extract_metric_value(traffic_payload, "paid_search_share") or traffic.get("paid_search_share")),
                _as_text(_extract_metric_value(traffic_payload, "social_share") or traffic.get("social_share")),
                _as_text(_extract_metric_value(traffic_payload, "mail_share") or traffic.get("mail_share")),
                _as_text(_extract_metric_value(traffic_payload, "display_share") or traffic.get("display_share")),
                json.dumps(traffic_payload),
                fetched_at,
            ),
        )
        rows.append(
            {
                "entity": "traffic_sources",
                "uid": uid,
                "domain": domain,
                "date": end_date,
            }
        )

        # 3) Social referrals
        social_path = f"v1/website/{domain}/traffic-sources/social"
        social_count = 0
        for item in _iter_paginated(social_path, params_base):
            network = item.get("social_network") or item.get("name") or item.get("source")
            share = item.get("traffic_share") or item.get("share")
            cur.execute(
                """
                INSERT OR REPLACE INTO similarweb_referrals
                (uid, domain, date, referring_domain, referral_share, raw_json, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    uid,
                    domain,
                    end_date,
                    _as_text(network) or "",
                    _as_text(share),
                    json.dumps(item),
                    fetched_at,
                ),
            )
            social_count += 1
            rows.append(
                {
                    "entity": "social_referral",
                    "uid": uid,
                    "domain": domain,
                    "date": end_date,
                    "referring_domain": _as_text(network),
                    "referral_share": _as_text(share),
                }
            )

        # 4) Search keywords
        search_path = f"v1/website/{domain}/search"
        keyword_count = 0
        for item in _iter_paginated(search_path, params_base):
            keyword = item.get("keyword") or item.get("search_term")
            cur.execute(
                """
                INSERT OR REPLACE INTO similarweb_search_keywords
                (uid, domain, date, keyword, search_volume, traffic_share, cpc, organic_vs_paid, raw_json, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    uid,
                    domain,
                    end_date,
                    _as_text(keyword) or "",
                    _as_text(item.get("search_volume")),
                    _as_text(item.get("traffic_share") or item.get("share")),
                    _as_text(item.get("cpc") or item.get("CPC")),
                    _as_text(item.get("organic_vs_paid")),
                    json.dumps(item),
                    fetched_at,
                ),
            )
            keyword_count += 1
            rows.append(
                {
                    "entity": "search_keyword",
                    "uid": uid,
                    "domain": domain,
                    "date": end_date,
                    "keyword": _as_text(keyword),
                    "traffic_share": _as_text(item.get("traffic_share") or item.get("share")),
                }
            )

        con.commit()
        con.close()

        save_state(uid, {"last_sync_date": end_date})

        dest_cfg = get_active_destination(uid)
        if not dest_cfg:
            return {
                "status": "success",
                "domain": domain,
                "social_referrals": social_count,
                "search_keywords": keyword_count,
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
            "domain": domain,
            "social_referrals": social_count,
            "search_keywords": keyword_count,
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


def disconnect_similarweb(uid):
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

    cur.execute("DELETE FROM similarweb_connections WHERE uid=?", (uid,))

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
