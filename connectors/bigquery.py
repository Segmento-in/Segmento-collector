import json
import sqlite3
import time
import datetime

from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import (
    GoogleAPICallError,
    NotFound,
    Forbidden,
    TooManyRequests,
)

from security.secure_fetch import fetchone_secure
from destinations.destination_router import push_to_destination

DB = "identity.db"
SOURCE = "bigquery"


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(message: str):
    print(f"[BIGQUERY-CONNECTOR] {message}")


def _get_config(uid: str):
    """
    Load encrypted connector configuration for this user.
    Expected JSON structure:
    {
      "project_id": "...",
      "dataset_id": "...",
      "service_account": { ... full service account json ... }
    }
    """
    con = get_db()
    cur = con.cursor()

    cur.execute(
        """
        SELECT config_json
        FROM connector_configs
        WHERE uid=? AND connector=?
        LIMIT 1
        """,
        (uid, SOURCE),
    )

    row = fetchone_secure(cur)
    con.close()

    if not row or not row.get("config_json"):
        return None

    try:
        return json.loads(row["config_json"])
    except Exception:
        return None


def _update_status(uid: str, status: str):
    con = get_db()
    cur = con.cursor()

    cur.execute(
        """
        UPDATE connector_configs
        SET status=?
        WHERE uid=? AND connector=?
        """,
        (status, uid, SOURCE),
    )

    con.commit()
    con.close()


def _set_connection_enabled(uid: str, enabled: bool):
    con = get_db()
    cur = con.cursor()

    cur.execute(
        """
        INSERT OR REPLACE INTO google_connections
        (uid, source, enabled)
        VALUES (?, ?, ?)
        """,
        (uid, SOURCE, 1 if enabled else 0),
    )

    con.commit()
    con.close()


def _build_client(config: dict):
    service_info = config.get("service_account") or {}
    project_id = config.get("project_id") or service_info.get("project_id")
    dataset_id = config.get("dataset_id")

    if not service_info:
        raise ValueError("Missing service account JSON in configuration")

    if not project_id:
        raise ValueError("Missing project_id in configuration")

    if not dataset_id:
        raise ValueError("Missing dataset_id in configuration")

    credentials = service_account.Credentials.from_service_account_info(
        service_info
    )

    client = bigquery.Client(project=project_id, credentials=credentials)
    return client, project_id, dataset_id


def _validate_dataset(client, project_id: str, dataset_id: str, retries: int = 5):
    """
    Validate that the configured dataset exists and is accessible.
    Handles 401/403/404/429 and transient errors with basic exponential backoff.
    """
    dataset_ref = f"{project_id}.{dataset_id}"

    for attempt in range(retries):
        try:
            client.get_dataset(dataset_ref)
            _log(f"Dataset OK: {dataset_ref}")
            return {
                "status": "success",
                "project_id": project_id,
                "dataset_id": dataset_id,
            }

        except NotFound:
            _log(f"Dataset not found: {dataset_ref}")
            return {
                "status": "error",
                "code": 404,
                "message": "BigQuery dataset not found",
            }

        except Forbidden as e:
            _log(f"Permission denied for dataset {dataset_ref}: {e}")
            return {
                "status": "error",
                "code": 403,
                "message": "Permission denied for BigQuery dataset",
            }

        except TooManyRequests as e:
            _log(f"Rate limited when accessing {dataset_ref}: {e}")
            if attempt == retries - 1:
                return {
                    "status": "error",
                    "code": 429,
                    "message": "BigQuery rate limit exceeded",
                }
            wait_s = min(2**attempt, 60)
            time.sleep(wait_s)
            continue

        except GoogleAPICallError as e:
            _log(f"BigQuery API error for {dataset_ref}: {e}")
            if attempt == retries - 1:
                code = getattr(e, "code", None)
                http_code = int(getattr(code, "value", 500)) if code else 500
                return {
                    "status": "error",
                    "code": http_code,
                    "message": str(e),
                }
            wait_s = min(2**attempt, 60)
            time.sleep(wait_s)
            continue

        except Exception as e:
            _log(f"Unexpected error validating dataset {dataset_ref}: {e}")
            if attempt == retries - 1:
                return {"status": "error", "message": str(e)}
            wait_s = min(2**attempt, 60)
            time.sleep(wait_s)


def connect_bigquery(uid: str):
    """
    Connect step:
    - Load encrypted configuration
    - Build BigQuery client
    - Validate dataset access
    - Mark connector as enabled on success
    """
    cfg = _get_config(uid)
    if not cfg:
        return {
            "status": "error",
            "message": "BigQuery not configured for this user",
        }

    try:
        client, project_id, dataset_id = _build_client(cfg)
    except ValueError as e:
        _update_status(uid, "error")
        return {"status": "error", "message": str(e)}
    except Exception as e:
        _update_status(uid, "error")
        return {"status": "error", "message": str(e)}

    result = _validate_dataset(client, project_id, dataset_id)

    if result.get("status") == "success":
        _set_connection_enabled(uid, True)
        _update_status(uid, "connected")
    else:
        _set_connection_enabled(uid, False)
        _update_status(uid, "error")

    return result


def disconnect_bigquery(uid: str):
    """
    Disable connector for this user.
    """
    _set_connection_enabled(uid, False)
    _update_status(uid, "disconnected")

def normalize_bigquery_row(row):
    """
    Convert BigQuery Row into a fully JSON-serializable dict.
    Handles datetime, date, decimal, nested structures, etc.
    """

    record = {}

    for key, value in dict(row).items():

        if isinstance(value, (datetime.datetime, datetime.date)):
            record[key] = value.isoformat()

        elif isinstance(value, (bytes, bytearray)):
            record[key] = value.decode("utf-8", errors="ignore")

        elif isinstance(value, dict):
            record[key] = json.loads(json.dumps(value, default=str))

        elif isinstance(value, list):
            record[key] = json.loads(json.dumps(value, default=str))

        else:
            record[key] = value

    return record

def sync_bigquery(uid: str, sync_type: str = "incremental"):
    """
    BigQuery SOURCE connector sync.

    Flow:
    - connect to BigQuery
    - list tables
    - extract rows
    - push rows to configured destination
    """

    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "BigQuery not configured"}

    try:
        client, project_id, dataset_id = _build_client(cfg)
    except Exception as e:
        _update_status(uid, "error")
        return {"status": "error", "message": str(e)}

    dataset_ref = f"{project_id}.{dataset_id}"

    validation = _validate_dataset(client, project_id, dataset_id)
    if validation.get("status") != "success":
        return validation

    rows_pushed = 0
    tables_processed = 0

    try:

        for table in client.list_tables(dataset_ref):

            table_id = table.table_id
            full_table = f"{dataset_ref}.{table_id}"

            _log(f"Extracting table: {full_table}")

            query = f"""
            SELECT *
            FROM `{full_table}`
            LIMIT 10000
            """

            try:

                results = client.query(query).result()

                rows = []

                for r in results:

                    record = {k: v for k, v in zip(r.keys(), r.values())}

                    rows.append({
                        "uid": uid,
                        "source": SOURCE,
                        "record_id": str(record.get("id") or hash(str(record))),
                        "data_json": json.dumps(record),
                        "raw_json": json.dumps(record),
                        "fetched_at": datetime.datetime.utcnow().isoformat() + "Z"
                    })

                if rows:
                    push_to_destination(uid, SOURCE, rows)
                    rows_pushed += len(rows)

                tables_processed += 1

            except Exception as e:
                _log(f"Failed extracting {full_table}: {e}")

    except Exception as e:
        return {"status": "error", "message": str(e)}

    return {
        "status": "success",
        "tables_processed": tables_processed,
        "rows_pushed": rows_pushed
    }