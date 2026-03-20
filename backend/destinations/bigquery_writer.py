print("### BIGQUERY FORMAT-AWARE WRITER LOADED ###", flush=True)

import json
import tempfile
import os
import pandas as pd

from google.cloud import bigquery
from google.oauth2 import service_account


# ---------------------------------------------------
# FORCE STRING NORMALIZATION (CRITICAL FIX)
# ---------------------------------------------------
def normalize_rows(rows):

    normalized = []

    for r in rows:
        clean = {}

        for k, v in r.items():

            if v is None:
                clean[k] = None
            else:
                clean[k] = str(v)

        normalized.append(clean)

    return normalized


def push_bigquery(dest, source, rows):

    if not rows:
        return 0

    # ---------------- FORMAT ----------------
    fmt = (dest.get("format") or "parquet").lower()
    print(f"[BIGQUERY] Upload format: {fmt}", flush=True)

    # ---------------- Credentials ----------------
    creds_dict = json.loads(dest["password"])

    credentials = service_account.Credentials.from_service_account_info(
        creds_dict
    )

    client = bigquery.Client(
        credentials=credentials,
        project=dest["host"]
    )

    project_id = dest["host"]
    dataset_id = dest["database_name"]
    table_id = f"{project_id}.{dataset_id}.{source}_data"

    # ---------------- Dataset ----------------
    dataset_ref = bigquery.Dataset(
        f"{project_id}.{dataset_id}"
    )

    try:
        client.create_dataset(dataset_ref)
    except Exception:
        import traceback; traceback.print_exc()
        print('Exception caught', flush=True)

    # ==================================================
    # 🔥 NORMALIZE DATA BEFORE ANY FORMAT WRITING
    # ==================================================
    rows = normalize_rows(rows)

    df = pd.DataFrame(rows)

    df["fetched_at"] = str(pd.Timestamp.utcnow())

    # ---------------- FILE CREATION ----------------
    if fmt == "parquet":

        tmp = tempfile.NamedTemporaryFile(
            delete=False,
            suffix=".parquet"
        )

        file_path = tmp.name
        tmp.close()

        df.to_parquet(
            file_path,
            engine="pyarrow",
            index=False
        )

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition="WRITE_APPEND",
            autodetect=True
        )

    elif fmt == "json":

        tmp = tempfile.NamedTemporaryFile(
            delete=False,
            suffix=".json"
        )

        file_path = tmp.name
        tmp.close()

        # IMPORTANT → ensures quoted JSON values
        with open(file_path, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row) + "\n")

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition="WRITE_APPEND",
            autodetect=True
        )

    else:
        raise Exception(f"Unsupported BigQuery format: {fmt}")

    # ---------------- LOAD ----------------
    with open(file_path, "rb") as f:

        job = client.load_table_from_file(
            f,
            table_id,
            job_config=job_config
        )

    try:
        job.result()

    except Exception as e:

        err = str(e)

        print("[BIGQUERY LOAD ERROR]", err, flush=True)

        # ---------------- SCHEMA CONFLICT FIX ----------------
        if "has changed type" in err:

            print("[BIGQUERY] Schema mismatch detected", flush=True)
            print("[BIGQUERY] Recreating table...", flush=True)

            client.delete_table(table_id, not_found_ok=True)

            with open(file_path, "rb") as f:
                job = client.load_table_from_file(
                    f,
                    table_id,
                    job_config=job_config
                )

            job.result()

        else:
            raise e

    os.unlink(file_path)

    print(f"[BIGQUERY] Loaded {len(rows)} rows ({fmt})", flush=True)

    return len(rows)