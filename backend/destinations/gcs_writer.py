print("### GCS FORMAT-AWARE WRITER LOADED ###", flush=True)

import json
import tempfile
import os
import time
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
from datetime import datetime


def push_gcs(dest, source, rows):
    """
    Push rows to Google Cloud Storage.
    Supports parquet, json, iceberg, hudi.
    Mapping:
    - bucket_name -> host
    - region -> port
    - access_key -> username
    - secret_key -> password
    """
    if not rows:
        return 0

    fmt = (dest.get("format") or "parquet").lower()
    print(f"[GCS] Upload format: {fmt}", flush=True)

    bucket_name = dest["host"]
    # GCS authentication using Service Account JSON key provided in dest["password"]
    if not dest.get("password"):
        raise Exception("GCS destination requires a valid Service Account JSON key.")

    try:
        key_json = json.loads(dest["password"])
        creds = service_account.Credentials.from_service_account_info(key_json)
        client = storage.Client(credentials=creds)
    except json.JSONDecodeError:
        raise Exception("GCS destination requires a valid Service Account JSON key (Invalid JSON).")
    except Exception as e:
        raise Exception(f"Failed to initialize GCS client: {str(e)}")

    bucket = client.bucket(bucket_name)

    df = pd.DataFrame(rows)

    for col in df.columns:
        df[col] = df[col].astype(str)

    df["fetched_at"] = pd.Timestamp.utcnow()

    # ------------------------------------------------------------------ #
    # FILE CREATION                                                        #
    # ------------------------------------------------------------------ #
    if fmt in ("parquet", "iceberg", "hudi"):
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
        file_path = tmp.name
        tmp.close()

        df.to_parquet(
            file_path,
            engine="pyarrow",
            index=False,
        )
        extension = "parquet"

    elif fmt == "json":
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
        file_path = tmp.name
        tmp.close()

        df.to_json(
            file_path,
            orient="records",
            lines=True,
        )
        extension = "json"

    else:
        raise Exception(f"Unsupported GCS format: {fmt}")

    # ------------------------------------------------------------------ #
    # PARTITION PATH  →  source/year=YYYY/month=MM/day=DD/file.parquet    #
    # ------------------------------------------------------------------ #
    now = datetime.utcnow()
    key = (
        f"{source}/"
        f"year={now.year}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}/"
        f"{source}_{int(time.time())}.{extension}"
    )

    print("[GCS] Uploading:", key, flush=True)

    blob = bucket.blob(key)
    blob.upload_from_filename(file_path)

    os.unlink(file_path)

    print(f"[GCS] Uploaded {len(rows)} rows → gs://{bucket_name}/{key}", flush=True)

    if fmt in ("iceberg", "hudi"):
        table_location = f"gs://{bucket_name}/{source}"

        if fmt == "iceberg":
            from backend.destinations.lakehouse_writer import register_iceberg_table
            register_iceberg_table(
                source=source,
                storage_type="gcs",
                table_location=table_location,
            )
        else:  # hudi
            from backend.destinations.lakehouse_writer import register_hudi_table
            register_hudi_table(
                source=source,
                storage_type="gcs",
                table_location=table_location,
            )

    return len(rows)
