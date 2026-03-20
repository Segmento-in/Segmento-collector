import os
import tempfile
import time
from datetime import datetime

import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient


def push_azure_datalake(dest, source, rows):

    if not rows:
        return 0

    fmt = (dest.get("format") or "parquet").lower()
    print(f"[ADLS] Upload format: {fmt}", flush=True)

    account_name = dest["host"]
    file_system  = dest.get("port") or "segmento"
    account_key  = dest["password"]
    base_path    = (dest.get("username") or "").strip("/")

    service = DataLakeServiceClient(
        account_url=f"https://{account_name}.dfs.core.windows.net",
        credential=account_key,
    )

    fs_client = service.get_file_system_client(file_system=file_system)
    try:
        fs_client.create_file_system()
    except Exception:
        pass  # container already exists — safe to ignore

    df = pd.DataFrame(rows)
    for col in df.columns:
        df[col] = df[col].astype(str)
    df["fetched_at"] = pd.Timestamp.utcnow()

    # ------------------------------------------------------------------ #
    # FILE CREATION                                                        #
    # "iceberg" and "hudi" both write identical Parquet files.            #
    # Table-format semantics live entirely in the lakehouse registry —    #
    # no external catalog calls, no JVM dependency.                       #
    # ------------------------------------------------------------------ #
    if fmt in ("parquet", "iceberg", "hudi"):

        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
        file_path = tmp.name
        tmp.close()

        df.to_parquet(file_path, engine="pyarrow", index=False)

        extension = "parquet"

    elif fmt == "json":

        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
        file_path = tmp.name
        tmp.close()

        df.to_json(file_path, orient="records", lines=True)

        extension = "json"

    else:
        raise Exception(f"Unsupported ADLS format: {fmt}")

    # ------------------------------------------------------------------ #
    # PARTITION PATH  →  source/year=YYYY/month=MM/day=DD/file.parquet    #
    # ------------------------------------------------------------------ #
    now = datetime.utcnow()

    rel_path = (
        f"{source}/"
        f"year={now.year}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}/"
        f"{source}_{int(time.time())}.{extension}"
    )

    adls_path = f"{base_path}/{rel_path}" if base_path else rel_path
    print("[ADLS] Uploading:", adls_path, flush=True)

    file_client = fs_client.get_file_client(adls_path)
    with open(file_path, "rb") as fh:
        file_client.upload_data(fh, overwrite=True)

    os.unlink(file_path)

    print(f"[ADLS] Uploaded {len(rows)} rows → adls://{file_system}/{adls_path}", flush=True)

    # ------------------------------------------------------------------ #
    # LAKEHOUSE REGISTRATION (metadata only — no data written here)       #
    # Canonical ABFSS URI is what Spark / Trino / Dremio expect when they #
    # later read the registry to discover the table location.             #
    # ------------------------------------------------------------------ #
    if fmt in ("iceberg", "hudi"):

        table_location = (
            f"abfss://{file_system}@{account_name}.dfs.core.windows.net"
            f"/{base_path}/{source}"
            if base_path
            else
            f"abfss://{file_system}@{account_name}.dfs.core.windows.net/{source}"
        )

        if fmt == "iceberg":
            from backend.destinations.lakehouse_writer import register_iceberg_table
            register_iceberg_table(
                source=source,
                storage_type="adls",
                table_location=table_location,
            )

        else:  # hudi
            from backend.destinations.lakehouse_writer import register_hudi_table
            register_hudi_table(
                source=source,
                storage_type="adls",
                table_location=table_location,
            )

    return len(rows)