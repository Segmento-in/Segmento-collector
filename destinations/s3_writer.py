print("### S3 FORMAT-AWARE WRITER LOADED ###")

import json
import tempfile
import os
import time
import pandas as pd
import boto3

from datetime import datetime


def push_s3(dest, source, rows):

    if not rows:
        return 0

    fmt = (dest.get("format") or "parquet").lower()
    print(f"[S3] Upload format: {fmt}")

    aws_access_key = dest["username"]
    aws_secret_key = dest["password"]
    bucket_name    = dest["host"]
    region         = dest.get("port") or "us-east-1"

    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region,
    )

    df = pd.DataFrame(rows)

    for col in df.columns:
        df[col] = df[col].astype(str)

    df["fetched_at"] = pd.Timestamp.utcnow()

    # ------------------------------------------------------------------ #
    # FILE CREATION                                                        #
    # "iceberg" and "hudi" both write identical Parquet files.            #
    # Table-format semantics live entirely in the lakehouse registry —    #
    # no PyArrow S3FileSystem, no Spark, no JVM dependency.               #
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
        raise Exception(f"Unsupported S3 format: {fmt}")

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

    print("[S3] Uploading:", key)

    s3.upload_file(file_path, bucket_name, key)

    os.unlink(file_path)

    print(f"[S3] Uploaded {len(rows)} rows → s3://{bucket_name}/{key}")

    if fmt in ("iceberg", "hudi"):

        table_location = f"s3://{bucket_name}/{source}"

        if fmt == "iceberg":
            from destinations.lakehouse_writer import register_iceberg_table
            register_iceberg_table(
                source=source,
                storage_type="s3",
                table_location=table_location,
            )

        else:  # hudi
            from destinations.lakehouse_writer import register_hudi_table
            register_hudi_table(
                source=source,
                storage_type="s3",
                table_location=table_location,
            )

    return len(rows)