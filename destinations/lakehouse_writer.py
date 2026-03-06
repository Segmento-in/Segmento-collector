"""
lakehouse_writer.py — Unified Lakehouse Metadata Registry
==========================================================

Supported table formats
-----------------------
  - iceberg   (existing)
  - hudi      (added in this revision)

Design contract
---------------
  * Does NOT write any data files.
  * Does NOT use PyIceberg / Apache Hudi / Spark / any JVM dependency.
  * Only records table metadata in a local SQLite registry so the same
    (source, storage_type, table_format) triple is never registered twice.
  * S3 and ADLS writers call register_*_table() AFTER they have already
    uploaded the Parquet file themselves.

Registry location
-----------------
  <project_root>/data/lakehouse_registry.db
  (path resolved relative to this file — works on any OS / directory depth)

Backward compatibility
----------------------
  * register_iceberg_table(), get_registered_table(), list_registered_tables()
    keep their original signatures exactly.  No existing caller needs changes.
  * The old iceberg_registry.db (if present on disk) is left untouched;
    this module uses a new, unified DB file: lakehouse_registry.db.
"""

import os
import sqlite3
import datetime

# ---------------------------------------------------------------------------
# Registry path
# ---------------------------------------------------------------------------
_HERE        = os.path.dirname(os.path.abspath(__file__))
REGISTRY_DIR = os.path.join(_HERE, "..", "data")          # project_root/data/
REGISTRY_DB  = os.path.join(REGISTRY_DIR, "lakehouse_registry.db")

# All table formats this module understands
_VALID_FORMATS = {"iceberg", "hudi"}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _get_connection() -> sqlite3.Connection:
    """Open (and bootstrap) the unified registry database."""
    os.makedirs(REGISTRY_DIR, exist_ok=True)
    con = sqlite3.connect(REGISTRY_DB)
    con.row_factory = sqlite3.Row
    _bootstrap(con)
    return con


def _bootstrap(con: sqlite3.Connection) -> None:
    """
    Create the unified lakehouse_tables registry table if it does not exist.

    Schema
    ------
    source         : connector / dataset name  e.g. 'gmail', 'chartbeat'
    storage_type   : 's3' or 'adls'
    table_format   : 'iceberg' or 'hudi'
    table_location : canonical storage URI understood by the query engine
    registered_at  : ISO-8601 UTC timestamp of first registration

    The UNIQUE constraint on (source, storage_type, table_format) means:
      - The same dataset can legitimately have both an Iceberg and a Hudi
        registration (e.g. during a migration), and each is tracked separately.
      - Repeated registrations of the same triple are silently idempotent.
    """
    con.execute("""
        CREATE TABLE IF NOT EXISTS lakehouse_tables (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            source         TEXT    NOT NULL,
            storage_type   TEXT    NOT NULL,
            table_format   TEXT    NOT NULL,
            table_location TEXT    NOT NULL,
            registered_at  TEXT    NOT NULL,
            UNIQUE (source, storage_type, table_format)
        )
    """)
    con.commit()


def _register_table(
    *,
    source: str,
    storage_type: str,
    table_format: str,
    table_location: str,
) -> dict:
    """
    Shared registration logic used by both public register_*_table() functions.

    Performs an idempotent upsert-by-check:
      1. If a row already exists for (source, storage_type, table_format),
         return it immediately without touching the DB.
      2. Otherwise insert a new row and return the new record.
    """
    if table_format not in _VALID_FORMATS:
        raise ValueError(
            f"Unknown table_format '{table_format}'. "
            f"Must be one of: {sorted(_VALID_FORMATS)}"
        )

    tag = table_format.upper()
    con = _get_connection()

    try:
        row = con.execute(
            """
            SELECT *
            FROM   lakehouse_tables
            WHERE  source=? AND storage_type=? AND table_format=?
            """,
            (source, storage_type, table_format),
        ).fetchone()

        if row:
            print(
                f"[{tag}] Table already registered — "
                f"source={source}, storage={storage_type}, "
                f"location={row['table_location']}"
            )
            return {
                "source":          row["source"],
                "storage_type":    row["storage_type"],
                "table_format":    row["table_format"],
                "table_location":  row["table_location"],
                "registered_at":   row["registered_at"],
                "already_existed": True,
            }

        # First-time registration
        now = datetime.datetime.utcnow().isoformat()

        con.execute(
            """
            INSERT INTO lakehouse_tables
                (source, storage_type, table_format, table_location, registered_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (source, storage_type, table_format, table_location, now),
        )
        con.commit()

        print(
            f"[{tag}] Registered new table — "
            f"source={source}, storage={storage_type}, "
            f"location={table_location}"
        )

        return {
            "source":          source,
            "storage_type":    storage_type,
            "table_format":    table_format,
            "table_location":  table_location,
            "registered_at":   now,
            "already_existed": False,
        }

    finally:
        con.close()


# ---------------------------------------------------------------------------
# Public API — Iceberg  (original signatures preserved exactly)
# ---------------------------------------------------------------------------

def register_iceberg_table(
    *,
    source: str,
    storage_type: str,
    table_location: str,
) -> dict:
    """
    Record an Iceberg table in the unified lakehouse registry.

    Signature is identical to the previous implementation so all existing
    callers (s3_writer, azure_datalake_writer) continue to work without
    any modification.

    Parameters
    ----------
    source        : connector name, e.g. 'chartbeat', 'instagram'
    storage_type  : 's3' or 'adls'
    table_location: root URI, e.g. 's3://my-bucket/chartbeat'
                    or 'abfss://container@account.dfs.core.windows.net/chartbeat'

    Returns
    -------
    dict with keys:
        source, storage_type, table_format, table_location,
        registered_at, already_existed
    """
    return _register_table(
        source=source,
        storage_type=storage_type,
        table_format="iceberg",
        table_location=table_location,
    )


# ---------------------------------------------------------------------------
# Public API — Hudi
# ---------------------------------------------------------------------------

def register_hudi_table(
    *,
    source: str,
    storage_type: str,
    table_location: str,
) -> dict:
    """
    Record a Hudi table in the unified lakehouse registry.

    Identical contract to register_iceberg_table() — same parameters,
    same return shape — only table_format differs ('hudi').

    Parameters
    ----------
    source        : connector name, e.g. 'gmail', 'chartbeat'
    storage_type  : 's3' or 'adls'
    table_location: root URI, e.g. 's3://my-bucket/gmail'
                    or 'abfss://container@account.dfs.core.windows.net/gmail'

    Returns
    -------
    dict with keys:
        source, storage_type, table_format, table_location,
        registered_at, already_existed
    """
    return _register_table(
        source=source,
        storage_type=storage_type,
        table_format="hudi",
        table_location=table_location,
    )


# ---------------------------------------------------------------------------
# Public API — Queries  (backward-compatible)
# ---------------------------------------------------------------------------

def get_registered_table(
    source: str,
    storage_type: str,
    table_format: str = "iceberg",
) -> dict | None:
    """
    Look up a single registration by (source, storage_type, table_format).

    table_format defaults to 'iceberg' so all existing two-argument callers
    continue to work with no changes.

    Returns None if not found.
    """
    con = _get_connection()
    try:
        row = con.execute(
            """
            SELECT *
            FROM   lakehouse_tables
            WHERE  source=? AND storage_type=? AND table_format=?
            """,
            (source, storage_type, table_format),
        ).fetchone()
        return dict(row) if row else None
    finally:
        con.close()


def list_registered_tables(table_format: str | None = None) -> list[dict]:
    """
    Return registered lakehouse tables, optionally filtered by format.

    Parameters
    ----------
    table_format : 'iceberg', 'hudi', or None (returns all formats).
                   Defaults to None so the existing zero-argument call still
                   returns every row, exactly as before.

    Returns
    -------
    List of dicts ordered by registered_at DESC.
    """
    con = _get_connection()
    try:
        if table_format is not None:
            rows = con.execute(
                """
                SELECT * FROM lakehouse_tables
                WHERE  table_format = ?
                ORDER BY registered_at DESC
                """,
                (table_format,),
            ).fetchall()
        else:
            rows = con.execute(
                "SELECT * FROM lakehouse_tables ORDER BY registered_at DESC"
            ).fetchall()

        return [dict(r) for r in rows]
    finally:
        con.close()