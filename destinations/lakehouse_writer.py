"""
lakehouse_writer.py — Iceberg Metadata Registry

Design contract:
  - Does NOT write any data files.
  - Does NOT use PyIceberg append / PyArrow S3FileSystem.
  - Only records table metadata (location, format, schema fingerprint)
    in a local SQLite registry so the same table is never registered twice.
  - S3 and ADLS writers call register_iceberg_table() AFTER they have
    already uploaded the Parquet file themselves.

Registry location: <project_root>/iceberg_registry.db
  (resolved relative to this file so it works on any OS / path depth)
"""

import os
import sqlite3
import datetime

# ---------------------------------------------------------------------------
# Registry DB path — sits next to this file inside the destinations/ package,
# or one level up if you prefer.  Adjust REGISTRY_DIR as needed.
# ---------------------------------------------------------------------------
_HERE = os.path.dirname(os.path.abspath(__file__))
REGISTRY_DIR = os.path.join(_HERE, "..", "data")          # project_root/data/
REGISTRY_DB  = os.path.join(REGISTRY_DIR, "iceberg_registry.db")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _get_connection() -> sqlite3.Connection:
    """Open (and bootstrap) the registry database."""
    os.makedirs(REGISTRY_DIR, exist_ok=True)
    con = sqlite3.connect(REGISTRY_DB)
    con.row_factory = sqlite3.Row
    _bootstrap(con)
    return con


def _bootstrap(con: sqlite3.Connection) -> None:
    """Create the registry table if it does not already exist."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS iceberg_tables (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            source        TEXT    NOT NULL,
            storage_type  TEXT    NOT NULL,          -- 's3' or 'adls'
            table_location TEXT   NOT NULL,          -- s3://bucket/source  or  adls://...
            registered_at TEXT    NOT NULL,
            UNIQUE (source, storage_type)            -- one entry per source+backend
        )
    """)
    con.commit()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def register_iceberg_table(
    *,
    source: str,
    storage_type: str,          # 's3' or 'adls'
    table_location: str,        # canonical URI understood by your query engine
) -> dict:
    """
    Record an Iceberg table in the local metadata registry.

    Parameters
    ----------
    source        : connector name, e.g. 'chartbeat', 'instagram'
    storage_type  : 's3' or 'adls'
    table_location: the root URI where Parquet files are written,
                    e.g. 's3://my-bucket/chartbeat'
                    or   'abfss://container@account.dfs.core.windows.net/chartbeat'

    Returns
    -------
    dict with keys: source, storage_type, table_location, registered_at, already_existed
    """

    con = _get_connection()

    try:
        # Check for an existing registration first
        row = con.execute(
            "SELECT * FROM iceberg_tables WHERE source=? AND storage_type=?",
            (source, storage_type),
        ).fetchone()

        if row:
            print(
                f"[ICEBERG] Table already registered — "
                f"source={source}, storage={storage_type}, "
                f"location={row['table_location']}"
            )
            return {
                "source":         row["source"],
                "storage_type":   row["storage_type"],
                "table_location": row["table_location"],
                "registered_at":  row["registered_at"],
                "already_existed": True,
            }

        # First-time registration
        now = datetime.datetime.utcnow().isoformat()

        con.execute(
            """
            INSERT INTO iceberg_tables
                (source, storage_type, table_location, registered_at)
            VALUES (?, ?, ?, ?)
            """,
            (source, storage_type, table_location, now),
        )
        con.commit()

        print(
            f"[ICEBERG] Registered new table — "
            f"source={source}, storage={storage_type}, "
            f"location={table_location}"
        )

        return {
            "source":         source,
            "storage_type":   storage_type,
            "table_location": table_location,
            "registered_at":  now,
            "already_existed": False,
        }

    finally:
        con.close()


def get_registered_table(source: str, storage_type: str) -> dict | None:
    """
    Look up a registration by source + storage_type.
    Returns None if not found.
    """
    con = _get_connection()
    try:
        row = con.execute(
            "SELECT * FROM iceberg_tables WHERE source=? AND storage_type=?",
            (source, storage_type),
        ).fetchone()
        return dict(row) if row else None
    finally:
        con.close()


def list_registered_tables() -> list[dict]:
    """Return all registered Iceberg tables (useful for admin/debug)."""
    con = _get_connection()
    try:
        rows = con.execute(
            "SELECT * FROM iceberg_tables ORDER BY registered_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        con.close()