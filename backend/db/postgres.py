import os

import psycopg2


def acquire_connection():
    return psycopg2.connect(
        os.getenv("DATABASE_URL"),
        sslmode="require",
    )


def release_connection(conn):
    conn.close()
