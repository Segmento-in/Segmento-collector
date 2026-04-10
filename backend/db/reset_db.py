import os
import sys
import logging

# Add project root to PYTHONPATH to use the sqlite3 shim
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(BASE_DIR)

import sqlite3

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DB_PATH = os.getenv("DB_PATH", "identity.db")

def reset_database():
    logger.info(f"Connecting to database: {DB_PATH}")
    try:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()

        # 1. Identify all tables to drop
        # Since we use a PostgreSQL shim in production, we check both SQLite and Postgres metadata styles
        is_postgres = os.getenv("DATABASE_URL") is not None
        
        if is_postgres:
            logger.info("Detected PostgreSQL environment. Dropping all public tables...")
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            """)
        else:
            logger.info("Detected SQLite environment. Dropping all tables...")
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")

        tables = [row[0] for row in cur.fetchall()]
        
        for table in tables:
            logger.info(f"Dropping table: {table}")
            try:
                cur.execute(f"DROP TABLE \"{table}\" CASCADE" if is_postgres else f"DROP TABLE {table}")
            except Exception as e:
                logger.warning(f"Could not drop table {table}: {e}")

        con.commit()
        logger.info("Database reset complete. Tables will be re-initialized by the application on next start.")
        con.close()
        
    except Exception as e:
        logger.error(f"Failed to reset database: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    confirm = input("Are you sure you want to WIP ALL DATA? (y/N): ")
    if confirm.lower() == 'y':
        reset_database()
    else:
        logger.info("Aborted.")
