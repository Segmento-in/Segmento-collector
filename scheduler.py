import time
import requests
import sqlite3
import datetime
from apscheduler.schedulers.background import BackgroundScheduler


DB_PATH = "identity.db"
BASE_URL = "http://localhost:4000"

# Prevent overlapping runs
RUNNING_JOBS = set()


# -------------------------------
# Get Active Jobs
# -------------------------------

def get_due_jobs():

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        SELECT
            cj.uid,
            cj.source,
            cj.sync_type,
            cj.schedule_time
        FROM connector_jobs cj
        JOIN google_connections gc
          ON cj.uid = gc.uid
         AND cj.source = gc.source
        WHERE
            cj.enabled = 1
        AND
            gc.enabled = 1
    """)

    rows = cur.fetchall()
    conn.close()

    return rows


# -------------------------------
# Run One Job (UNIVERSAL)
# -------------------------------

def run_job(job):

    uid, source, sync_type, schedule_time = job

    job_key = f"{uid}:{source}"

    # Prevent parallel runs
    if job_key in RUNNING_JOBS:
        print(f"[SCHEDULER] Skipping {job_key} (already running)")
        return

    RUNNING_JOBS.add(job_key)

    print(f"[SCHEDULER] Running {source} ({sync_type}) for {uid}")

    try:

        # Universal dynamic call
        url = (
            f"{BASE_URL}/connectors/{source}/sync"
            "?mode=scheduled&strategy=incremental"
        )

        r = requests.get(
            url,
            headers={
                "X-Internal-UID": uid,
                "X-Sync-Mode": "scheduled"
            },
            timeout=600
        )

        if r.status_code == 200:
            print(f"[SCHEDULER] {source} sync OK →", r.json())
        else:
            print(f"[SCHEDULER] {source} sync FAILED:", r.text)

    except Exception as e:
        print(f"[SCHEDULER] Error running {source}:", str(e))

    finally:
        RUNNING_JOBS.discard(job_key)


# -------------------------------
# Check Every Minute
# -------------------------------

def scheduler_tick():

    now = datetime.datetime.now().strftime("%H:%M")

    jobs = get_due_jobs()

    if not jobs:
        return

    for job in jobs:

        uid, source, sync_type, schedule_time = job

        if is_time_match(now, schedule_time):
            run_job(job)


# -------------------------------
# Time Matching (1 min window)
# -------------------------------

def is_time_match(now, target):

    try:
        fmt = "%H:%M"

        now_t = datetime.datetime.strptime(now, fmt)
        target_t = datetime.datetime.strptime(target, fmt)

        diff = abs((now_t - target_t).total_seconds())

        return diff <= 60

    except Exception:
        return False


# -------------------------------
# Start Scheduler
# -------------------------------

def start_scheduler():

    scheduler = BackgroundScheduler(
        timezone="Asia/Kolkata"
    )

    scheduler.add_job(
        scheduler_tick,
        "interval",
        seconds=60,
        max_instances=1,
        coalesce=True
    )

    scheduler.start()

    print("[SCHEDULER] Universal Scheduler Started (1-min interval)")


# -------------------------------
# Main
# -------------------------------

if __name__ == "__main__":

    start_scheduler()

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        print("\n[SCHEDULER] Stopped")