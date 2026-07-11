"""
Worker service.

Polls queues for eligible jobs, atomically claims them using Postgres row
locking (SELECT ... FOR UPDATE SKIP LOCKED), executes them concurrently in a
thread pool, and manages the full retry / dead-letter lifecycle.

Run multiple copies of this process (docker-compose up --scale worker=3) to
prove atomic claiming: no two workers should ever run the same job.
"""
import os
import sys
import time
import uuid
import signal
import logging
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

import psycopg2
import psycopg2.extras

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("worker")

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://scheduler:scheduler@db:5432/scheduler")
WORKER_ID = str(uuid.uuid4())
WORKER_NAME = os.environ.get("WORKER_NAME", f"worker-{WORKER_ID[:8]}")
POLL_INTERVAL_SECONDS = float(os.environ.get("POLL_INTERVAL_SECONDS", 2))
MAX_CONCURRENT_JOBS = int(os.environ.get("MAX_CONCURRENT_JOBS", 5))
HEARTBEAT_INTERVAL_SECONDS = 10

shutdown_event = threading.Event()
active_jobs_lock = threading.Lock()
active_job_count = 0


def get_connection():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    return conn


def register_worker():
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO workers (id, name, status, last_heartbeat, started_at) "
            "VALUES (%s, %s, 'alive', %s, %s)",
            (WORKER_ID, WORKER_NAME, datetime.utcnow(), datetime.utcnow()),
        )
    conn.commit()
    conn.close()
    log.info("Registered worker %s (%s)", WORKER_NAME, WORKER_ID)


def send_heartbeat():
    while not shutdown_event.is_set():
        try:
            conn = get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE workers SET last_heartbeat = %s WHERE id = %s",
                    (datetime.utcnow(), WORKER_ID),
                )
                cur.execute(
                    "INSERT INTO worker_heartbeats (id, worker_id, timestamp, active_jobs) "
                    "VALUES (%s, %s, %s, %s)",
                    (str(uuid.uuid4()), WORKER_ID, datetime.utcnow(), active_job_count),
                )
            conn.commit()
            conn.close()
        except Exception:
            log.exception("Heartbeat failed")
        shutdown_event.wait(HEARTBEAT_INTERVAL_SECONDS)


def claim_next_job():
    """
    The core atomicity trick: FOR UPDATE SKIP LOCKED lets many workers poll
    concurrently. Postgres locks the selected row for the duration of this
    transaction; any other worker's concurrent SELECT ... FOR UPDATE simply
    skips rows currently locked by someone else instead of blocking or
    double-claiming. Ordered by queue priority, then oldest-eligible-first.
    """
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT j.id, j.queue_id, j.payload, j.attempt_count, j.max_retries
                FROM jobs j
                JOIN queues q ON j.queue_id = q.id
                WHERE j.status IN ('queued', 'scheduled')
                  AND j.job_type != 'recurring'
                  AND j.run_at <= %s
                  AND q.is_paused = FALSE
                ORDER BY q.priority DESC, j.run_at ASC
                LIMIT 1
                FOR UPDATE OF j SKIP LOCKED
                """,
                (datetime.utcnow(),),
            )
            row = cur.fetchone()
            if not row:
                conn.commit()
                return None

            cur.execute(
                "UPDATE jobs SET status = 'claimed', claimed_by = %s, claimed_at = %s "
                "WHERE id = %s",
                (WORKER_ID, datetime.utcnow(), row["id"]),
            )
        conn.commit()
        return row
    except Exception:
        conn.rollback()
        log.exception("Failed to claim job")
        return None
    finally:
        conn.close()


def compute_backoff_seconds(strategy, base_delay, attempt_number):
    if strategy == "linear":
        return base_delay * attempt_number
    if strategy == "exponential":
        return base_delay * (2 ** (attempt_number - 1))
    return base_delay  # fixed


def get_retry_policy(conn, queue_id):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT strategy, max_retries, base_delay_seconds FROM retry_policies WHERE queue_id = %s", (queue_id,))
        return cur.fetchone() or {"strategy": "fixed", "max_retries": 3, "base_delay_seconds": 30}


def execute_job(job):
    """
    Runs the actual task. This is a stand-in for real work — swap this
    function out for your task registry (e.g. dispatch on payload['task']).
    Raises an exception to simulate/trigger failure handling.
    """
    payload = job["payload"] or {}
    duration = payload.get("simulate_duration_seconds", 1)
    should_fail = payload.get("simulate_failure", False)

    time.sleep(duration)
    if should_fail:
        raise RuntimeError(payload.get("failure_message", "Simulated job failure"))
    return {"result": "ok"}


def process_job(job):
    global active_job_count
    with active_jobs_lock:
        active_job_count += 1

    conn = get_connection()
    job_id = job["id"]
    attempt_number = job["attempt_count"] + 1
    started_at = datetime.utcnow()

    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE jobs SET status = 'running' WHERE id = %s", (job_id,))
        conn.commit()

        execute_job(job)

        with conn.cursor() as cur:
            cur.execute(
                "UPDATE jobs SET status = 'completed', attempt_count = %s, completed_at = %s WHERE id = %s",
                (attempt_number, datetime.utcnow(), job_id),
            )
            log_execution(cur, job_id, attempt_number, "completed", started_at, None)
        conn.commit()
        log.info("Job %s completed (attempt %s)", job_id, attempt_number)

    except Exception as exc:
        conn.rollback()
        handle_failure(conn, job, attempt_number, started_at, str(exc))
    finally:
        conn.close()
        with active_jobs_lock:
            active_job_count -= 1


def log_execution(cur, job_id, attempt_number, status, started_at, error_message):
    duration_ms = int((datetime.utcnow() - started_at).total_seconds() * 1000)
    cur.execute(
        "INSERT INTO job_executions (id, job_id, worker_id, attempt_number, status, "
        "started_at, finished_at, error_message, duration_ms) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (str(uuid.uuid4()), job_id, WORKER_ID, attempt_number, status,
         started_at, datetime.utcnow(), error_message, duration_ms),
    )


def handle_failure(conn, job, attempt_number, started_at, error_message):
    job_id = job["id"]
    max_retries = job["max_retries"]
    policy = get_retry_policy(conn, job["queue_id"])

    with conn.cursor() as cur:
        log_execution(cur, job_id, attempt_number, "failed", started_at, error_message)

        if attempt_number >= max_retries:
            cur.execute("UPDATE jobs SET status = 'dead', attempt_count = %s WHERE id = %s",
                        (attempt_number, job_id))
            cur.execute(
                "INSERT INTO dead_letter_queue (id, job_id, reason, moved_at) VALUES (%s,%s,%s,%s)",
                (str(uuid.uuid4()), job_id, error_message, datetime.utcnow()),
            )
            cur.execute(
                "INSERT INTO job_logs (id, job_id, message, level, created_at) VALUES (%s,%s,%s,%s,%s)",
                (str(uuid.uuid4()), job_id, f"Exhausted retries, moved to DLQ: {error_message}", "error", datetime.utcnow()),
            )
            log.warning("Job %s moved to Dead Letter Queue after %s attempts", job_id, attempt_number)
        else:
            delay = compute_backoff_seconds(policy["strategy"], policy["base_delay_seconds"], attempt_number)
            next_run = datetime.utcnow() + timedelta(seconds=delay)
            cur.execute(
                "UPDATE jobs SET status = 'scheduled', attempt_count = %s, run_at = %s WHERE id = %s",
                (attempt_number, next_run, job_id),
            )
            cur.execute(
                "INSERT INTO job_logs (id, job_id, message, level, created_at) VALUES (%s,%s,%s,%s,%s)",
                (str(uuid.uuid4()), job_id, f"Attempt {attempt_number} failed, retrying in {delay}s: {error_message}", "warn", datetime.utcnow()),
            )
            log.info("Job %s scheduled for retry in %ss (attempt %s/%s)", job_id, delay, attempt_number, max_retries)
    conn.commit()


def mark_worker_shutting_down():
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("UPDATE workers SET status = 'shutting_down' WHERE id = %s", (WORKER_ID,))
    conn.commit()
    conn.close()


def handle_signal(signum, frame):
    log.info("Received signal %s, finishing in-flight jobs before exit...", signum)
    shutdown_event.set()
    mark_worker_shutting_down()


def main():
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    register_worker()
    threading.Thread(target=send_heartbeat, daemon=True).start()

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_JOBS) as executor:
        futures = []
        while not shutdown_event.is_set():
            with active_jobs_lock:
                free_slots = MAX_CONCURRENT_JOBS - active_job_count

            if free_slots > 0:
                job = claim_next_job()
                if job:
                    log.info("Claimed job %s", job["id"])
                    futures.append(executor.submit(process_job, job))
                    continue  # try to fill more slots immediately

            futures = [f for f in futures if not f.done()]
            time.sleep(POLL_INTERVAL_SECONDS)

        log.info("Shutdown signal received, waiting for %d in-flight job(s) to finish...", len(
            [f for f in futures if not f.done()]))
        for f in futures:
            f.result()  # block until each in-flight job finishes

    log.info("Worker %s exited cleanly", WORKER_NAME)
    sys.exit(0)


if __name__ == "__main__":
    main()
