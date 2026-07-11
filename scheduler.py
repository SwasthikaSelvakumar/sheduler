"""
Recurring job scheduler.

Design: a `recurring` Job row acts as a *template*, never executed directly.
It stores a cron_expression and a next_run_at (piggybacked on run_at). This
process polls for templates whose next_run_at has passed, spawns a concrete
`immediate` job instance (which the worker pool then claims/executes
normally), and advances the template's run_at to the next cron occurrence.

Runs as its own container so cron scheduling logic never blocks or
competes with job execution. It only ever INSERTs new job rows and UPDATEs
its own template rows — it never touches jobs it spawned, so it can't
conflict with the worker pool's claiming logic.
"""
import os
import time
import uuid
import logging
from datetime import datetime

import psycopg2
import psycopg2.extras
from croniter import croniter

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("scheduler")

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://scheduler:scheduler@db:5432/scheduler")
POLL_INTERVAL_SECONDS = float(os.environ.get("SCHEDULER_POLL_INTERVAL_SECONDS", 10))


def get_connection():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    return conn


def spawn_due_recurring_jobs():
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Lock template rows so a second scheduler replica (if ever run)
            # can't double-spawn the same occurrence.
            cur.execute(
                """
                SELECT id, queue_id, payload, cron_expression, max_retries
                FROM jobs
                WHERE job_type = 'recurring' AND run_at <= %s
                FOR UPDATE SKIP LOCKED
                """,
                (datetime.utcnow(),),
            )
            due = cur.fetchall()

            for template in due:
                cron_expr = template["cron_expression"]
                if not cron_expr or not croniter.is_valid(cron_expr):
                    log.warning("Recurring job %s has invalid cron_expression %r, skipping",
                                template["id"], cron_expr)
                    continue

                # Spawn the concrete, executable job instance
                new_job_id = str(uuid.uuid4())
                cur.execute(
                    """
                    INSERT INTO jobs (id, queue_id, job_type, status, payload,
                                       run_at, max_retries, created_at, updated_at)
                    VALUES (%s, %s, 'immediate', 'queued', %s, %s, %s, %s, %s)
                    """,
                    (new_job_id, template["queue_id"], psycopg2.extras.Json(template["payload"]),
                     datetime.utcnow(), template["max_retries"], datetime.utcnow(), datetime.utcnow()),
                )

                # Advance the template to its next scheduled occurrence
                next_run = croniter(cron_expr, datetime.utcnow()).get_next(datetime)
                cur.execute("UPDATE jobs SET run_at = %s WHERE id = %s", (next_run, template["id"]))

                log.info("Spawned job %s from recurring template %s, next occurrence at %s",
                          new_job_id, template["id"], next_run)

        conn.commit()
    except Exception:
        conn.rollback()
        log.exception("Failed while spawning recurring jobs")
    finally:
        conn.close()


def main():
    log.info("Recurring job scheduler started (polling every %ss)", POLL_INTERVAL_SECONDS)
    while True:
        spawn_due_recurring_jobs()
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
