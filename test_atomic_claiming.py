"""
Proves the core reliability claim of this system: when many workers poll
concurrently, no job is ever claimed by more than one worker.

Requires a REAL Postgres instance (SQLite doesn't implement
FOR UPDATE SKIP LOCKED), so this is skipped automatically unless
TEST_DATABASE_URL is set.

Run it via docker-compose (recommended):
    docker-compose -f docker-compose.yml -f docker-compose.test.yml run --rm test

Or against a local Postgres:
    TEST_DATABASE_URL=postgresql://scheduler:scheduler@localhost:5432/scheduler \
        pytest tests/test_atomic_claiming.py -v
"""
import os
import uuid
import threading
from datetime import datetime

import pytest

TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not TEST_DATABASE_URL,
    reason="TEST_DATABASE_URL not set - this test requires real Postgres, see file docstring",
)

if TEST_DATABASE_URL:
    import psycopg2
    import psycopg2.extras
    os.environ["DATABASE_URL"] = TEST_DATABASE_URL
    from worker import claim_next_job  # noqa: E402


def get_conn():
    return psycopg2.connect(TEST_DATABASE_URL)


@pytest.fixture
def seeded_jobs():
    """Insert one project/queue/retry-policy and N queued jobs directly via SQL,
    bypassing the API so this test only depends on the DB, not Flask."""
    conn = get_conn()
    project_id, queue_id, user_id = str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())
    job_ids = [str(uuid.uuid4()) for _ in range(50)]

    with conn.cursor() as cur:
        cur.execute("INSERT INTO users (id, email, password_hash) VALUES (%s,%s,%s)",
                    (user_id, f"{user_id}@test.com", "x"))
        cur.execute("INSERT INTO projects (id, name, owner_id) VALUES (%s,%s,%s)",
                    (project_id, "Concurrency Test", user_id))
        cur.execute("INSERT INTO queues (id, project_id, name, priority, concurrency_limit) "
                    "VALUES (%s,%s,%s,%s,%s)", (queue_id, project_id, "test-queue", 0, 50))
        cur.execute("INSERT INTO retry_policies (id, queue_id, strategy, max_retries, base_delay_seconds) "
                    "VALUES (%s,%s,%s,%s,%s)", (str(uuid.uuid4()), queue_id, "fixed", 3, 5))
        for jid in job_ids:
            cur.execute(
                "INSERT INTO jobs (id, queue_id, job_type, status, payload, run_at, max_retries, "
                "attempt_count, created_at, updated_at) "
                "VALUES (%s,%s,'immediate','queued','{}', %s, 3, 0, %s, %s)",
                (jid, queue_id, datetime.utcnow(), datetime.utcnow(), datetime.utcnow()),
            )
    conn.commit()
    conn.close()
    return job_ids


def test_no_job_is_claimed_by_more_than_one_worker(seeded_jobs):
    """
    Simulates 10 workers hammering claim_next_job() concurrently for 50
    queued jobs. Correctness bar: the union of all claims exactly equals
    the seeded job set, with zero duplicates.
    """
    claimed_ids = []
    claimed_ids_lock = threading.Lock()

    def worker_loop():
        while True:
            job = claim_next_job()
            if job is None:
                break
            with claimed_ids_lock:
                claimed_ids.append(job["id"])

    threads = [threading.Thread(target=worker_loop) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    assert len(claimed_ids) == len(seeded_jobs), (
        f"expected {len(seeded_jobs)} claims, got {len(claimed_ids)} "
        "(some jobs were never claimed, or claim_next_job leaked)"
    )
    assert len(claimed_ids) == len(set(claimed_ids)), (
        "DUPLICATE CLAIM DETECTED: the same job was claimed by more than one "
        "worker — this is the exact bug atomic claiming exists to prevent"
    )
