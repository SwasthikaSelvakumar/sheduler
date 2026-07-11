# Distributed Job Scheduler

A production-inspired distributed job scheduling platform: submit background
jobs via REST API, have a pool of worker processes atomically claim and
execute them concurrently, with retries, dead-lettering, and a live
dashboard.

## Architecture

![Architecture Diagram](docs/architecture.png)

The API, scheduler, and workers never talk to each other directly — they
only communicate through Postgres. This means workers can be scaled
horizontally (`docker-compose up --scale worker=3`) with zero coordination
code, because atomicity is guaranteed by the database, not by the
application.

## Quick Start

```bash
docker-compose up --build
```

- API: http://localhost:5000
- Dashboard: http://localhost:8080
- Postgres: localhost:5432 (user/pass/db: `scheduler`/`scheduler`/`scheduler`)

To prove atomic job claiming with multiple workers:
```bash
docker-compose up --build --scale worker=3
```

## Example Walkthrough

```bash
# 1. Sign up
curl -X POST localhost:5000/api/auth/signup -H "Content-Type: application/json" \
  -d '{"email":"you@test.com","password":"pass123"}'
# copy the access_token from the response

TOKEN="<paste token here>"

# 2. Create a project
curl -X POST localhost:5000/api/projects -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" -d '{"name":"My Project"}'
# copy the project id

# 3. Create a queue
curl -X POST localhost:5000/api/queues -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"project_id":"<project_id>","name":"default","priority":1,"retry_strategy":"exponential","max_retries":3}'
# copy the queue id

# 4. Submit a job (simulate a failure to watch the retry/DLQ flow)
curl -X POST localhost:5000/api/jobs -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"queue_id":"<queue_id>","job_type":"immediate","max_retries":2,
       "payload":{"simulate_duration_seconds":1,"simulate_failure":true,"failure_message":"boom"}}'

# 5. Watch it happen in the dashboard, or poll:
curl localhost:5000/api/jobs -H "Authorization: Bearer $TOKEN"
```

## Job Types Supported
- `immediate` — runs as soon as a worker is free
- `delayed` — pass `delay_seconds`, runs after that delay
- `scheduled` — pass `run_at` (ISO timestamp), runs at that time
- `batch` — POST to `/api/jobs/batch` with a `payloads` list, all jobs share a `batch_id`
- `recurring` — pass `cron_expression` (e.g. `"*/5 * * * *"`). The scheduler
  service spawns a fresh `immediate` job at each occurrence and advances the
  template to the next one; the template row itself is never executed

## API Documentation

See [`docs/API.md`](docs/API.md) for the full endpoint reference (request/response shapes, error format, auth).

## Design Decisions & Trade-offs

See [`docs/DESIGN_DECISIONS.md`](docs/DESIGN_DECISIONS.md) for the full
write-up of what was built, what was intentionally deferred given the time
budget, and why.

## Database Schema

![ER Diagram](docs/er_diagram.png)

- **Indexes**: `jobs(status, run_at)` is the load-bearing index — it's
  exactly what the worker's claim query filters and sorts on. Foreign keys
  are indexed by default via SQLAlchemy relationship columns.
- **Cascading**: deleting a `User` cascades to their `Projects` → `Queues` →
  `Jobs` → `JobExecutions`/`JobLogs`. Deleting a `Job` does not delete its
  `DeadLetterEntry` audit trail independently (kept for forensics).
- **Normalization**: `RetryPolicy` is split out from `Queue` (1:1) rather
  than inlined, since it's a distinct concern that could become 1:N per
  queue in a future iteration (e.g. per-job-type policies) without a schema
  migration touching `Queue` itself.

## Repo Structure

```
scheduler/
├── api/                      # Flask REST API (auth, projects, queues, jobs, workers)
├── worker/
│   ├── worker.py             # Claim/execute/retry/heartbeat loop
│   └── scheduler.py          # Recurring/cron job spawner (separate process)
├── dashboard/                # Static HTML/JS dashboard, served via nginx
├── docs/                     # architecture.png, er_diagram.png, DESIGN_DECISIONS.md
├── tests/
│   ├── test_retry_backoff.py     # Pure unit tests, no DB needed
│   ├── test_api.py               # API contract tests (SQLite)
│   └── test_atomic_claiming.py   # Concurrency proof, needs real Postgres
├── docker-compose.yml
└── docker-compose.test.yml   # Runs the full suite against real Postgres
```

## Running Tests

**Tests that don't need Postgres** (retry math + API contracts):
```bash
cd api && pip install -r requirements.txt
cd ../tests && pip install -r requirements-test.txt
pytest test_retry_backoff.py test_api.py -v
```

**Full suite including the atomic-claiming concurrency proof** (needs Postgres):
```bash
docker-compose -f docker-compose.yml -f docker-compose.test.yml run --rm test
```
