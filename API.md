# API Documentation

Base URL: `http://localhost:5000/api`

All endpoints except `/auth/signup` and `/auth/login` require:
```
Authorization: Bearer <access_token>
```

---

## Auth

### `POST /auth/signup`
```json
// request
{ "email": "you@test.com", "password": "pass123", "name": "Your Name" }
// 201 response
{ "access_token": "...", "user_id": "uuid" }
```
`409` if the email is already registered. `400` if email/password missing.

### `POST /auth/login`
```json
// request
{ "email": "you@test.com", "password": "pass123" }
// 200 response
{ "access_token": "...", "user_id": "uuid" }
```
`401` on invalid credentials.

---

## Projects

### `GET /projects`
Returns all projects owned by the authenticated user.
```json
[{ "id": "uuid", "name": "My Project", "created_at": "iso-timestamp" }]
```

### `POST /projects`
```json
// request
{ "name": "My Project" }
// 201 response
{ "id": "uuid", "name": "My Project" }
```

### `GET /projects/<id>` / `DELETE /projects/<id>`
Standard get/delete. Delete cascades to all queues, jobs, and related records. `404` if not found or not owned by the caller.

---

## Queues

### `POST /queues`
```json
// request
{
  "project_id": "uuid",
  "name": "default",
  "priority": 0,               // optional, default 0. Higher = claimed first.
  "concurrency_limit": 5,      // optional, default 5
  "retry_strategy": "fixed",   // optional: fixed | linear | exponential
  "max_retries": 3,            // optional, default 3
  "base_delay_seconds": 30     // optional, default 30
}
// 201 response
{ "id": "uuid", "name": "default" }
```

### `GET /queues/by-project/<project_id>`
Lists all queues in a project.
```json
[{ "id": "uuid", "name": "default", "priority": 0, "concurrency_limit": 5, "is_paused": false }]
```

### `GET /queues/<id>`
Single queue detail (same shape as above, minus the array wrapper).

### `POST /queues/<id>/pause` / `POST /queues/<id>/resume`
Toggles whether the worker pool will claim jobs from this queue. Response: `{ "id": "uuid", "is_paused": true|false }`

### `GET /queues/<id>/stats`
```json
{
  "queue_id": "uuid",
  "is_paused": false,
  "counts": { "queued": 3, "scheduled": 1, "claimed": 0, "running": 1,
              "completed": 40, "failed": 0, "retrying": 0, "dead": 2 }
}
```

---

## Jobs

### `POST /jobs`
```json
// request (immediate)
{ "queue_id": "uuid", "job_type": "immediate", "payload": {"task": "send_email"} }

// request (delayed)
{ "queue_id": "uuid", "job_type": "delayed", "delay_seconds": 60, "payload": {} }

// request (scheduled)
{ "queue_id": "uuid", "job_type": "scheduled", "run_at": "2026-07-15T09:00:00", "payload": {} }

// request (recurring)
{ "queue_id": "uuid", "job_type": "recurring", "cron_expression": "*/5 * * * *", "payload": {} }
```
Optional fields on any type: `priority` (int), `max_retries` (int, default 3).

`payload` is passed through as-is to the worker's `execute_job()`. For this
submission's demo task runner, it recognizes:
- `simulate_duration_seconds` — how long the fake task "runs"
- `simulate_failure` — if true, raises an exception (triggers retry/DLQ flow)
- `failure_message` — the exception message used

201 response: `{ "id": "uuid", "status": "queued"|"scheduled", "run_at": "iso-timestamp" }`

`400` if `job_type` invalid, or type-specific required fields are missing
(`run_at` for scheduled, valid `cron_expression` for recurring). `404` if
`queue_id` doesn't exist.

### `POST /jobs/batch`
```json
// request
{ "queue_id": "uuid", "payloads": [{"n": 1}, {"n": 2}, {"n": 3}] }
// 201 response
{ "batch_id": "uuid", "job_ids": ["uuid1", "uuid2", "uuid3"] }
```
Creates N jobs sharing one `batch_id`, each `immediate` and `queued`.

### `GET /jobs`
Query params: `queue_id`, `status`, `page` (default 1), `per_page` (default 20, max 100).
```json
{
  "page": 1, "per_page": 20, "total": 42,
  "jobs": [{ "id": "uuid", "status": "completed", "job_type": "immediate",
             "attempt_count": 1, "run_at": "iso", "created_at": "iso" }]
}
```

### `GET /jobs/<id>`
Full job detail including payload and execution logs:
```json
{
  "id": "uuid", "status": "failed", "job_type": "immediate",
  "payload": {}, "attempt_count": 2, "max_retries": 3,
  "run_at": "iso", "created_at": "iso", "completed_at": null,
  "logs": [{ "message": "Attempt 1 failed, retrying in 30s: boom", "level": "warn", "created_at": "iso" }]
}
```

### `POST /jobs/<id>/retry`
Manually re-queues a `failed` or `dead` job (resets `attempt_count` to 0,
`run_at` to now). `400` if the job isn't in a retryable state.

### `GET /jobs/dead-letter`
Lists all Dead Letter Queue entries:
```json
[{ "id": "uuid", "job_id": "uuid", "reason": "boom", "moved_at": "iso" }]
```

---

## Workers

### `GET /workers`
```json
[{ "id": "uuid", "name": "worker-a1b2c3d4", "status": "alive", "last_heartbeat": "iso" }]
```
`status` is computed on read: a worker is reported `dead` if its
`last_heartbeat` is older than 30 seconds, regardless of its last known
stored status — this is how the dashboard detects crashed workers without a
separate reaper process.

---

## Error Format

All errors follow the same shape:
```json
{ "error": "human-readable message" }
```
with a matching HTTP status code (`400` validation, `401` auth, `404` not found, `409` conflict).

## Notes for Extension
- Pagination follows `page`/`per_page`/`total` convention on every list endpoint that could grow large; only `/jobs` has it currently since it's the highest-volume table. Extend `/queues/by-project` similarly if a project could reasonably have hundreds of queues.
- No rate limiting is implemented (listed as a bonus feature, not built — see `DESIGN_DECISIONS.md`).
