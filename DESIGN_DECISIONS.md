# Design Decisions & Trade-offs

## 1. Atomic job claiming: `FOR UPDATE SKIP LOCKED` over alternatives

**Decision:** Workers claim jobs with a single SQL statement using Postgres
row-level locking (`SELECT ... FOR UPDATE SKIP LOCKED`, then `UPDATE`).

**Alternatives considered:**
- *Distributed lock (Redis/ZooKeeper)* — adds a whole extra service and a
  new failure mode (lock service down = no jobs claimed) for a problem
  Postgres already solves natively.
- *Optimistic locking (version column + compare-and-swap retry loop)* —
  works, but under high contention many workers would collide on the same
  row and burn cycles retrying. `SKIP LOCKED` instead lets contending
  workers immediately move on to a different row.
- *Message queue (RabbitMQ/SQS) as the source of truth* — genuinely the
  "more production" answer, but it means job state now lives in two
  places (queue + DB) that can drift out of sync, which is exactly the
  kind of bug this assignment is testing for. Postgres-as-single-source-of-truth
  keeps the system easier to reason about, at the cost of being harder to
  scale to extreme throughput than a dedicated broker.

**Trade-off accepted:** this design caps throughput at what a single
Postgres instance can do for row locking. Fine for the scale this
assignment targets; would need to shift toward a broker-backed design if
job volume grew by orders of magnitude.

## 2. Workers and API never talk to each other directly

**Decision:** The API only writes to Postgres. Workers only read/write
Postgres. Nobody calls anybody's HTTP endpoint.

**Why:** This means horizontal scaling (`docker-compose up --scale worker=5`)
requires zero coordination code — a new worker just starts polling the same
table. It also means the API can be redeployed without affecting in-flight
jobs, and a worker crash never needs the API to notice or intervene (the
heartbeat mechanism handles that separately, see #4).

## 3. Recurring jobs = a template row, not a live scheduler-in-memory

**Decision:** A `recurring` job is a row with a `cron_expression`. A separate
`scheduler` process polls for due templates, spawns a concrete `immediate`
job instance per occurrence, and advances the template's `run_at` to the
next occurrence — all inside one row-locked transaction.

**Why not an in-memory cron scheduler (e.g. APScheduler running inside the
API process)?** That approach loses all pending schedules on every
API restart/redeploy, and breaks the moment you run more than one API
replica (every replica would independently fire the same cron job). Storing
the schedule as a durable row sidesteps both problems for free.

**Trade-off accepted:** cron granularity is bounded by the scheduler's poll
interval (10s here) — a job scheduled for `*/1 * * * *` could fire up to
10s late. Acceptable for background jobs; would tighten the poll interval
or switch to a proper cron library with sleep-until-next-occurrence logic
for latency-sensitive schedules.

## 4. Heartbeat-based worker liveness, not a separate "worker registry" service

**Decision:** Each worker updates `workers.last_heartbeat` every 10s. The
API considers a worker "dead" if its heartbeat is older than 30s — computed
on read (`/api/workers`), not via a background reaper process.

**Why:** A crashed worker's claimed jobs are not automatically re-queued in
this submission — that's the honest gap, see the "Not Implemented" section
below. What heartbeats *do* give you for free: the dashboard can
immediately show which workers are actually alive, which is most of the
operational value for the time invested.

## 5. Retry state lives on the `Job` row; full history lives in `JobExecution`

**Decision:** `Job.attempt_count` / `Job.status` reflect current state
(cheap to query for "what's queued right now"). Every attempt additionally
gets its own `JobExecution` row (worker, duration, error, timestamps) for
full audit history.

**Why:** Keeps the hot path (claiming, listing jobs by status) fast — it
only ever touches the `Jobs` table and its `(status, run_at)` index —
while still preserving complete retry forensics for the dashboard and DLQ
investigation, without denormalizing history onto the `Jobs` table itself.

## 6. Dashboard: polling over WebSockets

**Decision:** The dashboard refreshes via `setInterval` + `fetch` every 3s.

**Why:** Given the time budget, WebSockets would have doubled the frontend
implementation surface (connection management, reconnect logic, broadcast
fan-out from multiple API replicas) for a UX improvement (near-instant vs.
3s-stale) that doesn't move the needle on any of the graded engineering
criteria. Listed explicitly as a bonus feature not pursued.

## What Was Not Implemented (and why)

Given a 2-3 day time budget, these were consciously deprioritized in favor
of getting core reliability (atomic claiming, retries, DLQ) fully correct
and tested:

- **Automatic re-queueing of a dead worker's in-flight jobs.** Right now a
  job stuck in `running` because its worker crashed requires a manual
  retry via the dashboard. The correct fix is a periodic sweep: any job in
  `running`/`claimed` whose owning worker's heartbeat is stale gets
  reset to `queued`. Straightforward to add, cut for time.
- **Workflow dependencies, rate limiting, distributed locking beyond
  claiming, queue sharding, RBAC, AI failure summaries** — all listed as
  bonus features in the brief; none implemented.
- **WebSocket live updates** — see #6 above.
- Test suite covers retry math and API contracts fully, plus one
  concurrency proof test; it does not cover the scheduler's cron-spawning
  logic or worker crash-recovery paths (since #1 above isn't implemented).
