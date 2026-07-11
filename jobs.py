from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
import uuid
import enum

db = SQLAlchemy()


def gen_uuid():
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# Enums — keep job/worker state transitions explicit and validated in code
# ---------------------------------------------------------------------------
class JobStatus(str, enum.Enum):
    QUEUED = "queued"
    SCHEDULED = "scheduled"
    CLAIMED = "claimed"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    DEAD = "dead"          # moved to Dead Letter Queue


class JobType(str, enum.Enum):
    IMMEDIATE = "immediate"
    DELAYED = "delayed"
    SCHEDULED = "scheduled"
    RECURRING = "recurring"
    BATCH = "batch"


class RetryStrategy(str, enum.Enum):
    FIXED = "fixed"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"


class WorkerStatus(str, enum.Enum):
    ALIVE = "alive"
    DEAD = "dead"
    SHUTTING_DOWN = "shutting_down"


# ---------------------------------------------------------------------------
# Core entities
# ---------------------------------------------------------------------------
class User(db.Model):
    __tablename__ = "users"
    id = db.Column(db.String(36), primary_key=True, default=gen_uuid)
    email = db.Column(db.String(255), unique=True, nullable=False, index=True)
    password_hash = db.Column(db.String(255), nullable=False)
    name = db.Column(db.String(120))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    projects = db.relationship("Project", backref="owner", cascade="all, delete-orphan")


class Project(db.Model):
    __tablename__ = "projects"
    id = db.Column(db.String(36), primary_key=True, default=gen_uuid)
    name = db.Column(db.String(120), nullable=False)
    owner_id = db.Column(db.String(36), db.ForeignKey("users.id"), nullable=False, index=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    queues = db.relationship("Queue", backref="project", cascade="all, delete-orphan")


class Queue(db.Model):
    __tablename__ = "queues"
    id = db.Column(db.String(36), primary_key=True, default=gen_uuid)
    project_id = db.Column(db.String(36), db.ForeignKey("projects.id"), nullable=False, index=True)
    name = db.Column(db.String(120), nullable=False)
    priority = db.Column(db.Integer, default=0, index=True)   # higher = claimed first
    concurrency_limit = db.Column(db.Integer, default=5)
    is_paused = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    __table_args__ = (db.UniqueConstraint("project_id", "name", name="uq_queue_name_per_project"),)

    jobs = db.relationship("Job", backref="queue", cascade="all, delete-orphan")
    retry_policy = db.relationship("RetryPolicy", uselist=False, backref="queue", cascade="all, delete-orphan")


class RetryPolicy(db.Model):
    __tablename__ = "retry_policies"
    id = db.Column(db.String(36), primary_key=True, default=gen_uuid)
    queue_id = db.Column(db.String(36), db.ForeignKey("queues.id"), nullable=False, unique=True)
    strategy = db.Column(db.Enum(RetryStrategy), default=RetryStrategy.FIXED)
    max_retries = db.Column(db.Integer, default=3)
    base_delay_seconds = db.Column(db.Integer, default=30)


class Job(db.Model):
    __tablename__ = "jobs"
    id = db.Column(db.String(36), primary_key=True, default=gen_uuid)
    queue_id = db.Column(db.String(36), db.ForeignKey("queues.id"), nullable=False, index=True)

    job_type = db.Column(db.Enum(JobType), default=JobType.IMMEDIATE)
    status = db.Column(db.Enum(JobStatus), default=JobStatus.QUEUED, index=True)
    payload = db.Column(db.JSON, default=dict)          # arbitrary task data
    priority = db.Column(db.Integer, default=0)

    run_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)  # for delayed/scheduled
    cron_expression = db.Column(db.String(120), nullable=True)            # for recurring
    batch_id = db.Column(db.String(36), nullable=True, index=True)        # groups batch jobs

    attempt_count = db.Column(db.Integer, default=0)
    max_retries = db.Column(db.Integer, default=3)

    claimed_by = db.Column(db.String(36), db.ForeignKey("workers.id"), nullable=True)
    claimed_at = db.Column(db.DateTime, nullable=True)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    completed_at = db.Column(db.DateTime, nullable=True)

    executions = db.relationship("JobExecution", backref="job", cascade="all, delete-orphan")
    logs = db.relationship("JobLog", backref="job", cascade="all, delete-orphan")

    # index on (status, run_at) is the single most important index in this
    # schema: it's exactly what the worker's claim query filters/sorts on
    __table_args__ = (db.Index("ix_jobs_status_runat", "status", "run_at"),)


class JobExecution(db.Model):
    """One row per attempt — gives full retry history per job."""
    __tablename__ = "job_executions"
    id = db.Column(db.String(36), primary_key=True, default=gen_uuid)
    job_id = db.Column(db.String(36), db.ForeignKey("jobs.id"), nullable=False, index=True)
    worker_id = db.Column(db.String(36), db.ForeignKey("workers.id"), nullable=True)
    attempt_number = db.Column(db.Integer, nullable=False)
    status = db.Column(db.Enum(JobStatus), nullable=False)
    started_at = db.Column(db.DateTime, default=datetime.utcnow)
    finished_at = db.Column(db.DateTime, nullable=True)
    error_message = db.Column(db.Text, nullable=True)
    duration_ms = db.Column(db.Integer, nullable=True)


class JobLog(db.Model):
    __tablename__ = "job_logs"
    id = db.Column(db.String(36), primary_key=True, default=gen_uuid)
    job_id = db.Column(db.String(36), db.ForeignKey("jobs.id"), nullable=False, index=True)
    message = db.Column(db.Text, nullable=False)
    level = db.Column(db.String(20), default="info")
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class Worker(db.Model):
    __tablename__ = "workers"
    id = db.Column(db.String(36), primary_key=True, default=gen_uuid)
    name = db.Column(db.String(120))
    status = db.Column(db.Enum(WorkerStatus), default=WorkerStatus.ALIVE)
    last_heartbeat = db.Column(db.DateTime, default=datetime.utcnow)
    started_at = db.Column(db.DateTime, default=datetime.utcnow)

    heartbeats = db.relationship("WorkerHeartbeat", backref="worker", cascade="all, delete-orphan")


class WorkerHeartbeat(db.Model):
    """Optional detailed heartbeat history (last_heartbeat on Worker is enough
    for liveness checks — this table is for the dashboard's activity graph)."""
    __tablename__ = "worker_heartbeats"
    id = db.Column(db.String(36), primary_key=True, default=gen_uuid)
    worker_id = db.Column(db.String(36), db.ForeignKey("workers.id"), nullable=False, index=True)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)
    active_jobs = db.Column(db.Integer, default=0)


class DeadLetterEntry(db.Model):
    __tablename__ = "dead_letter_queue"
    id = db.Column(db.String(36), primary_key=True, default=gen_uuid)
    job_id = db.Column(db.String(36), db.ForeignKey("jobs.id"), nullable=False, index=True)
    reason = db.Column(db.Text)
    moved_at = db.Column(db.DateTime, default=datetime.utcnow)

    job = db.relationship("Job")
