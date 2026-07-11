import uuid
from datetime import datetime, timedelta

from croniter import croniter
from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required

from ..models import db, Job, JobStatus, JobType, Queue, JobLog, DeadLetterEntry

jobs_bp = Blueprint("jobs", __name__)


@jobs_bp.post("")
@jwt_required()
def create_job():
    data = request.get_json(silent=True) or {}
    queue_id = data.get("queue_id")
    job_type = data.get("job_type", "immediate")

    if not queue_id:
        return jsonify(error="queue_id is required"), 400
    if not Queue.query.get(queue_id):
        return jsonify(error="queue not found"), 404
    if job_type not in [t.value for t in JobType]:
        return jsonify(error=f"invalid job_type, must be one of {[t.value for t in JobType]}"), 400

    run_at = datetime.utcnow()
    status = JobStatus.QUEUED

    if job_type == "delayed":
        delay_seconds = data.get("delay_seconds", 0)
        run_at = datetime.utcnow() + timedelta(seconds=delay_seconds)
        status = JobStatus.SCHEDULED
    elif job_type == "scheduled":
        run_at_str = data.get("run_at")
        if not run_at_str:
            return jsonify(error="run_at (ISO timestamp) is required for scheduled jobs"), 400
        run_at = datetime.fromisoformat(run_at_str)
        status = JobStatus.SCHEDULED
    elif job_type == "recurring":
        cron_expr = data.get("cron_expression")
        if not cron_expr or not croniter.is_valid(cron_expr):
            return jsonify(error="a valid cron_expression is required for recurring jobs"), 400
        # This row is a template picked up by the scheduler service, which
        # spawns concrete 'immediate' job instances at each occurrence and
        # advances run_at to the next one. The template itself is never run.
        run_at = croniter(cron_expr, datetime.utcnow()).get_next(datetime)
        status = JobStatus.QUEUED

    job = Job(
        queue_id=queue_id,
        job_type=job_type,
        status=status,
        payload=data.get("payload", {}),
        priority=data.get("priority", 0),
        run_at=run_at,
        cron_expression=data.get("cron_expression"),
        batch_id=data.get("batch_id"),
        max_retries=data.get("max_retries", 3),
    )
    db.session.add(job)
    db.session.commit()

    return jsonify(id=job.id, status=job.status.value, run_at=job.run_at.isoformat()), 201


@jobs_bp.post("/batch")
@jwt_required()
def create_batch():
    """Create several jobs sharing one batch_id in a single call."""
    data = request.get_json(silent=True) or {}
    queue_id = data.get("queue_id")
    payloads = data.get("payloads", [])

    if not queue_id or not payloads:
        return jsonify(error="queue_id and a non-empty payloads list are required"), 400
    if not Queue.query.get(queue_id):
        return jsonify(error="queue not found"), 404

    batch_id = str(uuid.uuid4())
    created = []
    for payload in payloads:
        job = Job(queue_id=queue_id, job_type=JobType.BATCH, status=JobStatus.QUEUED,
                  payload=payload, batch_id=batch_id)
        db.session.add(job)
        created.append(job)
    db.session.commit()

    return jsonify(batch_id=batch_id, job_ids=[j.id for j in created]), 201


@jobs_bp.get("")
@jwt_required()
def list_jobs():
    queue_id = request.args.get("queue_id")
    status = request.args.get("status")
    page = int(request.args.get("page", 1))
    per_page = min(int(request.args.get("per_page", 20)), 100)

    query = Job.query
    if queue_id:
        query = query.filter_by(queue_id=queue_id)
    if status:
        query = query.filter_by(status=status)

    query = query.order_by(Job.created_at.desc())
    paginated = query.paginate(page=page, per_page=per_page, error_out=False)

    return jsonify(
        page=page,
        per_page=per_page,
        total=paginated.total,
        jobs=[
            {
                "id": j.id, "status": j.status.value, "job_type": j.job_type.value,
                "attempt_count": j.attempt_count, "run_at": j.run_at.isoformat(),
                "created_at": j.created_at.isoformat(),
            }
            for j in paginated.items
        ],
    )


@jobs_bp.get("/<job_id>")
@jwt_required()
def get_job(job_id):
    job = Job.query.get(job_id)
    if not job:
        return jsonify(error="job not found"), 404
    return jsonify(
        id=job.id, status=job.status.value, job_type=job.job_type.value,
        payload=job.payload, attempt_count=job.attempt_count, max_retries=job.max_retries,
        run_at=job.run_at.isoformat(), created_at=job.created_at.isoformat(),
        completed_at=job.completed_at.isoformat() if job.completed_at else None,
        logs=[{"message": l.message, "level": l.level, "created_at": l.created_at.isoformat()} for l in job.logs],
    )


@jobs_bp.post("/<job_id>/retry")
@jwt_required()
def retry_job(job_id):
    """Manually re-queue a dead/failed job from the dashboard."""
    job = Job.query.get(job_id)
    if not job:
        return jsonify(error="job not found"), 404
    if job.status not in (JobStatus.DEAD, JobStatus.FAILED):
        return jsonify(error="only failed or dead jobs can be manually retried"), 400

    job.status = JobStatus.QUEUED
    job.attempt_count = 0
    job.run_at = datetime.utcnow()
    db.session.add(JobLog(job_id=job.id, message="Manually re-queued from dashboard", level="info"))
    db.session.commit()

    return jsonify(id=job.id, status=job.status.value)


@jobs_bp.get("/dead-letter")
@jwt_required()
def list_dead_letter():
    entries = DeadLetterEntry.query.order_by(DeadLetterEntry.moved_at.desc()).all()
    return jsonify([
        {"id": e.id, "job_id": e.job_id, "reason": e.reason, "moved_at": e.moved_at.isoformat()}
        for e in entries
    ])
