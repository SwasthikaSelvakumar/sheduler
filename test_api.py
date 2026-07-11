"""
API integration tests using Flask's test client against an in-memory
SQLite DB. This validates request/response contracts, validation, and job
lifecycle transitions at the API layer.

NOTE: SQLite doesn't support `FOR UPDATE SKIP LOCKED`, so these tests never
touch the worker's claiming logic — that's covered separately in
test_atomic_claiming.py, which requires a real Postgres instance.
"""
import os
import pytest

os.environ["DATABASE_URL"] = "sqlite:///:memory:"

from app import create_app  # noqa: E402
from app.models import db  # noqa: E402


@pytest.fixture
def client():
    app = create_app()
    app.config["TESTING"] = True
    with app.app_context():
        db.create_all()
        yield app.test_client()
        db.drop_all()


@pytest.fixture
def auth_headers(client):
    client.post("/api/auth/signup", json={"email": "test@test.com", "password": "pass123"})
    r = client.post("/api/auth/login", json={"email": "test@test.com", "password": "pass123"})
    token = r.get_json()["access_token"]
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def queue_id(client, auth_headers):
    r = client.post("/api/projects", json={"name": "Test Project"}, headers=auth_headers)
    project_id = r.get_json()["id"]
    r = client.post("/api/queues", json={"project_id": project_id, "name": "default"}, headers=auth_headers)
    return r.get_json()["id"]


def test_signup_requires_email_and_password(client):
    r = client.post("/api/auth/signup", json={"email": "a@a.com"})
    assert r.status_code == 400


def test_duplicate_signup_rejected(client):
    client.post("/api/auth/signup", json={"email": "dup@test.com", "password": "pass123"})
    r = client.post("/api/auth/signup", json={"email": "dup@test.com", "password": "pass123"})
    assert r.status_code == 409


def test_login_wrong_password_rejected(client):
    client.post("/api/auth/signup", json={"email": "u@test.com", "password": "correct"})
    r = client.post("/api/auth/login", json={"email": "u@test.com", "password": "wrong"})
    assert r.status_code == 401


def test_job_creation_requires_valid_queue(client, auth_headers):
    r = client.post("/api/jobs", json={"queue_id": "does-not-exist", "job_type": "immediate"},
                     headers=auth_headers)
    assert r.status_code == 404


def test_job_creation_rejects_invalid_job_type(client, auth_headers, queue_id):
    r = client.post("/api/jobs", json={"queue_id": queue_id, "job_type": "not-a-real-type"},
                     headers=auth_headers)
    assert r.status_code == 400


def test_scheduled_job_requires_run_at(client, auth_headers, queue_id):
    r = client.post("/api/jobs", json={"queue_id": queue_id, "job_type": "scheduled"},
                     headers=auth_headers)
    assert r.status_code == 400


def test_recurring_job_requires_valid_cron(client, auth_headers, queue_id):
    r = client.post("/api/jobs", json={"queue_id": queue_id, "job_type": "recurring",
                                        "cron_expression": "not a cron"}, headers=auth_headers)
    assert r.status_code == 400

    r = client.post("/api/jobs", json={"queue_id": queue_id, "job_type": "recurring",
                                        "cron_expression": "*/5 * * * *"}, headers=auth_headers)
    assert r.status_code == 201


def test_immediate_job_is_queued_immediately(client, auth_headers, queue_id):
    r = client.post("/api/jobs", json={"queue_id": queue_id, "job_type": "immediate"},
                     headers=auth_headers)
    assert r.status_code == 201
    assert r.get_json()["status"] == "queued"


def test_batch_job_creation(client, auth_headers, queue_id):
    r = client.post("/api/jobs/batch", json={
        "queue_id": queue_id,
        "payloads": [{"n": 1}, {"n": 2}, {"n": 3}],
    }, headers=auth_headers)
    assert r.status_code == 201
    body = r.get_json()
    assert len(body["job_ids"]) == 3


def test_only_failed_or_dead_jobs_can_be_manually_retried(client, auth_headers, queue_id):
    r = client.post("/api/jobs", json={"queue_id": queue_id, "job_type": "immediate"},
                     headers=auth_headers)
    job_id = r.get_json()["id"]
    # freshly created job is 'queued', not eligible for manual retry
    r = client.post(f"/api/jobs/{job_id}/retry", headers=auth_headers)
    assert r.status_code == 400


def test_queue_pause_resume(client, auth_headers, queue_id):
    r = client.post(f"/api/queues/{queue_id}/pause", headers=auth_headers)
    assert r.get_json()["is_paused"] is True
    r = client.post(f"/api/queues/{queue_id}/resume", headers=auth_headers)
    assert r.get_json()["is_paused"] is False


def test_job_listing_is_paginated_and_filterable(client, auth_headers, queue_id):
    for _ in range(5):
        client.post("/api/jobs", json={"queue_id": queue_id, "job_type": "immediate"}, headers=auth_headers)

    r = client.get(f"/api/jobs?queue_id={queue_id}&status=queued&per_page=3", headers=auth_headers)
    body = r.get_json()
    assert body["total"] == 5
    assert len(body["jobs"]) == 3  # per_page respected
