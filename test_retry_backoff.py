"""Unit tests for retry backoff math. Pure functions, no DB needed —
run these with plain `pytest tests/test_retry_backoff.py`."""
from worker import compute_backoff_seconds


def test_fixed_backoff_is_constant():
    assert compute_backoff_seconds("fixed", 30, attempt_number=1) == 30
    assert compute_backoff_seconds("fixed", 30, attempt_number=5) == 30


def test_linear_backoff_grows_linearly():
    assert compute_backoff_seconds("linear", 30, attempt_number=1) == 30
    assert compute_backoff_seconds("linear", 30, attempt_number=2) == 60
    assert compute_backoff_seconds("linear", 30, attempt_number=3) == 90


def test_exponential_backoff_doubles():
    assert compute_backoff_seconds("exponential", 30, attempt_number=1) == 30
    assert compute_backoff_seconds("exponential", 30, attempt_number=2) == 60
    assert compute_backoff_seconds("exponential", 30, attempt_number=3) == 120
    assert compute_backoff_seconds("exponential", 30, attempt_number=4) == 240


def test_unknown_strategy_falls_back_to_fixed():
    assert compute_backoff_seconds("not-a-real-strategy", 30, attempt_number=3) == 30
