"""Infrastructure-free tests for pipeline/api.py.

The DB connection pool is patched at the module level so no Postgres is needed.
Each test configures a mock connection whose execute() returns canned rows.
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from pipeline.api import app, get_pool


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _no_real_pool():
    """Prevent the lifespan from opening a real DB pool during tests."""
    with patch("pipeline.api.ConnectionPool"):
        yield


def _cursor(fetchone=None, fetchall=None):
    cur = MagicMock()
    cur.fetchone.return_value = fetchone
    cur.fetchall.return_value = fetchall if fetchall is not None else []
    return cur


def _pool_with(*execute_returns):
    """Return a mock pool whose connection.execute() yields successive cursors."""
    conn = MagicMock()
    conn.execute.side_effect = list(execute_returns)
    pool = MagicMock()
    pool.connection.return_value.__enter__.return_value = conn
    pool.connection.return_value.__exit__.return_value = False
    return pool, conn


@pytest.fixture
def client(request):
    """TestClient with a per-test pool override supplied via request.param."""
    pool = getattr(request, "param", MagicMock())
    app.dependency_overrides[get_pool] = lambda: pool
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# /health
# ---------------------------------------------------------------------------


def test_health_ok():
    pool, conn = _pool_with(_cursor())
    app.dependency_overrides[get_pool] = lambda: pool
    with TestClient(app) as c:
        resp = c.get("/health")
    app.dependency_overrides.clear()
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_health_db_error():
    pool = MagicMock()
    conn = MagicMock()
    conn.execute.side_effect = Exception("db down")
    pool.connection.return_value.__enter__.return_value = conn
    pool.connection.return_value.__exit__.return_value = False
    app.dependency_overrides[get_pool] = lambda: pool
    with TestClient(app) as c:
        resp = c.get("/health")
    app.dependency_overrides.clear()
    assert resp.status_code == 503


# ---------------------------------------------------------------------------
# /users/{user_id}/spend
# ---------------------------------------------------------------------------


def test_user_spend_happy_path():
    pool, _ = _pool_with(
        _cursor(fetchone=(42,)),  # user exists
        _cursor(fetchone=(Decimal("150.50"), 3)),  # spend result
    )
    app.dependency_overrides[get_pool] = lambda: pool
    with TestClient(app) as c:
        resp = c.get("/users/42/spend?from_date=2026-01-01&to_date=2026-01-31")
    app.dependency_overrides.clear()
    assert resp.status_code == 200
    data = resp.json()
    assert data["user_id"] == 42
    assert data["txn_count"] == 3
    assert float(data["total_gbp"]) == pytest.approx(150.50)
    assert data["from_date"] == "2026-01-01"
    assert data["to_date"] == "2026-01-31"


def test_user_spend_user_not_found():
    pool, _ = _pool_with(_cursor(fetchone=None))
    app.dependency_overrides[get_pool] = lambda: pool
    with TestClient(app) as c:
        resp = c.get("/users/999/spend")
    app.dependency_overrides.clear()
    assert resp.status_code == 404
    assert resp.json()["detail"] == "user not found"


def test_user_spend_date_window_defaults():
    pool, _ = _pool_with(
        _cursor(fetchone=(1,)),
        _cursor(fetchone=(Decimal("50.00"), 1)),
    )
    app.dependency_overrides[get_pool] = lambda: pool
    with TestClient(app) as c:
        resp = c.get("/users/1/spend")
    app.dependency_overrides.clear()
    assert resp.status_code == 200
    data = resp.json()
    today = date.today()
    assert data["to_date"] == str(today)
    assert data["from_date"] == str(today - timedelta(days=30))


def test_user_spend_invalid_date_422():
    pool, _ = _pool_with()
    app.dependency_overrides[get_pool] = lambda: pool
    with TestClient(app) as c:
        resp = c.get("/users/1/spend?from_date=not-a-date")
    app.dependency_overrides.clear()
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# /daily-spend
# ---------------------------------------------------------------------------


def test_daily_spend_happy_path():
    rows = [
        (date(2026, 5, 1), "Groceries", Decimal("100.00"), 5, 0),
        (date(2026, 4, 30), "Travel", Decimal("200.00"), 2, 1),
    ]
    pool, _ = _pool_with(_cursor(fetchall=rows))
    app.dependency_overrides[get_pool] = lambda: pool
    with TestClient(app) as c:
        resp = c.get("/daily-spend")
    app.dependency_overrides.clear()
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    assert data[0]["merchant_category"] == "Groceries"
    assert data[1]["flagged_count"] == 1


def test_daily_spend_category_filter():
    rows = [(date(2026, 5, 1), "Travel", Decimal("200.00"), 2, 1)]
    pool, conn = _pool_with(_cursor(fetchall=rows))
    app.dependency_overrides[get_pool] = lambda: pool
    with TestClient(app) as c:
        resp = c.get("/daily-spend?category=Travel&limit=5")
    app.dependency_overrides.clear()
    assert resp.status_code == 200
    # Confirm category was forwarded as a query parameter
    call_params = conn.execute.call_args[0][1]
    assert "Travel" in call_params


def test_daily_spend_limit_too_large_422():
    pool = MagicMock()
    app.dependency_overrides[get_pool] = lambda: pool
    with TestClient(app) as c:
        resp = c.get("/daily-spend?limit=366")
    app.dependency_overrides.clear()
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# /merchants/top
# ---------------------------------------------------------------------------


def test_merchants_top_happy_path():
    rows = [
        (1, "Tesco", "Groceries", Decimal("500.00"), 10),
        (2, "British Airways", "Travel", Decimal("300.00"), 3),
    ]
    pool, _ = _pool_with(_cursor(fetchall=rows))
    app.dependency_overrides[get_pool] = lambda: pool
    with TestClient(app) as c:
        resp = c.get("/merchants/top")
    app.dependency_overrides.clear()
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    assert data[0]["merchant_name"] == "Tesco"
    assert data[0]["merchant_category"] == "Groceries"


def test_merchants_top_limit_too_large_422():
    pool = MagicMock()
    app.dependency_overrides[get_pool] = lambda: pool
    with TestClient(app) as c:
        resp = c.get("/merchants/top?limit=101")
    app.dependency_overrides.clear()
    assert resp.status_code == 422
