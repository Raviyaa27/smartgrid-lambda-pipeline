"""
The shared clock anchor, against the real PostgreSQL from docker compose.

Deliberately never calls `reset_clock`: that would restart the simulation
under any pipeline component that happens to be running.
"""

import psycopg
import pytest

from smartgrid.common.config import get_settings

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module", autouse=True)
def require_postgres():
    try:
        psycopg.connect(get_settings().postgres_dsn, connect_timeout=3).close()
    except psycopg.OperationalError:
        pytest.skip("PostgreSQL is not reachable; run `docker compose up -d` first")


def test_every_caller_gets_the_same_anchor():
    from smartgrid.common.clock_store import shared_clock

    first = shared_clock()
    second = shared_clock()
    assert first.real_start == second.real_start
    assert first.day_seconds == second.day_seconds
    assert first.start == second.start


def test_the_anchor_is_a_single_row():
    with psycopg.connect(get_settings().postgres_dsn) as conn:
        (rows,) = conn.execute("SELECT count(*) FROM ops.sim_clock").fetchone()
    assert rows == 1
