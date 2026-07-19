"""
Integration tests for the `prefer_primary` setting.

The pgdog.toml under test sets `read_write_split = "prefer_primary"`. This means:

  * by default every query (including plain SELECTs) is routed to the primary, and
  * a query only reaches a replica when it explicitly opts in.

`pg_is_in_recovery()` is the oracle: it returns False on the primary and True on
a streaming standby, independent of replication lag, so it deterministically tells
us which database served the query.
"""

import asyncpg
import pytest
import pytest_asyncio

DSN = "postgres://postgres:postgres@127.0.0.1:6432/postgres"

# Repeat each assertion enough times that round-robin would have hit a replica
# at least once if routing were not pinned to the primary.
ITERATIONS = 20


@pytest_asyncio.fixture
async def conn():
    conn = await asyncpg.connect(DSN)
    yield conn
    await conn.close()


async def in_recovery(conn, query="SELECT pg_is_in_recovery()"):
    return await conn.fetchval(query)


@pytest.mark.asyncio
async def test_default_routes_to_primary(conn):
    """With prefer_primary, plain reads stay on the primary."""
    for _ in range(ITERATIONS):
        assert await in_recovery(conn) is False


@pytest.mark.asyncio
async def test_session_set_replica_routes_to_replica(conn):
    """SET pgdog.role TO 'replica' opts the session onto replicas."""
    await conn.execute("SET pgdog.role TO 'replica'")
    for _ in range(ITERATIONS):
        assert await in_recovery(conn) is True


@pytest.mark.asyncio
async def test_session_reset_back_to_primary(conn):
    """Resetting the role returns subsequent reads to the primary."""
    await conn.execute("SET pgdog.role TO 'replica'")
    assert await in_recovery(conn) is True

    await conn.execute("SET pgdog.role TO 'primary'")
    for _ in range(ITERATIONS):
        assert await in_recovery(conn) is False


@pytest.mark.asyncio
async def test_query_comment_routes_to_replica(conn):
    """A `/* pgdog_role: replica */` comment opts a single query onto replicas."""
    query = "/* pgdog_role: replica */ SELECT pg_is_in_recovery()"
    for _ in range(ITERATIONS):
        assert await in_recovery(conn, query) is True

    # The comment is per-query: the next uncommented read is back on the primary.
    assert await in_recovery(conn) is False
