import asyncio
import socket

import asyncpg
import pytest
import pytest_asyncio

from asyncpg.connection import Connection

PGDOG_DSN = "postgres://pgdog:pgdog@127.0.0.1:6432/pgdog"
DIRECT_DSN = "postgres://pgdog:pgdog@127.0.0.1:5432/pgdog"


def redis_available() -> bool:
    try:
        with socket.create_connection(("127.0.0.1", 6379), timeout=1):
            return True
    except OSError:
        return False


skip_if_no_redis = pytest.mark.skipif(
    not redis_available(), reason="Redis required at 127.0.0.1:6379"
)


@pytest_asyncio.fixture(scope="function")
async def conn():
    c = await asyncpg.connect(PGDOG_DSN)
    yield c
    await c.close()


@pytest_asyncio.fixture(scope="function")
async def direct():
    c = await asyncpg.connect(DIRECT_DSN)
    yield c
    await c.close()


# ---------------------------------------------------------------------------


@skip_if_no_redis
@pytest.mark.asyncio
async def test_cache_hit(conn: Connection, direct: Connection):
    """Verifies that a second identical SELECT is served from the cache instead of PostgreSQL."""
    await conn.execute(
        "CREATE TABLE IF NOT EXISTS cache_test_hit (id BIGINT PRIMARY KEY, val TEXT)"
    )
    await conn.execute("TRUNCATE cache_test_hit")
    await conn.execute("INSERT INTO cache_test_hit VALUES (1, 'hello')")

    rows = await conn.fetch(
        "/* pgdog_cache: no_cache */ SELECT id, val FROM cache_test_hit WHERE id = 1"
    )
    assert len(rows) == 1, "row must exist in PG before cache warm-up"

    # Warm up the cache with a regular (cacheable) SELECT.
    first = await conn.fetch("SELECT id, val FROM cache_test_hit WHERE id = 1")
    assert len(first) == 1, "first SELECT must return the row"

    # Delete the row directly in Postgres, bypassing pgdog so the cache is not invalidated.
    await direct.execute("DELETE FROM cache_test_hit WHERE id = 1")

    gone = await conn.fetch(
        "/* pgdog_cache: no_cache */ SELECT id, val FROM cache_test_hit WHERE id = 1"
    )
    assert len(gone) == 0, "row must be gone in PG after direct delete"

    cached = await conn.fetch("SELECT id, val FROM cache_test_hit WHERE id = 1")
    assert len(cached) == 1, "cached SELECT must still return the row from the cache"
    assert cached[0]["val"] == "hello"

    await conn.execute("DROP TABLE IF EXISTS cache_test_hit")


@skip_if_no_redis
@pytest.mark.asyncio
async def test_cache_bypassed_in_transaction(conn):
    """Verifies that queries inside an explicit transaction are never served from the cache."""
    await conn.execute(
        "CREATE TABLE IF NOT EXISTS cache_test_txn (id BIGINT PRIMARY KEY, val TEXT)"
    )
    await conn.execute("TRUNCATE cache_test_txn")
    await conn.execute("INSERT INTO cache_test_txn VALUES (1, 'original')")

    # Warm up the cache.
    await conn.fetch("SELECT id, val FROM cache_test_txn WHERE id = 1")

    async with conn.transaction():
        await conn.execute("UPDATE cache_test_txn SET val = 'updated' WHERE id = 1")
        in_tx = await conn.fetch("SELECT id, val FROM cache_test_txn WHERE id = 1")
        assert (
            in_tx[0]["val"] == "updated"
        ), "SELECT inside a transaction must see its own write, not the cached one"

    await conn.execute("DROP TABLE IF EXISTS cache_test_txn")


@skip_if_no_redis
@pytest.mark.asyncio
async def test_cache_ttl_expiry(conn: Connection, direct: Connection):
    """Verifies that the cached results expire after the configured TTL."""
    await conn.execute(
        "CREATE TABLE IF NOT EXISTS cache_test_ttl (id BIGINT PRIMARY KEY, val TEXT)"
    )
    await conn.execute("TRUNCATE cache_test_ttl")
    await conn.execute("INSERT INTO cache_test_ttl VALUES (1, 'original')")

    # Warm up the cache with ttl=1.
    await conn.fetch(
        "/* pgdog_cache: cache ttl=1 */ SELECT id, val FROM cache_test_ttl WHERE id = 1"
    )

    # Remove the row directly so the cache is stale.
    await direct.execute("DELETE FROM cache_test_ttl WHERE id = 1")

    # Wait for the cache entry to expire.
    await asyncio.sleep(2)

    rows = await conn.fetch(
        "/* pgdog_cache: cache ttl=1 */ SELECT id, val FROM cache_test_ttl WHERE id = 1"
    )
    assert len(rows) == 0, "after TTL expiry the cached row must no longer be returned"

    await conn.execute("DROP TABLE IF EXISTS cache_test_ttl")


@skip_if_no_redis
@pytest.mark.asyncio
async def test_extended_protocol_different_params_have_different_cache_keys(
    conn: Connection, direct: Connection
):
    """Verifies that the extended protocol uses bind parameter values in the cache key."""
    await conn.execute(
        "CREATE TABLE IF NOT EXISTS cache_test_ext (id BIGINT PRIMARY KEY, val TEXT)"
    )
    await conn.execute("TRUNCATE cache_test_ext")
    await conn.execute("INSERT INTO cache_test_ext VALUES (1, 'one'), (2, 'two')")

    # Warm up the cache for id=1 and id=2.
    r1 = await conn.fetch("SELECT id, val FROM cache_test_ext WHERE id = $1", 1)
    assert len(r1) == 1 and r1[0]["val"] == "one"

    r2 = await conn.fetch("SELECT id, val FROM cache_test_ext WHERE id = $1", 2)
    assert len(r2) == 1 and r2[0]["val"] == "two"

    # Delete both rows directly in PG so that any result must come from the cache.
    await direct.execute("DELETE FROM cache_test_ext")

    cached1 = await conn.fetch("SELECT id, val FROM cache_test_ext WHERE id = $1", 1)
    assert len(cached1) == 1, "id=1 entry must be served from cache"
    assert cached1[0]["val"] == "one"

    cached2 = await conn.fetch("SELECT id, val FROM cache_test_ext WHERE id = $1", 2)
    assert len(cached2) == 1, "id=2 entry must be served from cache"
    assert cached2[0]["val"] == "two"

    await conn.execute("DROP TABLE IF EXISTS cache_test_ext")


@skip_if_no_redis
@pytest.mark.asyncio
async def test_force_cache(conn: Connection, direct: Connection):
    """Verifies that `/* pgdog_cache: force_cache */` updates the cache."""
    await conn.execute(
        "CREATE TABLE IF NOT EXISTS cache_test_force (id BIGINT PRIMARY KEY, val TEXT)"
    )
    await conn.execute("TRUNCATE cache_test_force")
    await conn.execute("INSERT INTO cache_test_force VALUES (1, 'not_forced')")

    # Warm up the cache.
    r1 = await conn.fetch(
        "/* pgdog_cache: cache */ SELECT id, val FROM cache_test_force WHERE id = 1"
    )
    assert len(r1) == 1 and r1[0]["val"] == "not_forced"

    # Update directly in PG so the cache is stale.
    await direct.execute("UPDATE cache_test_force SET val = 'forced' WHERE id = 1")

    # force_cache must re-fetch from PG and overwrite the cached entry.
    r2 = await conn.fetch(
        "/* pgdog_cache: force_cache */ SELECT id, val FROM cache_test_force WHERE id = 1"
    )
    assert len(r2) == 1 and r2[0]["val"] == "forced"

    # Subsequent plain SELECT must now return the updated cached value.
    cached = await conn.fetch("SELECT id, val FROM cache_test_force WHERE id = 1")
    assert len(cached) == 1 and cached[0]["val"] == "forced"

    await conn.execute("DROP TABLE IF EXISTS cache_test_force")


@skip_if_no_redis
@pytest.mark.asyncio
async def test_no_cache_hint_does_not_warm_cache(conn: Connection, direct: Connection):
    """Verifies that `/* pgdog_cache: no_cache */` prevents the response from being stored in cache."""
    await conn.execute(
        "CREATE TABLE IF NOT EXISTS cache_test_no_warm (id BIGINT PRIMARY KEY, val TEXT)"
    )
    await conn.execute("TRUNCATE cache_test_no_warm")
    await conn.execute("INSERT INTO cache_test_no_warm VALUES (1, 'original')")

    # Fetch with no_cache — must NOT warm up the cache.
    r = await conn.fetch(
        "/* pgdog_cache: no_cache */ SELECT id, val FROM cache_test_no_warm WHERE id = 1"
    )
    assert len(r) == 1

    await direct.execute("DELETE FROM cache_test_no_warm WHERE id = 1")

    after = await conn.fetch("SELECT id, val FROM cache_test_no_warm WHERE id = 1")
    assert (
        len(after) == 0
    ), "no_cache hint must not warm up the cache, so PG miss returns 0 rows"

    await conn.execute("DROP TABLE IF EXISTS cache_test_no_warm")


@skip_if_no_redis
@pytest.mark.asyncio
async def test_connection_option_no_cache_bypasses_cache(
    conn: Connection, direct: Connection
):
    """Verifies that passing `pgdog.cache=no_cache` in the connection DSN options
    bypasses the cache for all queries on that connection."""
    await conn.execute(
        "CREATE TABLE IF NOT EXISTS cache_test_param (id BIGINT PRIMARY KEY, val TEXT)"
    )
    await conn.execute("TRUNCATE cache_test_param")
    await conn.execute("INSERT INTO cache_test_param VALUES (1, 'cached_val')")

    # Warm up the cache via a normal connection, then delete row from db.
    await conn.fetch("SELECT id, val FROM cache_test_param WHERE id = 1")
    await direct.execute("DELETE FROM cache_test_param WHERE id = 1")

    # A connection with pgdog.cache=no_cache must bypass the cache and hit PG,
    # returning 0 rows because the row was deleted.
    no_cache_conn = await asyncpg.connect(
        PGDOG_DSN, server_settings={"pgdog.cache": "no_cache"}
    )
    try:
        rows = await no_cache_conn.fetch(
            "SELECT id, val FROM cache_test_param WHERE id = 1"
        )
        assert (
            len(rows) == 0
        ), "connection-level no_cache must bypass the cache and see the deleted row"
    finally:
        await no_cache_conn.close()

    await conn.execute("DROP TABLE IF EXISTS cache_test_param")


@skip_if_no_redis
@pytest.mark.asyncio
async def test_error_response_not_cached(conn: Connection):
    """Verifies that error responses are never stored in the cache."""
    await conn.execute("DROP TABLE IF EXISTS cache_test_error")

    # This SELECT will produce an error (table does not exist).
    try:
        await conn.fetch("SELECT id, val FROM cache_test_error WHERE id = 1")
        assert False, "query on missing table must return an error"
    except asyncpg.exceptions.UndefinedTableError:
        pass

    # Now create the table and insert a row.
    await conn.execute(
        "CREATE TABLE cache_test_error (id BIGINT PRIMARY KEY, val TEXT)"
    )
    await conn.execute("INSERT INTO cache_test_error VALUES (1, 'live')")

    # The same query must now hit PG (the previous error was not cached).
    rows = await conn.fetch("SELECT id, val FROM cache_test_error WHERE id = 1")
    assert len(rows) == 1, "error must not be cached; must return live row"
    assert rows[0]["val"] == "live"

    await conn.execute("DROP TABLE IF EXISTS cache_test_error")
