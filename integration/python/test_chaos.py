"""Chaos tests using toxiproxy to inject failures through pgdog.

All tests use SELECT-only transactions through the "failover" database (routed
through toxiproxy). Every returned result is validated for correctness.
"""

import asyncio
import random
import time

import asyncpg
import pytest

from globals import normal_async_db
from stats import Stats
from toxiproxy import toxi, reset_peer  # noqa: F401
from conftest import auth_md5

NUM_CLIENTS = 100


@pytest.mark.asyncio
async def test_primary_transaction(reset_peer, auth_md5):
    stats = Stats()
    tasks = [_test_primary_transaction(stats) for _ in range(NUM_CLIENTS)]
    await asyncio.gather(*tasks)

    print(f"\nchaos stats: {stats}")
    assert stats.get("ok") > 0
    assert stats.get("query_errors") > 0

async def _test_primary_transaction(stats):
    conn = await normal_async_db("failover_primary")

    for _ in range(250):
        val = random.randint(1, 1_000_000_000)

        if conn is None or conn.is_closed():
            try:
                conn = await normal_async_db("failover_primary")
            except Exception:
                stats.incr("connection_errors")
                conn = None
                continue

        try:
            await conn.execute("BEGIN")
            res = await conn.fetch("SELECT $1::bigint", val)

            assert val == res[0][0]
            await conn.execute("COMMIT")
            stats.incr("ok")
        except AssertionError:
            raise
        except Exception:
            stats.incr("query_errors")
            try:
                await conn.close()
            except Exception:
                pass
            conn = None

    if conn is not None:
        await conn.close()

@pytest.mark.asyncio
async def test_primary_idle_in_transaction(idle_timeout, reset_peer, auth_md5):
    stats = Stats()
    tasks = [_test_primary_idle_in_transaction(stats) for _ in range(NUM_CLIENTS)]
    await asyncio.gather(*tasks)

    print(f"\nidle in txn stats: {stats}")
    assert stats.get("idle_killed") > 0


async def _test_primary_idle_in_transaction(stats):
    conn = await normal_async_db("failover_primary")

    for _ in range(250):
        val = random.randint(1, 1_000_000_000)

        if conn is None or conn.is_closed():
            try:
                conn = await normal_async_db("failover_primary")
            except Exception:
                stats.incr("connection_errors")
                conn = None
                continue

        try:
            await conn.execute("BEGIN")
            res = await conn.fetch("SELECT $1::bigint", val)
            assert val == res[0][0]

            # 30% chance: idle past the 50ms timeout to trigger kill.
            if random.random() < 0.3:
                await asyncio.sleep(0.06)

            # This should fail — Postgres killed the session.
            await conn.fetch("SELECT $1::bigint", val)
            await conn.execute("COMMIT")
            stats.incr("ok")
        except AssertionError:
            raise
        except Exception:
            stats.incr("idle_killed")
            try:
                await conn.close()
            except Exception:
                pass
            conn = None

    if conn is not None:
        await conn.close()
