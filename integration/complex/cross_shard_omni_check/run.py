"""
Regression test for <https://github.com/pgdogdev/pgdog/issues/1374>
This exception throws when not working as intended: asyncpg.exceptions.PostgresSystemError: cannot write to an omnisharded table in a direct-to-shard transaction
Used to trigger when...
    (1) in pgdog.toml, read_write_strategy = conservative (default). This is why it's in integration/complex. Integration pgdog.toml uses aggressive.
    (2) SET pgdog.shard (or comment directive)
    (3) running within a transaction
    (4) querying an omnisharded table (SELECT, read)

Being that it's a transaction, it's treated as a write due to read_write_strategy setting.
SET pgdog.shard is higher priority than search_path, so it overtakes it in `ShardsWithPriority`.
Thus, when `peek()`` is called in `ShardsWithPriority`, it will show the Set, and therefore,
when `!is_search_path() -> `requires_full_shard_coverage()` -> `is_omnishard_unsafe()`, it'll return as true (marking it unsafe),
which did not consider the SET (manual routing) in the schema sharding setup.
"""

import asyncio
import sys

import asyncpg
import psycopg2

from asyncpg import Record

APPLICATION_NAME = "pgdog_cross_shard_omni"

async def test_cross_shard_omni() -> None:
    conn = await asyncpg.connect(
        host="127.0.0.1",
        port=6432,
        database="pgdog",
        user="pgdog",
        password="pgdog",
    )

    try:
        await conn.execute(f"SET application_name = '{APPLICATION_NAME}'")
        await conn.execute("CREATE SCHEMA IF NOT EXISTS customer_a")
        await conn.execute("CREATE SCHEMA IF NOT EXISTS customer_b")

        async with conn.transaction():
            await conn.execute("SET LOCAL search_path TO customer_a")
            await conn.execute("SET LOCAL pgdog.shard TO 0")
            row = await conn.fetchrow("""
                    SELECT
                        typname AS name, oid, typarray AS array_oid,
                        oid::regtype::text AS regtype, typdelim AS delimiter
                    FROM pg_type t
                    WHERE t.oid = to_regtype('int4')
                    ORDER BY t.oid;
                    """)

            # Not necessary, the error will bypass to the finally block, but why not?
            assert row is not None
            assert type(row) is Record
            assert row['name'] == "int4"

    finally:
        try:
            await conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(test_cross_shard_omni())
