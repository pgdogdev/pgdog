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
from asyncpg.exceptions import PostgresSystemError

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

        # Test without using search_path. Just pgdog.shard set.
        # The expected behavior is that it will not get an omnisharded write error solely because we have
        # schema sharding configured and are *NOT* using table sharding.
        async with conn.transaction():
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

        # Test without using pgdog.shard; just use search_path.
        # Nothing is abnormal about this (vs the last one); it should work fine.
        async with conn.transaction():
             await conn.execute("SET LOCAL search_path TO customer_a")
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

        # Test using both pgdog.shard and search_path.
        # pgdog.shard has priority being a `Set` over `SearchPath`
        # The expected behavior is that it will not get an omnisharded write error solely because we have
        # schema sharding configured and are *NOT* using table sharding.
        async with conn.transaction():
             await conn.execute("SET LOCAL pgdog.shard TO 0")
             await conn.execute("SET LOCAL search_path TO customer_a")
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

        # Test using both pgdog.shard and search_path when they disagree on shard mapping.
        # pgdog.shard has priority being a `Set` over `SearchPath`
        # The expected behavior is that it will not get an omnisharded write error solely because we have
        # schema sharding configured and are *NOT* using table sharding.
        # Even though they disagree on a shard mapping, we expect people using SET to know what they're doing,
        # and the query should work fine.
        async with conn.transaction():
             await conn.execute("SET LOCAL pgdog.shard TO 1")
             # customer_b is mapped to 0. Not 1.
             await conn.execute("SET LOCAL search_path TO customer_a")
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


# Try using it in a mixed context now... we expect an error when both sharding tables and schema are configured.
async def test_cross_shard_omni_mixed() -> None:
    # We're using the secondary configuration now ("pgdog2") which is configured as mixed.
    conn = await asyncpg.connect(
        host="127.0.0.1",
        port=6432,
        database="pgdog2",
        user="pgdog",
        password="pgdog",
    )

    try:
        await conn.execute(f"SET application_name = '{APPLICATION_NAME}'")
        await conn.execute("CREATE SCHEMA IF NOT EXISTS customer_a")
        await conn.execute("CREATE SCHEMA IF NOT EXISTS customer_b")

        # The expected behavior is that it WILL get an omnisharded write error solely because we have
        # both schema sharding and table sharding configured (as opposed to not just schema sharding).
        try:
            async with conn.transaction():
                await conn.execute("SET LOCAL pgdog.shard TO 0")
                try:
                    row = await conn.fetchrow("""
                            SELECT
                                typname AS name, oid, typarray AS array_oid,
                                oid::regtype::text AS regtype, typdelim AS delimiter
                            FROM pg_type t
                            WHERE t.oid = to_regtype('int4')
                            ORDER BY t.oid;
                            """)

                    # TODO: We should consider in the future what should happen
                    #       if people are using multiple sharding functions.

                    assert False, "The run should fail because we have both schema sharding and table sharding configured in the database."
                except Exception as e:
                    assert type(e) is PostgresSystemError
                    assert "cannot write to an omnisharded table with a shard directive" in str(e)
        except:
            # Don't worry about the transaction() throwing an error.
            pass
    finally:
        try:
            await conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    asyncio.run(test_cross_shard_omni())
    asyncio.run(test_cross_shard_omni_mixed())
