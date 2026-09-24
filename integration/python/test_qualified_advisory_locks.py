import uuid

import psycopg
import pytest


@pytest.mark.parametrize("extended", [False, True])
def test_qualified_advisory_lock_keeps_session_ownership(extended):
    key = uuid.uuid4().int & ((1 << 63) - 1)
    dsn = "host=127.0.0.1 port=6432 user=pgdog password=pgdog dbname=pgdog_sharded"
    with psycopg.connect(dsn, autocommit=True) as owner, psycopg.connect(
        dsn, autocommit=True
    ) as contender:
        def call(connection, function):
            with connection.cursor(binary=extended) as cursor:
                if extended:
                    cursor.execute(
                        f"/* pgdog_shard: 0 */ SELECT pg_catalog.{function}(%s::bigint)",
                        (key,),
                        prepare=True,
                    )
                else:
                    cursor.execute(
                        f"/* pgdog_shard: 0 */ SELECT pg_catalog.{function}({key})",
                        prepare=False,
                    )
                return cursor.fetchone()[0]

        try:
            assert call(owner, "pg_try_advisory_lock") is True
            assert call(contender, "pg_try_advisory_lock") is False
            assert call(owner, "pg_advisory_unlock") is True
            assert call(contender, "pg_try_advisory_lock") is True
            assert call(contender, "pg_advisory_unlock") is True
        finally:
            for connection in (owner, contender):
                connection.execute(
                    "/* pgdog_shard: 0 */ SELECT pg_advisory_unlock_all()"
                )
