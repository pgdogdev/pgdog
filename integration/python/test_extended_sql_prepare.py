import uuid

import asyncpg
import pytest
from globals import admin, no_out_of_sync, normal_async, sharded_async


@pytest.fixture
def full_prepared_statements():
    with admin() as connection:
        connection.execute("SET prepared_statements TO 'full'")
        try:
            yield
        finally:
            connection.execute("RELOAD")


@pytest.mark.asyncio
@pytest.mark.parametrize("connect", [normal_async, sharded_async])
@pytest.mark.parametrize("extended_prepare", [False, True])
async def test_sql_prepare_execute_extended(connect, extended_prepare, full_prepared_statements):
    connection = await connect()
    name = "extended_sql_" + uuid.uuid4().hex
    try:
        sql = f"PREPARE {name} AS SELECT $1::integer * 2"
        if extended_prepare:
            outer = await connection.prepare(sql, timeout=5)
            assert outer.get_parameters() == ()
            assert outer.get_attributes() == ()
            for _ in range(3):
                assert await outer.fetch(timeout=5) == []
                assert outer.get_statusmsg() == "PREPARE"
        else:
            assert await connection.execute(sql, timeout=5) == "PREPARE"

        execute = await connection.prepare(f"EXECUTE {name}(21)", timeout=5)
        for _ in range(3):
            assert [tuple(row) for row in await execute.fetch(timeout=5)] == [(42,)]
        assert await connection.fetchval("SELECT 1", timeout=5) == 1
        no_out_of_sync()
    finally:
        await connection.close(timeout=5)


@pytest.mark.asyncio
async def test_extended_sql_prepare_error_recovers(full_prepared_statements):
    connection = await normal_async()
    name = "extended_sql_error_" + uuid.uuid4().hex
    try:
        outer = await connection.prepare(
            f"PREPARE {name} AS SELECT nonexistent_issue1403_column", timeout=5
        )
        with pytest.raises(asyncpg.UndefinedColumnError):
            await outer.fetch(timeout=5)
        assert await connection.fetchval("SELECT 1", timeout=5) == 1
        no_out_of_sync()
    finally:
        await connection.close(timeout=5)
