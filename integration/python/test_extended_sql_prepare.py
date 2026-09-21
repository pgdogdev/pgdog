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


@pytest.fixture
def rewritten_prepared_statements(full_prepared_statements):
    with admin() as connection:
        connection.execute("SET rewrite_enabled TO true")
    yield


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


@pytest.mark.asyncio
@pytest.mark.parametrize("extended_prepare", [False, True])
async def test_extended_execute_limit_offset(extended_prepare, rewritten_prepared_statements):
    connection = await sharded_async()
    schema = "extended_limit_" + uuid.uuid4().hex
    try:
        await connection.execute(f'CREATE SCHEMA "{schema}"')
        await connection.execute(f'CREATE TABLE "{schema}".sharded (id BIGINT PRIMARY KEY)')
        for value in range(1, 56):
            await connection.execute(f'INSERT INTO "{schema}".sharded VALUES ($1)', value)
        cases = [
            ("", "LIMIT 5 OFFSET $1", "(10)", list(range(45, 40, -1))),
            ("", "LIMIT $2 OFFSET $1", "(5, 10)", list(range(50, 40, -1))),
            ("", "LIMIT $1 OFFSET $2", "(5, 10)", list(range(45, 40, -1))),
            ("", "LIMIT 10 OFFSET 5", "", list(range(50, 40, -1))),
            ("WHERE id < $2", "LIMIT $3 OFFSET $1", "(5, 25, 10)", list(range(19, 9, -1))),
            ("WHERE id = 35", "LIMIT 1 OFFSET 0", "", [35]),
        ]
        for index, (predicate, clause, arguments, expected) in enumerate(cases):
            name = f"{schema}_{index}"
            sql = f'PREPARE {name} AS SELECT id FROM "{schema}".sharded {predicate} ORDER BY id DESC {clause}'
            if extended_prepare:
                prepare = await connection.prepare(sql, timeout=5)
                await prepare.fetch(timeout=5)
            else:
                await connection.execute(sql, timeout=5)
            execute = await connection.prepare(f"EXECUTE {name}{arguments}", timeout=5)
            for _ in range(3):
                assert [row[0] for row in await execute.fetch(timeout=5)] == expected
        no_out_of_sync()
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await connection.close(timeout=5)


@pytest.mark.asyncio
@pytest.mark.parametrize("extended_prepare", [False, True])
async def test_extended_execute_generated_values(extended_prepare, rewritten_prepared_statements):
    connection = await sharded_async()
    name = "extended_id_" + uuid.uuid4().hex
    try:
        sql = f"PREPARE {name} AS SELECT pgdog.unique_id()"
        if extended_prepare:
            prepare = await connection.prepare(sql, timeout=5)
            await prepare.fetch(timeout=5)
        else:
            await connection.execute(sql, timeout=5)
        execute = await connection.prepare(f"EXECUTE {name}", timeout=5)
        values = [await execute.fetchval(timeout=5) for _ in range(10)]
        assert len(set(values)) == len(values), "each execution must generate a fresh ID"
        assert all(isinstance(value, int) for value in values)
        no_out_of_sync()
    finally:
        await connection.close(timeout=5)


@pytest.mark.asyncio
async def test_sql_prepare_registers_each_clients_name(full_prepared_statements):
    connections = [await normal_async(), await normal_async()]
    name = "shared_sql_" + uuid.uuid4().hex
    try:
        for value, connection in enumerate(connections):
            prepare = await connection.prepare(f"PREPARE {name} AS SELECT $1::integer", timeout=5)
            await prepare.fetch(timeout=5)
            execute = await connection.prepare(f"EXECUTE {name}({value})", timeout=5)
            assert await execute.fetchval(timeout=5) == value
    finally:
        for connection in connections:
            await connection.close(timeout=5)


@pytest.mark.asyncio
async def test_repeated_unnamed_sql_execute(full_prepared_statements):
    connection = await normal_async()
    name = "unnamed_sql_" + uuid.uuid4().hex
    try:
        await connection.execute(f"PREPARE {name} AS SELECT 42::integer")
        execute = await connection.prepare(f"EXECUTE {name}", name="", timeout=5)
        for _ in range(3):
            assert await execute.fetchval(timeout=5) == 42
    finally:
        await connection.close(timeout=5)
