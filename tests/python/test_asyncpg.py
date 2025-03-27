import asyncio
import asyncpg
import pytest
import random

async def sharded():
    return await asyncpg.connect(
		user='pgdog',
		password='pgdog',
		database='pgdog_sharded',
		host='127.0.0.1',
		port=6432,
		statement_cache_size=250)

async def normal():
    return await asyncpg.connect(
		user='pgdog',
		password='pgdog',
		database='pgdog',
		host='127.0.0.1',
		port=6432,
		statement_cache_size=250)

async def both():
    return [await sharded(), await normal()]

@pytest.mark.asyncio
async def test_connect():
    for c in await both():
        result = await c.fetch("SELECT 1")
        assert result[0][0] == 1

    conn = await normal()
    result = await conn.fetch("SELECT 1")
    assert result[0][0] == 1

@pytest.mark.asyncio
async def test_transaction():
    for c in await both():
        for j in range(50):
            async with c.transaction():
                for i in range(25):
                    result = await c.fetch("SELECT $1::int", i * j)
                    assert result[0][0] == i * j


@pytest.mark.asyncio
async def test_error():
    for c in await both():
        for _ in range(250):
            try:
                await c.execute("SELECT sdfsf")
            except asyncpg.exceptions.UndefinedColumnError:
                pass

@pytest.mark.asyncio
async def test_error_transaction():
    for c in await both():
        for _ in range(250):
            async with c.transaction():
                try:
                    await c.execute("SELECT sdfsf")
                except asyncpg.exceptions.UndefinedColumnError:
                    pass
            await c.execute("SELECT 1")

@pytest.mark.asyncio
async def test_insert_allshard():
    conn = await sharded();
    try:
        async with conn.transaction():
            await conn.execute("""CREATE TABLE pytest (
                id BIGINT,
                one TEXT,
                two TIMESTAMPTZ,
                three FLOAT,
                four DOUBLE PRECISION
            )""")
    except asyncpg.exceptions.DuplicateTableError:
        pass
    async with conn.transaction():
        for i in range(250):
            result = await conn.fetch("""
                INSERT INTO pytest (id, one, two, three, four) VALUES($1, $2, NOW(), $3, $4)
                RETURNING *
                """, i, f"one_{i}", i * 25.0, i * 50.0)
            for shard in range(2):
                assert result[shard][0] == i
                assert result[shard][1] == f"one_{i}"
                assert result[shard][3] == i * 25.0
                assert result[shard][4] == i * 50.0
    await conn.execute("DROP TABLE pytest")

@pytest.mark.asyncio
async def test_direct_shard():
    conn = await sharded()
    # try:
    #     await conn.execute("DROP TABLE sharded")
    # except asyncpg.exceptions.UndefinedTableError:
    #     pass
    # await conn.execute("""CREATE TABLE sharded (
    #     id BIGINT,
    #     value TEXT,
    #     created_at TIMESTAMPTZ
    # )""")

    for i in range(1):
        id = random.randint(i, i + 1000)
        result = await conn.fetch("""
            INSERT INTO sharded (
                id,
                value,
                created_at
            ) VALUES ($1, $2, NOW()) RETURNING *""",
            id,
            f"value_{id}"
        )
        assert len(result) == 1
        assert result[0][0] == id
        assert result[0][1] == f"value_{id}"
