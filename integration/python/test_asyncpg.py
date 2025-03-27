import asyncio
import asyncpg
import pytest
import random
import psycopg

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

def admin():
    conn = psycopg.connect("dbname=admin user=admin password=pgdog host=127.0.0.1 port=6432")
    conn.autocommit = True
    return conn

def no_out_of_sync():
    conn = admin()
    cur = conn.cursor()
    cur.execute("SHOW POOLS;")
    pools = cur.fetchall()
    for pool in pools:
        print(pools)
        assert pool[-1] == 0

async def setup(conn):
    try:
        await conn.execute("DROP TABLE sharded")
    except asyncpg.exceptions.UndefinedTableError:
        pass
    await conn.execute("""CREATE TABLE sharded (
        id BIGINT,
        value TEXT,
        created_at TIMESTAMPTZ
    )""")
    await conn.execute("TRUNCATE TABLE sharded")

@pytest.mark.asyncio
async def test_connect():
    for c in await both():
        result = await c.fetch("SELECT 1")
        assert result[0][0] == 1

    conn = await normal()
    result = await conn.fetch("SELECT 1")
    assert result[0][0] == 1
    no_out_of_sync()

@pytest.mark.asyncio
async def test_transaction():
    for c in await both():
        for j in range(50):
            async with c.transaction():
                for i in range(25):
                    result = await c.fetch("SELECT $1::int", i * j)
                    assert result[0][0] == i * j
    no_out_of_sync()


@pytest.mark.asyncio
async def test_error():
    for c in await both():
        for _ in range(250):
            try:
                await c.execute("SELECT sdfsf")
            except asyncpg.exceptions.UndefinedColumnError:
                pass
    no_out_of_sync()

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
    no_out_of_sync()

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
    no_out_of_sync()

@pytest.mark.asyncio
async def test_direct_shard():
    conn = await sharded()
    try:
        await conn.execute("DROP TABLE sharded")
    except asyncpg.exceptions.UndefinedTableError:
        pass
    await conn.execute("""CREATE TABLE sharded (
        id BIGINT,
        value TEXT,
        created_at TIMESTAMPTZ
    )""")
    await conn.execute("TRUNCATE TABLE sharded")

    for r in [100_000, 4_000_000_000_000]:
        for id in range(r, r+250):
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

            result = await conn.fetch("""SELECT * FROM sharded WHERE id = $1""", id)
            assert len(result) == 1
            assert result[0][0] == id
            assert result[0][1] == f"value_{id}"

            result = await conn.fetch("""UPDATE sharded SET value = $1 WHERE id = $2 RETURNING *""", f"value_{id+1}", id)
            assert len(result) == 1
            assert result[0][0] == id
            assert result[0][1] == f"value_{id+1}"

            await conn.execute("""DELETE FROM sharded WHERE id = $1""", id)
            result = result = await conn.fetch("""SELECT * FROM sharded WHERE id = $1""", id)
            assert len(result) == 0
    no_out_of_sync()

@pytest.mark.asyncio
async def test_delete():
    conn = await sharded()
    await setup(conn)

    for id in range(250):
        await conn.execute("DELETE FROM sharded WHERE id = $1", id)

    no_out_of_sync()
