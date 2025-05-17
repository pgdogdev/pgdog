import asyncpg
import pytest
import pytest_asyncio
from datetime import datetime
import json
from time import sleep
import random
import asyncio

@pytest_asyncio.fixture
async def conn():
    conn = await asyncpg.connect("postgres://postgres:postgres@127.0.0.1:6432/postgres")
    yield conn
    await conn.close()


@pytest.mark.asyncio
async def test_connect(conn):
    for _ in range(25):
        result = await conn.fetch("SELECT 1::integer")
        assert result[0][0] == 1

@pytest.mark.asyncio
async def test_prepared_statements(conn):
    async with conn.transaction():
        await conn.execute("CREATE TABLE IF NOT EXISTS users (id BIGINT, email VARCHAR, created_at TIMESTAMPTZ, data JSONB)")
        result = await conn.fetch("""
            INSERT INTO users (id, email, created_at, data)
            VALUES ($1, $2, $3, $4), ($1, $2, $3, $4) RETURNING *
        """, 1, "test@test.com", datetime.now(), json.dumps({"banned": False}))

        assert len(result) == 2
        for row in result:
            assert row[0] == 1
            assert row[1] == "test@test.com"
            assert row[3] == json.dumps({"banned": False})

    for _ in range(3):
        try:
            row = await conn.fetch("SELECT * FROM users WHERE id = $1", 1)
            assert row[0][1] == "test@test.com"
            break
        except:
            # Replica lag
            sleep(1)

@pytest.mark.asyncio
async def test_concurrent():
    pool = await asyncpg.create_pool("postgres://postgres:postgres@127.0.0.1:6432/postgres")
    tasks = []
    for _ in range(25):
        task = asyncio.create_task(concurrent(pool))
        tasks.append(task)
    for task in tasks:
        await task

async def concurrent(pool):
    for _ in range(25):
        async with pool.acquire() as conn:
            i = random.randint(1, 1_000_000_000)
            row = await conn.fetch("INSERT INTO users (id, created_at) VALUES ($1, NOW()) RETURNING id", i)
            assert row[0][0] == i

        # Read from primary
        async with pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetch("SELECT * FROM users WHERE id = $1", i)
                assert row[0][0] == i

        async with pool.acquire() as conn:
            # Try read from replica
            for _ in range(3):
                try:
                    row = await conn.fetch("SELECT * FROM users WHERE id = $1", i)
                    assert row[0][0] == i
                    break
                except Exception as e:
                    assert "list index out of range" in str(e)
                    sleep(1)

        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM users WHERE id = $1", i)
