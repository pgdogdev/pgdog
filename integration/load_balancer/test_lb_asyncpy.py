import asyncpg
import pytest
import pytest_asyncio
from datetime import datetime
import json

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

    row = await conn.fetch("SELECT * FROM users WHERE id = $1", 1)
    assert row[0][1] == "test@test.com"
