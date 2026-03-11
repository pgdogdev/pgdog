import asyncpg
import psycopg
import pytest
from globals import no_out_of_sync


def session_conn(schema):
    return psycopg.connect(
        user="pgdog_session_no_cross_shard",
        password="pgdog",
        dbname="pgdog_schema_no_cross",
        host="127.0.0.1",
        port=6432,
        options=f"-c search_path={schema},public",
    )


async def async_session_conn(schema):
    return await asyncpg.connect(
        user="pgdog_session_no_cross_shard",
        password="pgdog",
        database="pgdog_schema_no_cross",
        host="127.0.0.1",
        port=6432,
        server_settings={"search_path": f"{schema},public"},
        statement_cache_size=0,
    )


def test_session_simple_queries():
    for schema in ["shard_0", "shard_1"]:
        conn = session_conn(schema)
        cur = conn.cursor()
        cur.execute("SELECT 1::bigint")
        assert cur.fetchone()[0] == 1
        conn.commit()
        conn.close()
    no_out_of_sync()


def test_session_ddl_and_dml():
    for schema in ["shard_0", "shard_1"]:
        conn = session_conn(schema)
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute("DROP TABLE IF EXISTS test_session_mode")
        cur.execute("CREATE TABLE test_session_mode(id BIGINT, value TEXT)")

        cur.execute(
            "INSERT INTO test_session_mode (id, value) "
            "VALUES (%s, %s) RETURNING *",
            (1, "hello"),
        )
        row = cur.fetchone()
        assert row[0] == 1
        assert row[1] == "hello"

        cur.execute("SELECT * FROM test_session_mode WHERE id = %s", (1,))
        assert cur.fetchone()[0] == 1

        cur.execute(
            "UPDATE test_session_mode SET value = %s WHERE id = %s RETURNING *",
            ("world", 1),
        )
        row = cur.fetchone()
        assert row[1] == "world"

        cur.execute("DELETE FROM test_session_mode WHERE id = %s", (1,))
        assert cur.rowcount == 1

        cur.execute("DROP TABLE test_session_mode")
        conn.close()
    no_out_of_sync()


def test_session_transactions():
    for schema in ["shard_0", "shard_1"]:
        conn = session_conn(schema)
        cur = conn.cursor()

        cur.execute("DROP TABLE IF EXISTS test_session_tx")
        conn.commit()

        cur.execute("CREATE TABLE test_session_tx(id BIGINT, value TEXT)")
        conn.commit()

        cur.execute(
            "INSERT INTO test_session_tx (id, value) VALUES (%s, %s)", (1, "a")
        )
        cur.execute(
            "INSERT INTO test_session_tx (id, value) VALUES (%s, %s)", (2, "b")
        )
        conn.commit()

        cur.execute("SELECT count(*) FROM test_session_tx")
        assert cur.fetchone()[0] == 2
        conn.commit()

        cur.execute(
            "INSERT INTO test_session_tx (id, value) VALUES (%s, %s)", (3, "c")
        )
        conn.rollback()

        cur.execute("SELECT count(*) FROM test_session_tx")
        assert cur.fetchone()[0] == 2
        conn.commit()

        cur.execute("DROP TABLE test_session_tx")
        conn.commit()
        conn.close()
    no_out_of_sync()


def test_session_transaction_set_local():
    for schema in ["shard_0", "shard_1"]:
        conn = session_conn(schema)
        cur = conn.cursor()

        cur.execute("SET LOCAL statement_timeout TO '10s'")
        cur.execute("SELECT 1::bigint")
        assert cur.fetchone()[0] == 1
        conn.commit()
        conn.close()
    no_out_of_sync()


def test_session_search_path_visible():
    for schema in ["shard_0", "shard_1"]:
        conn = session_conn(schema)
        cur = conn.cursor()
        cur.execute("SHOW search_path")
        search_path = cur.fetchone()[0]
        assert schema in search_path
        conn.commit()
        conn.close()
    no_out_of_sync()


def test_session_multiple_statements_in_transaction():
    for schema in ["shard_0", "shard_1"]:
        conn = session_conn(schema)
        cur = conn.cursor()

        cur.execute("DROP TABLE IF EXISTS test_session_multi")
        conn.commit()
        cur.execute("CREATE TABLE test_session_multi(id BIGINT)")
        conn.commit()

        for i in range(10):
            cur.execute("INSERT INTO test_session_multi (id) VALUES (%s)", (i,))
        conn.commit()

        cur.execute("SELECT count(*) FROM test_session_multi")
        assert cur.fetchone()[0] == 10
        conn.commit()

        cur.execute("DROP TABLE test_session_multi")
        conn.commit()
        conn.close()
    no_out_of_sync()


def no_search_path_conn():
    return psycopg.connect(
        user="pgdog_session_no_cross_shard",
        password="pgdog",
        dbname="pgdog_schema_no_cross",
        host="127.0.0.1",
        port=6432,
    )


def _create_no_sp_test_table():
    for db in ["shard_0", "shard_1"]:
        direct = psycopg.connect(
            user="pgdog", password="pgdog", dbname=db,
            host="127.0.0.1", port=5432,
        )
        direct.autocommit = True
        for schema in ["shard_0", "shard_1"]:
            direct.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            direct.cursor().execute(
                f"CREATE TABLE IF NOT EXISTS {schema}.no_sp_test(id BIGINT, value TEXT)"
            )
            direct.cursor().execute("CREATE TABLE IF NOT EXISTS no_sp_test(id BIGINT, value TEXT)")
        direct.close()


def test_no_search_path_cross_shard_insert_blocked():
    _create_no_sp_test_table()

    conn = no_search_path_conn()
    conn.autocommit = True
    cur = conn.cursor()
    with pytest.raises(psycopg.errors.SystemError, match="cross-shard queries are disabled"):
        cur.execute("INSERT INTO no_sp_test (id, value) VALUES (1, 'test')")
    conn.close()
    no_out_of_sync()


def test_no_search_path_cross_shard_update_blocked():
    conn = no_search_path_conn()
    conn.autocommit = True
    cur = conn.cursor()
    with pytest.raises(psycopg.errors.SystemError, match="cross-shard queries are disabled"):
        cur.execute("UPDATE no_sp_test SET value = 'changed'")
    conn.close()
    no_out_of_sync()


@pytest.mark.asyncio
async def test_async_session_simple_queries():
    for schema in ["shard_0", "shard_1"]:
        conn = await async_session_conn(schema)
        row = await conn.fetchrow("SELECT 1::bigint AS v")
        assert row["v"] == 1
        await conn.close()
    no_out_of_sync()


@pytest.mark.asyncio
async def test_async_session_ddl_and_dml():
    for schema in ["shard_0", "shard_1"]:
        conn = await async_session_conn(schema)

        await conn.execute("DROP TABLE IF EXISTS test_async_session_mode")
        await conn.execute("CREATE TABLE test_async_session_mode(id BIGINT, value TEXT)")

        row = await conn.fetchrow(
            "INSERT INTO test_async_session_mode (id, value) VALUES ($1, $2) RETURNING *",
            1, "hello",
        )
        assert row["id"] == 1
        assert row["value"] == "hello"

        row = await conn.fetchrow(
            "SELECT * FROM test_async_session_mode WHERE id = $1", 1
        )
        assert row["id"] == 1

        row = await conn.fetchrow(
            "UPDATE test_async_session_mode SET value = $1 WHERE id = $2 RETURNING *",
            "world", 1,
        )
        assert row["value"] == "world"

        result = await conn.execute(
            "DELETE FROM test_async_session_mode WHERE id = $1", 1
        )
        assert result == "DELETE 1"

        await conn.execute("DROP TABLE test_async_session_mode")
        await conn.close()
    no_out_of_sync()


@pytest.mark.asyncio
async def test_async_session_transactions():
    for schema in ["shard_0", "shard_1"]:
        conn = await async_session_conn(schema)

        await conn.execute("DROP TABLE IF EXISTS test_async_session_tx")
        await conn.execute("CREATE TABLE test_async_session_tx(id BIGINT, value TEXT)")

        async with conn.transaction():
            await conn.execute(
                "INSERT INTO test_async_session_tx (id, value) VALUES ($1, $2)", 1, "a"
            )
            await conn.execute(
                "INSERT INTO test_async_session_tx (id, value) VALUES ($1, $2)", 2, "b"
            )

        row = await conn.fetchrow("SELECT count(*)::bigint AS c FROM test_async_session_tx")
        assert row["c"] == 2

        try:
            async with conn.transaction():
                await conn.execute(
                    "INSERT INTO test_async_session_tx (id, value) VALUES ($1, $2)",
                    3, "c",
                )
                raise Exception("force rollback")
        except Exception:
            pass

        row = await conn.fetchrow("SELECT count(*)::bigint AS c FROM test_async_session_tx")
        assert row["c"] == 2

        await conn.execute("DROP TABLE test_async_session_tx")
        await conn.close()
    no_out_of_sync()


@pytest.mark.asyncio
async def test_async_session_transaction_set_local():
    for schema in ["shard_0", "shard_1"]:
        conn = await async_session_conn(schema)
        async with conn.transaction():
            await conn.execute("SET LOCAL statement_timeout TO '10s'")
            row = await conn.fetchrow("SELECT 1::bigint AS v")
            assert row["v"] == 1
        await conn.close()
    no_out_of_sync()


@pytest.mark.asyncio
async def test_async_session_search_path_visible():
    for schema in ["shard_0", "shard_1"]:
        conn = await async_session_conn(schema)
        row = await conn.fetchrow("SHOW search_path")
        assert schema in row["search_path"]
        await conn.close()
    no_out_of_sync()


@pytest.mark.asyncio
async def test_async_session_multiple_statements_in_transaction():
    for schema in ["shard_0", "shard_1"]:
        conn = await async_session_conn(schema)

        await conn.execute("DROP TABLE IF EXISTS test_async_session_multi")
        await conn.execute("CREATE TABLE test_async_session_multi(id BIGINT)")

        async with conn.transaction():
            for i in range(10):
                await conn.execute(
                    "INSERT INTO test_async_session_multi (id) VALUES ($1)", i
                )

        row = await conn.fetchrow(
            "SELECT count(*)::bigint AS c FROM test_async_session_multi"
        )
        assert row["c"] == 10

        await conn.execute("DROP TABLE test_async_session_multi")
        await conn.close()
    no_out_of_sync()
