import asyncpg
import pytest
from datetime import datetime
from globals import normal_async, sharded_async, no_out_of_sync, admin
import random
import string
import pytest_asyncio


@pytest_asyncio.fixture
async def conns():
    schema = "".join(
        random.choice(string.ascii_uppercase + string.digits) for _ in range(5)
    )
    conns = await both()
    for conn in conns:
        await setup(conn, schema)

    yield conns

    admin_conn = admin()
    admin_conn.execute("RECONNECT") # Remove lock on schema

    for conn in conns:
        await conn.execute(f'DROP SCHEMA "{schema}" CASCADE')


async def both():
    return [await normal_async(), await sharded_async()]


async def setup(conn, schema):
    await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    await conn.execute(f'SET search_path TO "{schema}",public')
    try:
        await conn.execute("DROP TABLE IF EXISTS sharded")
    except asyncpg.exceptions.UndefinedTableError:
        pass
    await conn.execute(
        """CREATE TABLE sharded (
        id BIGINT PRIMARY KEY,
        value TEXT,
        created_at TIMESTAMPTZ
    )"""
    )


@pytest.mark.asyncio
async def test_connect(conns):
    for c in conns:
        result = await c.fetch("SELECT 1")
        assert result[0][0] == 1

    conn = await normal_async()
    result = await conn.fetch("SELECT 1")
    assert result[0][0] == 1
    no_out_of_sync()


@pytest.mark.asyncio
async def test_multiple_queries(conns):
    for c in conns:
        try:
            await c.fetch("SELECT 1;SELECT 2;")
        except asyncpg.exceptions.PostgresSyntaxError as e:
            assert str(e) == "cannot insert multiple commands into a prepared statement"


@pytest.mark.asyncio
async def test_transaction(conns):
    for c in conns:
        for j in range(50):
            async with c.transaction():
                for i in range(25):
                    result = await c.fetch("SELECT $1::int", i * j)
                    assert result[0][0] == i * j
    no_out_of_sync()


@pytest.mark.asyncio
async def test_error(conns):
    for c in conns:
        for _ in range(250):
            try:
                await c.execute("SELECT sdfsf")
            except asyncpg.exceptions.UndefinedColumnError:
                pass
    no_out_of_sync()


@pytest.mark.asyncio
async def test_error_transaction(conns):
    for c in conns:
        for _ in range(250):
            async with c.transaction():
                try:
                    await c.execute("SELECT sdfsf")
                except asyncpg.exceptions.UndefinedColumnError:
                    pass
            await c.execute("SELECT 1")
    no_out_of_sync()


@pytest.mark.asyncio
async def test_insert_allshard(conns):
    conn = conns[1]
    try:
        async with conn.transaction():
            await conn.execute(
                """CREATE TABLE pytest (
                id BIGINT,
                one TEXT,
                two TIMESTAMPTZ,
                three FLOAT,
                four DOUBLE PRECISION
            )"""
            )
    except asyncpg.exceptions.DuplicateTableError:
        pass
    async with conn.transaction():
        for i in range(250):
            result = await conn.fetch(
                """
                INSERT INTO pytest (id, one, two, three, four) VALUES($1, $2, NOW(), $3, $4)
                RETURNING *
                """,
                i,
                f"one_{i}",
                i * 25.0,
                i * 50.0,
            )
            for shard in range(2):
                assert result[shard][0] == i
                assert result[shard][1] == f"one_{i}"
                assert result[shard][3] == i * 25.0
                assert result[shard][4] == i * 50.0
    await conn.execute("DROP TABLE pytest")
    no_out_of_sync()


@pytest.mark.asyncio
async def test_direct_shard(conns):
    conn = conns[1]
    try:
        await conn.execute("DROP TABLE sharded")
    except asyncpg.exceptions.UndefinedTableError:
        pass
    await conn.execute(
        """CREATE TABLE sharded (
        id BIGINT,
        value TEXT,
        created_at TIMESTAMPTZ
    )"""
    )
    await conn.execute("TRUNCATE TABLE sharded")

    for r in [100_000, 4_000_000_000_000]:
        for id in range(r, r + 250):
            result = await conn.fetch(
                """
                INSERT INTO sharded (
                    id,
                    value,
                    created_at
                ) VALUES ($1, $2, NOW()) RETURNING *""",
                id,
                f"value_{id}",
            )
            assert len(result) == 1
            assert result[0][0] == id
            assert result[0][1] == f"value_{id}"

            result = await conn.fetch("""SELECT * FROM sharded WHERE id = $1""", id)
            assert len(result) == 1
            assert result[0][0] == id
            assert result[0][1] == f"value_{id}"

            result = await conn.fetch(
                """UPDATE sharded SET value = $1 WHERE id = $2 RETURNING *""",
                f"value_{id+1}",
                id,
            )
            assert len(result) == 1
            assert result[0][0] == id
            assert result[0][1] == f"value_{id+1}"

            await conn.execute("""DELETE FROM sharded WHERE id = $1""", id)
            result = await conn.fetch("""SELECT * FROM sharded WHERE id = $1""", id)
            assert len(result) == 0
    no_out_of_sync()


@pytest.mark.asyncio
async def test_delete(conns):
    conn = conns[1]

    for id in range(250):
        await conn.execute("DELETE FROM sharded WHERE id = $1", id)

    no_out_of_sync()


@pytest.mark.asyncio
async def test_copy(conns):
    records = 250
    for i in range(50):
        for conn in conns:
            rows = [[x, f"value_{x}", datetime.now()] for x in range(records)]
            await conn.copy_records_to_table(
                "sharded", records=rows, columns=["id", "value", "created_at"]
            )
            count = await conn.fetch("SELECT COUNT(*) FROM sharded")
            assert len(count) == 1
            assert count[0][0] == records
            await conn.execute("DELETE FROM sharded")


@pytest.mark.asyncio
async def test_execute_many(conns):
    #
    # This WON'T work for multi-shard queries.
    # PgDog decides which shard to go to based on the first Bind
    # message and it can't disconnect from a shard until the connection
    # is synchronized with Sync.
    #
    # TODO: we could do the same thing as we do for COPY
    #       i.e. checkout all connections and manage
    #       their states manually.
    #
    for conn in conns:
        values = [[x, f"value_{x}"] for x in range(50)]
        rows = await conn.fetchmany(
            "INSERT INTO sharded (id, value) VALUES ($1, $2) RETURNING *", values
        )
        assert len(rows) == 50

@pytest.mark.asyncio
async def test_stress():
    for i in range(100):
        # Reconnect
        normal = await normal_async()
        await normal.execute("SET search_path TO '$user', public")
        num = random.randint(1, 1_000_000)
        # assert not await in_transaction(normal)
        await normal.execute("DROP TABLE IF EXISTS test_stress")
        # await not_in_transaction(normal)
        await normal.execute("CREATE TABLE test_stress (id BIGINT)")
        # await not_in_transaction(normal)
        result = await normal.fetch("INSERT INTO test_stress VALUES ($1) RETURNING *", num)
        assert result[0][0] == num

        # await not_in_transaction(normal)
        result = await normal.fetch("SELECT * FROM test_stress WHERE id = $1", num)
        assert result[0][0] == num

        # await not_in_transaction(normal)
        await normal.fetch("TRUNCATE test_stress")

        # await not_in_transaction(normal)
        assert (await normal.fetch("SELECT COUNT(*) FROM test_stress"))[0][0] == 0

        for i in range(50):
            await normal.execute("SELECT 1")

        # await not_in_transaction(normal)
        await normal.execute("DROP TABLE test_stress")


async def in_transaction(conn):
    await conn.fetch("SELECT now() != statement_timestamp()")


@pytest.mark.asyncio
async def test_timestamp_sorting_binary_format():
    """Test timestamp sorting with binary format."""
    from datetime import datetime, timedelta, timezone

    conn = await sharded_async()

    try:
        try:
            await conn.execute("DROP TABLE IF EXISTS timestamp_test CASCADE")
        except asyncpg.exceptions.UndefinedTableError:
            pass

        await conn.execute("""
            CREATE TABLE timestamp_test (
                id BIGINT PRIMARY KEY,
                name TEXT,
                ts TIMESTAMP NOT NULL
            )
        """)

        base_time = datetime.now(timezone.utc).replace(tzinfo=None)
        test_data = [
            (1, "Oldest", base_time - timedelta(days=10)),
            (101, "Old", base_time - timedelta(days=5)),
            (2, "Recent", base_time - timedelta(days=1)),
            (102, "Current", base_time),
            (3, "Future", base_time + timedelta(days=1)),
            (103, "Far future", base_time + timedelta(days=10)),
        ]

        for id_val, name, ts in test_data:
            await conn.execute(
                "INSERT INTO timestamp_test (id, name, ts) VALUES ($1, $2, $3)",
                id_val, name, ts
            )

        rows = await conn.fetch(
            "SELECT id, name, ts FROM timestamp_test ORDER BY ts DESC"
        )

        actual_order = [(row['id'], row['name']) for row in rows]

        expected_order = [
            (103, "Far future"),
            (3, "Future"),
            (102, "Current"),
            (2, "Recent"),
            (101, "Old"),
            (1, "Oldest"),
        ]

        await conn.execute("DROP TABLE IF EXISTS timestamp_test CASCADE")

        assert actual_order == expected_order, "Timestamp sorting failed with asyncpg binary format"

    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_copy_jsonb():
    """Test COPY with JSONB column."""
    import json

    conn = await normal_async()

    try:
        await conn.execute("DROP TABLE IF EXISTS jsonb_copy_test")

        await conn.execute("""
            CREATE TABLE jsonb_copy_test (
                id BIGINT,
                data JSONB
            )
        """)

        test_data = [
            [1, json.dumps({"name": "Alice", "age": 30, "active": True}, sort_keys=True)],
            [2, json.dumps({"name": "Bob", "scores": [95, 87, 92], "metadata": {"region": "US"}}, sort_keys=True)],
            [3, json.dumps({"products": [{"id": 1, "price": 29.99}, {"id": 2, "price": 15.50}]}, sort_keys=True)],
            [4, json.dumps(None)],
            [5, json.dumps({"empty": {}, "list": [], "string": "test"}, sort_keys=True)],
        ]

        await conn.copy_records_to_table(
            "jsonb_copy_test",
            records=test_data,
            columns=["id", "data"]
        )

        result = await conn.fetch("SELECT COUNT(*) FROM jsonb_copy_test")
        assert result[0][0] == 5

        rows = await conn.fetch("SELECT id, data FROM jsonb_copy_test ORDER BY id")

        for (i, row) in enumerate(rows):
            expected = json.loads(test_data[i][1])
            got = json.loads(row[1])
            assert json.dumps(got, sort_keys=True) == json.dumps(expected, sort_keys=True)

    finally:
        await conn.execute("DROP TABLE IF EXISTS jsonb_copy_test")
        await conn.close()
