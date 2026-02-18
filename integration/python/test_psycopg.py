import psycopg
import pytest
from globals import direct_sync, no_out_of_sync, normal_sync, sharded_sync


def setup(conn):
    try:
        conn.cursor().execute("DROP TABLE sharded")
    except psycopg.errors.UndefinedTable:
        conn.rollback()
        pass
    conn.cursor().execute(
        """CREATE TABLE sharded (
        id BIGINT,
        value TEXT,
        created_at TIMESTAMPTZ
    )"""
    )
    conn.cursor().execute("TRUNCATE TABLE sharded")
    conn.commit()


def test_connect():
    for conn in [normal_sync(), sharded_sync()]:
        cur = conn.cursor()
        cur.execute("SELECT 1::bigint")
        one = cur.fetchall()
        conn.commit()
        assert len(one) == 1
        assert one[0][0] == 1
    no_out_of_sync()


def test_insert_sharded():
    _run_insert_test(sharded_sync())


def test_insert_normal():
    _run_insert_test(normal_sync())


def _run_insert_test(conn):
    setup(conn)

    for start in [
        1,
        10_000,
        100_000,
        1_000_000_000,
        10_000_000_000,
        10_000_000_000_000,
    ]:
        for offset in range(250):
            id = start + offset
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO sharded (id, value) VALUES (%s, %s) RETURNING *",
                (id, "test"),
            )
            results = cur.fetchall()
            conn.commit()

            assert len(results) == 1
            assert results[0][0] == id

            cur.execute("SELECT * FROM sharded WHERE id = %s", (id,))
            results = cur.fetchall()
            conn.commit()

            assert len(results) == 1
            assert results[0][0] == id
    no_out_of_sync()


def _execute_parameter_count(conn, count: int) -> int:
    placeholders = ", ".join("%s" for _ in range(count))
    query = f"SELECT array_length(ARRAY[{placeholders}], 1)"
    params = list(range(count))
    cur = conn.cursor()
    cur.execute(query, params)
    value = cur.fetchone()[0]
    conn.commit()
    return value


@pytest.mark.parametrize(
    "count, expect_error_keywords",
    [
        (65_535, ()),
        (65_536, ("between 0 and 65535", "too many")),
    ],
)
def test_postgres_variants_parameter_limits(count, expect_error_keywords):
    successes = []
    errors = []
    for connector in (direct_sync, normal_sync):
        conn = connector()
        try:
            if expect_error_keywords:
                with pytest.raises(psycopg.Error) as excinfo:
                    _execute_parameter_count(conn, count)

                message = str(excinfo.value).lower()
                errors.append(message)
                try:
                    conn.rollback()
                except psycopg.Error:
                    pass
            else:
                successes.append(_execute_parameter_count(conn, count))
        finally:
            conn.close()

    if expect_error_keywords:
        assert len(errors) == 2
        for message in errors:
            assert any(keyword in message for keyword in expect_error_keywords)
    else:
        assert successes == [count, count]


def test_pipeline():
    conn = normal_sync()
    conn.autocommit = True

    with conn.pipeline():
        cur = conn.cursor()
        cur.execute("SELECT 1::bigint")
        cur2 = conn.cursor()
        cur2.execute("SELECT 2::bigint")
        cur3 = conn.cursor()
        cur3.execute("SELECT 3::bigint")

        assert cur.fetchone()[0] == 1
        assert cur2.fetchone()[0] == 2
        assert cur3.fetchone()[0] == 3

    conn.close()
    no_out_of_sync()


def test_pipeline_many_queries():
    """Stress the splicing logic with many queries in a single pipeline.

    pgdog splits multi-Execute pipelines into separate sub-requests.
    If any response is dropped during splicing, libpq can't exit pipeline mode.
    """
    conn = normal_sync()
    conn.autocommit = True

    with conn.pipeline():
        cursors = []
        for i in range(50):
            cur = conn.cursor()
            cur.execute("SELECT %s::bigint", (i,))
            cursors.append((cur, i))

        for cur, expected in cursors:
            assert cur.fetchone()[0] == expected

    conn.close()
    no_out_of_sync()


def test_pipeline_executemany():
    """executemany in pipeline mode sends Parse once, then multiple Bind/Execute.

    This is a different splicing pattern than multiple separate execute() calls.
    """
    conn = normal_sync()

    conn.execute("DROP TABLE IF EXISTS pipeline_test")
    conn.execute("CREATE TABLE pipeline_test (id BIGINT PRIMARY KEY, value TEXT)")
    conn.commit()

    with conn.pipeline():
        cur = conn.cursor()
        cur.executemany(
            "INSERT INTO pipeline_test (id, value) VALUES (%s, %s)",
            [(i, f"val_{i}") for i in range(100)],
        )
        conn.commit()

    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM pipeline_test")
    assert cur.fetchone()[0] == 100
    conn.commit()

    conn.execute("DROP TABLE pipeline_test")
    conn.commit()
    conn.close()
    no_out_of_sync()


def test_pipeline_transaction():
    conn = normal_sync()

    conn.execute("DROP TABLE IF EXISTS pipeline_test")
    conn.execute("CREATE TABLE pipeline_test (id BIGINT PRIMARY KEY, value TEXT)")
    conn.commit()

    with conn.pipeline():
        conn.execute("INSERT INTO pipeline_test (id, value) VALUES (%s, %s)", (1, "a"))
        conn.execute("INSERT INTO pipeline_test (id, value) VALUES (%s, %s)", (2, "b"))
        conn.execute("INSERT INTO pipeline_test (id, value) VALUES (%s, %s)", (3, "c"))
        conn.commit()

        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM pipeline_test")
        assert cur.fetchone()[0] == 3
        conn.commit()

    conn.execute("DROP TABLE pipeline_test")
    conn.commit()
    conn.close()
    no_out_of_sync()


def test_pipeline_fetch_after_exit():
    """Results fetched after exiting pipeline context.

    psycopg must drain all pending results when exiting the pipeline block.
    If pgdog didn't send all responses, this triggers 'cannot exit pipeline
    mode while busy'.
    """
    conn = normal_sync()
    conn.autocommit = True

    with conn.pipeline():
        cur1 = conn.cursor()
        cur1.execute("SELECT 1::bigint")
        cur2 = conn.cursor()
        cur2.execute("SELECT 2::bigint")

    assert cur1.fetchone()[0] == 1
    assert cur2.fetchone()[0] == 2

    conn.close()
    no_out_of_sync()


def test_pipeline_repeated():
    """Multiple pipeline blocks on the same connection.

    Tests that pgdog properly resets state between pipelines.
    """
    conn = normal_sync()
    conn.autocommit = True

    for batch in range(10):
        with conn.pipeline():
            cursors = []
            for i in range(10):
                cur = conn.cursor()
                cur.execute("SELECT %s::bigint", (batch * 10 + i,))
                cursors.append((cur, batch * 10 + i))

            for cur, expected in cursors:
                assert cur.fetchone()[0] == expected

    conn.close()
    no_out_of_sync()


def test_pipeline_query_then_nonpipeline():
    """Pipeline followed immediately by non-pipeline query.

    If pipeline exit fails, the subsequent simple query triggers
    'PQsendQuery not allowed in pipeline mode'.
    """
    conn = normal_sync()
    conn.autocommit = True

    with conn.pipeline():
        cur = conn.cursor()
        cur.execute("SELECT 1::bigint")
        cur.fetchone()

    cur = conn.cursor()
    cur.execute("SELECT 2::bigint")
    assert cur.fetchone()[0] == 2

    conn.close()
    no_out_of_sync()


def test_pipeline_error_recovery():
    conn = normal_sync()
    conn.autocommit = True

    with conn.pipeline():
        cur = conn.cursor()
        cur.execute("SELECT 1::bigint")
        assert cur.fetchone()[0] == 1

    with pytest.raises(psycopg.errors.UndefinedTable):
        with conn.pipeline():
            cur = conn.cursor()
            cur.execute("SELECT * FROM nonexistent_table_pipeline_test")
            cur.fetchall()

    with conn.pipeline():
        cur = conn.cursor()
        cur.execute("SELECT 42::bigint")
        assert cur.fetchone()[0] == 42

    conn.close()
    no_out_of_sync()


def test_pipeline_multiple_errors():
    """Pipeline with multiple queries to a non-existent table.

    Tests that pipeline mode properly handles errors when multiple
    queries are sent without waiting for server responses.
    """
    conn = normal_sync()
    conn.autocommit = True

    # Pipeline with multiple errors should return the first error
    # and not timeout or get stuck in "cannot exit pipeline mode"
    with pytest.raises(psycopg.errors.UndefinedTable):
        with conn.pipeline():
            cur = conn.cursor()
            for i in range(5):
                cur.execute("SELECT * FROM no_existing_table")
            cur.fetchone()

    conn.close()
    no_out_of_sync()
