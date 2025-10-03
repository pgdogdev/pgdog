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
