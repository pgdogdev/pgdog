import psycopg
import pytest


CASES = [
    ("smallint", -(1 << 15), (1 << 15) - 1),
    ("integer", -(1 << 31), (1 << 31) - 1),
    ("bigint", -(1 << 63), (1 << 63) - 1),
]


def connect(port, database):
    return psycopg.connect(
        host="127.0.0.1", port=port, user="pgdog", password="pgdog",
        dbname=database, autocommit=True,
    )


@pytest.mark.parametrize("binary", [False, True])
@pytest.mark.parametrize("sql_type,minimum,maximum", CASES)
def test_integer_sum_reports_overflow(sql_type, minimum, maximum, binary):
    with connect(5432, "pgdog") as direct, connect(6432, "pgdog_sharded") as setup:
        try:
            for connection in (direct, setup):
                connection.execute("DROP TABLE IF EXISTS sql_regression_samples")
                connection.execute("CREATE TABLE sql_regression_samples (id BIGINT PRIMARY KEY, amount BIGINT)")
            for left, right in [(maximum, 1), (minimum, -1)]:
                for connection in (direct, setup):
                    connection.execute("TRUNCATE sql_regression_samples")
                    connection.execute("INSERT INTO sql_regression_samples VALUES (1, %s)", (left,))
                    connection.execute("INSERT INTO sql_regression_samples VALUES (101, %s)", (right,))
                query = f"SELECT SUM(amount)::{sql_type} FROM sql_regression_samples"
                with pytest.raises(psycopg.errors.NumericValueOutOfRange):
                    direct.execute(query)
                with connect(6432, "pgdog_sharded") as sharded:
                    with sharded.cursor(binary=binary) as cursor:
                        with pytest.raises(psycopg.errors.NumericValueOutOfRange):
                            cursor.execute(query)
            assert setup.execute("SELECT 1").fetchone() == (1,)
        finally:
            for connection in (direct, setup):
                connection.execute("DROP TABLE IF EXISTS sql_regression_samples")


@pytest.mark.parametrize("binary", [False, True])
@pytest.mark.parametrize("sql_type,minimum,maximum", CASES)
def test_integer_sum_preserves_boundaries_and_cancellation(sql_type, minimum, maximum, binary):
    with connect(6432, "pgdog_sharded") as connection:
        try:
            connection.execute("DROP TABLE IF EXISTS sql_regression_samples")
            connection.execute("CREATE TABLE sql_regression_samples (id BIGINT PRIMARY KEY, amount BIGINT)")
            for left, right, expected in [(maximum - 1, 1, maximum), (minimum + 1, -1, minimum), (maximum, minimum, -1)]:
                connection.execute("TRUNCATE sql_regression_samples")
                connection.execute("INSERT INTO sql_regression_samples VALUES (1, %s)", (left,))
                connection.execute("INSERT INTO sql_regression_samples VALUES (101, %s)", (right,))
                with connection.cursor(binary=binary) as cursor:
                    cursor.execute(f"SELECT SUM(amount)::{sql_type} FROM sql_regression_samples")
                    assert cursor.fetchone() == (expected,)
        finally:
            connection.execute("DROP TABLE IF EXISTS sql_regression_samples")
