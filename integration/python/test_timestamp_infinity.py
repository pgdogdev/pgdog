import pytest
from psycopg.adapt import Loader
from psycopg.pq import Format

from globals import admin, direct_sync, sharded_sync


class RawTimestampLoader(Loader):
    # Python datetime cannot represent PostgreSQL infinity. Compare wire values.
    def load(self, data):
        return bytes(data)


class RawTimestampBinaryLoader(RawTimestampLoader):
    format = Format.BINARY


@pytest.mark.parametrize("data_type", ["timestamp", "timestamptz"])
@pytest.mark.parametrize("binary", [False, True])
def test_timestamp_infinity(data_type, binary):
    with direct_sync() as direct, sharded_sync() as sharded:
        for conn in (direct, sharded):
            conn.autocommit = True
            conn.adapters.register_loader(data_type, RawTimestampLoader)
            conn.adapters.register_loader(data_type, RawTimestampBinaryLoader)
            conn.execute("DROP TABLE IF EXISTS timestamp_infinity")
            conn.execute(
                f"CREATE TABLE timestamp_infinity (customer_id BIGINT PRIMARY KEY, value {data_type})"
            )

        with admin() as control:
            control.execute("RELOAD")

        try:
            for conn in (direct, sharded):
                for row_id, value in enumerate(
                    ["infinity", "-infinity", "2000-01-01 00:00:00.123456+00"] * 12
                ):
                    conn.execute(
                        "INSERT INTO timestamp_infinity VALUES (%s, %s)",
                        (row_id, value),
                    )

            queries = [
                "SELECT customer_id, value FROM timestamp_infinity ORDER BY value, customer_id",
                "SELECT customer_id, value FROM timestamp_infinity ORDER BY value DESC, customer_id",
                "SELECT MIN(value), MAX(value) FROM timestamp_infinity",
                "SELECT value, COUNT(*) FROM timestamp_infinity GROUP BY value ORDER BY value",
            ]
            for query in queries:
                expected = direct.execute(query, binary=binary).fetchall()
                actual = sharded.execute(query, binary=binary).fetchall()
                assert actual == expected, query
        finally:
            for conn in (direct, sharded):
                conn.execute("DROP TABLE IF EXISTS timestamp_infinity")
