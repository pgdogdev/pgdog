import psycopg
from globals import no_out_of_sync, sharded_sync, normal_sync


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
