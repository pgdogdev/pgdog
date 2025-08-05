from globals import normal_sync


def test_prepared_full():
    for _ in range(5):
        conn = normal_sync()
        conn.autocommit = True

        cur = conn.cursor()
        cur.execute("PREPARE test_stmt AS SELECT 1")
        cur.execute("PREPARE test_stmt AS SELECT 2")
