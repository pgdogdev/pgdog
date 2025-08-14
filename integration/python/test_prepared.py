from globals import normal_sync


def test_prepared_full():
    for _ in range(5):
        conn = normal_sync()
        conn.autocommit = True

        cur = conn.cursor()
        cur.execute("PREPARE test_stmt AS SELECT 1")
        cur.execute("PREPARE test_stmt AS SELECT 2")

    conn = normal_sync()
    conn.autocommit = True
    for _ in range(5):
        cur = conn.cursor()

        for i in range(5):
            cur.execute(f"PREPARE test_stmt_{i} AS SELECT $1::bigint")
            cur.execute(f"EXECUTE test_stmt_{i}({i})")
            result = cur.fetchone()
            assert result[0] == i
        conn.commit()
