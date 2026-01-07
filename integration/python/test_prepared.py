from globals import normal_sync, no_out_of_sync, admin
from multiprocessing import Pool
import time


def run_prepare_execute(worker_id):
    conn = normal_sync()
    conn.autocommit = True
    cur = conn.cursor()

    stmt_name = f"stmt_{worker_id}"
    cur.execute(f"PREPARE {stmt_name} AS SELECT $1::bigint * 2")
    time.sleep(0.01)

    for i in range(100):
        cur.execute(f"EXECUTE {stmt_name}({i})")
        result = cur.fetchone()
        assert result[0] == i * 2
        time.sleep(0.01)

    cur.execute(f"DEALLOCATE {stmt_name}")
    conn.close()
    return True


def test_prepare_execute_parallel():
    admin().execute("SET prepared_statements TO 'full'")

    with Pool(5) as pool:
        results = pool.map(run_prepare_execute, range(5))
    assert all(results)
    no_out_of_sync()
    admin().execute("RELOAD")
