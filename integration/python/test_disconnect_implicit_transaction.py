import select
import time
from uuid import uuid4

import pytest
from psycopg import pq

from globals import direct_sync


def flush(conn):
    deadline = time.monotonic() + 5
    while conn.flush():
        remaining = deadline - time.monotonic()
        assert remaining > 0, "timed out sending the pipeline"
        select.select([], [conn.socket], [], remaining)


def result(conn):
    deadline = time.monotonic() + 5
    while True:
        conn.consume_input()
        if not conn.is_busy():
            return conn.get_result()
        remaining = deadline - time.monotonic()
        assert remaining > 0, "timed out reading the pipeline"
        select.select([conn.socket], [], [], remaining)


def wait_for_recovery(observer, pid):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        row = observer.execute(
            "SELECT state FROM pg_stat_activity WHERE pid = %s", (pid,)
        ).fetchone()
        if row is None or row == ("idle",):
            return
        time.sleep(0.01)
    raise AssertionError(f"backend {pid} was not released after disconnect: {row}")


@pytest.mark.parametrize("user", ["pgdog", "pgdog_session"])
@pytest.mark.parametrize("send_sync", [False, True])
def test_disconnect_preserves_implicit_transaction_boundary(user, send_sync):
    table = f"disconnect_implicit_{uuid4().hex}"
    with direct_sync() as observer:
        observer.autocommit = True
        observer.execute(f"CREATE TABLE {table} AS SELECT 10::integer AS value")
        conn = pq.PGconn.connect(
            f"host=127.0.0.1 port=6432 dbname=pgdog user={user} password=pgdog".encode()
        )
        try:
            assert conn.status == pq.ConnStatus.OK, conn.error_message
            conn.nonblocking = 1
            conn.enter_pipeline_mode()
            conn.send_query_params(
                f"UPDATE {table} SET value = 77 RETURNING pg_backend_pid()".encode(), []
            )
            conn.send_flush_request()
            flush(conn)
            updated = result(conn)
            assert updated is not None and updated.status == pq.ExecStatus.TUPLES_OK
            pid = int(updated.get_value(0, 0))
            # CommandComplete confirms execution, but the write must remain
            # invisible to other connections until the client sends Sync.
            assert observer.execute(f"SELECT value FROM {table}").fetchone()[0] == 10

            if send_sync:
                conn.pipeline_sync()
                flush(conn)
                synced = result(conn)
                if synced is None:
                    synced = result(conn)
                assert synced is not None and synced.status == pq.ExecStatus.PIPELINE_SYNC

            conn.finish()
            wait_for_recovery(observer, pid)
            value = observer.execute(f"SELECT value FROM {table}").fetchone()[0]
            assert value == (77 if send_sync else 10)
        finally:
            conn.finish()
            observer.execute(f"DROP TABLE {table}")
