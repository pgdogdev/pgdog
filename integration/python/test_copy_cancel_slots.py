import time
from contextlib import ExitStack
from uuid import uuid4

import psycopg
import pytest

from globals import admin, direct_sync


def wait_for(predicate, description, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(0.1)
    raise AssertionError(f"Timed out waiting for {description}")


def task_status(control, task_id):
    cursor = control.execute("SHOW TASKS")
    columns = [column.name for column in cursor.description]
    tasks = [dict(zip(columns, row)) for row in cursor.fetchall()]
    return next(task for task in tasks if task["id"] == task_id)


@pytest.mark.parametrize("blocked_copy", [False, True])
def test_cancel_copy_data_removes_replication_slots(blocked_copy):
    name = f"cancel_slots_{uuid4().hex[:12]}"
    slot = name + "_0"
    task_id = None

    with ExitStack() as stack:
        source = stack.enter_context(direct_sync())
        source.autocommit = True
        control = stack.enter_context(admin())
        control.prepare_threshold = None
        destinations = [
            stack.enter_context(
                psycopg.connect(
                    host="127.0.0.1", port=5432, user="pgdog", password="pgdog",
                    dbname=database, autocommit=True,
                )
            )
            for database in ("shard_0", "shard_1")
        ]

        def blocked_backends():
            return source.execute(
                "SELECT pid FROM pg_stat_activity WHERE datname = 'shard_0' "
                "AND wait_event = 'PgSleep' AND query LIKE %s",
                (f"%{name}%",),
            ).fetchall()

        try:
            source.execute(f"CREATE SCHEMA {name}")
            source.execute(f"CREATE TABLE {name}.items (id bigint PRIMARY KEY, value text)")
            source.execute(f"INSERT INTO {name}.items VALUES (1, 'one')")
            source.execute(f"CREATE PUBLICATION {name} FOR TABLE {name}.items")

            if blocked_copy:
                destination = destinations[0]
                destination.execute(f"CREATE SCHEMA {name}")
                destination.execute(f"CREATE TABLE {name}.items (id bigint PRIMARY KEY, value text)")
                destination.execute(
                    f"CREATE FUNCTION {name}.delay_copy() RETURNS trigger LANGUAGE plpgsql AS $$ "
                    "BEGIN PERFORM pg_sleep(600); RETURN NEW; END $$"
                )
                destination.execute(
                    f"CREATE TRIGGER delay_copy BEFORE INSERT ON {name}.items "
                    f"FOR EACH ROW EXECUTE FUNCTION {name}.delay_copy()"
                )
                # COPY uses session_replication_role=replica, so this trigger must
                # also fire for replication. It blocks COPY completion until cleanup.
                destination.execute(f"ALTER TABLE {name}.items ENABLE ALWAYS TRIGGER delay_copy")

            task_id = int(control.execute(f"COPY_DATA pgdog pgdog_sharded {name} {name}").fetchone()[0])
            if blocked_copy:
                wait_for(blocked_backends, "COPY blocked in the destination trigger")
            else:
                wait_for(
                    lambda: task_status(control, task_id)["inner_status"] == "replicating",
                    "COPY to enter replication",
                )

            assert source.execute(
                "SELECT count(*) FROM pg_replication_slots WHERE slot_name = %s", (slot,)
            ).fetchone()[0] == 1
            control.execute(f"STOP_TASK {task_id}")
            # A blocked COPY exhausts ReshardTask's 60-second cooperative grace
            # period. Cleanup must also run when Tokio force-aborts its future.
            wait_for(
                lambda: task_status(control, task_id)["status"] == "cancelled",
                "migration cancellation", timeout=90,
            )
            wait_for(
                lambda: source.execute(
                    "SELECT count(*) = 0 FROM pg_replication_slots WHERE slot_name = %s", (slot,)
                ).fetchone()[0],
                "cancelled migration's replication slot to be removed",
                timeout=10,
            )
        finally:
            if task_id is not None:
                control.execute(f"STOP_TASK {task_id}")
            for (pid,) in blocked_backends():
                source.execute("SELECT pg_terminate_backend(%s)", (pid,))
            source.execute(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                "WHERE slot_name = %s AND NOT active", (slot,),
            )
            source.execute(f"DROP PUBLICATION IF EXISTS {name}")
            for connection in [source, *destinations]:
                connection.execute(f"DROP SCHEMA IF EXISTS {name} CASCADE")
