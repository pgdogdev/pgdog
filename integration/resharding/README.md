# Resharding integration tests

The tests reshard `source` (2 shards, ports 15432 and 15433) into `destination`
(2 shards, ports 15434 and 15435). Postgres runs in Docker (`docker-compose.yaml`).

## Run a test script

```sh
cargo build
bash integration/resharding/resumable.sh
```

Each script starts new containers and removes them when it ends.

## Run a flow manually

Load the helpers into your shell. `shared.sh` sets the `PG*` variables for the
PgDog admin port (6432). It does not stop the containers when the shell exits.

```sh
cargo build
cd integration/resharding
source shared.sh
```

Optional logs. Without them, the output goes to the terminal:

```sh
PGDOG_LOG=/tmp/pgdog.log PGBENCH_LOG=/tmp/pgbench.log DATA_SYNC_LOG=/tmp/data-sync.log
```

### Prepare

```sh
start_databases
pgdog_cli schema-sync
```

### Example: replication resume with admin commands

```sh
SLOT=resume_test
pgdog_cli data-sync --sync-only --skip-schema-sync --replication-slot $SLOT
pgdog_cli schema-sync --phase post

start_pgdog; wait_for_pgdog_admin
admin "REPLICATE source destination pgdog $SLOT WITH (cleanup_on_cancel false)"
start_pgbench
state                                   # running, slots active

admin "STOP_TASK <task id>"
stop_pgbench
state                                   # paused, slots kept

psql source -c "INSERT INTO ..."        # write while paused
admin "REPLICATE source destination pgdog WITH (resume)"
start_pgbench
state                                   # running, same slots
verify
```

To test a crash, use `kill_pgdog` and then `start_pgdog` in place of `STOP_TASK`.

### Example: replication resume with the CLI

The two examples are alternatives. Each example starts after "Prepare". Run
only one of them, or clean up before you run the other one: both use the same
run key.

```sh
SLOT=resume_test
pgdog_cli data-sync --sync-only --skip-schema-sync --replication-slot $SLOT
pgdog_cli schema-sync --phase post

start_pgdog; wait_for_pgdog_admin
start_data_sync --replicate-only --skip-schema-sync --replication-slot $SLOT --cleanup-on-cancel false
start_pgbench
stop_data_sync                          # pause: same as Ctrl+C
state                                   # reshard, paused, slots kept
start_data_sync --replicate-only --skip-schema-sync --resume
verify
```

PgDog must run (`start_pgdog`) also in the CLI flow: pgbench and `verify`
connect through it.

What a stop does to a CLI run:

| Run started with | Ctrl+C or `stop_data_sync` (`TERM`) | `stop_data_sync KILL` |
|---|---|---|
| `--cleanup-on-cancel false` | Pause. The run can resume. | Crash. The run can resume. |
| Default `--cleanup-on-cancel true` | The run drops the slots that it created, its origins and its row. It cannot resume. Slots from an earlier `--sync-only` run stay. | Crash. The run can resume. |
| `--resume` | Pause. A resumed run always keeps its slots and its row. | Crash. The run can resume. |

Ctrl+C never cuts over. `data-sync` has no cutover: it replicates until you
stop it.

#### Finish a CLI run

A paused or resumed run keeps its row and its slots after the stop. Finish
it in three steps:

```sh
stop_data_sync                          # 1. stop replication
pgdog_cli schema-sync --cutover         # 2. cutover schema phase
pgdog_cli data-sync --abort             # 3. remove the row, the slots and the origins
state                                   # no run, no slots
```

If you skip step 3, the next `data-sync` with the same source, destination and
publication fails with `is unfinished at stage replication; resume or abort it`.

### Clean up

```sh
admin "STOP_TASK <task id>"                               # admin run: stop it first
admin "REPLICATE source destination pgdog WITH (abort)"
stop_data_sync                                            # CLI run: stop it first
pgdog_cli data-sync --abort
stop_pgbench; stop_pgdog
docker compose down -v
```

## Helpers

| Function | Use |
|---|---|
| `start_databases` | New containers, then wait for the ports. |
| `pgdog_cli <command> [args]` | PgDog CLI for `source` → `destination`, publication `pgdog`. |
| `start_pgdog`, `stop_pgdog`, `kill_pgdog`, `restart_pgdog` | Start, stop (`TERM`), crash (`KILL`), or restart PgDog. |
| `wait_for_pgdog_admin` | Wait until the admin port accepts connections. |
| `start_pgbench`, `stop_pgbench` | Write load on `source`. |
| `start_data_sync [args]`, `stop_data_sync [SIGNAL]` | `data-sync` in the background. |
| `admin "<sql>"` | Run a command on the admin database. |
| `state` | `SHOW RESHARDING`, `SHOW TASKS`, and the source slots. |
| `slots` | Replication slots on the source shards. |
| `verify` | Stop pgbench, wait for replication, compare the data. |

`verify` writes a marker row with `settings.id = 0` and a new value on each
call, then waits for that value on all four shards. You can run it many times.
