use std::time::Duration;

use crate::setup::{admin_sqlx, connection_sqlx_direct, connection_sqlx_direct_db};
use sqlx::{Executor, Pool, Postgres, Row};
use tokio::time::{sleep, timeout};

use super::Tasks;

// ─── Constants ──────────────────────────────────────────────────────────────

/// Source table propagated to the shards; tests run serially and own it exclusively.
const TEST_TABLE: &str = "_pgdog_test_task";

const TEST_PUB: &str = "pgdog_test_pub";

/// Matches every replication slot these tests create — all auto-named by
/// COPY_DATA / RESHARD / REPLICATE. Used by [`cleanup`].
const SLOT_FILTER: &str = "slot_name LIKE '__pgdog_repl_%'";

/// Interval between status polls in the `wait_*` / `cleanup` loops below; each
/// loop is bounded by a `timeout(window, …)` rather than a fixed iteration count.
const POLL: Duration = Duration::from_millis(200);

// ─── Helpers ────────────────────────────────────────────────────────────────

/// Drop `table` and any orphaned row-type from `pool` (idempotent).
async fn drop_table(pool: &Pool<Postgres>, table: &str) {
    let _ = pool
        .execute(format!("DROP TABLE IF EXISTS {table} CASCADE").as_str())
        .await;
    let _ = pool
        .execute(format!("DROP TYPE  IF EXISTS {table} CASCADE").as_str())
        .await;
}

/// Drop `table` from the source and both shards; per-shard failures are ignored.
async fn drop_table_everywhere(table: &str, direct: &Pool<Postgres>) {
    drop_table(direct, table).await;
    for db in &["shard_0", "shard_1"] {
        drop_table(&connection_sqlx_direct_db(db).await, table).await;
    }
}

/// Idempotent cleanup, safe to run before and after each test.
async fn cleanup(admin: &Pool<Postgres>, direct: &Pool<Postgres>) {
    // Drain every task to a terminal state *before* touching anything else. An
    // in-flight cutover holds backend pools and, past its point of no return,
    // runs to completion (then spawns a reverse-replication task) — a `RELOAD`
    // mid-cutover would shut those pools down and fail it with "pool is shut
    // down". STOP_TASK every still-running task each pass (the reverse-replication
    // task spawned during winddown is caught on a later pass) until all are terminal.
    timeout(Duration::from_secs(60), async {
        let is_terminal = |status: &str| {
            matches!(status, "finished" | "cancelled")
                || status.starts_with("failed")
                || status.starts_with("panicked")
        };
        loop {
            let tasks = Tasks::fetch(admin).await;
            if tasks.rows.iter().all(|t| is_terminal(t.status.as_str())) {
                break;
            }
            // Only stop tasks that are still running; terminal ones are left as-is
            // (re-stopping a reverse-replication task spawned during winddown is
            // handled on the next pass once it appears as running).
            for task in &tasks.rows {
                if is_terminal(task.status.as_str()) {
                    continue;
                }
                if let Some(id) = task.id {
                    let _ = admin.execute(format!("STOP_TASK {id}").as_str()).await;
                }
            }
            sleep(POLL).await;
        }
    })
    .await
    .expect("tasks did not drain to a terminal state");

    // No task is mid-cutover now, so it is safe to restore the topology: a
    // completed auto_cutover swaps the db configs in memory (not persisted —
    // `cutover_save_config` is off), so RELOAD reloads the pristine on-disk config.
    let _ = admin.execute("RELOAD").await;
    sleep(Duration::from_millis(500)).await;

    // Terminate WAL senders on any test slot.
    let _ = direct
        .execute(
            format!(
                "SELECT pg_terminate_backend(active_pid) \
                 FROM pg_replication_slots \
                 WHERE ({SLOT_FILTER}) AND active_pid IS NOT NULL"
            )
            .as_str(),
        )
        .await;

    // Wait for those slots to deactivate.
    timeout(Duration::from_secs(10), async {
        loop {
            let any_active = direct
                .fetch_optional(sqlx::query(&format!(
                    "SELECT bool_or(active) AS active FROM pg_replication_slots WHERE {SLOT_FILTER}"
                )))
                .await
                .ok()
                .flatten()
                .and_then(|row: sqlx::postgres::PgRow| row.get::<Option<bool>, _>("active"))
                .unwrap_or(false);
            if !any_active {
                break;
            }
            sleep(POLL).await;
        }
    })
    .await
    .expect("replication slots did not deactivate");

    // Drop inactive test slots.
    let _ = direct
        .execute(
            format!(
                "SELECT pg_drop_replication_slot(slot_name) \
                 FROM pg_replication_slots \
                 WHERE ({SLOT_FILTER}) AND NOT active"
            )
            .as_str(),
        )
        .await;

    // Drop the test publication.
    let _ = direct
        .execute(format!("DROP PUBLICATION IF EXISTS {TEST_PUB}").as_str())
        .await;

    // Drop the shared test table everywhere.
    drop_table_everywhere(TEST_TABLE, direct).await;
}

/// Start `pgdog` -> `pgdog_sharded` replication; return the task id once it
/// appears in `SHOW TASKS`.
async fn start_replication(admin: &Pool<Postgres>, direct: &Pool<Postgres>) -> i64 {
    admin.execute("RELOAD").await.unwrap();
    sleep(Duration::from_millis(500)).await;

    direct
        .execute(format!("CREATE PUBLICATION {TEST_PUB} FOR ALL TABLES").as_str())
        .await
        .unwrap();

    let row = admin
        .fetch_one(format!("REPLICATE pgdog pgdog_sharded {TEST_PUB}").as_str())
        .await
        .unwrap();
    let task_id: i64 = row.get::<String, _>("task_id").parse().unwrap();

    let appeared = timeout(Duration::from_secs(10), async {
        loop {
            if Tasks::fetch(admin)
                .await
                .find(task_id)
                .is_some_and(|t| t.kind == "replication pgdog -> pgdog_sharded")
            {
                return;
            }
            sleep(POLL).await;
        }
    })
    .await;
    assert!(
        appeared.is_ok(),
        "replication task {task_id} did not appear in SHOW TASKS in time"
    );

    task_id
}

/// Poll until `task_id` reaches `status` in `SHOW TASKS` (up to 30 s).
async fn wait_for_task_status(admin: &Pool<Postgres>, task_id: i64, status: &str) {
    let result = timeout(Duration::from_secs(30), async {
        loop {
            if let Some(task) = Tasks::fetch(admin).await.find(task_id) {
                if task.status == status {
                    return;
                }
                // Fail fast on an unexpected error state — the status text carries
                // the failure message — instead of waiting out the window.
                if task.status.starts_with("failed") || task.status.starts_with("panicked") {
                    panic!(
                        "task {task_id} errored while waiting for {status:?}: {} (inner_status {:?})",
                        task.status, task.inner_status
                    );
                }
            }
            sleep(POLL).await;
        }
    })
    .await;
    if result.is_err() {
        panic!("task {task_id} did not reach status {status:?} in SHOW TASKS in time");
    }
}

/// Current `SHOW TASKS` status line for `task_id`, for timeout diagnostics.
async fn task_status_line(admin: &Pool<Postgres>, task_id: i64) -> String {
    match Tasks::fetch(admin).await.find(task_id) {
        Some(t) => format!("status {:?}, inner_status {:?}", t.status, t.inner_status),
        None => "task absent from SHOW TASKS".to_string(),
    }
}

/// Panic with the task's status (which carries the error message) if `task_id`
/// reached an error state.
async fn fail_if_task_errored(admin: &Pool<Postgres>, task_id: i64) {
    if let Some(t) = Tasks::fetch(admin).await.find(task_id)
        && (t.status.starts_with("failed") || t.status.starts_with("panicked"))
    {
        panic!(
            "task {task_id} errored: {} (inner_status {:?})",
            t.status, t.inner_status
        );
    }
}

/// Whether relation `name` exists on `db` (resolved via the connection's search_path).
async fn relation_present(pool: &Pool<Postgres>, name: &str) -> bool {
    pool.fetch_one(format!("SELECT to_regclass('{name}') IS NOT NULL AS present").as_str())
        .await
        .unwrap()
        .get::<bool, _>("present")
}

/// Poll until relation `name` exists on both shards (up to 30 s), failing fast
/// with the task's status if `task_id` errors meanwhile.
async fn wait_for_relation_on_shards(admin: &Pool<Postgres>, task_id: i64, name: &str) {
    let shard_0 = connection_sqlx_direct_db("shard_0").await;
    let shard_1 = connection_sqlx_direct_db("shard_1").await;
    let result = timeout(Duration::from_secs(30), async {
        loop {
            fail_if_task_errored(admin, task_id).await;
            if relation_present(&shard_0, name).await && relation_present(&shard_1, name).await {
                return;
            }
            sleep(POLL).await;
        }
    })
    .await;
    if result.is_err() {
        panic!(
            "relation {name} did not propagate to all shards in time ({})",
            task_status_line(admin, task_id).await
        );
    }
}

/// Rows of `table` on shard `db`, queried directly (bypassing pgdog); an absent
/// table counts 0.
async fn shard_row_count(db: &str, table: &str) -> i64 {
    let pool = connection_sqlx_direct_db(db).await;
    if !relation_present(&pool, table).await {
        return 0;
    }
    pool.fetch_one(format!("SELECT COUNT(*)::bigint AS n FROM {table}").as_str())
        .await
        .unwrap()
        .get::<i64, _>("n")
}

/// Poll until every destination shard holds exactly `expected` rows of `table`
/// (up to 30 s). `table` is omni (not in `sharded_tables`), so each shard ends
/// up with a full copy — the reference-table semantics also asserted by
/// `check_omni_each_shard` in the resharding suite. Fails fast on task error.
async fn wait_for_rows_each_shard(
    admin: &Pool<Postgres>,
    task_id: i64,
    table: &str,
    expected: i64,
) {
    let result = timeout(Duration::from_secs(30), async {
        loop {
            fail_if_task_errored(admin, task_id).await;
            if shard_row_count("shard_0", table).await == expected
                && shard_row_count("shard_1", table).await == expected
            {
                return;
            }
            sleep(POLL).await;
        }
    })
    .await;
    if result.is_err() {
        panic!(
            "table {table} did not reach {expected} rows on each shard in time \
             (shard_0={}, shard_1={}, {})",
            shard_row_count("shard_0", table).await,
            shard_row_count("shard_1", table).await,
            task_status_line(admin, task_id).await
        );
    }
}

/// Poll `SHOW TASKS` until a row satisfies `pred` (up to 30 s).
async fn wait_for_task(admin: &Pool<Postgres>, desc: &str, pred: impl Fn(&super::Task) -> bool) {
    let result = timeout(Duration::from_secs(30), async {
        loop {
            if Tasks::fetch(admin).await.rows.iter().any(&pred) {
                return;
            }
            sleep(POLL).await;
        }
    })
    .await;
    if result.is_err() {
        panic!("no task matching {desc:?} appeared in SHOW TASKS in time");
    }
}

/// Create the test table on the source.
async fn create_test_table(direct: &Pool<Postgres>) {
    direct
        .execute(format!("CREATE TABLE {TEST_TABLE} (id BIGSERIAL PRIMARY KEY, val TEXT)").as_str())
        .await
        .unwrap();
}

/// Insert `n` rows into the test table.
async fn seed_rows(direct: &Pool<Postgres>, n: i64) {
    direct
        .execute(
            format!(
                "INSERT INTO {TEST_TABLE} (val) SELECT 'v' || g FROM generate_series(1, {n}) g"
            )
            .as_str(),
        )
        .await
        .unwrap();
}

/// Create the test publication (`FOR TABLE TEST_TABLE`) on the source.
async fn create_publication(direct: &Pool<Postgres>) {
    direct
        .execute(format!("CREATE PUBLICATION {TEST_PUB} FOR TABLE {TEST_TABLE}").as_str())
        .await
        .unwrap();
}

/// Run an admin command that returns a `task_id` column; parse and return it.
async fn run_task_command(admin: &Pool<Postgres>, command: &str) -> i64 {
    admin
        .fetch_one(command)
        .await
        .unwrap()
        .get::<String, _>("task_id")
        .parse()
        .unwrap()
}

// ─── Tests ──────────────────────────────────────────────────────────────────

/// `STOP_TASK` on an unknown id returns `"task not found"`.
#[tokio::test]
async fn test_stop_nonexistent_task() {
    let admin = admin_sqlx().await;

    let row = admin.fetch_one("STOP_TASK 999999999").await.unwrap();
    assert_eq!(row.get::<String, _>("stop_task"), "task not found");
}

/// `CUTOVER` with no replication task errors but leaves the pool usable.
#[tokio::test]
async fn test_cutover_without_replication_task() {
    let direct = connection_sqlx_direct().await;
    let admin = admin_sqlx().await;
    cleanup(&admin, &direct).await;

    let err = admin.fetch_one("CUTOVER").await.unwrap_err();
    assert!(
        matches!(err, sqlx::Error::Database(_)),
        "expected a database error, got: {err:?}"
    );
    admin.fetch_one("SHOW VERSION").await.unwrap();
}

/// `STOP_TASK <id>` cancels a replication task (returns `"OK"`, status -> `cancelled`).
#[tokio::test]
async fn test_stop_task() {
    let direct = connection_sqlx_direct().await;
    let admin = admin_sqlx().await;
    cleanup(&admin, &direct).await;

    let task_id = start_replication(&admin, &direct).await;

    let row = admin
        .fetch_one(format!("STOP_TASK {task_id}").as_str())
        .await
        .unwrap();
    assert_eq!(row.get::<String, _>("stop_task"), "OK");

    wait_for_task_status(&admin, task_id, "cancelled").await;
    cleanup(&admin, &direct).await;
}

/// Operator `CUTOVER` drives a running replication through the full traffic
/// switch to completion. The replication is established with `COPY_DATA` (which
/// loads the schema and leaves a task awaiting an operator cutover — a bare
/// `REPLICATE` never loads the schema, so its cutover would fail in
/// `schema_sync_cutover`). `CUTOVER` returns `"OK"` and the task reaches
/// `finished` once the swap, post-cutover schema sync, and reverse-replication
/// setup all complete. `cleanup` RELOADs to revert the resulting config swap.
#[tokio::test]
async fn test_cutover() {
    let direct = connection_sqlx_direct().await;
    let admin = admin_sqlx().await;
    cleanup(&admin, &direct).await;

    create_test_table(&direct).await;
    seed_rows(&direct, 20).await;
    create_publication(&direct).await;

    let task_id =
        run_task_command(&admin, &format!("COPY_DATA pgdog pgdog_sharded {TEST_PUB}")).await;

    // Wait until the task reaches its replication phase: schema is loaded and it
    // is registered to receive an operator CUTOVER.
    wait_for_task(&admin, "copy_data replicating", |t| {
        t.id == Some(task_id) && t.inner_status == "replicating"
    })
    .await;

    // Issue CUTOVER; retry briefly to cover the gap between the replication
    // phase being reported and the task registering for operator cutover.
    let cutover_ok = timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(row) = admin.fetch_one("CUTOVER").await
                && row.get::<String, _>("cutover") == "OK"
            {
                return;
            }
            sleep(POLL).await;
        }
    })
    .await;
    assert!(
        cutover_ok.is_ok(),
        "CUTOVER never returned OK ({})",
        task_status_line(&admin, task_id).await
    );

    // The switch runs to completion: the task ends in `finished`.
    wait_for_task_status(&admin, task_id, "finished").await;
    cleanup(&admin, &direct).await;
}

/// `SCHEMA_SYNC pre` creates the table on both shards.
#[tokio::test]
async fn test_schema_sync_pre() {
    let direct = connection_sqlx_direct().await;
    let admin = admin_sqlx().await;
    cleanup(&admin, &direct).await;

    create_test_table(&direct).await;
    create_publication(&direct).await;

    let task_id = run_task_command(
        &admin,
        &format!("SCHEMA_SYNC pre pgdog pgdog_sharded {TEST_PUB}"),
    )
    .await;

    // cleanup dropped the table pre-flight, so its presence proves pre created it.
    wait_for_relation_on_shards(&admin, task_id, TEST_TABLE).await;

    cleanup(&admin, &direct).await;
}

/// `SCHEMA_SYNC post` adds the secondary index, which `pre` does not.
#[tokio::test]
async fn test_schema_sync_post() {
    let direct = connection_sqlx_direct().await;
    let admin = admin_sqlx().await;
    cleanup(&admin, &direct).await;

    let secondary_index = format!("{TEST_TABLE}_val_idx");
    create_test_table(&direct).await;
    direct
        .execute(format!("CREATE INDEX {secondary_index} ON {TEST_TABLE} (val)").as_str())
        .await
        .unwrap();
    create_publication(&direct).await;

    // pre creates the table but not the secondary index (that's post-data).
    let pre_task_id = run_task_command(
        &admin,
        &format!("SCHEMA_SYNC pre pgdog pgdog_sharded {TEST_PUB}"),
    )
    .await;
    wait_for_relation_on_shards(&admin, pre_task_id, TEST_TABLE).await;

    // post adds the secondary index on both shards.
    let task_id = run_task_command(
        &admin,
        &format!("SCHEMA_SYNC post pgdog pgdog_sharded {TEST_PUB}"),
    )
    .await;

    wait_for_relation_on_shards(&admin, task_id, &secondary_index).await;

    cleanup(&admin, &direct).await;
}

/// `COPY_DATA` syncs schema, copies data, then replicates; assert the table and
/// its rows reach both shards.
#[tokio::test]
async fn test_copy_data() {
    let direct = connection_sqlx_direct().await;
    let admin = admin_sqlx().await;
    cleanup(&admin, &direct).await;

    create_test_table(&direct).await;
    seed_rows(&direct, 20).await;
    create_publication(&direct).await;

    let row = admin
        .fetch_one(format!("COPY_DATA pgdog pgdog_sharded {TEST_PUB}").as_str())
        .await
        .unwrap();
    let task_id: i64 = row.get::<String, _>("task_id").parse().unwrap();
    let slot_name: String = row.get("replication_slot");
    assert!(!slot_name.is_empty(), "replication_slot must be non-empty");

    // Schema phase creates the table; the omni table is fully copied to every
    // shard, so each holds all 20 rows.
    wait_for_relation_on_shards(&admin, task_id, TEST_TABLE).await;
    wait_for_rows_each_shard(&admin, task_id, TEST_TABLE, 20).await;

    cleanup(&admin, &direct).await;
}

/// `RESHARD` (auto_cutover) is non-blocking — returns a task id immediately and
/// registers a `reshard` task — copies schema + data to the shards, and enters
/// its auto-cutover replication phase.
#[tokio::test]
async fn test_reshard() {
    let direct = connection_sqlx_direct().await;
    let admin = admin_sqlx().await;
    cleanup(&admin, &direct).await;

    create_test_table(&direct).await;
    seed_rows(&direct, 20).await;
    create_publication(&direct).await;

    // Non-blocking: RESHARD returns a task id straight away.
    let task_id =
        run_task_command(&admin, &format!("RESHARD pgdog pgdog_sharded {TEST_PUB}")).await;

    // Registers immediately as a `reshard` task.
    wait_for_task(&admin, "reshard task", |t| {
        t.id == Some(task_id) && t.kind.starts_with("reshard ")
    })
    .await;

    // Schema + data reach both shards (omni table fully copied to each -> 20 per shard).
    wait_for_relation_on_shards(&admin, task_id, TEST_TABLE).await;
    wait_for_rows_each_shard(&admin, task_id, TEST_TABLE, 20).await;

    // auto_cutover drives into the replication phase on its own.
    wait_for_task(&admin, "reshard replicating", |t| {
        t.id == Some(task_id) && t.inner_status == "replicating"
    })
    .await;

    // Wind down and confirm a terminal state.
    let _ = admin.execute(format!("STOP_TASK {task_id}").as_str()).await;
    timeout(Duration::from_secs(30), async {
        loop {
            if Tasks::fetch(&admin)
                .await
                .find(task_id)
                .is_some_and(|t| matches!(t.status.as_str(), "cancelled" | "finished"))
            {
                return;
            }
            sleep(POLL).await;
        }
    })
    .await
    .expect("reshard task did not reach a terminal state");

    cleanup(&admin, &direct).await;
}
