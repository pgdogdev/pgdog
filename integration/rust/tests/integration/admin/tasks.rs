use std::time::Duration;

use rust::setup::{admin_sqlx, connection_sqlx_direct, connection_sqlx_direct_db};
use sqlx::{Executor, Pool, Postgres, Row};
use tokio::time::sleep;

use super::Tasks;

// ─── Constants ──────────────────────────────────────────────────────────────

/// Shared table created in the source `pgdog` database and propagated to the
/// destination shards by schema_sync and copy_data tests.  Sequential
/// execution (`test-threads = 1`) means each test owns it exclusively.
const TEST_TABLE: &str = "_pgdog_test_task";

const STOP_TASK_PUB: &str = "pgdog_stop_task_test_pub";
const STOP_TASK_SLOT: &str = "pgdog_stop_task_test_slot";
const CUTOVER_PUB: &str = "pgdog_cutover_test_pub";
const CUTOVER_SLOT: &str = "pgdog_cutover_test_slot";
const SCHEMA_SYNC_PRE_PUB: &str = "pgdog_schema_sync_pre_test_pub";
const SCHEMA_SYNC_POST_PUB: &str = "pgdog_schema_sync_post_test_pub";
const COPY_DATA_PUB: &str = "pgdog_copy_data_test_pub";

/// WHERE predicate that matches every replication slot created by these tests.
///
/// Used verbatim in three consecutive queries inside [`cleanup`]:
/// terminate active WAL senders → wait until inactive → drop.
const SLOT_FILTER: &str = "   slot_name LIKE 'pgdog_stop_task_test_slot_%' \
                            OR slot_name LIKE 'pgdog_cutover_test_slot_%' \
                            OR slot_name LIKE '__pgdog_repl_%'";

// ─── Helpers ────────────────────────────────────────────────────────────────

/// Drop `table` and its orphaned row-type (left behind by interrupted DDL)
/// from `pool`.  Both statements use `IF EXISTS` so the function is safe to
/// call when the objects do not exist.
async fn drop_table(pool: &Pool<Postgres>, table: &str) {
    let _ = pool
        .execute(format!("DROP TABLE IF EXISTS {table} CASCADE").as_str())
        .await;
    let _ = pool
        .execute(format!("DROP TYPE  IF EXISTS {table} CASCADE").as_str())
        .await;
}

/// Drop `table` from the source database (`direct`) and from every shard,
/// reusing the same [`drop_table`] call for each.
///
/// `pgdog_sharded` uses shards `shard_0` and `shard_1` in the integration
/// setup.  Connection failures for individual shards are ignored so that a
/// single bad shard does not block cleanup of the rest.
async fn drop_table_everywhere(table: &str, direct: &Pool<Postgres>) {
    drop_table(direct, table).await;
    for db in &["shard_0", "shard_1"] {
        drop_table(&connection_sqlx_direct_db(db).await, table).await;
    }
}

/// Full cleanup for all task tests — idempotent and safe to call as both
/// pre-flight (evict prior-run leftovers) and post-flight (leave state clean).
///
/// 1. Stop every live PgDog task via `STOP_TASK`.
/// 2. Wait for the task map to drain.
/// 3. Terminate WAL senders still holding any test slot.
/// 4. Wait until all those slots are inactive.
/// 5. Drop the now-inactive test slots.
/// 6. Drop all test publications (`IF EXISTS` — idempotent).
/// 7. Drop [`TEST_TABLE`] from the source database and from every shard.
async fn cleanup(admin: &Pool<Postgres>, direct: &Pool<Postgres>) {
    // 1. Cooperative stop.
    for task in &Tasks::fetch(admin).await.rows {
        let _ = admin
            .execute(format!("STOP_TASK {}", task.id).as_str())
            .await;
    }

    // 2. Wait for the task map to drain.
    for _ in 0..20 {
        if Tasks::fetch(admin).await.is_empty() {
            break;
        }
        sleep(Duration::from_millis(500)).await;
    }

    // 3. Terminate WAL senders on any test slot.
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

    // 4. Wait for those slots to deactivate.
    for _ in 0..20 {
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
        sleep(Duration::from_millis(500)).await;
    }

    // 5. Drop inactive test slots.
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

    // 6. Drop all test publications.
    for pub_name in &[
        STOP_TASK_PUB,
        CUTOVER_PUB,
        SCHEMA_SYNC_PRE_PUB,
        SCHEMA_SYNC_POST_PUB,
        COPY_DATA_PUB,
    ] {
        let _ = direct
            .execute(format!("DROP PUBLICATION IF EXISTS {pub_name}").as_str())
            .await;
    }

    // 7. Drop shared test table from source and every shard.
    drop_table_everywhere(TEST_TABLE, direct).await;
}

/// Start `pgdog` → `pgdog_sharded` replication using a `FOR ALL TABLES`
/// publication.  Waits until the task appears in `SHOW TASKS` with kind
/// `"replication"` and returns its id.
async fn start_replication(
    pub_name: &str,
    slot_name: &str,
    admin: &Pool<Postgres>,
    direct: &Pool<Postgres>,
) -> i64 {
    admin.execute("RELOAD").await.unwrap();
    sleep(Duration::from_millis(500)).await;

    direct
        .execute(format!("CREATE PUBLICATION {pub_name} FOR ALL TABLES").as_str())
        .await
        .unwrap();

    let row = admin
        .fetch_one(format!("REPLICATE pgdog pgdog_sharded {pub_name} {slot_name}").as_str())
        .await
        .unwrap();
    // REPLICATE returns task_id as TEXT on the wire.
    let task_id: i64 = row.get::<String, _>("task_id").parse().unwrap();

    let mut appeared = false;
    for _ in 0..20 {
        if Tasks::fetch(admin)
            .await
            .find(task_id)
            .is_some_and(|t| t.kind == "replication pgdog -> pgdog_sharded")
        {
            appeared = true;
            break;
        }
        sleep(Duration::from_millis(500)).await;
    }
    assert!(
        appeared,
        "replication task {task_id} did not appear in SHOW TASKS within 10s"
    );

    task_id
}

/// Poll until `task_id` is absent from `SHOW TASKS` (up to 30 s).
async fn wait_for_task_gone(admin: &Pool<Postgres>, task_id: i64) {
    for _ in 0..60 {
        if Tasks::fetch(admin).await.find(task_id).is_none() {
            return;
        }
        sleep(Duration::from_millis(500)).await;
    }
    panic!("task {task_id} still present in SHOW TASKS after 30s");
}

/// Whether the relation `name` (table or index) exists on `db`, resolved
/// through the connection's search_path — these tests create objects in the
/// `pgdog` schema (the `$user` schema for role `pgdog`).
async fn relation_present(pool: &Pool<Postgres>, name: &str) -> bool {
    pool.fetch_one(format!("SELECT to_regclass('{name}') IS NOT NULL AS present").as_str())
        .await
        .unwrap()
        .get::<bool, _>("present")
}

/// Poll until relation `name` exists on both destination shards (up to 30 s),
/// proving a schema sync actually propagated it. Panics on timeout.
async fn wait_for_relation_on_shards(name: &str) {
    let shard_0 = connection_sqlx_direct_db("shard_0").await;
    let shard_1 = connection_sqlx_direct_db("shard_1").await;
    for _ in 0..60 {
        if relation_present(&shard_0, name).await && relation_present(&shard_1, name).await {
            return;
        }
        sleep(Duration::from_millis(500)).await;
    }
    panic!("relation {name} did not propagate to all shards within 30s");
}

// ─── Tests ──────────────────────────────────────────────────────────────────

/// `STOP_TASK` on an id that does not exist returns `"task not found"` rather
/// than a connection error.
#[tokio::test]
async fn test_stop_nonexistent_task() {
    let admin = admin_sqlx().await;

    let row = admin.fetch_one("STOP_TASK 999999999").await.unwrap();
    assert_eq!(row.get::<String, _>("stop_task"), "task not found");
}

/// `CUTOVER` with no replication task running returns a server error; the
/// connection pool stays healthy afterward.
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
    // Pool must still be usable.
    admin.fetch_one("SHOW VERSION").await.unwrap();
}

/// A replication task can be cancelled via `STOP_TASK <id>`, which returns
/// `"OK"` and removes the task from `SHOW TASKS`.
#[tokio::test]
async fn test_stop_task() {
    let direct = connection_sqlx_direct().await;
    let admin = admin_sqlx().await;
    cleanup(&admin, &direct).await;

    let task_id = start_replication(STOP_TASK_PUB, STOP_TASK_SLOT, &admin, &direct).await;

    let row = admin
        .fetch_one(format!("STOP_TASK {task_id}").as_str())
        .await
        .unwrap();
    assert_eq!(row.get::<String, _>("stop_task"), "OK");

    wait_for_task_gone(&admin, task_id).await;
    cleanup(&admin, &direct).await;
}

/// A replication task can also be stopped via `CUTOVER`, which triggers a
/// final sync, returns `"OK"`, and removes the task from `SHOW TASKS`.
#[tokio::test]
async fn test_cutover() {
    let direct = connection_sqlx_direct().await;
    let admin = admin_sqlx().await;
    cleanup(&admin, &direct).await;

    let task_id = start_replication(CUTOVER_PUB, CUTOVER_SLOT, &admin, &direct).await;

    let row = admin.fetch_one("CUTOVER").await.unwrap();
    assert_eq!(row.get::<String, _>("cutover"), "OK");

    wait_for_task_gone(&admin, task_id).await;
    cleanup(&admin, &direct).await;
}

/// `SCHEMA_SYNC pre` syncs the table structure from the source to the
/// destination shards. Asserts the table actually appears on both shards —
/// not merely that the task registered.
#[tokio::test]
async fn test_schema_sync_pre() {
    let direct = connection_sqlx_direct().await;
    let admin = admin_sqlx().await;
    cleanup(&admin, &direct).await;

    direct
        .execute(format!("CREATE TABLE {TEST_TABLE} (id SERIAL PRIMARY KEY, val TEXT)").as_str())
        .await
        .unwrap();
    direct
        .execute(
            format!("CREATE PUBLICATION {SCHEMA_SYNC_PRE_PUB} FOR TABLE {TEST_TABLE}").as_str(),
        )
        .await
        .unwrap();

    let row = admin
        .fetch_one(format!("SCHEMA_SYNC pre pgdog pgdog_sharded {SCHEMA_SYNC_PRE_PUB}").as_str())
        .await
        .unwrap();
    // Response carries the task id as TEXT; ensure it parses.
    let _task_id: i64 = row.get::<String, _>("task_id").parse().unwrap();

    // cleanup() dropped the table from the shards pre-flight, so its presence
    // here proves the pre sync created it on both shards.
    wait_for_relation_on_shards(TEST_TABLE).await;

    cleanup(&admin, &direct).await;
}

/// `SCHEMA_SYNC post` adds indexes/constraints to tables that already exist on
/// the destination. Syncs the table with `pre` first, then asserts `post`
/// propagates a secondary index — an effect `pre` does not produce.
#[tokio::test]
async fn test_schema_sync_post() {
    let direct = connection_sqlx_direct().await;
    let admin = admin_sqlx().await;
    cleanup(&admin, &direct).await;

    let secondary_index = format!("{TEST_TABLE}_val_idx");
    direct
        .execute(format!("CREATE TABLE {TEST_TABLE} (id SERIAL PRIMARY KEY, val TEXT)").as_str())
        .await
        .unwrap();
    direct
        .execute(format!("CREATE INDEX {secondary_index} ON {TEST_TABLE} (val)").as_str())
        .await
        .unwrap();
    direct
        .execute(
            format!("CREATE PUBLICATION {SCHEMA_SYNC_POST_PUB} FOR TABLE {TEST_TABLE}").as_str(),
        )
        .await
        .unwrap();

    // pre creates the table (and primary key) on the shards, but not the
    // secondary index — that is post-data.
    admin
        .fetch_one(format!("SCHEMA_SYNC pre pgdog pgdog_sharded {SCHEMA_SYNC_POST_PUB}").as_str())
        .await
        .unwrap();
    wait_for_relation_on_shards(TEST_TABLE).await;

    // post adds the secondary index on both destination shards.
    let row = admin
        .fetch_one(format!("SCHEMA_SYNC post pgdog pgdog_sharded {SCHEMA_SYNC_POST_PUB}").as_str())
        .await
        .unwrap();
    let _task_id: i64 = row.get::<String, _>("task_id").parse().unwrap();

    wait_for_relation_on_shards(&secondary_index).await;

    cleanup(&admin, &direct).await;
}

/// `COPY_DATA` syncs the schema, copies data, then starts replication. Asserts
/// the table is actually created on both destination shards (the schema phase),
/// then `cleanup` stops the long-running replication the task spawns.
#[tokio::test]
async fn test_copy_data() {
    let direct = connection_sqlx_direct().await;
    let admin = admin_sqlx().await;
    cleanup(&admin, &direct).await;

    direct
        .execute(format!("CREATE TABLE {TEST_TABLE} (id SERIAL PRIMARY KEY, val TEXT)").as_str())
        .await
        .unwrap();
    direct
        .execute(format!("CREATE PUBLICATION {COPY_DATA_PUB} FOR TABLE {TEST_TABLE}").as_str())
        .await
        .unwrap();

    // Response: task_id TEXT, replication_slot TEXT.
    let row = admin
        .fetch_one(format!("COPY_DATA pgdog pgdog_sharded {COPY_DATA_PUB}").as_str())
        .await
        .unwrap();
    let _task_id: i64 = row.get::<String, _>("task_id").parse().unwrap();
    let slot_name: String = row.get("replication_slot");
    assert!(!slot_name.is_empty(), "replication_slot must be non-empty");

    // copy_data's schema-sync phase must create the table on both shards.
    wait_for_relation_on_shards(TEST_TABLE).await;

    cleanup(&admin, &direct).await;
}
