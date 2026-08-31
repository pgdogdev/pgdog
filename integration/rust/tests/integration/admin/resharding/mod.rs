pub mod copy_data;
pub mod replication;
#[allow(clippy::module_inception)]
pub mod resharding;
pub mod schema_sync;

use std::time::Duration;

use crate::setup::connection_sqlx_direct_db;
use sqlx::{Executor, Pool, Postgres, Row};
use tokio::time::{sleep, timeout};

use super::{Task, Tasks, assert_layout};

const TEST_TABLE: &str = "_pgdog_test_task";

const TEST_PUB: &str = "pgdog_test_pub";

const SLOT_FILTER: &str = "slot_name LIKE '__pgdog_repl_%'";

const POLL: Duration = Duration::from_millis(200);

async fn drop_table(pool: &Pool<Postgres>, table: &str) {
    let _ = pool
        .execute(format!("DROP TABLE IF EXISTS {table} CASCADE").as_str())
        .await;
    let _ = pool
        .execute(format!("DROP TYPE  IF EXISTS {table} CASCADE").as_str())
        .await;
}

async fn drop_table_everywhere(table: &str, direct: &Pool<Postgres>) {
    drop_table(direct, table).await;
    for db in &["shard_0", "shard_1"] {
        drop_table(&connection_sqlx_direct_db(db).await, table).await;
    }
}

fn is_terminal(status: &str) -> bool {
    matches!(status, "finished" | "cancelled")
        || status.starts_with("failed")
        || status.starts_with("panicked")
}

async fn drain_tasks(admin: &Pool<Postgres>) {
    timeout(Duration::from_secs(60), async {
        loop {
            let tasks = Tasks::fetch(admin).await;
            if tasks.rows.iter().all(|t| is_terminal(t.status.as_str())) {
                break;
            }
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
}

async fn drop_test_slots(direct: &Pool<Postgres>) {
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
}

async fn cleanup(admin: &Pool<Postgres>, direct: &Pool<Postgres>) {
    drain_tasks(admin).await;

    let _ = admin.execute("RELOAD").await;
    sleep(Duration::from_millis(500)).await;

    drop_test_slots(direct).await;

    let _ = direct
        .execute(format!("DROP PUBLICATION IF EXISTS {TEST_PUB}").as_str())
        .await;

    drop_table_everywhere(TEST_TABLE, direct).await;
}

async fn wait_for_task_status(admin: &Pool<Postgres>, task_id: i64, status: &str) {
    let result = timeout(Duration::from_secs(30), async {
        loop {
            if let Some(task) = Tasks::fetch(admin).await.find(task_id) {
                if task.status == status {
                    return;
                }
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

async fn task_status_line(admin: &Pool<Postgres>, task_id: i64) -> String {
    match Tasks::fetch(admin).await.find(task_id) {
        Some(t) => format!("status {:?}, inner_status {:?}", t.status, t.inner_status),
        None => "task absent from SHOW TASKS".to_string(),
    }
}

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

async fn relation_present(pool: &Pool<Postgres>, name: &str) -> bool {
    pool.fetch_one(format!("SELECT to_regclass('{name}') IS NOT NULL AS present").as_str())
        .await
        .unwrap()
        .get::<bool, _>("present")
}

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

async fn wait_for_task(admin: &Pool<Postgres>, desc: &str, pred: impl Fn(&Task) -> bool) {
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

async fn create_test_table(direct: &Pool<Postgres>) {
    direct
        .execute(format!("CREATE TABLE {TEST_TABLE} (id BIGSERIAL PRIMARY KEY, val TEXT)").as_str())
        .await
        .unwrap();
}

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

async fn create_publication(direct: &Pool<Postgres>) {
    direct
        .execute(format!("CREATE PUBLICATION {TEST_PUB} FOR TABLE {TEST_TABLE}").as_str())
        .await
        .unwrap();
}

async fn run_task_command(admin: &Pool<Postgres>, command: &str) -> i64 {
    admin
        .fetch_one(command)
        .await
        .unwrap()
        .get::<String, _>("task_id")
        .parse()
        .unwrap()
}
