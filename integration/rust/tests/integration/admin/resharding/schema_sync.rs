use crate::setup::{admin_sqlx, connection_sqlx_direct};
use sqlx::{Executor, Pool, Postgres, Row};

use super::{
    TEST_PUB, TEST_TABLE, assert_layout, cleanup, create_publication, create_test_table,
    run_task_command, wait_for_relation_on_shards,
};

const SHOW_SCHEMA_SYNC_LAYOUT: &[(&str, &str)] = &[
    ("parent_id", "INT8"),
    ("id", "INT8"),
    ("source", "TEXT"),
    ("destination", "TEXT"),
    ("sync_state", "TEXT"),
    ("shard", "INT8"),
    ("status", "TEXT"),
    ("inner_status", "TEXT"),
    ("started_at", "TEXT"),
    ("elapsed", "TEXT"),
    ("elapsed_ms", "INT8"),
];

#[derive(Debug, Clone)]
struct SchemaSyncRow {
    parent_id: Option<i64>,
    id: i64,
    source: String,
    destination: String,
    sync_state: String,
    shard: Option<i64>,
    status: String,
    inner_status: String,
}

async fn schema_sync_rows(admin: &Pool<Postgres>) -> Vec<SchemaSyncRow> {
    let raw = admin.fetch_all("SHOW SCHEMA_SYNC").await.unwrap();

    if !raw.is_empty() {
        assert_layout(&raw, SHOW_SCHEMA_SYNC_LAYOUT);
    }

    raw.iter()
        .map(|row| {
            let id: i64 = row.get("id");
            let parent_id: Option<i64> = row.get("parent_id");
            let shard: Option<i64> = row.get("shard");
            let status: String = row.get("status");
            let elapsed_ms: i64 = row.get("elapsed_ms");

            assert!(
                !row.get::<String, _>("started_at").is_empty(),
                "row {id}: started_at is empty"
            );
            assert!(!status.is_empty(), "row {id}: status is empty");
            assert!(elapsed_ms >= 0, "row {id}: elapsed_ms is negative");
            assert!(
                shard.is_none() || parent_id.is_some(),
                "row {id}: a shard row must name its parent"
            );

            SchemaSyncRow {
                parent_id,
                id,
                source: row.get("source"),
                destination: row.get("destination"),
                sync_state: row.get("sync_state"),
                shard,
                status,
                inner_status: row.get("inner_status"),
            }
        })
        .collect()
}

async fn assert_schema_sync_rows(admin: &Pool<Postgres>, task_id: i64, sync_state: &str) {
    let rows = schema_sync_rows(admin).await;

    let task = rows
        .iter()
        .find(|row| row.id == task_id && row.shard.is_none())
        .unwrap_or_else(|| panic!("SHOW SCHEMA_SYNC has no row for task {task_id}"));
    assert_eq!(task.sync_state, sync_state);
    assert_eq!(task.source, "pgdog");
    assert_eq!(task.destination, "pgdog_sharded");
    assert!(
        matches!(task.status.as_str(), "running" | "finished"),
        "task {task_id}: unexpected status {:?}",
        task.status
    );
    assert!(!task.inner_status.is_empty());

    let shards = rows
        .iter()
        .filter(|row| row.parent_id == Some(task_id) && row.shard.is_some())
        .collect::<Vec<_>>();
    let mut seen = shards
        .iter()
        .filter_map(|row| row.shard)
        .collect::<Vec<_>>();
    seen.sort_unstable();
    assert_eq!(
        seen,
        vec![0, 1],
        "SHOW SCHEMA_SYNC must report one row per destination shard"
    );
    for row in shards {
        assert_eq!(row.sync_state, sync_state);
        assert!(
            row.inner_status.starts_with("shard "),
            "shard row {} must report its cursor, got {:?}",
            row.id,
            row.inner_status
        );
    }
}

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

    wait_for_relation_on_shards(&admin, task_id, TEST_TABLE).await;

    assert_schema_sync_rows(&admin, task_id, "pre_data").await;

    cleanup(&admin, &direct).await;
}

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

    let pre_task_id = run_task_command(
        &admin,
        &format!("SCHEMA_SYNC pre pgdog pgdog_sharded {TEST_PUB}"),
    )
    .await;
    wait_for_relation_on_shards(&admin, pre_task_id, TEST_TABLE).await;

    let task_id = run_task_command(
        &admin,
        &format!("SCHEMA_SYNC post pgdog pgdog_sharded {TEST_PUB}"),
    )
    .await;

    wait_for_relation_on_shards(&admin, task_id, &secondary_index).await;
    assert_schema_sync_rows(&admin, task_id, "post_data").await;

    cleanup(&admin, &direct).await;
}
