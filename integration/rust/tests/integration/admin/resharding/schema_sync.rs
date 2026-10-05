use crate::setup::{admin_sqlx, connection_sqlx_direct, connection_sqlx_direct_db};
use pgdog_stats::TaskProgress;
use sqlx::{Executor, Pool, Postgres, Row};
use std::time::Duration;
use tokio::time::{sleep, timeout};

use super::{
    POLL, TEST_PUB, TEST_SCHEMA, TEST_TABLE, assert_layout, cleanup, create_publication,
    create_test_table, run_task_command, wait_for_relation_on_shards, wait_for_task_status,
    with_cleanup,
};

const VALID_FK: &str = "validation_children_parent_fk";
const SOURCE_NOT_VALID_FK: &str = "validation_unchecked_parent_fk";
const VALIDATION_PARENT: &str = "validation_parents";
const VALIDATION_CHILD: &str = "validation_children";
const VALIDATION_UNCHECKED_CHILD: &str = "validation_unchecked_children";

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
    status: TaskProgress,
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
            let status: TaskProgress = status.parse().unwrap();
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
        matches!(task.status, TaskProgress::Running | TaskProgress::Finished),
        "task {task_id}: unexpected status {}",
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

async fn create_validation_schema(direct: &Pool<Postgres>) {
    let script = format!(
        "CREATE SCHEMA {TEST_SCHEMA};

         CREATE TABLE {TEST_SCHEMA}.{VALIDATION_PARENT} (id BIGINT PRIMARY KEY);

         CREATE TABLE {TEST_SCHEMA}.{VALIDATION_CHILD} (
             id BIGINT PRIMARY KEY,
             parent_id BIGINT,
             CONSTRAINT {VALID_FK} FOREIGN KEY (parent_id)
                 REFERENCES {TEST_SCHEMA}.{VALIDATION_PARENT}(id)
         );

         CREATE TABLE {TEST_SCHEMA}.{VALIDATION_UNCHECKED_CHILD} (
             id BIGINT PRIMARY KEY,
             parent_id BIGINT
         );

         ALTER TABLE {TEST_SCHEMA}.{VALIDATION_UNCHECKED_CHILD}
             ADD CONSTRAINT {SOURCE_NOT_VALID_FK}
             FOREIGN KEY (parent_id)
             REFERENCES {TEST_SCHEMA}.{VALIDATION_PARENT}(id)
             NOT VALID;

         CREATE PUBLICATION {TEST_PUB} FOR TABLE
             {TEST_SCHEMA}.{VALIDATION_PARENT},
             {TEST_SCHEMA}.{VALIDATION_CHILD},
             {TEST_SCHEMA}.{VALIDATION_UNCHECKED_CHILD};"
    );

    direct.execute(script.as_str()).await.unwrap();
}

async fn run_schema_sync_phase(admin: &Pool<Postgres>, phase: &str) -> i64 {
    let task_id = run_task_command(
        admin,
        &format!("SCHEMA_SYNC {phase} pgdog pgdog_sharded {TEST_PUB}"),
    )
    .await;
    wait_for_task_status(admin, task_id, TaskProgress::Finished).await;
    task_id
}

async fn constraint_is_validated(database: &str, constraint: &str) -> bool {
    let database = connection_sqlx_direct_db(database).await;
    sqlx::query_scalar(
        "SELECT constraint_row.convalidated
         FROM pg_constraint AS constraint_row
         JOIN pg_namespace AS namespace
           ON namespace.oid = constraint_row.connamespace
         WHERE namespace.nspname = $1
           AND constraint_row.conname = $2",
    )
    .bind(TEST_SCHEMA)
    .bind(constraint)
    .fetch_one(&database)
    .await
    .unwrap()
}

async fn assert_constraint_validation(constraint: &str, expected: bool) {
    for database in ["shard_0", "shard_1"] {
        assert_eq!(
            constraint_is_validated(database, constraint).await,
            expected,
            "{constraint} validation state on {database}"
        );
    }
}

async fn wait_for_validation_failure(admin: &Pool<Postgres>, task_id: i64) -> SchemaSyncRow {
    timeout(Duration::from_secs(30), async {
        loop {
            if let Some(row) = schema_sync_rows(admin)
                .await
                .into_iter()
                .find(|row| row.id == task_id && row.shard.is_none())
                && row.status.is_error()
            {
                return row;
            }
            sleep(POLL).await;
        }
    })
    .await
    .expect("validation task did not fail in time")
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

    wait_for_task_status(&admin, task_id, TaskProgress::Finished).await;
    wait_for_relation_on_shards(&admin, task_id, TEST_TABLE).await;

    assert_schema_sync_rows(&admin, task_id, "pre_data").await;

    cleanup(&admin, &direct).await;
}

#[tokio::test]
async fn test_schema_sync_post() {
    let direct = connection_sqlx_direct().await;
    let admin = admin_sqlx().await;
    cleanup(&admin, &direct).await;

    with_cleanup(&admin, &direct, async {
        create_validation_schema(&direct).await;
        run_schema_sync_phase(&admin, "pre").await;

        let task_id = run_schema_sync_phase(&admin, "post").await;
        assert_schema_sync_rows(&admin, task_id, "post_data").await;

        for database in ["shard_0", "shard_1"] {
            let shard = connection_sqlx_direct_db(database).await;
            let error = shard
                .execute(
                    format!(
                        "INSERT INTO {TEST_SCHEMA}.{VALIDATION_CHILD} (id, parent_id) VALUES (1, 42)"
                    )
                    .as_str(),
                )
                .await
                .unwrap_err();
            assert_eq!(
                error.as_database_error().unwrap().code().as_deref(),
                Some("23503")
            );
        }
    })
    .await;
}

#[tokio::test]
async fn test_schema_sync_validation() {
    let direct = connection_sqlx_direct().await;
    let admin = admin_sqlx().await;
    cleanup(&admin, &direct).await;

    with_cleanup(&admin, &direct, async {
        create_validation_schema(&direct).await;
        run_schema_sync_phase(&admin, "pre").await;
        run_schema_sync_phase(&admin, "post").await;

        assert_constraint_validation(VALID_FK, false).await;
        assert_constraint_validation(SOURCE_NOT_VALID_FK, false).await;

        let task_id = run_schema_sync_phase(&admin, "post-data-validation").await;

        assert_constraint_validation(VALID_FK, true).await;
        assert_constraint_validation(SOURCE_NOT_VALID_FK, false).await;
        assert_schema_sync_rows(&admin, task_id, "post_data_validation").await;
    })
    .await;
}

#[tokio::test]
async fn test_schema_sync_validation_rejects_existing_orphan() {
    let direct = connection_sqlx_direct().await;
    let admin = admin_sqlx().await;
    cleanup(&admin, &direct).await;

    with_cleanup(&admin, &direct, async {
        create_validation_schema(&direct).await;
        run_schema_sync_phase(&admin, "pre").await;

        let shard = connection_sqlx_direct_db("shard_0").await;
        shard
            .execute(
                format!(
                    "INSERT INTO {TEST_SCHEMA}.{VALIDATION_CHILD} (id, parent_id) VALUES (1, 42)"
                )
                .as_str(),
            )
            .await
            .unwrap();

        run_schema_sync_phase(&admin, "post").await;

        let task_id = run_task_command(
            &admin,
            &format!("SCHEMA_SYNC post-data-validation pgdog pgdog_sharded {TEST_PUB}"),
        )
        .await;
        let failure = wait_for_validation_failure(&admin, task_id).await;

        assert_eq!(failure.sync_state, "post_data_validation");
        assert_eq!(failure.source, "pgdog");
        assert_eq!(failure.destination, "pgdog_sharded");
        let TaskProgress::Error { message } = failure.status else {
            panic!("validation task did not report an error");
        };
        assert!(message.contains("23503"), "{message}");
        assert!(message.contains(VALID_FK), "{message}");
        assert!(!constraint_is_validated("shard_0", VALID_FK).await);
    })
    .await;
}
