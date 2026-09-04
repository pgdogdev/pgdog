use std::time::Duration;

use crate::setup::{admin_sqlx, connection_sqlx_direct};
use pgdog_stats::TaskProgress;
use sqlx::postgres::PgRow;
use sqlx::{Executor, Pool, Postgres, Row};
use tokio::time::{sleep, timeout};

use super::super::assert_layout;
use super::{
    POLL, TEST_PUB, TEST_TABLE, cleanup, create_publication, create_test_table, run_task_command,
    seed_rows, wait_for_rows_each_shard, wait_for_task,
};

/// Wire layout expected from `SHOW TABLE_COPIES`.
const SHOW_TABLE_COPIES_LAYOUT: &[(&str, &str)] = &[
    ("task_id", "INT8"),
    ("schema", "TEXT"),
    ("table", "TEXT"),
    ("source_shard", "INT8"),
    ("progress", "TEXT"),
    ("status", "TEXT"),
    ("attempt", "INT8"),
    ("rows", "INT8"),
    ("rows_human", "TEXT"),
    ("estimated_rows", "INT8"),
    ("estimated_rows_human", "TEXT"),
    ("rows_per_sec", "INT8"),
    ("rows_per_sec_human", "TEXT"),
    ("bytes", "INT8"),
    ("bytes_human", "TEXT"),
    ("estimated_bytes", "INT8"),
    ("estimated_bytes_human", "TEXT"),
    ("bytes_per_sec", "INT8"),
    ("bytes_per_sec_human", "TEXT"),
    ("elapsed", "TEXT"),
    ("elapsed_ms", "INT8"),
    ("last_error", "TEXT"),
];

const SEEDED_ROWS: i64 = 200;

pub(super) async fn copy_row(admin: &Pool<Postgres>, table: &str) -> Option<PgRow> {
    let rows = admin.fetch_all("SHOW TABLE_COPIES").await.unwrap();
    if !rows.is_empty() {
        assert_layout(&rows, SHOW_TABLE_COPIES_LAYOUT);
    }
    rows.into_iter()
        .find(|r| r.get::<String, _>("table") == table)
}

pub(super) async fn poll<T>(desc: &str, mut check: impl AsyncFnMut() -> Option<T>) -> T {
    let result = timeout(Duration::from_secs(30), async {
        loop {
            if let Some(value) = check().await {
                return value;
            }
            sleep(POLL).await;
        }
    })
    .await;
    match result {
        Ok(value) => value,
        Err(_) => panic!("timed out waiting for {desc}"),
    }
}

/// `SHOW TABLE_COPIES` lists the tables of the current copy-data run and
/// keeps them visible as history once the run finishes.
#[tokio::test]
async fn test_show_table_copies_during_copy() {
    let direct = connection_sqlx_direct().await;
    let admin = admin_sqlx().await;
    cleanup(&admin, &direct).await;

    create_test_table(&direct).await;
    seed_rows(&direct, SEEDED_ROWS).await;
    direct
        .execute(format!("ANALYZE {TEST_TABLE}").as_str())
        .await
        .unwrap();
    create_publication(&direct).await;

    let task_id =
        run_task_command(&admin, &format!("COPY_DATA pgdog pgdog_sharded {TEST_PUB}")).await;

    wait_for_task(&admin, "the copy_data child task", |task| {
        task.parent_id == Some(task_id) && task.kind.split_whitespace().next() == Some("copy_data")
    })
    .await;

    let row = poll("the copy to appear in SHOW TABLE_COPIES", || {
        copy_row(&admin, TEST_TABLE)
    })
    .await;
    assert_eq!(row.get::<String, _>("schema"), "public");
    let progress = row
        .get::<String, _>("progress")
        .parse::<TaskProgress>()
        .expect("table copy progress must be valid");
    assert!(
        matches!(
            progress,
            TaskProgress::Started | TaskProgress::Running | TaskProgress::Finished
        ),
        "unexpected table copy progress: {progress}"
    );

    wait_for_rows_each_shard(&admin, task_id, TEST_TABLE, SEEDED_ROWS).await;

    let row = poll("the finished copy to show as history", || async {
        copy_row(&admin, TEST_TABLE)
            .await
            .filter(|row| row.get::<String, _>("progress").parse() == Ok(TaskProgress::Finished))
    })
    .await;
    let estimated_rows = row.get::<Option<i64>, _>("estimated_rows");
    assert!(
        estimated_rows.is_some_and(|rows| rows > 0),
        "estimated_rows must be set on an analyzed table: {estimated_rows:?}"
    );
    let estimated_bytes = row.get::<Option<i64>, _>("estimated_bytes");
    assert!(
        estimated_bytes.is_some_and(|bytes| bytes > 0),
        "estimated_bytes must be set: {estimated_bytes:?}"
    );

    cleanup(&admin, &direct).await;
}
