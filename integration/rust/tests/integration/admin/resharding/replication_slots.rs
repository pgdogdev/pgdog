use crate::setup::{admin_sqlx, connection_sqlx_direct};
use pgdog_stats::{Lsn, TaskProgress};
use sqlx::postgres::PgRow;
use sqlx::{Executor, Pool, Postgres, Row};

use super::super::assert_layout;
use super::replication::{prepare_replication, start_replication, wait_for_values};
use super::table_copies::poll;
use super::{cleanup, seed_rows, wait_for_task_status};

const SLOT_PREFIX: &str = "__pgdog_repl_admin_slots";
const SLOT_NAME: &str = "__pgdog_repl_admin_slots_0";

const SHOW_REPLICATION_SLOTS_LAYOUT: &[(&str, &str)] = &[
    ("host", "TEXT"),
    ("port", "INT8"),
    ("database_name", "TEXT"),
    ("name", "TEXT"),
    ("lsn", "TEXT"),
    ("lag", "TEXT"),
    ("lag_bytes", "INT8"),
    ("copy_data", "BOOL"),
    ("last_transaction", "TEXT"),
    ("last_transaction_ms", "INT8"),
    ("task_id", "INT8"),
];

async fn slot_row(admin: &Pool<Postgres>) -> Option<PgRow> {
    let rows = admin
        .fetch_all("SHOW REPLICATION_SLOTS")
        .await
        .expect("replication slots must be readable");
    if !rows.is_empty() {
        assert_layout(&rows, SHOW_REPLICATION_SLOTS_LAYOUT);
    }
    let mut matching = rows
        .into_iter()
        .filter(|row| row.get::<String, _>("name") == SLOT_NAME);
    let row = matching.next();
    assert!(matching.next().is_none(), "slot must appear only once");
    row
}

#[tokio::test]
async fn test_show_replication_slots_tracks_named_stream_until_stopped() {
    let direct = connection_sqlx_direct().await;
    let admin = admin_sqlx().await;
    cleanup(&admin, &direct).await;
    prepare_replication(&admin, &direct).await;

    let task_id = start_replication(&admin, Some(SLOT_PREFIX)).await;
    let row = poll("the named replication slot", || slot_row(&admin)).await;
    assert_eq!(row.get::<String, _>("database_name"), "pgdog");
    assert!(!row.get::<bool, _>("copy_data"));

    let before: String = sqlx::query_scalar("SELECT pg_current_wal_lsn()::text")
        .fetch_one(&direct)
        .await
        .expect("source WAL position must be readable");
    let before: Lsn = before.parse().expect("source WAL position must be valid");
    seed_rows(&direct, 2).await;
    wait_for_values(&admin, task_id, &[(1, "v1"), (2, "v2")]).await;

    let row = poll("the slot to acknowledge new writes", || async {
        let row = slot_row(&admin).await?;
        let lsn: Lsn = row
            .get::<String, _>("lsn")
            .parse()
            .expect("displayed WAL position must be valid");
        (lsn.lsn > before.lsn).then_some(row)
    })
    .await;
    assert!(row.get::<Option<String>, _>("last_transaction").is_some());
    assert!(row.get::<Option<i64>, _>("last_transaction_ms").is_some());
    assert!(row.get::<Option<i64>, _>("task_id").is_some());

    admin
        .execute(format!("STOP_TASK {task_id}").as_str())
        .await
        .expect("replication stop must succeed");
    wait_for_task_status(&admin, task_id, TaskProgress::Cancelled).await;
    poll("the stopped slot to disappear", || async {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
        )
        .bind(SLOT_NAME)
        .fetch_one(&direct)
        .await
        .expect("source slots must be readable");
        (!exists && slot_row(&admin).await.is_none()).then_some(())
    })
    .await;

    cleanup(&admin, &direct).await;
}
