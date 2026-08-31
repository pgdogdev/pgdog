use crate::setup::{admin_sqlx, connection_sqlx_direct};
use sqlx::{Executor, Row};

use super::{
    TEST_PUB, TEST_TABLE, cleanup, create_publication, create_test_table, seed_rows,
    wait_for_relation_on_shards, wait_for_rows_each_shard,
};

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

    wait_for_relation_on_shards(&admin, task_id, TEST_TABLE).await;
    wait_for_rows_each_shard(&admin, task_id, TEST_TABLE, 20).await;

    cleanup(&admin, &direct).await;
}
