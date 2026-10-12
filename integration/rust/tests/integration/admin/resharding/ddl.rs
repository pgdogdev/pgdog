//! DDL made on the source while it replicates to the destination.

use crate::setup::{admin_sqlx, connection_sqlx_direct, connection_sqlx_direct_db};
use sqlx::{Executor, Row};

use super::{
    TEST_PUB, TEST_TABLE, cleanup, create_publication, create_test_table, run_task_command,
    seed_rows, wait_for_rows_each_shard, with_cleanup,
};

const ROWS: i64 = 20;

/// Run a query returning a single text value on one of the shards.
async fn shard_value(db: &str, query: &str) -> Option<String> {
    connection_sqlx_direct_db(db)
        .await
        .fetch_one(query)
        .await
        .ok()?
        .try_get::<Option<String>, _>(0)
        .ok()?
}

#[tokio::test]
async fn test_ddl_key_is_not_replicated() {
    // A publication of all tables has the tables of our schema, like the one of the key
    // the source signs its DDL with. The destination has its own, or none of them.
    let source = connection_sqlx_direct().await;
    let admin = admin_sqlx().await;
    cleanup(&admin, &source).await;

    with_cleanup(&admin, &source, async {
        create_test_table(&source).await;
        seed_rows(&source, ROWS).await;
        create_publication(&source).await;
        let publish = format!(
            "ALTER PUBLICATION {TEST_PUB} ADD TABLE pgdog.ddl_replication_key, pgdog.config"
        );
        source.execute(publish.as_str()).await.unwrap();

        let command = format!("COPY_DATA pgdog pgdog_sharded {TEST_PUB}");
        let task_id = run_task_command(&admin, &command).await;
        wait_for_rows_each_shard(&admin, task_id, TEST_TABLE, ROWS).await;

        let key = "SELECT key FROM pgdog.ddl_replication_key";
        let source_key = shard_value("pgdog", key).await;
        for shard in ["shard_0", "shard_1"] {
            assert_ne!(shard_value(shard, key).await, source_key);
        }
    })
    .await;
}
