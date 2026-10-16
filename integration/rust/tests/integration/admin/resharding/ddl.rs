//! DDL replication tests

use crate::setup::{admin_sqlx, connection_sqlx_direct, connection_sqlx_direct_db};
use sqlx::Executor;

use super::{
    TEST_PUB, TEST_TABLE, cleanup, create_publication, create_test_table, run_task_command,
    seed_rows, wait_for_rows_each_shard, with_cleanup,
};

const ROWS: i64 = 20;

async fn ddl_key(database: &str) -> Option<String> {
    sqlx::query_scalar("SELECT key FROM pgdog.ddl_replication_key")
        .fetch_optional(&connection_sqlx_direct_db(database).await)
        .await
        .unwrap()
}

/// Test that PgDog's tables are **not** copied to the destination.
/// This is necessary as a publication can include them (e.g., with `FOR ALL TABLES`), but
/// they strictly belong to the database they're in; shouldn't be copied. Copying them would either fail
/// (e.g., on a duplicate key, if the setup was also run on the destination), or give the
/// destination the source's secret key.
#[tokio::test]
async fn test_ddl_key_is_not_replicated() {
    let source = connection_sqlx_direct().await;
    let admin = admin_sqlx().await;
    cleanup(&admin, &source).await;

    with_cleanup(&admin, &source, async {
        // Step 1: create a table with rows on the source & a publication.
        create_test_table(&source).await;
        seed_rows(&source, ROWS).await;
        create_publication(&source).await;

        // Step 2: we add PgDog's tables to the publication like `FOR ALL TABLES` would
        let add_tables = format!(
            "ALTER PUBLICATION {TEST_PUB} ADD TABLE pgdog.ddl_replication_key, pgdog.config"
        );
        source.execute(add_tables.as_str()).await.unwrap();

        // Step 3: copy the data, wait for the rows to be on each shard.
        let copy_data = format!("COPY_DATA pgdog pgdog_sharded {TEST_PUB}");
        let task_id = run_task_command(&admin, &copy_data).await;
        wait_for_rows_each_shard(&admin, task_id, TEST_TABLE, ROWS).await;

        // Step 4: check that the source's key was **not** copied to the shards.
        let source_key = ddl_key("pgdog").await;
        assert!(source_key.is_some());
        assert_ne!(ddl_key("shard_0").await, source_key);
        assert_ne!(ddl_key("shard_1").await, source_key);
    })
    .await;
}
