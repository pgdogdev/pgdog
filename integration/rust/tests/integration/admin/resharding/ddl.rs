//! DDL replication tests

use crate::setup::{admin_sqlx, connection_sqlx_direct, connection_sqlx_direct_db};
use sqlx::{Executor, Pool, Postgres};

use super::table_copies::poll;
use super::{
    TEST_PUB, TEST_SCHEMA, TEST_TABLE, cleanup, create_publication, create_test_table,
    fail_if_task_errored, run_task_command, seed_rows, wait_for_rows_each_shard, wait_for_task,
    with_cleanup,
};

const ROWS: i64 = 20;

/// Create the test table with rows and a publication, copy the table to the shards,
/// and wait until the table's changes are being replicated.
async fn start_replication(admin: &Pool<Postgres>, source: &Pool<Postgres>) -> i64 {
    create_test_table(source).await;
    seed_rows(source, ROWS).await;
    create_publication(source).await;

    let copy_data = format!("COPY_DATA pgdog pgdog_sharded {TEST_PUB}");
    let task_id = run_task_command(admin, &copy_data).await;
    wait_for_task(admin, "replication", |task| {
        task.id == Some(task_id) && task.inner_status == "replicating"
    })
    .await;

    task_id
}

async fn query_shard(shard: &str, query: &str) -> Option<String> {
    sqlx::query_scalar(query)
        .fetch_one(&connection_sqlx_direct_db(shard).await)
        .await
        .ok()
        .flatten()
}

/// Wait until the query returns the expected value on both shards.
async fn wait_for_shards(admin: &Pool<Postgres>, task_id: i64, query: &str, expected: &str) {
    poll(
        &format!("{expected:?} for {query:?} on each shard"),
        || async {
            fail_if_task_errored(admin, task_id).await;
            let shard_0 = query_shard("shard_0", query).await;
            let shard_1 = query_shard("shard_1", query).await;
            (shard_0.as_deref() == Some(expected) && shard_1.as_deref() == Some(expected))
                .then_some(())
        },
    )
    .await;
}

#[tokio::test]
async fn test_ddl_add_column() {
    let source = connection_sqlx_direct().await;
    let admin = admin_sqlx().await;
    cleanup(&admin, &source).await;

    with_cleanup(&admin, &source, async {
        // copy the table and wait until the table is being replicated.
        let task_id = start_replication(&admin, &source).await;

        // add a column on the source and insert a row that uses the column
        let alter = format!("ALTER TABLE {TEST_SCHEMA}.{TEST_TABLE} ADD COLUMN extra INT");
        let insert =
            format!("INSERT INTO {TEST_SCHEMA}.{TEST_TABLE} (val, extra) VALUES ('new', 9)");
        source.execute(alter.as_str()).await.unwrap();
        source.execute(insert.as_str()).await.unwrap();

        // wait for the column and the row to be replayed on both shards
        let select =
            format!("SELECT extra::text FROM {TEST_SCHEMA}.{TEST_TABLE} WHERE val = 'new'");
        wait_for_shards(&admin, task_id, &select, "9").await;
    })
    .await;
}

#[tokio::test]
async fn test_ddl_in_transaction() {
    let source = connection_sqlx_direct().await;
    let admin = admin_sqlx().await;
    cleanup(&admin, &source).await;

    with_cleanup(&admin, &source, async {
        // copy the table and wait until the table is being replicated
        let task_id = start_replication(&admin, &source).await;

        // add a column in a transaction with rows before and after the column
        let transaction = format!(
            "BEGIN;
             INSERT INTO {TEST_SCHEMA}.{TEST_TABLE} (val) VALUES ('before');
             ALTER TABLE {TEST_SCHEMA}.{TEST_TABLE} ADD COLUMN extra INT;
             INSERT INTO {TEST_SCHEMA}.{TEST_TABLE} (val, extra) VALUES ('after', 9);
             COMMIT"
        );
        sqlx::raw_sql(&transaction).execute(&source).await.unwrap();

        // wait for the row from after the column to be on both shards with the column
        let after =
            format!("SELECT extra::text FROM {TEST_SCHEMA}.{TEST_TABLE} WHERE val = 'after'");
        wait_for_shards(&admin, task_id, &after, "9").await;

        // check that the row from before the column is there too with no value for the column
        let before = format!(
            "SELECT (extra IS NULL)::text FROM {TEST_SCHEMA}.{TEST_TABLE} WHERE val = 'before'"
        );

        for shard in ["shard_0", "shard_1"] {
            assert_eq!(query_shard(shard, &before).await.as_deref(), Some("true"));
        }
    })
    .await;
}

#[tokio::test]
async fn test_ddl_unqualified_name() {
    let source = connection_sqlx_direct().await;
    let admin = admin_sqlx().await;
    cleanup(&admin, &source).await;

    with_cleanup(&admin, &source, async {
        // copy the table and wait until the table is being replicated.
        let task_id = start_replication(&admin, &source).await;

        // add a column without qualifying the table's name.
        // table is only available through the session's search_path
        let session = format!(
            "SET search_path TO {TEST_SCHEMA};
             ALTER TABLE {TEST_TABLE} ADD COLUMN extra INT;
             INSERT INTO {TEST_TABLE} (val, extra) VALUES ('new', 9);
             RESET search_path"
        );
        sqlx::raw_sql(&session).execute(&source).await.unwrap();

        // wait for the column and the row to be replayed on both shards.
        let select =
            format!("SELECT extra::text FROM {TEST_SCHEMA}.{TEST_TABLE} WHERE val = 'new'");
        wait_for_shards(&admin, task_id, &select, "9").await;
    })
    .await;
}

#[tokio::test]
async fn test_ddl_keeps_owner() {
    let source = connection_sqlx_direct().await;
    let admin = admin_sqlx().await;
    cleanup(&admin, &source).await;

    with_cleanup(&admin, &source, async {
        // copy the table and wait until the table is being replicated
        let task_id = start_replication(&admin, &source).await;

        // create a table as another role then insert a row
        let as_other_role = format!(
            "DO $$ BEGIN CREATE ROLE ddl_owner; EXCEPTION WHEN duplicate_object THEN NULL; END $$;
             GRANT CREATE, USAGE ON SCHEMA {TEST_SCHEMA} TO ddl_owner;
             SET ROLE ddl_owner;
             CREATE TABLE {TEST_SCHEMA}.owned (id BIGINT);
             RESET ROLE"
        );

        let insert = format!("INSERT INTO {TEST_SCHEMA}.{TEST_TABLE} (val) VALUES ('new')");

        sqlx::raw_sql(&as_other_role)
            .execute(&source)
            .await
            .unwrap();
        source.execute(insert.as_str()).await.unwrap();

        // wait for the row to be on both shards (replication carried on)
        let select = format!("SELECT val FROM {TEST_SCHEMA}.{TEST_TABLE} WHERE id > {ROWS}");
        wait_for_shards(&admin, task_id, &select, "new").await;

        // check that the table has the same owner on the shards as on the source
        for shard in ["shard_0", "shard_1"] {
            let owner = format!(
                "SELECT tableowner::text FROM pg_tables \
                 WHERE schemaname = '{TEST_SCHEMA}' AND tablename = 'owned'"
            );

            assert_eq!(
                query_shard(shard, &owner).await.as_deref(),
                Some("ddl_owner")
            );
        }
    })
    .await;
}

#[tokio::test]
async fn test_ddl_forged_message_not_replayed() {
    let source = connection_sqlx_direct().await;
    let admin = admin_sqlx().await;
    cleanup(&admin, &source).await;

    with_cleanup(&admin, &source, async {
        // copy the table and wait until the table is being replicated
        let task_id = start_replication(&admin, &source).await;

        // write a DDL message to the WAL ourselves, with a fake signature, and
        // insert a row. (Any role can do this)
        let forged = format!(
            r#"SELECT pg_logical_emit_message(true, 'ddl_replication', repeat('0', 64) ||
               '{{"role": "pgdog", "search_path": "public",
                 "query": "CREATE INDEX items_forged ON {TEST_SCHEMA}.{TEST_TABLE} (val)"}}')"#
        );
        let insert = format!("INSERT INTO {TEST_SCHEMA}.{TEST_TABLE} (val) VALUES ('new')");

        source.execute(forged.as_str()).await.unwrap();
        source.execute(insert.as_str()).await.unwrap();

        // wait for the row to be on both shards (replication carried on)
        let select = format!("SELECT val FROM {TEST_SCHEMA}.{TEST_TABLE} WHERE id > {ROWS}");
        wait_for_shards(&admin, task_id, &select, "new").await;

        // check that the index was **NOT** created on the shards (as it was
        // not a real signature)
        for shard in ["shard_0", "shard_1"] {
            let index = format!("SELECT (to_regclass('{TEST_SCHEMA}.items_forged') IS NULL)::text");
            assert_eq!(query_shard(shard, &index).await.as_deref(), Some("true"));
        }
    })
    .await;
}

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
