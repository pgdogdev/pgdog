use crate::setup::{admin_sqlx, connection_sqlx_direct, connection_sqlx_direct_db};
use pgdog_stats::TaskProgress;
use sqlx::postgres::PgRow;
use sqlx::{Executor, Pool, Postgres, Row};

use super::table_copies::{copy_row, poll};
use super::{
    TEST_PUB, TEST_SCHEMA, TEST_TABLE, cleanup, create_publication, create_test_table,
    fail_if_task_errored, relation_present, run_task_command, seed_rows, shard_row_count,
    wait_for_relation_on_shards, wait_for_rows_each_shard, wait_for_task, with_cleanup,
};

const SIBLING_ROWS: i64 = 200_000;
const TABLE_COUNT: usize = 5;

fn numbered_table(i: usize) -> String {
    format!("{TEST_TABLE}_{i}")
}

async fn create_table(pool: &Pool<Postgres>, table: &str) {
    pool.execute(format!("CREATE SCHEMA IF NOT EXISTS {TEST_SCHEMA}").as_str())
        .await
        .expect("test schema creation must succeed");
    pool.execute(
        format!("CREATE TABLE {TEST_SCHEMA}.{table} (id BIGSERIAL PRIMARY KEY, val TEXT)").as_str(),
    )
    .await
    .unwrap();
}

async fn seed_table(direct: &Pool<Postgres>, table: &str, n: i64) {
    direct
        .execute(
            format!("INSERT INTO {TEST_SCHEMA}.{table} (val) SELECT 'v' || g FROM generate_series(1, {n}) g")
                .as_str(),
        )
        .await
        .unwrap();
}

fn table_progress(row: &PgRow) -> TaskProgress {
    row.get::<String, _>("progress").parse().unwrap()
}

async fn set_parallel_within_table(admin: &Pool<Postgres>, readers: usize) {
    admin
        .execute(format!("SET resharding_parallel_within_table_copies TO {readers}").as_str())
        .await
        .unwrap();
}

async fn copy_data(parallel_within_table: usize) {
    const ROW_COUNT: i64 = 10_000;

    let direct = connection_sqlx_direct().await;
    let admin = admin_sqlx().await;
    cleanup(&admin, &direct).await;

    set_parallel_within_table(&admin, parallel_within_table).await;

    with_cleanup(&admin, &direct, async {
        let secondary_index = format!("{TEST_TABLE}_val_key");
        create_test_table(&direct).await;
        // pg_dump emits REPLICA IDENTITY USING INDEX right after the
        // index it references, which is created in the post-data step.
        // Running it in pre-data fails, and a restore that ignores
        // errors silently leaves the table with its default identity.
        direct
            .execute(
                format!(
                    "ALTER TABLE {TEST_SCHEMA}.{TEST_TABLE} ALTER COLUMN val SET NOT NULL;
                     ALTER TABLE {TEST_SCHEMA}.{TEST_TABLE} ADD CONSTRAINT {secondary_index} UNIQUE (val);
                     ALTER TABLE {TEST_SCHEMA}.{TEST_TABLE} REPLICA IDENTITY USING INDEX {secondary_index}"
                )
                .as_str(),
            )
            .await
            .unwrap();
        seed_rows(&direct, ROW_COUNT).await;
        create_publication(&direct).await;

        let row = admin
            .fetch_one(format!("COPY_DATA pgdog pgdog_sharded {TEST_PUB}").as_str())
            .await
            .unwrap();
        let task_id: i64 = row.get::<String, _>("task_id").parse().unwrap();
        let slot_name: String = row.get("replication_slot");
        assert!(!slot_name.is_empty(), "replication_slot must be non-empty");

        wait_for_relation_on_shards(&admin, task_id, TEST_TABLE).await;
        wait_for_rows_each_shard(&admin, task_id, TEST_TABLE, ROW_COUNT).await;
        wait_for_task(&admin, "copy data to finish synchronization", |task| {
            task.parent_id == Some(task_id)
                && task.kind == "copy_data"
                && task.status == TaskProgress::Finished
        })
        .await;

        // The index it references is created earlier in the same step.
        for database in ["shard_0", "shard_1"] {
            let shard = connection_sqlx_direct_db(database).await;
            let identity: String = sqlx::query_scalar(
                "SELECT relreplident::text FROM pg_class WHERE oid = to_regclass($1)",
            )
            .bind(format!("{TEST_SCHEMA}.{TEST_TABLE}"))
            .fetch_one(&shard)
            .await
            .unwrap();
            assert_eq!(identity, "i");
            let error = shard
                .execute(
                    format!("INSERT INTO {TEST_SCHEMA}.{TEST_TABLE} (id, val) VALUES (21, 'v1')")
                        .as_str(),
                )
                .await
                .unwrap_err();
            assert_eq!(
                error.as_database_error().unwrap().code().as_deref(),
                Some("23505")
            );
        }
    })
    .await;
}

/// `copy_data` test in serial
#[tokio::test]
async fn test_copy_data_serial() {
    copy_data(1).await;
}

/// `copy_data` test in parallel
#[tokio::test]
async fn test_copy_data_parallel() {
    copy_data(3).await;
}

/// Runs `seed` to create the rows within the test table that will be copied to the dest.
async fn copy_seeded(seed: &str, expected: i64) {
    let direct = connection_sqlx_direct().await;
    let admin = admin_sqlx().await;
    cleanup(&admin, &direct).await;

    set_parallel_within_table(&admin, 3).await;

    with_cleanup(&admin, &direct, async {
        create_test_table(&direct).await;
        direct.execute(seed).await.unwrap();
        create_publication(&direct).await;

        let task_id =
            run_task_command(&admin, &format!("COPY_DATA pgdog pgdog_sharded {TEST_PUB}")).await;

        wait_for_relation_on_shards(&admin, task_id, TEST_TABLE).await;
        wait_for_rows_each_shard(&admin, task_id, TEST_TABLE, expected).await;
        wait_for_task(&admin, "copy data to finish synchronization", |task| {
            task.parent_id == Some(task_id)
                && task.kind == "copy_data"
                && task.status == TaskProgress::Finished
        })
        .await;
    })
    .await;
}

#[tokio::test]
async fn test_copy_data_empty_table() {
    copy_seeded("SELECT 1", 0).await;
}

#[tokio::test]
async fn test_copy_data_leading_dead_blocks() {
    let seed = format!(
        "INSERT INTO {TEST_SCHEMA}.{TEST_TABLE} (val) SELECT 'v' || g FROM generate_series(1, 10000) g;
         DELETE FROM {TEST_SCHEMA}.{TEST_TABLE} WHERE id <= 5000"
    );
    copy_seeded(&seed, 5000).await;
}

async fn table_checksum(pool: &Pool<Postgres>) -> (i64, String) {
    sqlx::query_as(&format!(
        "SELECT count(*), coalesce(sum(hashtextextended(id::text || ':' || val, 0)), 0)::text \
         FROM {TEST_SCHEMA}.{TEST_TABLE}"
    ))
    .fetch_one(pool)
    .await
    .unwrap()
}

/// Wrapper to test out both serial and parallel table reads, to ensure that
/// (1) COPY works, as well as (2) replication afterwards.
async fn copy_data_with_concurrent_writes(parallel_within_table: usize) {
    const ROW_COUNT: i64 = 100_000;
    const WRITES: i64 = 2_000;

    let direct = connection_sqlx_direct().await;
    let admin = admin_sqlx().await;
    cleanup(&admin, &direct).await;

    set_parallel_within_table(&admin, parallel_within_table).await;

    with_cleanup(&admin, &direct, async {
        create_test_table(&direct).await;
        seed_rows(&direct, ROW_COUNT).await;
        create_publication(&direct).await;

        // Ensure replication works as intended after our parallel COPY is complete.
        let writes = async {
            for i in 0..WRITES {
                direct
                    .execute(
                        format!(
                            "INSERT INTO {TEST_SCHEMA}.{TEST_TABLE} (val) VALUES ('new' || {i});
                             UPDATE {TEST_SCHEMA}.{TEST_TABLE} SET val = val || 'x' WHERE id = {};
                             DELETE FROM {TEST_SCHEMA}.{TEST_TABLE} WHERE id = {}",
                            i * 37 % ROW_COUNT + 1,
                            i * 101 % ROW_COUNT + 1,
                        )
                        .as_str(),
                    )
                    .await
                    .unwrap();
            }
        };

        let command = format!("COPY_DATA pgdog pgdog_sharded {TEST_PUB}");
        let copy = run_task_command(&admin, &command);
        let (task_id, ()) = tokio::join!(copy, writes);

        let expected = table_checksum(&direct).await;
        for database in ["shard_0", "shard_1"] {
            let shard = connection_sqlx_direct_db(database).await;
            poll(&format!("{database} to match the source"), || async {
                fail_if_task_errored(&admin, task_id).await;

                if !relation_present(&shard, TEST_TABLE).await {
                    return None;
                }

                (table_checksum(&shard).await == expected).then_some(())
            })
            .await;
        }
    })
    .await;
}

#[tokio::test]
async fn test_copy_data_concurrent_writes_serial() {
    copy_data_with_concurrent_writes(1).await;
}

#[tokio::test]
async fn test_copy_data_concurrent_writes_parallel() {
    copy_data_with_concurrent_writes(3).await;
}

#[tokio::test]
async fn test_failed_copy_cancels_siblings_and_rolls_back() {
    let direct = connection_sqlx_direct().await;
    let admin = admin_sqlx().await;
    cleanup(&admin, &direct).await;

    for i in 1..=TABLE_COUNT {
        create_table(&direct, &numbered_table(i)).await;
    }
    for i in 1..TABLE_COUNT {
        seed_table(&direct, &numbered_table(i), SIBLING_ROWS).await;
    }
    seed_table(&direct, &numbered_table(TABLE_COUNT), 1_000).await;

    let tables = (1..=TABLE_COUNT)
        .map(|i| format!("{TEST_SCHEMA}.{}", numbered_table(i)))
        .collect::<Vec<_>>()
        .join(", ");
    direct
        .execute(format!("CREATE PUBLICATION {TEST_PUB} FOR TABLE {tables}").as_str())
        .await
        .unwrap();

    let poisoned = numbered_table(TABLE_COUNT);
    for db in ["shard_0", "shard_1"] {
        let shard = connection_sqlx_direct_db(db).await;
        create_table(&shard, &poisoned).await;
        shard
            .execute(
                format!("INSERT INTO {TEST_SCHEMA}.{poisoned} (id, val) VALUES (1, 'poison')")
                    .as_str(),
            )
            .await
            .unwrap();
    }

    let task_id =
        run_task_command(&admin, &format!("COPY_DATA pgdog pgdog_sharded {TEST_PUB}")).await;

    wait_for_task(&admin, "the copy-data run to fail", |task| {
        task.id == Some(task_id) && matches!(task.status, TaskProgress::Error { .. })
    })
    .await;

    let rows = poll("all table copies to reach a terminal state", || async {
        let mut rows = Vec::with_capacity(TABLE_COUNT);
        for i in 1..=TABLE_COUNT {
            let row = copy_row(&admin, &numbered_table(i)).await?;
            if !table_progress(&row).is_terminal() {
                return None;
            }
            rows.push(row);
        }
        Some(rows)
    })
    .await;

    let progress = table_progress(&rows[TABLE_COUNT - 1]);
    assert!(
        matches!(progress, TaskProgress::Error { .. }),
        "unexpected progress for the poisoned table: {progress}"
    );
    assert_eq!(shard_row_count("shard_0", &poisoned).await, 1);
    assert_eq!(shard_row_count("shard_1", &poisoned).await, 1);

    let mut cancelled = 0;
    for (i, row) in rows.iter().enumerate().take(TABLE_COUNT - 1) {
        let table = numbered_table(i + 1);
        let expected_rows = match table_progress(row) {
            TaskProgress::Finished => SIBLING_ROWS,
            TaskProgress::Cancelled => {
                cancelled += 1;
                0
            }
            progress => panic!("unexpected progress for {table}: {progress}"),
        };
        assert_eq!(
            shard_row_count("shard_0", &table).await,
            expected_rows,
            "{table}"
        );
        assert_eq!(
            shard_row_count("shard_1", &table).await,
            expected_rows,
            "{table}"
        );
    }
    assert!(cancelled > 0, "no sibling copy was cancelled");

    cleanup(&admin, &direct).await;
}
