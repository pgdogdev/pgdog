use crate::setup::{admin_sqlx, connection_sqlx_direct, connection_sqlx_direct_db};
use pgdog_stats::TaskProgress;
use sqlx::postgres::PgRow;
use sqlx::{Executor, Pool, Postgres, Row};

use super::table_copies::{copy_row, poll};
use super::{
    TEST_PUB, TEST_SCHEMA, TEST_TABLE, cleanup, create_publication, create_test_table,
    run_task_command, seed_rows, shard_row_count, wait_for_relation_on_shards,
    wait_for_rows_each_shard, wait_for_task, with_cleanup,
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

#[tokio::test]
async fn test_copy_data() {
    let direct = connection_sqlx_direct().await;
    let admin = admin_sqlx().await;
    cleanup(&admin, &direct).await;

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
