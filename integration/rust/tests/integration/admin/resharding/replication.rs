use std::time::Duration;

use crate::setup::{
    admin_sqlx, connection_sqlx_direct, connection_sqlx_direct_db, connections_sqlx,
};
use pgdog_stats::TaskProgress;
use sqlx::{Executor, Pool, Postgres, Row};
use tokio::time::{sleep, timeout};

use super::table_copies::poll;
use super::{
    POLL, TEST_PUB, TEST_SCHEMA, TEST_TABLE, cleanup, create_publication, create_test_table,
    fail_if_task_errored, run_task_command, seed_rows, task_status_line, test_slot_names,
    wait_for_task, wait_for_task_status, with_cleanup,
};

pub(super) async fn prepare_replication(admin: &Pool<Postgres>, direct: &Pool<Postgres>) {
    create_test_table(direct).await;
    for database in ["shard_0", "shard_1"] {
        create_test_table(&connection_sqlx_direct_db(database).await).await;
    }
    create_publication(direct).await;
    admin.execute("RELOAD").await.expect("reload must succeed");
}

pub(super) async fn start_replication(admin: &Pool<Postgres>, slot: Option<&str>) -> i64 {
    let command = match slot {
        Some(slot) => format!("REPLICATE pgdog pgdog_sharded {TEST_PUB} {slot}"),
        None => format!("REPLICATE pgdog pgdog_sharded {TEST_PUB}"),
    };
    let task_id = run_task_command(admin, &command).await;
    wait_for_task(admin, "replication ready", |task| {
        task.id == Some(task_id) && task.inner_status == "replicating"
    })
    .await;

    task_id
}

pub(super) async fn wait_for_values(
    admin: &Pool<Postgres>,
    task_id: i64,
    expected: &[(i64, &str)],
) {
    let expected: Vec<(i64, String)> = expected
        .iter()
        .map(|(id, value)| (*id, (*value).to_owned()))
        .collect();
    for database in ["shard_0", "shard_1"] {
        let shard = connection_sqlx_direct_db(database).await;
        poll(&format!("replicated values on {database}"), || async {
            fail_if_task_errored(admin, task_id).await;
            let actual = sqlx::query_as::<_, (i64, String)>(&format!(
                "SELECT id, val FROM {TEST_SCHEMA}.{TEST_TABLE} ORDER BY id"
            ))
            .fetch_all(&shard)
            .await
            .expect("destination rows must be readable");
            (actual == expected).then_some(())
        })
        .await;
    }
}

#[tokio::test]
async fn test_replicate_streams_changes_without_copying_existing_rows() {
    let direct = connection_sqlx_direct().await;
    let admin = admin_sqlx().await;
    cleanup(&admin, &direct).await;

    with_cleanup(&admin, &direct, async {
        prepare_replication(&admin, &direct).await;
        seed_rows(&direct, 1).await;

        let task_id = start_replication(&admin, None).await;
        direct
            .execute(
                format!(
                    "INSERT INTO {TEST_SCHEMA}.{TEST_TABLE} (id, val) \
                     VALUES (2, 'inserted'), (3, 'removed')"
                )
                .as_str(),
            )
            .await
            .expect("source inserts must succeed");
        wait_for_values(&admin, task_id, &[(2, "inserted"), (3, "removed")]).await;

        direct
            .execute(
                format!("UPDATE {TEST_SCHEMA}.{TEST_TABLE} SET val = 'updated' WHERE id = 2")
                    .as_str(),
            )
            .await
            .expect("source update must succeed");
        direct
            .execute(format!("DELETE FROM {TEST_SCHEMA}.{TEST_TABLE} WHERE id = 3").as_str())
            .await
            .expect("source delete must succeed");
        wait_for_values(&admin, task_id, &[(2, "updated")]).await;
    })
    .await;
}

#[tokio::test]
async fn test_cutover_without_replication_task() {
    let direct = connection_sqlx_direct().await;
    let admin = admin_sqlx().await;
    cleanup(&admin, &direct).await;

    let err = admin.fetch_one("CUTOVER").await.unwrap_err();
    assert!(
        matches!(err, sqlx::Error::Database(_)),
        "expected a database error, got: {err:?}"
    );
    admin.fetch_one("SHOW VERSION").await.unwrap();
}

#[tokio::test]
async fn test_stop_task() {
    let direct = connection_sqlx_direct().await;
    let admin = admin_sqlx().await;
    cleanup(&admin, &direct).await;

    with_cleanup(&admin, &direct, async {
        prepare_replication(&admin, &direct).await;
        let task_id = start_replication(&admin, None).await;

        let slots = test_slot_names(&direct).await;
        assert_eq!(
            slots.len(),
            1,
            "replication must create one slot on the source: {slots:?}"
        );

        let row = admin
            .fetch_one(format!("STOP_TASK {task_id}").as_str())
            .await
            .unwrap();
        assert_eq!(row.get::<String, _>("stop_task"), "OK");

        wait_for_task_status(&admin, task_id, TaskProgress::Cancelled).await;

        poll(
            "the replication slot to be dropped on the source",
            || async { test_slot_names(&direct).await.is_empty().then_some(()) },
        )
        .await;
    })
    .await;
}

#[tokio::test]
async fn test_cutover_starts_reverse_replication() {
    let direct = connection_sqlx_direct().await;
    let admin = admin_sqlx().await;
    cleanup(&admin, &direct).await;

    with_cleanup(&admin, &direct, async {
        create_test_table(&direct).await;
        seed_rows(&direct, 20).await;
        create_publication(&direct).await;

        let task_id =
            run_task_command(&admin, &format!("COPY_DATA pgdog pgdog_sharded {TEST_PUB}")).await;

        wait_for_task(&admin, "copy_data replicating", |t| {
            t.id == Some(task_id) && t.inner_status == "replicating"
        })
        .await;

        let cutover_ok = timeout(Duration::from_secs(10), async {
            loop {
                if let Ok(row) = admin.fetch_one("CUTOVER").await
                    && row.get::<String, _>("cutover") == "OK"
                {
                    return;
                }
                sleep(POLL).await;
            }
        })
        .await;
        assert!(
            cutover_ok.is_ok(),
            "CUTOVER never returned OK ({})",
            task_status_line(&admin, task_id).await
        );

        let connections = connections_sqlx().await;
        poll("traffic to switch to the destination", || async {
            fail_if_task_errored(&admin, task_id).await;
            let database = sqlx::query_scalar::<_, String>("SELECT current_database()")
                .fetch_one(&connections[0])
                .await
                .ok()?;
            matches!(database.as_str(), "shard_0" | "shard_1").then_some(())
        })
        .await;
        connections[0]
            .execute(
                format!(
                    "INSERT INTO {TEST_SCHEMA}.{TEST_TABLE} (id, val) \
                     VALUES (1001, 'written_after_cutover')"
                )
                .as_str(),
            )
            .await
            .expect("writes through the new source must succeed");

        for database in ["shard_0", "shard_1"] {
            let shard = connection_sqlx_direct_db(database).await;
            let value: String = sqlx::query_scalar(&format!(
                "SELECT val FROM {TEST_SCHEMA}.{TEST_TABLE} WHERE id = 1001"
            ))
            .fetch_one(&shard)
            .await
            .expect("the new source must contain the post-cutover row");
            assert_eq!(value, "written_after_cutover");
        }

        poll(
            "the post-cutover row to replicate back to the old source",
            || async {
                fail_if_task_errored(&admin, task_id).await;
                let value: Option<String> = sqlx::query_scalar(&format!(
                    "SELECT val FROM {TEST_SCHEMA}.{TEST_TABLE} WHERE id = 1001"
                ))
                .fetch_optional(&direct)
                .await
                .expect("the old source must remain readable");
                (value.as_deref() == Some("written_after_cutover")).then_some(())
            },
        )
        .await;

        admin
            .execute(format!("STOP_TASK {task_id}").as_str())
            .await
            .expect("the migration task must stop");
        wait_for_task_status(&admin, task_id, TaskProgress::Finished).await;
    })
    .await;
}

async fn request_cutover(admin: &Pool<Postgres>, task_id: i64) {
    let accepted = timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(row) = admin.fetch_one("CUTOVER").await
                && row.get::<String, _>("cutover") == "OK"
            {
                return;
            }
            sleep(POLL).await;
        }
    })
    .await;

    assert!(
        accepted.is_ok(),
        "CUTOVER never returned OK ({})",
        task_status_line(admin, task_id).await
    );
}

async fn wait_for_traffic(admin: &Pool<Postgres>, task_id: i64, expected: &[&str]) {
    let connections = connections_sqlx().await;
    poll("traffic to switch", || async {
        fail_if_task_errored(admin, task_id).await;
        let database = sqlx::query_scalar::<_, String>("SELECT current_database()")
            .fetch_one(&connections[0])
            .await
            .ok()?;
        expected.contains(&database.as_str()).then_some(())
    })
    .await;
}

#[tokio::test]
async fn test_three_cutovers_alternate_the_traffic_target() {
    let direct = connection_sqlx_direct().await;
    let admin = admin_sqlx().await;
    cleanup(&admin, &direct).await;

    with_cleanup(&admin, &direct, async {
        create_test_table(&direct).await;
        seed_rows(&direct, 20).await;
        create_publication(&direct).await;

        let task_id =
            run_task_command(&admin, &format!("COPY_DATA pgdog pgdog_sharded {TEST_PUB}")).await;

        wait_for_task(&admin, "copy_data replicating", |t| {
            t.id == Some(task_id) && t.inner_status == "replicating"
        })
        .await;

        request_cutover(&admin, task_id).await;
        wait_for_traffic(&admin, task_id, &["shard_0", "shard_1"]).await;

        request_cutover(&admin, task_id).await;
        wait_for_traffic(&admin, task_id, &["pgdog"]).await;

        request_cutover(&admin, task_id).await;
        wait_for_traffic(&admin, task_id, &["shard_0", "shard_1"]).await;

        fail_if_task_errored(&admin, task_id).await;

        admin
            .execute(format!("STOP_TASK {task_id}").as_str())
            .await
            .expect("the migration task must stop");
        wait_for_task_status(&admin, task_id, TaskProgress::Finished).await;
    })
    .await;
}
