use std::time::Duration;

use crate::setup::{admin_sqlx, connection_sqlx_direct};
use sqlx::{Executor, Pool, Postgres, Row};
use tokio::time::{sleep, timeout};

use super::{
    POLL, TEST_PUB, Tasks, cleanup, create_publication, create_test_table, run_task_command,
    seed_rows, task_status_line, wait_for_task, wait_for_task_status,
};

async fn start_replication(admin: &Pool<Postgres>, direct: &Pool<Postgres>) -> i64 {
    admin.execute("RELOAD").await.unwrap();
    sleep(Duration::from_millis(500)).await;

    direct
        .execute(format!("CREATE PUBLICATION {TEST_PUB} FOR ALL TABLES").as_str())
        .await
        .unwrap();

    let row = admin
        .fetch_one(format!("REPLICATE pgdog pgdog_sharded {TEST_PUB}").as_str())
        .await
        .unwrap();
    let task_id: i64 = row.get::<String, _>("task_id").parse().unwrap();

    let appeared = timeout(Duration::from_secs(10), async {
        loop {
            if Tasks::fetch(admin)
                .await
                .find(task_id)
                .is_some_and(|t| t.kind == "replication pgdog -> pgdog_sharded")
            {
                return;
            }
            sleep(POLL).await;
        }
    })
    .await;
    assert!(
        appeared.is_ok(),
        "replication task {task_id} did not appear in SHOW TASKS in time"
    );

    task_id
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

    let task_id = start_replication(&admin, &direct).await;

    let row = admin
        .fetch_one(format!("STOP_TASK {task_id}").as_str())
        .await
        .unwrap();
    assert_eq!(row.get::<String, _>("stop_task"), "OK");

    wait_for_task_status(&admin, task_id, "cancelled").await;
    cleanup(&admin, &direct).await;
}

#[tokio::test]
async fn test_cutover() {
    let direct = connection_sqlx_direct().await;
    let admin = admin_sqlx().await;
    cleanup(&admin, &direct).await;

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

    wait_for_task_status(&admin, task_id, "finished").await;
    cleanup(&admin, &direct).await;
}
