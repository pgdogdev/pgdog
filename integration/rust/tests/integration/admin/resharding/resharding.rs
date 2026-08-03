use std::time::Duration;

use crate::setup::{admin_sqlx, connection_sqlx_direct};
use sqlx::Executor;
use tokio::time::{sleep, timeout};

use super::{
    POLL, TEST_PUB, TEST_TABLE, Tasks, cleanup, create_publication, create_test_table,
    run_task_command, seed_rows, wait_for_relation_on_shards, wait_for_rows_each_shard,
    wait_for_task,
};

#[tokio::test]
async fn test_reshard() {
    let direct = connection_sqlx_direct().await;
    let admin = admin_sqlx().await;
    cleanup(&admin, &direct).await;

    create_test_table(&direct).await;
    seed_rows(&direct, 20).await;
    create_publication(&direct).await;

    let task_id =
        run_task_command(&admin, &format!("RESHARD pgdog pgdog_sharded {TEST_PUB}")).await;

    wait_for_task(&admin, "reshard task", |t| {
        t.id == Some(task_id) && t.kind.starts_with("reshard ")
    })
    .await;

    wait_for_relation_on_shards(&admin, task_id, TEST_TABLE).await;
    wait_for_rows_each_shard(&admin, task_id, TEST_TABLE, 20).await;

    wait_for_task(&admin, "reshard replicating", |t| {
        t.id == Some(task_id) && t.inner_status == "replicating"
    })
    .await;

    let _ = admin.execute(format!("STOP_TASK {task_id}").as_str()).await;
    timeout(Duration::from_secs(30), async {
        loop {
            if Tasks::fetch(&admin)
                .await
                .find(task_id)
                .is_some_and(|t| matches!(t.status.as_str(), "cancelled" | "finished"))
            {
                return;
            }
            sleep(POLL).await;
        }
    })
    .await
    .expect("reshard task did not reach a terminal state");

    cleanup(&admin, &direct).await;
}
