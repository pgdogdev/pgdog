use crate::setup::{admin_sqlx, connection_sqlx_direct};
use pgdog_stats::TaskProgress;
use sqlx::{Executor, Pool, Postgres};

use super::abort::{admin_error, show_resharding};
use super::replication::{prepare_replication, wait_for_values};
use super::{
    TEST_PUB, TEST_SCHEMA, TEST_TABLE, cleanup, run_task_command, stop_task, wait_for_source_slot,
    wait_for_task, with_cleanup,
};

const SLOT: &str = "__pgdog_repl_resume";

fn resume(command: &str, slot: &str) -> String {
    format!("{command} pgdog pgdog_sharded {TEST_PUB}{slot} WITH (resume)")
}

async fn insert(direct: &Pool<Postgres>, id: i64, val: &str) {
    direct
        .execute(unsafe_sql!(
            "INSERT INTO {TEST_SCHEMA}.{TEST_TABLE} (id, val) VALUES ({id}, '{val}')"
        ))
        .await
        .expect("source insert must succeed");
}

async fn wait_for_replicating(admin: &Pool<Postgres>, task_id: i64, desc: &str) {
    wait_for_task(admin, desc, |task| {
        task.id == Some(task_id)
            && task.status == TaskProgress::Running
            && task.inner_status == "replicating"
    })
    .await;
}

#[tokio::test]
async fn test_resume_continues_a_paused_replication() {
    let direct = connection_sqlx_direct().await;
    let admin = admin_sqlx().await;
    cleanup(&admin, &direct).await;

    with_cleanup(&admin, &direct, async {
        prepare_replication(&admin, &direct).await;

        let task_id = run_task_command(
            &admin,
            unsafe_sql!(
                "REPLICATE pgdog pgdog_sharded {TEST_PUB} {SLOT} WITH (cleanup_on_cancel false)"
            ),
        )
        .await;
        wait_for_replicating(&admin, task_id, "replication ready").await;
        wait_for_source_slot(&admin, task_id, Some(SLOT)).await;
        insert(&direct, 1, "before").await;
        wait_for_values(&admin, task_id, &[(1, "before")]).await;

        let locked = admin_error(&admin, &resume("REPLICATE", "")).await;
        assert!(
            locked.starts_with(&format!(
                "resharding pgdog -> pgdog_sharded ({TEST_PUB}) is owned by another session ("
            )),
            "resume of a running resharding: {locked}"
        );
        assert_eq!(
            admin_error(&admin, &resume("RESHARD", "")).await,
            r#"resharding command is "replicate", not "reshard""#
        );
        assert_eq!(
            admin_error(&admin, &resume("REPLICATE", " other")).await,
            format!(r#"resharding slot is "{SLOT}", not "other""#)
        );
        wait_for_replicating(&admin, task_id, "replication not stopped by the resume").await;

        stop_task(&admin, task_id, TaskProgress::Cancelled).await;
        insert(&direct, 2, "paused").await;

        let resumed_id = run_task_command(&admin, unsafe_sql!("{}", resume("REPLICATE", ""))).await;
        wait_for_replicating(&admin, resumed_id, "resumed replication ready").await;
        insert(&direct, 3, "after").await;
        wait_for_values(
            &admin,
            resumed_id,
            &[(1, "before"), (2, "paused"), (3, "after")],
        )
        .await;
        wait_for_replicating(&admin, resumed_id, "resumed replication still running").await;

        assert_eq!(
            show_resharding(&admin).await,
            [vec![
                Some("replicate".into()),
                Some(TEST_PUB.into()),
                Some("running".into()),
                Some(SLOT.into()),
                Some(resumed_id.to_string()),
            ]]
        );
    })
    .await;
}
