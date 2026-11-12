use crate::setup::{admin_sqlx, connection_sqlx_direct, connection_sqlx_direct_db};
use pgdog_stats::TaskProgress;
use sqlx::{Executor, Pool, Postgres};

use super::replication::prepare_replication;
use super::{
    TEST_PUB, cleanup, run_task_command, test_slot_names, wait_for_task, wait_for_task_status,
    with_cleanup,
};

const SLOT: &str = "__pgdog_repl_abort";

fn abort(command: &str, slot: &str) -> String {
    format!("{command} pgdog pgdog_sharded {TEST_PUB}{slot} WITH (abort)")
}

async fn admin_error(admin: &Pool<Postgres>, sql: &str) -> String {
    match admin.execute(sql).await {
        Err(sqlx::Error::Database(err)) => err.message().to_owned(),
        other => panic!("{sql} must fail with a database error, got {other:?}"),
    }
}

async fn resharding_records() -> i64 {
    let shard = connection_sqlx_direct_db("shard_0").await;
    let exists: bool = sqlx::query_scalar("SELECT to_regclass('pgdog.resharding') IS NOT NULL")
        .fetch_one(&shard)
        .await
        .unwrap();
    if !exists {
        return 0;
    }
    sqlx::query_scalar("SELECT count(*) FROM pgdog.resharding")
        .fetch_one(&shard)
        .await
        .unwrap()
}

#[tokio::test]
async fn test_abort_checks_the_record_and_stops_the_run() {
    let direct = connection_sqlx_direct().await;
    let admin = admin_sqlx().await;
    cleanup(&admin, &direct).await;

    with_cleanup(&admin, &direct, async {
        prepare_replication(&admin, &direct).await;
        let missing = format!("no unfinished resharding pgdog -> pgdog_sharded ({TEST_PUB})");

        assert_eq!(
            admin_error(&admin, &abort("REPLICATE", "")).await,
            missing,
            "abort before any run"
        );

        let task_id = run_task_command(
            &admin,
            &format!(
                "REPLICATE pgdog pgdog_sharded {TEST_PUB} {SLOT} WITH (cleanup_on_cancel false)"
            ),
        )
        .await;
        wait_for_task(&admin, "replication ready", |task| {
            task.id == Some(task_id) && task.inner_status == "replicating"
        })
        .await;
        assert_eq!(resharding_records().await, 1);

        let wrong_command = r#"resharding command is "replicate", not "reshard""#;
        for (sql, expected) in [
            (abort("RESHARD", ""), wrong_command.to_owned()),
            (abort("COPY_DATA", ""), wrong_command.to_owned()),
            (
                abort("REPLICATE", " other"),
                format!(r#"resharding slot is "{SLOT}", not "other""#),
            ),
        ] {
            assert_eq!(admin_error(&admin, &sql).await, expected, "{sql}");
        }
        wait_for_task(&admin, "replication still running", |task| {
            task.id == Some(task_id) && task.inner_status == "replicating"
        })
        .await;

        let abort_id = run_task_command(&admin, &abort("REPLICATE", &format!(" {SLOT}"))).await;
        wait_for_task(&admin, "abort task listed", |task| {
            task.id == Some(abort_id) && task.kind == "abort"
        })
        .await;
        wait_for_task_status(&admin, task_id, TaskProgress::Cancelled).await;
        wait_for_task_status(&admin, abort_id, TaskProgress::Finished).await;

        assert_eq!(resharding_records().await, 0);
        assert_eq!(test_slot_names(&direct).await, Vec::<String>::new());
        assert_eq!(
            admin_error(&admin, &abort("REPLICATE", "")).await,
            missing,
            "abort after the abort"
        );
    })
    .await;
}
