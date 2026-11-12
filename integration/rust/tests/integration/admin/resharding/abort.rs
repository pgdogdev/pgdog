use crate::setup::{admin_sqlx, connection_sqlx_direct, connection_sqlx_direct_db};
use pgdog_stats::TaskProgress;
use sqlx::{Executor, Pool, Postgres, Row};

use super::replication::prepare_replication;
use super::table_copies::poll;
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

const RESHARDING_COLUMNS: [&str; 7] = [
    "command",
    "publication",
    "state",
    "replication_slot",
    "slots",
    "origins",
    "config",
];

async fn show_resharding(admin: &Pool<Postgres>) -> Vec<Vec<String>> {
    admin
        .fetch_all("SHOW RESHARDING")
        .await
        .unwrap()
        .iter()
        .filter(|row| {
            row.get::<String, _>("source") == "pgdog"
                && row.get::<String, _>("destination") == "pgdog_sharded"
        })
        .map(|row| {
            RESHARDING_COLUMNS
                .iter()
                .map(|column| row.get::<String, _>(*column))
                .collect()
        })
        .collect()
}

fn complete(count: &str) -> bool {
    matches!(count.split_once('/'), Some((present, expected)) if present == expected && present != "0")
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
        let running = poll("SHOW RESHARDING to list the running resharding", || async {
            let rows = show_resharding(&admin).await;
            (rows.len() == 1 && complete(&rows[0][4]) && complete(&rows[0][5])).then_some(rows)
        })
        .await;
        assert_eq!(
            running[0]
                .iter()
                .enumerate()
                .filter(|(column, _)| ![4, 5].contains(column))
                .map(|(_, value)| value.as_str())
                .collect::<Vec<_>>(),
            ["replicate", TEST_PUB, "running", SLOT, "ok"]
        );

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
        assert_eq!(show_resharding(&admin).await, Vec::<Vec<String>>::new());
        assert_eq!(test_slot_names(&direct).await, Vec::<String>::new());
        assert_eq!(
            admin_error(&admin, &abort("REPLICATE", "")).await,
            missing,
            "abort after the abort"
        );
    })
    .await;
}
