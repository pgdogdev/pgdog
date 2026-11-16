use crate::setup::{admin_sqlx, connection_sqlx_direct, connection_sqlx_direct_db};
use pgdog_stats::TaskProgress;
use sqlx::{Executor, Pool, Postgres, Row};

use super::replication::prepare_replication;
use super::table_copies::poll;
use super::{
    TEST_PUB, assert_layout, cleanup, fail_if_task_errored, run_task_command, test_slot_names,
    wait_for_task, wait_for_task_status, with_cleanup,
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

async fn origin_count(db: &str) -> i64 {
    let shard = connection_sqlx_direct_db(db).await;
    sqlx::query_scalar("SELECT count(*) FROM pg_replication_origin WHERE starts_with(roname, $1)")
        .bind(format!("__pgdog_origin_{SLOT}"))
        .fetch_one(&shard)
        .await
        .expect("replication origins must be readable")
}

const SHOW_RESHARDING_LAYOUT: &[(&str, &str)] = &[
    ("source", "TEXT"),
    ("destination", "TEXT"),
    ("command", "TEXT"),
    ("publication", "TEXT"),
    ("stage", "TEXT"),
    ("state", "TEXT"),
    ("owner", "TEXT"),
    ("last_task", "TEXT"),
    ("replication_slot", "TEXT"),
    ("slot_existed", "BOOL"),
    ("slots", "TEXT"),
    ("origins", "TEXT"),
    ("config", "TEXT"),
    ("started_at", "TEXT"),
    ("resumed_at", "TEXT"),
];

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
    let rows = admin.fetch_all("SHOW RESHARDING").await.unwrap();
    if !rows.is_empty() {
        assert_layout(&rows, SHOW_RESHARDING_LAYOUT);
    }
    rows.iter()
        .filter(|row| {
            row.get::<Option<String>, _>("source").as_deref() == Some("pgdog")
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
async fn test_abort_refuses_a_running_run_and_cleans_up_a_paused_one() {
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
            task.id == Some(task_id)
                && task.status == TaskProgress::Running
                && task.inner_status == "replicating"
        })
        .await;
        assert_eq!(resharding_records().await, 1);
        let running = poll("SHOW RESHARDING to list the running resharding", || async {
            fail_if_task_errored(&admin, task_id).await;
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
            task.id == Some(task_id)
                && task.status == TaskProgress::Running
                && task.inner_status == "replicating"
        })
        .await;

        let abort_slot = abort("REPLICATE", &format!(" {SLOT}"));
        let locked = admin_error(&admin, &abort_slot).await;
        assert!(
            locked.starts_with(&format!(
                "resharding pgdog -> pgdog_sharded ({TEST_PUB}) is owned by another session ("
            )) && locked.ends_with("; stop it with STOP_TASK first"),
            "abort of a running resharding: {locked}"
        );
        wait_for_task(&admin, "replication not stopped by the abort", |task| {
            task.id == Some(task_id)
                && task.status == TaskProgress::Running
                && task.inner_status == "replicating"
        })
        .await;
        assert_eq!(resharding_records().await, 1);

        let stopped = admin
            .fetch_one(format!("STOP_TASK {task_id}").as_str())
            .await
            .expect("replication stop must succeed");
        assert_eq!(stopped.get::<String, _>("stop_task"), "OK");
        wait_for_task_status(&admin, task_id, TaskProgress::Cancelled).await;
        assert_eq!(resharding_records().await, 1);
        assert_eq!(test_slot_names(&direct).await, [format!("{SLOT}_0")]);
        let paused = poll("SHOW RESHARDING to list the paused resharding", || async {
            let rows = show_resharding(&admin).await;
            (rows.len() == 1 && rows[0][2] == "paused").then_some(rows)
        })
        .await;
        assert_eq!(
            paused,
            [["replicate", TEST_PUB, "paused", SLOT, "1/1", "2/2", "ok"]]
        );

        let abort_id = run_task_command(&admin, &abort_slot).await;
        wait_for_task(&admin, "abort task listed", |task| {
            task.id == Some(abort_id) && task.kind == "abort"
        })
        .await;
        wait_for_task_status(&admin, abort_id, TaskProgress::Finished).await;

        assert_eq!(resharding_records().await, 0);
        assert_eq!(show_resharding(&admin).await, Vec::<Vec<String>>::new());
        assert_eq!(test_slot_names(&direct).await, Vec::<String>::new());
        assert_eq!(origin_count("shard_0").await, 0);
        assert_eq!(origin_count("shard_1").await, 0);
        assert_eq!(
            admin_error(&admin, &abort("REPLICATE", "")).await,
            missing,
            "abort after the abort"
        );
    })
    .await;
}
