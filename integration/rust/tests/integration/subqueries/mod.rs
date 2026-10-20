//! Shared test skeleton for subqueries that runs each statement against pgdog **AND** Postgres
//! to ensure they return same output.
//!
//! Re-writing subqueries are a work in progress (still in development), and are not recommended to be used in production yet.
//! Please do not work on this code without contacting @jkaczman to discuss first.

use pg_stream::PgConnection;
use sqlx::{Executor, postgres::PgPoolOptions};
use std::{
    fs::{self, DirEntry},
    path::PathBuf,
    sync::Arc,
};
use tokio::{
    net::TcpStream,
    sync::{Mutex, mpsc},
    task::JoinSet,
};

use integration_tests_rust::setup::{admin_sqlx, connection_sqlx_testing_direct};

use crate::integration::subqueries::wire::Protocol;

// Test for exact wire protocol messages match
// between Postgres and PgDog
mod wire;

/// Run this amount of data-modifying (e.g., INSERT, UPDATE, DELETE) tests in parallel
/// with separate tables (subpackages_X, suborders_X) where X = (1, 2, ..., PARALLEL_DATA_MODIFYING_TESTS)
const PARALLEL_DATA_MODIFYING_TESTS: usize = 5;

/// Run this amount of regular (e.g., SELECT) tests in parallel
/// with the same table
const PARALLEL_REGULAR_TESTS: usize = 1;

/// Table name we use for regular tests (e.g., SELECT); shared among all
const REGULAR_TEST_TABLE_NAME: &str = "read";

/// Tables we see in the provided SQL files we have to replace
const TABLES: [&str; 4] = ["subusers", "suborders", "subpackages", "substates"];

/// Accepts `suffix` if it's part of `PARALLEL_DATA_MODIFYING_TESTS` to differentiate the tables
async fn setup_subqueries_suite(suffix: &str, first_time_running: bool) {
    let direct_postgres = connection_sqlx_testing_direct().await;
    let sharded_conn = PgPoolOptions::new()
        .max_connections(1)
        .connect(
            "postgres://pgdog:pgdog@127.0.0.1:6432/pgdog_sharded_subqueries?application_name=sqlx",
        )
        .await
        .unwrap();

    // We can't use NOW() in here as that wouldn't be deterministic when we need the 'testing' (direct Postgres conn) and the
    // sharded PgDog conn to return the same RowDescription / DataRows for each
    // which is fine, we can just make up fake times
    let subpackages_table = format!("subpackages_{suffix}");
    let suborders_table = format!("suborders_{suffix}");
    let subusers_table = format!("subusers_{suffix}");
    let substates_table = format!("substates_{suffix}");

    let setup_sql_list = if first_time_running {
        vec![
            format!(
                "DROP TABLE IF EXISTS {subpackages_table}, {suborders_table}, {subusers_table}, {substates_table}"
            ),
            format!(
                "CREATE TABLE {subusers_table} (id BIGINT PRIMARY KEY, username text, status text)"
            ),
            format!("CREATE TABLE {substates_table} (state text PRIMARY KEY, shipping text)"),
            format!(
                "CREATE TABLE {suborders_table} (id BIGINT PRIMARY KEY, user_id BIGINT REFERENCES {subusers_table}(id), state text REFERENCES {substates_table}(state), created_at TIMESTAMPTZ)"
            ),
            format!(
                "CREATE TABLE {subpackages_table} (id BIGINT PRIMARY KEY, user_id BIGINT REFERENCES {subusers_table}(id), order_id BIGINT REFERENCES {suborders_table}(id), shipped_at TIMESTAMPTZ , estimated_arrival_time TIMESTAMPTZ)"
            ),
        ]
    } else {
        vec![format!(
            "TRUNCATE {subusers_table}, {substates_table}, {suborders_table}, {subpackages_table}"
        )]
    };

    let setup_sql_inserts = [
        format!("INSERT INTO {substates_table}(state, shipping) VALUES ('CA', 'quick')"),
        format!("INSERT INTO {substates_table}(state, shipping) VALUES ('NC', 'not quick')"),
        format!("INSERT INTO {subusers_table}(id, username, status) VALUES (1, 'test', 'VIP')"),
        format!(
            "INSERT INTO {subusers_table}(id, username, status) VALUES (2, 'test2', 'regular')"
        ),
        format!("INSERT INTO {subusers_table}(id, username, status) VALUES (3, 'test3', 'VIP')"),
        format!("INSERT INTO {subusers_table}(id, username, status) VALUES (4, 'test4', NULL)"),
        format!(
            "INSERT INTO {suborders_table}(id, user_id, state, created_at) VALUES (1, 1, 'CA', '2026-10-12 14:30:00 America/New_York')"
        ),
        format!(
            "INSERT INTO {suborders_table}(id, user_id, state, created_at) VALUES (2, 1, 'CA', '2026-10-13 14:30:00 America/New_York')"
        ),
        format!(
            "INSERT INTO {suborders_table}(id, user_id, state, created_at) VALUES (3, 2, 'NC', '2026-10-13 14:25:00 America/New_York')"
        ),
    ];

    // Aggregate both our direct database and the proxy with our tables...
    for setup_sql in setup_sql_list {
        sqlx::raw_sql(setup_sql.as_str())
            .execute(&sharded_conn)
            .await
            .unwrap();

        sqlx::raw_sql(setup_sql.as_str())
            .execute(&direct_postgres)
            .await
            .unwrap();
    }

    // Aggregate both our direct database and the proxy with our data
    for setup_sql_insert in setup_sql_inserts {
        sqlx::raw_sql(setup_sql_insert.as_str())
            .execute(&sharded_conn)
            .await
            .unwrap();

        sqlx::raw_sql(setup_sql_insert.as_str())
            .execute(&direct_postgres)
            .await
            .unwrap();
    }

    // Don't reload unless DDL is performed.
    if !first_time_running {
        return;
    }

    // Reload schema
    let admin_conn = admin_sqlx().await;
    admin_conn.execute("RELOAD").await.unwrap();

    // Ignored by default.
    admin_conn
        .execute("SET rewrite_subqueries TO rewrite")
        .await
        .unwrap();
}

#[tokio::test]
async fn test_subqueries_suite() {
    // Setup the tables we'll be using to test with
    {
        setup_subqueries_suite(REGULAR_TEST_TABLE_NAME, true).await;

        for i in 1..=PARALLEL_DATA_MODIFYING_TESTS {
            setup_subqueries_suite(i.to_string().as_str(), true).await;
        }
    }

    let (mut regular_cases, mut data_modifying_cases) = (Vec::new(), Vec::new());

    // Load all cases from the 'tests' folder
    let list_of_files: fs::ReadDir = fs::read_dir("tests/integration/subqueries/tests").unwrap();
    for file in list_of_files {
        let file_contents: String = {
            let dir_entry: DirEntry = file.unwrap();
            let path: PathBuf = dir_entry.path();
            fs::read_to_string(path).unwrap()
        };

        let mut cases = file_contents.split("-- case");
        cases.next(); // first will always be empty

        for test_case in cases {
            // Fetch text after end of series of comments
            let sql: String = {
                let last_comment_and_after = test_case.split("--").last().unwrap();
                let end_of_last_comment_index = last_comment_and_after.find("\n").unwrap();

                last_comment_and_after[end_of_last_comment_index + 1..]
                    .trim()
                    .to_string()
            };

            let test_case_stripped = test_case.replace(sql.as_str(), "");

            let is_data_modifying_case =
                sql.contains("INSERT") || sql.contains("DELETE") || sql.contains("UPDATE");

            let test_case: TestCase = TestCase::new(sql, test_case_stripped.as_str());

            // Fetch text for series of comments to get special test commands
            if is_data_modifying_case {
                data_modifying_cases.push(test_case);
            } else {
                regular_cases.push(test_case);
            }
        }
    }
    {
        let mut all_tests = JoinSet::new();
        all_tests.spawn(run_tests_for_case(
            regular_cases,
            (1..=PARALLEL_REGULAR_TESTS)
                .map(|_| REGULAR_TEST_TABLE_NAME.to_string())
                .collect(),
        ));

        all_tests.spawn(run_tests_for_case(
            data_modifying_cases,
            (1..=PARALLEL_DATA_MODIFYING_TESTS)
                .map(|i| i.to_string())
                .collect(),
        ));

        all_tests.join_all().await;
    }

    // TODO(jkaczman): total success & failure
}

/// Execute `tests` with `workers_names` amount of workers, using the table suffixes
/// provided in `worker_names`. This occurs in parallel
pub async fn run_tests_for_case(tests: Vec<TestCase>, worker_names: Vec<String>) {
    let (tx, rx) = mpsc::channel::<TestCase>(1000);
    let rx = Arc::new(Mutex::new(rx));

    let mut join_set = JoinSet::new();
    for worker_name in worker_names {
        let rx = Arc::clone(&rx);

        join_set.spawn(async move {
            let mut direct_conn = wire::connect(5432, "testing").await;
            let mut sharded_conn = wire::connect(6432, "pgdog_sharded_subqueries").await;
            loop {
                let job: Option<TestCase> = {
                    let mut lock = rx.lock().await;
                    lock.recv().await
                };

                let Some(job) = job else {
                    break;
                };

                execute_test_case(
                    job,
                    worker_name.as_str(),
                    &mut direct_conn,
                    &mut sharded_conn,
                )
                .await;
            }
        });
    }

    for test in tests {
        tx.send(test).await.unwrap();
    }

    drop(tx);

    join_set.join_all().await;
}

// TODO(jkaczman): complete context in nearby comment to assert...
//         - (1) correct routing,
//         - (2) expected error,
//         - (3) ordered vs unordered
//
// TODO(jkaczman): PREPARE
pub(crate) async fn execute_test_case(
    commands: TestCase,
    suffix: &str,
    direct_conn: &mut PgConnection<TcpStream>,
    sharded_conn: &mut PgConnection<TcpStream>,
) {
    let mut sql = commands.sql.clone();
    for table in TABLES {
        sql = sql.replace(table, format!("{table}_{suffix}").as_str());
    }

    for protocol in [Protocol::Simple, Protocol::Extended] {
        let expected = wire::run(direct_conn, sql.to_string(), protocol, Some(&commands)).await;
        let actual = wire::run(sharded_conn, sql.to_string(), protocol, Some(&commands)).await;
        assert_eq!(actual, expected, "{protocol:?} protocol: {sql}");

        // Run `command_to_verify` (e.g., SELECT * ALL ...) to ensure the rows are equal on each.
        if let Some(command_to_verify) = commands.command_to_verify.as_ref() {
            let mut command_to_verify = command_to_verify.to_string();

            for table in TABLES {
                command_to_verify =
                    command_to_verify.replace(table, format!("{table}_{suffix}").as_str());
            }

            let expected =
                wire::run(direct_conn, command_to_verify.to_string(), protocol, None).await;
            let actual =
                wire::run(sharded_conn, command_to_verify.to_string(), protocol, None).await;

            assert_eq!(
                actual, expected,
                "{protocol:?} protocol: {command_to_verify}\n(verifying: {sql})"
            );
        }

        // Run after potentially modifying something with an INSERT/UPDATE/etc to reset
        // back to the original tables + data, if requested by the test case.
        if let Some(setup_command) = commands.setup_command.as_ref() {
            // TODO(jkaczman): make automatic based on kind of worker (data-modifying vs. not)
            if setup_command.eq("reset tables with fresh data") {
                setup_subqueries_suite(suffix, false).await;
            } else {
                panic!("unsupported command: {setup_command}");
            }
        }

        println!("[SUCCESS] {protocol:?} with {sql}!");
    }
}

pub struct TestCase {
    // SQL to execute as the test
    sql: String,

    /// Runs this after performing the action to determine
    /// if the test was successful, e.g., if we don't want to add
    /// a RETURNING * after an UPDATE, DELETE, etc. to ensure
    /// that the table is in the same state as the Postgres
    /// instance.
    pub command_to_verify: Option<String>,

    /// Used to specify a certain test setup; e.g., if we want
    /// the tables reset beforehand with fresh data.
    pub setup_command: Option<String>,

    /// Assuming an continuous increasing order: $1, $2, $3...
    /// Not all of these will be Strings; they haven't been parsed yet,
    /// so they could be integers, floats... or Strings.
    pub bind_params: Vec<String>,
}

impl TestCase {
    pub(crate) fn new(sql: String, stripped_case: &str) -> Self {
        let lines = stripped_case.split("--");

        let mut command_to_verify: Option<String> = None;
        let mut setup_command: Option<String> = None;

        // Assume an order of $1, $2, $3... (continuous increasing)
        let mut bind_params: Vec<String> = Vec::new();
        for line in lines {
            let line = line.trim();
            if line.starts_with("verify: ") {
                command_to_verify = Some(line.trim().replace("verify: ", ""));
            } else if line.starts_with("setup") {
                setup_command = Some(line.trim().replace("setup: ", ""));
            } else if line.starts_with("$") {
                // We'll support any number of arbitrary bind parameters (one per line)
                // $<number> = <content>
                let content_for_param: &str = line.split("=").nth(1).unwrap();
                let content_for_param: &str = content_for_param.trim();

                bind_params.push(content_for_param.to_string());
            }
        }

        Self {
            sql,
            command_to_verify,
            setup_command,
            bind_params,
        }
    }
}
