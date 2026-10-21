//! Shared test skeleton for subqueries that runs each statement against pgdog **AND** Postgres
//! to ensure they return same output.
//!
//! Re-writing subqueries are a work in progress (still in development), and are not recommended to be used in production yet.
//! Please do not work on this code without contacting @jkaczman to discuss first.

use sqlx::{Executor, postgres::PgPoolOptions};
use std::{
    fs::{self, DirEntry},
    path::PathBuf,
};
use tokio::task::JoinSet;

use integration_tests_rust::setup::{admin_sqlx, connection_sqlx_testing_direct};

use crate::integration::subqueries::test_case::TestCase;

// Test for exact wire protocol messages match
// between Postgres and PgDog
mod wire;

// Parsing, loading and executing tests in parallel
mod test_case;

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
    admin_conn
        .execute("SET rewrite_split_inserts TO rewrite")
        .await
        .unwrap();
}

/// Create tables. Reset pg_stat_monitor.
async fn initial_setup() {
    {
        setup_subqueries_suite(REGULAR_TEST_TABLE_NAME, true).await;

        for i in 1..=PARALLEL_DATA_MODIFYING_TESTS {
            setup_subqueries_suite(i.to_string().as_str(), true).await;
        }
    }

    for shard in [0, 1] {
        let sharded_conn = PgPoolOptions::new()
            .max_connections(1)
            .connect(
                format!(
                    "postgres://pgdog:pgdog@127.0.0.1:5432/shard_{shard}?application_name=sqlx"
                )
                .as_str(),
            )
            .await
            .unwrap();

        // Note: this will error if pg_stat_monitor extension is not installed.
        sqlx::raw_sql("SELECT pg_stat_monitor_reset();")
            .execute(&sharded_conn)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn test_subqueries_suite() {
    initial_setup().await;

    let (mut regular_cases, mut data_modifying_cases) = (Vec::new(), Vec::new());
    // Run a single case by name, e.g. SUBQUERY_TEST=<case_name>
    let test_target: Option<String> = std::env::var("SUBQUERY_TEST").ok();

    // Load all cases from the 'tests' folder
    let list_of_files: fs::ReadDir = fs::read_dir("tests/integration/subqueries/tests").unwrap();
    let mut identifier: usize = 0;
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

            let test_case: TestCase = TestCase::new(sql, test_case_stripped.as_str(), identifier);
            if let Some(name) = test_case.name.as_ref()
                && let Some(test_target) = test_target.as_ref()
                && name.eq(test_target)
            {
                // Run the targeted test now. Afterwards, DO NOT proceed.

                let mut direct_conn = wire::connect(5432, "testing").await;
                let mut sharded_conn = wire::connect(6432, "pgdog_sharded_subqueries").await;

                test_case::execute_test_case(
                    &test_case,
                    "read",
                    &mut direct_conn,
                    &mut sharded_conn,
                )
                .await
                .unwrap();

                return;
            }

            // Fetch text for series of comments to get special test commands
            if test_case.data_modifying {
                data_modifying_cases.push(test_case);
            } else {
                regular_cases.push(test_case);
            }

            identifier += 1;
        }
    }

    // If the test target environmental variable is specified, but we don't come
    // across a valid target, then we should panic instead of running the full suite.
    if test_target.is_some() {
        panic!("No match for test target")
    }

    let success_fail = {
        let mut all_tests = JoinSet::new();

        all_tests.spawn(test_case::run_tests_for_case(
            regular_cases,
            (1..=PARALLEL_REGULAR_TESTS)
                .map(|_| REGULAR_TEST_TABLE_NAME.to_string())
                .collect(),
        ));

        all_tests.spawn(test_case::run_tests_for_case(
            data_modifying_cases,
            (1..=PARALLEL_DATA_MODIFYING_TESTS)
                .map(|i| i.to_string())
                .collect(),
        ));

        all_tests.join_all().await
    };

    let (total_success, total_failure) = {
        let mut total_success = 0;
        let mut total_failure: Vec<TestCase> = Vec::new();

        for case_block in success_fail {
            for worker in case_block {
                total_success += worker.0;
                total_failure.extend(worker.1);
            }
        }

        (total_success, total_failure)
    };

    println!("Total success: {total_success}");
    if !total_failure.is_empty() {
        println!("Failures: ");

        for fail_case in total_failure {
            println!("   FAIL: {fail_case}");
        }

        panic!("at least 1 failure");
    }
}
