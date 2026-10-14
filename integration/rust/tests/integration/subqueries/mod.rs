// Shared test skeleton that runs each statement against pgdog **AND** Postgres
// to ensure they return same output.

use pg_stream::PgConnection;
use sqlx::{Executor, postgres::PgPoolOptions};
use std::{
    fs::{self, DirEntry},
    path::PathBuf,
};
use tokio::net::TcpStream;

use integration_tests_rust::setup::{admin_sqlx, connection_sqlx_testing_direct};

use crate::integration::subqueries::wire::Protocol;

// Test for exact wire protocol messages match
// between Postgres and PgDog
mod wire;

async fn setup_subqueries_suite() {
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
    let setup_sql_list = [
        "DROP TABLE IF EXISTS subpackages",
        "DROP TABLE IF EXISTS suborders",
        "DROP TABLE IF EXISTS subusers",
        "DROP TABLE IF EXISTS substates",
        "CREATE TABLE subusers (id BIGINT PRIMARY KEY, username text, status text)",
        "CREATE TABLE substates (state text PRIMARY KEY, shipping text)",
        "CREATE TABLE suborders (id BIGINT PRIMARY KEY, user_id BIGINT REFERENCES subusers(id), state text REFERENCES substates(state), created_at TIMESTAMPTZ)",
        "CREATE TABLE subpackages (id BIGINT PRIMARY KEY, order_id BIGINT REFERENCES suborders(id), shipped_at TIMESTAMPTZ , estimated_arrival_time TIMESTAMPTZ)",
    ];

    let setup_sql_inserts = [
        "INSERT INTO substates(state, shipping) VALUES ('CA', 'quick')",
        "INSERT INTO substates(state, shipping) VALUES ('NC', 'not quick')",
        "INSERT INTO subusers(id, username, status) VALUES (1, 'test', 'VIP')",
        "INSERT INTO subusers(id, username, status) VALUES (2, 'test2', 'regular')",
        "INSERT INTO subusers(id, username, status) VALUES (3, 'test3', 'VIP')",
        "INSERT INTO subusers(id, username, status) VALUES (4, 'test4', NULL)",
        "INSERT INTO suborders(id, user_id, state, created_at) VALUES (1, 1, 'CA', '2026-10-12 14:30:00 America/New_York')",
        "INSERT INTO suborders(id, user_id, state, created_at) VALUES (2, 1, 'CA', '2026-10-13 14:30:00 America/New_York')",
        "INSERT INTO suborders(id, user_id, state, created_at) VALUES (3, 2, 'NC', '2026-10-13 14:25:00 America/New_York')",
    ];

    // Aggregate both our direct database and the proxy with our tables...
    for setup_sql in setup_sql_list {
        sqlx::raw_sql(setup_sql)
            .execute(&sharded_conn)
            .await
            .unwrap();

        sqlx::raw_sql(setup_sql)
            .execute(&direct_postgres)
            .await
            .unwrap();
    }

    // Aggregate both our direct database and the proxy with our data
    for setup_sql_insert in setup_sql_inserts {
        sqlx::raw_sql(setup_sql_insert)
            .execute(&sharded_conn)
            .await
            .unwrap();

        sqlx::raw_sql(setup_sql_insert)
            .execute(&direct_postgres)
            .await
            .unwrap();
    }

    // Reload schema
    let admin_conn = admin_sqlx().await;
    admin_conn.execute("RELOAD").await.unwrap();
}

#[tokio::test]
async fn test_subqueries_suite() {
    setup_subqueries_suite().await;

    let mut direct_conn = wire::connect(5432, "testing").await;
    let mut sharded_conn = wire::connect(6432, "pgdog_sharded_subqueries").await;

    // Load all cases from the 'tests' folder
    // and run them individually
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

            // Fetch text for series of comments to get special test commands
            let commands: Commands = Commands::new(test_case.replace(sql.as_str(), "").as_str());
            execute_test_case(commands, &mut direct_conn, &mut sharded_conn, sql.as_str()).await;
        }
    }

    // TODO: complete context in nearby comment to assert...
    //         - (1) correct routing,
    //         - (2) expected error,
    //         - (3) ordered vs unordered
    //
    // TODO:  maybe have separate tables for each for parallelization for
    //          INSERT / UPDATE / etc). Depends on overhead; we don't have much data
    //
    // TODO: parallelize SELECT / etc that doesn't include inserts (can have a command for this)
    //
    // TODO: prepare?

    pub(crate) async fn execute_test_case(
        commands: Commands,
        direct_conn: &mut PgConnection<TcpStream>,
        sharded_conn: &mut PgConnection<TcpStream>,
        sql: &str,
    ) {
        for protocol in [Protocol::Simple, Protocol::Extended] {
            if let Some(setup_command) = commands.setup_command.as_ref() {
                if setup_command.eq("reset tables with fresh data") {
                    setup_subqueries_suite().await;
                } else {
                    panic!("unsupported command: {setup_command}");
                }
            }

            let expected = wire::run(direct_conn, sql.to_string(), protocol, Some(&commands)).await;
            let actual = wire::run(sharded_conn, sql.to_string(), protocol, Some(&commands)).await;
            assert_eq!(actual, expected, "{protocol:?} protocol: {sql}");

            // Run `command_to_verify` (e.g., SELECT * ALL ...) to ensure the rows are equal on each.
            if let Some(command_to_verify) = commands.command_to_verify.as_ref() {
                let expected =
                    wire::run(direct_conn, command_to_verify.to_string(), protocol, None).await;
                let actual =
                    wire::run(sharded_conn, command_to_verify.to_string(), protocol, None).await;

                assert_eq!(
                    actual, expected,
                    "{protocol:?} protocol: {command_to_verify}\n(verifying: {sql})"
                );
            }

            println!("[SUCCESS] {protocol:?} with {sql}!");
        }
    }

    // TODO: Emit a total success / failure (if wanted)
}

pub struct Commands {
    /// Runs this after performing the action to determine
    /// if the test was successful, e.g., if we don't want to add
    /// a RETURNING * after an UPDATE, DELETE, etc. to ensure
    /// that the table is in the same state as the Postgres
    /// instance.
    pub command_to_verify: Option<String>,

    /// Used to specify a certain test setup; e.g., if we want
    /// the tables reset beforehand with fresh data.
    ///
    /// TODO: Parallel option
    pub setup_command: Option<String>,

    /// Assuming an continuous increasing order: $1, $2, $3...
    /// Not all of these will be Strings; they haven't been parsed yet,
    /// so they could be integers, floats... or Strings.
    pub bind_params: Vec<String>,
}

impl Commands {
    pub(crate) fn new(stripped_case: &str) -> Self {
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
            command_to_verify,
            setup_command,
            bind_params,
        }
    }
}
