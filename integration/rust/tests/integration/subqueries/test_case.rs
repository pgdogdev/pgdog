use std::{fmt::Display, sync::Arc};

use anyhow::anyhow;
use pg_stream::PgConnection;
use tokio::{
    net::TcpStream,
    sync::{Mutex, mpsc},
    task::JoinSet,
};

use crate::integration::subqueries::{
    TABLES, setup_subqueries_suite,
    wire::{self, Protocol},
};

/// Execute `tests` with `workers_names` amount of workers, using the table suffixes
/// provided in `worker_names`. This occurs in parallel
pub async fn run_tests_for_case(
    tests: Vec<TestCase>,
    worker_names: Vec<String>,
) -> Vec<(usize, Vec<TestCase>)> {
    let (tx, rx) = mpsc::channel::<TestCase>(1000);
    let rx = Arc::new(Mutex::new(rx));

    let mut join_set = JoinSet::new();

    for worker_name in worker_names {
        let rx = Arc::clone(&rx);

        join_set.spawn(async move {
            let mut direct_conn = wire::connect(5432, "testing").await;
            let mut sharded_conn = wire::connect(6432, "pgdog_sharded_subqueries").await;

            let mut success = 0usize;
            let mut fail: Vec<TestCase> = Vec::new();
            loop {
                let job: Option<TestCase> = {
                    let mut lock = rx.lock().await;
                    lock.recv().await
                };

                let Some(job) = job else {
                    break;
                };

                let result = execute_test_case(
                    &job,
                    worker_name.as_str(),
                    &mut direct_conn,
                    &mut sharded_conn,
                )
                .await;

                if let Err(err) = result {
                    println!("FAILED HERE: {} {}\n{err:?}", worker_name.as_str(), job.sql);
                    // TRUNCATE in case leftover state from error
                    setup_subqueries_suite(worker_name.as_str(), false).await;

                    // Re-connect in case we broke PgDog
                    direct_conn = wire::connect(5432, "testing").await;
                    sharded_conn = wire::connect(6432, "pgdog_sharded_subqueries").await;

                    fail.push(job);
                } else {
                    success += 1;
                }
            }

            (success, fail)
        });
    }

    for test in tests {
        tx.send(test).await.unwrap();
    }

    drop(tx);

    join_set.join_all().await
}

// TODO(jkaczman): complete context in nearby comment to assert...
//         - (2) expected error,
//         - (3) ordered vs unordered
//
// TODO(jkaczman): PREPARE
pub(crate) async fn execute_test_case(
    commands: &TestCase,
    suffix: &str,
    direct_conn: &mut PgConnection<TcpStream>,
    sharded_conn: &mut PgConnection<TcpStream>,
) -> Result<(), anyhow::Error> {
    let mut sql = commands.sql.clone();
    for table in TABLES {
        sql = sql.replace(table, format!("{table}_{suffix}").as_str());
    }

    for protocol in [Protocol::Simple, Protocol::Extended] {
        // Don't run with bind params in simple protocol.
        if protocol == Protocol::Simple && !commands.bind_params.is_empty() {
            continue;
        }

        let expected = wire::run(
            direct_conn,
            sql.to_string(),
            protocol,
            Some(commands),
            suffix,
            true,
        )
        .await?;

        let actual = wire::run(
            sharded_conn,
            sql.to_string(),
            protocol,
            Some(commands),
            suffix,
            false,
        )
        .await?;

        if expected != actual {
            return Err(anyhow!(
                "{protocol:?} protocol {sql},\n expected = {expected:#?}, \nactual = {actual:#?}"
            ));
        }

        // Run `command_to_verify` (e.g., SELECT * ALL ...) to ensure the rows are equal on each.
        if let Some(command_to_verify) = commands.command_to_verify.as_ref() {
            let mut command_to_verify = command_to_verify.to_string();

            for table in TABLES {
                command_to_verify =
                    command_to_verify.replace(table, format!("{table}_{suffix}").as_str());
            }

            let expected = wire::run(
                direct_conn,
                command_to_verify.to_string(),
                protocol,
                None,
                suffix,
                true,
            )
            .await?;

            let actual = wire::run(
                sharded_conn,
                command_to_verify.to_string(),
                protocol,
                None,
                suffix,
                false,
            )
            .await?;

            if expected != actual {
                println!("FAIL {protocol:?} {command_to_verify}");
                return Err(anyhow!("verifying mismatch"));
            }
        }

        // Run after modifying something with an INSERT/UPDATE/etc to reset
        // back to the original tables + data.
        if commands.data_modifying {
            setup_subqueries_suite(suffix, false).await;
        }

        println!("[SUCCESS] {protocol:?} with {sql}!");
    }

    Ok(())
}

impl Display for TestCase {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(fmt, "{}", self.sql)
    }
}

pub struct TestCase {
    /// Internal identifier unique to the test case to find relevant queries
    /// in `pg_stat_statements`
    pub identifier: usize,

    /// SQL to execute as the test
    pub sql: String,

    /// If we want to target a specific test, instead of running the
    /// entire suite in parallel, then we should be able to specify
    /// that this is the only test we want to run (via optional name)
    pub name: Option<String>,

    /// Runs this after performing the action to determine
    /// if the test was successful, e.g., if we don't want to add
    /// a RETURNING * after an UPDATE, DELETE, etc. to ensure
    /// that the table is in the same state as the Postgres
    /// instance.
    pub command_to_verify: Option<String>,

    /// The SQL contains an INSERT, UPDATE or DELETE, so the tables
    /// are reset with fresh data after each run.
    pub data_modifying: bool,

    /// Assuming an continuous increasing order: $1, $2, $3...
    /// Not all of these will be Strings; they haven't been parsed yet,
    /// so they could be integers, floats... or Strings.
    pub bind_params: Vec<String>,

    /// Lets us assert on an exact amount of statements executed for statements containing subqueries,
    /// as well as their general routing (by seeing which ones end up on what shards)
    ///
    /// Optional `sql: <query>` comment after `route` comment also asserts exact query ran
    pub routing_expectations: Vec<ExpectedStatement>,
}

const SHARDS: usize = 2;

#[derive(PartialEq, Debug, Clone)]
pub enum ExpectedShards {
    // `route <type>: 0,1`
    Shards(Vec<usize>),

    // `route <type>: any` (proxy picks shard)
    Any,
}

// Statement that we expect proxy to send to the shards (`route <type>: <shards>`).
#[derive(PartialEq, Debug, Clone)]
pub struct ExpectedStatement {
    pub statement_type: StatementType,

    pub shards: ExpectedShards,

    // Optional `sql: <query>` comment after `route` comment also asserts exact query ran
    pub sql: Option<String>,
}

impl ExpectedStatement {
    // Returns true if `actual_query` (from pg_stat_monitor) eq the statement type and the query text that we expect.
    pub fn matches(&self, actual_query: &str, table_suffix: &str) -> bool {
        if StatementType::parse(actual_query) != self.statement_type {
            return false;
        }

        let Some(expected_sql) = self.sql.as_ref() else {
            return true;
        };

        let mut expected_sql = expected_sql.clone();
        for table in TABLES {
            expected_sql = expected_sql.replace(table, format!("{table}_{table_suffix}").as_str());
        }

        without_formatting(expected_sql.as_str()) == without_formatting(actual_query)
    }
}

// Returns the query with single spaces between the words and without the semicolon at the end,
// to ensure that we can write `sql:` comments without matching the whitespace
fn without_formatting(query: &str) -> String {
    query
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim_end_matches(';')
        .trim_end()
        .to_string()
}

#[derive(PartialEq, Debug, Clone)]
pub enum StatementType {
    Select,
    Insert,
    Update,
    Delete,
}

impl StatementType {
    pub fn parse(query: &str) -> Self {
        let query = query.to_lowercase();
        if query.starts_with("insert") {
            StatementType::Insert
        } else if query.starts_with("update") {
            StatementType::Update
        } else if query.starts_with("delete") {
            StatementType::Delete
        } else if query.contains("select") {
            // This deliberately uses `contains` instead of `starts_with` to match on CTEs.
            // Also deliberately matched last to match INSERT / UPDATE / DELETE beforehand.
            StatementType::Select
        } else {
            panic!("Unknown query type! {query:?}");
        }
    }
}

impl TestCase {
    pub(crate) fn new(sql: String, stripped_case: &str, identifier: usize) -> Option<Self> {
        let lines = stripped_case.split("--");

        // TODO(jkaczman): might be worthwhile in the future
        // to instead make these dynamically put into a map
        // as opposed to optional in a struct (maybe)
        let mut command_to_verify: Option<String> = None;
        let mut name: Option<String> = None;

        let mut routing_expectations: Vec<ExpectedStatement> = Vec::new();

        // Assume an order of $1, $2, $3... (continuous increasing)
        let mut bind_params: Vec<String> = Vec::new();

        let mut remaining_lines = lines.clone().peekable();
        while let Some(line) = remaining_lines.next() {
            let line = line.trim();

            // Can have cases where we don't want them executed yet (e.g.,
            // if we have TODO cases for later and we want to expand on our
            // current functionality, but they'd fail right now)
            if line.eq("marked TODO") {
                return None;
            }

            if line.starts_with("verify: ") {
                command_to_verify = Some(line.trim().replace("verify: ", ""));
            } else if line.starts_with("name") {
                name = Some(line.trim().replace("name: ", ""));
            } else if line.starts_with("$") {
                // We'll support any number of arbitrary bind parameters (one per line)
                // $<number> = <content>
                let content_for_param: &str = line.split("=").nth(1).unwrap();
                let content_for_param: &str = content_for_param.trim();

                bind_params.push(content_for_param.to_string());
            } else if line.starts_with("route") {
                let statement_type =
                    StatementType::parse(line.split(" ").nth(1).unwrap().trim_end_matches(':'));
                let shards = line.split(" ").nth(2).unwrap();

                let sql = remaining_lines
                    .next_if(|next_line| next_line.trim().starts_with("sql: "))
                    .map(|sql_line| sql_line.trim().replacen("sql: ", "", 1));

                let shards = if shards == "any" {
                    ExpectedShards::Any
                } else {
                    ExpectedShards::Shards(
                        shards
                            .split(",")
                            .map(|shard| shard.parse().unwrap())
                            .collect(),
                    )
                };

                routing_expectations.push(ExpectedStatement {
                    statement_type,
                    shards,
                    sql,
                });
            } else if line.starts_with("sql: ") {
                panic!(
                    "`sql:` has to be right after the `route` comment of the statement: {line:?}"
                );
            }
        }

        // Guard against these cases.
        if sql.trim().is_empty() {
            panic!("Found empty SQL case lines: {lines:?}");
        }

        let data_modifying =
            sql.contains("INSERT") || sql.contains("DELETE") || sql.contains("UPDATE");

        Some(Self {
            identifier,
            sql,
            name,
            command_to_verify,
            data_modifying,
            bind_params,
            routing_expectations,
        })
    }

    // Returns the statements that we expect all shards to run (the first entry is for Shard 0, then Shard 1, and so on),
    // assuming that the proxy sends `route <type>: any` statements to the `any_shard` shard.
    pub(crate) fn expected_statements_per_shard(
        &self,
        any_shard: usize,
    ) -> Vec<Vec<&ExpectedStatement>> {
        (0..SHARDS)
            .map(|shard| {
                self.routing_expectations
                    .iter()
                    .filter(|statement| match &statement.shards {
                        ExpectedShards::Shards(shards) => shards.contains(&shard),
                        ExpectedShards::Any => shard == any_shard,
                    })
                    .collect()
            })
            .collect()
    }
}
