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

                if execute_test_case(
                    &job,
                    worker_name.as_str(),
                    &mut direct_conn,
                    &mut sharded_conn,
                )
                .await
                .is_ok()
                {
                    success += 1;
                } else {
                    // TRUNCATE in case leftover state from error
                    setup_subqueries_suite(worker_name.as_str(), false).await;

                    // Re-connect in case we broke PgDog
                    direct_conn = wire::connect(5432, "testing").await;
                    sharded_conn = wire::connect(6432, "pgdog_sharded_subqueries").await;

                    fail.push(job);
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

        let expected =
            wire::run(direct_conn, sql.to_string(), protocol, Some(commands), true).await?;

        let actual = wire::run(
            sharded_conn,
            sql.to_string(),
            protocol,
            Some(commands),
            false,
        )
        .await?;

        if expected != actual {
            return Err(anyhow!("{protocol:?} protocol {sql}"));
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
                true,
            )
            .await?;

            let actual = wire::run(
                sharded_conn,
                command_to_verify.to_string(),
                protocol,
                None,
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
    sql: String,

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

    /// First entry here will be Shard 0, second Shard 1, and so on.
    /// Stores each Shard's expected statemnet types, in order.
    ///
    /// Lets us assert on an exact amount of statements executed for statements containing subqueries,
    /// as well as their general routing (by seeing which ones end up on what shards)
    ///
    /// TODO: In the future, `StatementType` should more clearly assert,
    /// to prevent matching on things that are different.
    pub routing_expectations: Vec<Vec<StatementType>>,

    /// Statements expected first on exactly one shard (picked by PgDog) (`route <type>: any`).
    pub routing_any: Vec<StatementType>,
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
        if query.contains("select") {
            StatementType::Select
        } else if query.contains("insert") {
            StatementType::Insert
        } else if query.contains("update") {
            StatementType::Update
        } else if query.contains("delete") {
            StatementType::Delete
        } else {
            panic!("Unknown query type!");
        }
    }
}

impl TestCase {
    pub(crate) fn new(sql: String, stripped_case: &str, identifier: usize) -> Self {
        let lines = stripped_case.split("--");

        // TODO(jkaczman): might be worthwhile in the future
        // to instead make these dynamically put into a map
        // as opposed to optional in a struct (maybe)
        let mut command_to_verify: Option<String> = None;
        let mut name: Option<String> = None;

        const SHARDS: usize = 2;
        let mut routing_expectations: Vec<Vec<StatementType>> = Vec::new();
        for _ in 0..SHARDS {
            routing_expectations.push(vec![]);
        }
        let mut routing_any: Vec<StatementType> = Vec::new();

        // Assume an order of $1, $2, $3... (continuous increasing)
        let mut bind_params: Vec<String> = Vec::new();
        for line in lines {
            let line = line.trim();
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

                if shards == "any" {
                    routing_any.push(statement_type);
                    continue;
                }

                for shard in shards.split(",") {
                    let num_shard: usize = shard.parse().unwrap();

                    routing_expectations
                        .get_mut(num_shard)
                        .unwrap()
                        .push(statement_type.clone());
                }
            }
        }

        let data_modifying =
            sql.contains("INSERT") || sql.contains("DELETE") || sql.contains("UPDATE");

        Self {
            identifier,
            sql,
            name,
            command_to_verify,
            data_modifying,
            bind_params,
            routing_expectations,
            routing_any,
        }
    }
}
