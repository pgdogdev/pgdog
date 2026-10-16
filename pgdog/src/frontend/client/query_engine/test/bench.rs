//! Micro benchmarking the QueryEngine

#[cfg(feature = "bench")]
use divan::AllocProfiler;

/// Profile memory usage in bench
#[cfg(feature = "bench")]
#[global_allocator]
static ALLOC: AllocProfiler = AllocProfiler::system();

#[test]
#[cfg(feature = "bench")]
fn run_benches() {
    // Starts off the benchmarks below (identified by #[divan::bench])
    divan::Divan::default().run_benches();
}

use std::fmt;

use super::prelude::*;
use crate::config::load_test_sharded;
use crate::frontend::router::parser::Cache;

struct Case(&'static str, &'static str);

impl fmt::Display for Case {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

const CASES: &[Case] = &[
    Case("select_by_key", "SELECT * FROM sharded WHERE id = 1"),
    Case(
        "select_in_list",
        "SELECT * FROM sharded WHERE id IN (1, 2, 3)",
    ),
    Case("select_all_shards", "SELECT * FROM sharded"),
    Case("select_omni", "SELECT * FROM sharded_omni"),
    Case(
        "join",
        "SELECT * FROM sharded s JOIN sharded_omni o ON s.id = o.id WHERE s.id = 1",
    ),
    Case(
        "subquery",
        "SELECT * FROM sharded WHERE id IN (SELECT id FROM sharded_omni)",
    ),
    Case(
        "cte",
        "WITH t AS (SELECT * FROM sharded WHERE id = 1) SELECT * FROM t",
    ),
    Case("aggregate", "SELECT COUNT(*), AVG(value) FROM sharded"),
    Case(
        "order_limit_offset",
        "SELECT * FROM sharded ORDER BY id LIMIT 10 OFFSET 5",
    ),
    Case("insert", "INSERT INTO sharded (id, value) VALUES (1, 'a')"),
    Case(
        "insert_split",
        "INSERT INTO sharded (id, value) VALUES (1, 'a'), (2, 'b')",
    ),
    Case(
        "update_by_key",
        "UPDATE sharded SET value = 'a' WHERE id = 1",
    ),
    Case("delete_by_key", "DELETE FROM sharded WHERE id = 1"),
    Case("begin", "BEGIN"),
    Case("set", "SET statement_timeout TO 1000"),
];

fn messages(sql: &str, extended: bool) -> Vec<ProtocolMessage> {
    if extended {
        vec![
            Parse::new_anonymous(sql).into(),
            Bind::new_statement("").into(),
            Execute::new().into(),
            Sync.into(),
        ]
    } else {
        vec![Query::new(sql).into()]
    }
}

async fn route(client: &mut Client, engine: &mut QueryEngine) {
    let (mut context, request) = QueryEngineContext::new(client);

    engine
        .rewrite_extended(&mut context, &mut request.messages)
        .unwrap();

    let rewrite = engine
        .parse_and_rewrite(&mut context, request)
        .await
        .unwrap();

    engine
        .route_query(&mut context, request, rewrite.as_ref())
        .await
        .unwrap();
}

/// Build the Client & QueryEngine outside of the timed section, so that we only measure the
/// query engine.
///
/// When **cached** is set to false, we clear the AST cache first, so that we measure
/// parsing too (i.e., the first time PgDog sees a query).
fn bench(bencher: divan::Bencher, case: &Case, extended: bool, cached: bool) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let _guard = rt.enter();

    load_test_sharded();

    bencher
        .with_inputs(|| {
            if !cached {
                Cache::reset();
            }

            let mut client = Client::new_test(Stream::dev_null(), Parameters::default());
            client.client_request = messages(case.1, extended).into();

            let engine = QueryEngine::from_client(&client).unwrap();

            (client, engine)
        })
        .bench_local_values(|(mut client, mut engine)| {
            rt.block_on(route(&mut client, &mut engine));

            (client, engine)
        });
}

#[divan::bench(args = CASES, sample_count = 1000)]
fn simple_cold(bencher: divan::Bencher, case: &Case) {
    bench(bencher, case, false, false);
}

#[divan::bench(args = CASES, sample_count = 1000)]
fn extended_cold(bencher: divan::Bencher, case: &Case) {
    bench(bencher, case, true, false);
}

#[divan::bench(args = CASES, sample_count = 1000)]
fn extended_cached(bencher: divan::Bencher, case: &Case) {
    bench(bencher, case, true, true);
}

#[tokio::test]
async fn test_bench_cases_route() {
    load_test_sharded();
    for case in CASES {
        for extended in [false, true] {
            let mut client = Client::new_test(Stream::dev_null(), Parameters::default());
            client.client_request = messages(case.1, extended).into();

            let mut engine = QueryEngine::from_client(&client).unwrap();

            assert!(
                engine.backend.cluster().is_ok(),
                "{case}: no cluster to route to"
            );

            route(&mut client, &mut engine).await;
        }
    }
}
