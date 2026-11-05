//! Microbenchmarks of PgDog:
//! - (1) routing alone
//! - (2) whole requests against fake in-memory Postgres servers
//!
//! TODO(jkaczman): move this module out of the query engine, since full req now covers client loop, pool, backend, etc.

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

use std::cell::RefCell;
use std::fmt;
use std::ops::Deref;

use tokio::io::AsyncWriteExt;

#[cfg(all(feature = "bench", target_os = "linux"))]
mod instructions;

use pgdog_config::{DataType, ShardedTableConfig};

use super::prelude::*;
use crate::backend::databases::{databases, reload_from_existing};
use crate::backend::fake_server;
use crate::config::{config, load_test_sharded, set};
use crate::frontend::router::parser::Cache;

struct Case(&'static str, &'static str);

impl fmt::Display for Case {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl Case {
    fn sql(&self) -> &'static str {
        self.1
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
    Case("distinct", "SELECT DISTINCT value FROM sharded"),
    Case(
        "group_by",
        "SELECT value, COUNT(*) FROM sharded GROUP BY value",
    ),
    Case(
        "min_max_sum",
        "SELECT MIN(id), MAX(id), SUM(id) FROM sharded",
    ),
    Case(
        "insert_returning",
        "INSERT INTO sharded (id, value) VALUES (1, 'a') RETURNING id",
    ),
    Case("update_all_shards", "UPDATE sharded SET value = 'a'"),
    Case("delete_all_shards", "DELETE FROM sharded"),
    Case(
        "lookup",
        "SELECT * FROM sharded_lookup WHERE org_id = 'acme'",
    ),
    Case("begin", "BEGIN"),
    Case("set", "SET statement_timeout TO 1000"),
];

#[derive(Clone, Copy)]
enum ClientProtocol {
    Simple,
    Unnamed,
    Named,
}

#[derive(Clone, Copy)]
enum Caches {
    Cold,
    Warm,
    Evicting,
}

#[derive(Clone, Copy)]
enum Scope {
    Route,
    Full,
}

struct Group {
    name: &'static str,
    protocol: ClientProtocol,
    caches: Caches,
    scope: Scope,
}

const SIMPLE_COLD: Group = Group {
    name: "simple_cold",
    protocol: ClientProtocol::Simple,
    caches: Caches::Cold,
    scope: Scope::Route,
};
const EXTENDED_COLD: Group = Group {
    name: "extended_cold",
    protocol: ClientProtocol::Unnamed,
    caches: Caches::Cold,
    scope: Scope::Route,
};
const EXTENDED_CACHED: Group = Group {
    name: "extended_cached",
    protocol: ClientProtocol::Unnamed,
    caches: Caches::Warm,
    scope: Scope::Route,
};
const EXTENDED_EVICTING: Group = Group {
    name: "extended_evicting",
    protocol: ClientProtocol::Unnamed,
    caches: Caches::Evicting,
    scope: Scope::Route,
};
const FULL_SIMPLE: Group = Group {
    name: "full_simple",
    protocol: ClientProtocol::Simple,
    caches: Caches::Cold,
    scope: Scope::Full,
};
const FULL_EXTENDED_CACHED: Group = Group {
    name: "full_extended_cached",
    protocol: ClientProtocol::Unnamed,
    caches: Caches::Warm,
    scope: Scope::Full,
};
const FULL_NAMED_CACHED: Group = Group {
    name: "full_named_cached",
    protocol: ClientProtocol::Named,
    caches: Caches::Warm,
    scope: Scope::Full,
};

const GROUPS: &[Group] = &[
    SIMPLE_COLD,
    EXTENDED_COLD,
    EXTENDED_CACHED,
    EXTENDED_EVICTING,
    FULL_SIMPLE,
    FULL_EXTENDED_CACHED,
    FULL_NAMED_CACHED,
];

const NAMED_STATEMENT: &str = "bench";

fn messages(sql: &str, protocol: ClientProtocol) -> Vec<ProtocolMessage> {
    match protocol {
        ClientProtocol::Simple => vec![Query::new(sql).into()],
        ClientProtocol::Unnamed => vec![
            Parse::new_anonymous(sql).into(),
            Bind::new_statement("").into(),
            Describe::new_portal("").into(),
            Execute::new().into(),
            Sync.into(),
        ],
        ClientProtocol::Named => vec![
            Bind::new_statement(NAMED_STATEMENT).into(),
            Describe::new_portal("").into(),
            Execute::new().into(),
            Sync.into(),
        ],
    }
}

struct BenchClient {
    stream: Stream,
}

impl BenchClient {
    fn new() -> Self {
        let (ours, pgdog) = tokio::io::duplex(1024 * 1024);
        let client = Client::new_test(Stream::memory(pgdog), Parameters::default());
        tokio::spawn(client.spawn_test());

        Self {
            stream: Stream::memory(ours),
        }
    }

    async fn prepare(&mut self, sql: &str) {
        let parse = [Parse::named(NAMED_STATEMENT, sql).into(), Sync.into()];
        assert!(self.request(&parse).await, "{sql}: can't prepare");
    }

    async fn request(&mut self, messages: &[ProtocolMessage]) -> bool {
        for message in messages {
            self.stream.send(message).await.unwrap();
        }
        self.stream.flush().await.unwrap();

        let mut ok = true;
        loop {
            let Ok(message) = self.stream.read().await else {
                return false;
            };
            ok &= message.code() != 'E';
            if message.code() == 'Z' {
                return ok;
            }
        }
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

enum Input {
    Route(Box<(Client, QueryEngine)>),
    Full,
}

struct Runner {
    messages: Vec<ProtocolMessage>,
    caches: Caches,
    bench_client: Option<BenchClient>,
}

impl Runner {
    async fn new(case: &Case, group: &Group) -> Self {
        let bench_client = match group.scope {
            Scope::Route => None,
            Scope::Full => {
                let mut bench_client = BenchClient::new();
                if let ClientProtocol::Named = group.protocol {
                    Cache::reset();
                    bench_client.prepare(case.sql()).await;
                }
                Some(bench_client)
            }
        };

        Self {
            messages: messages(case.sql(), group.protocol),
            caches: group.caches,
            bench_client,
        }
    }

    fn input(&self) -> Input {
        prepare_caches(self.caches);

        match self.bench_client {
            None => {
                let mut client = Client::new_test(Stream::dev_null(), Parameters::default());
                client.client_request = self.messages.clone().into();
                let engine = QueryEngine::from_client(&client).unwrap();
                Input::Route(Box::new((client, engine)))
            }
            Some(_) => Input::Full,
        }
    }

    async fn request(&mut self, input: &mut Input) -> bool {
        match (input, &mut self.bench_client) {
            (Input::Route(route_input), _) => {
                let (client, engine) = &mut **route_input;
                route(client, engine).await;
                true
            }
            (Input::Full, Some(bench_client)) => bench_client.request(&self.messages).await,
            (Input::Full, None) => unreachable!("full requests always have a bench client"),
        }
    }
}

fn prepare_caches(caches: Caches) {
    let query_cache_limit = config().config.general.query_cache_limit;

    match caches {
        Caches::Cold => {
            Cache::resize(query_cache_limit);
            Cache::reset();
            for cluster in databases().all().values() {
                cluster.sharded_tables().lookup_cache().clear();
            }
        }
        Caches::Warm => Cache::resize(query_cache_limit),
        Caches::Evicting => {
            Cache::reset();
            Cache::resize(1);
            Cache::get().record("SELECT 'evicted'").unwrap();
        }
    }
}

fn bench_config() {
    load_test_sharded();

    let mut config = config().deref().clone();
    for database in &mut config.config.databases {
        database.host = fake_server::HOST.into();
    }

    config.config.sharded_tables.push(ShardedTableConfig {
        database: "pgdog".into(),
        name: Some("sharded_lookup".into()),
        column: "org_id".into(),
        data_type: DataType::Varchar,
        lookup_query: Some("SELECT root_org_id FROM org_roots WHERE org_id = $1 LIMIT 1".into()),
        ..Default::default()
    });

    set(config).unwrap();
    reload_from_existing().unwrap();
}

fn bench(bencher: divan::Bencher, case: &Case, group: &Group) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let _guard = rt.enter();
    bench_config();

    let runner = RefCell::new(rt.block_on(Runner::new(case, group)));

    bencher
        .with_inputs(|| runner.borrow().input())
        .bench_local_refs(|input| rt.block_on(runner.borrow_mut().request(input)));
}

#[divan::bench(args = CASES, sample_count = 1000)]
#[inline(never)]
fn simple_cold(bencher: divan::Bencher, case: &Case) {
    bench(bencher, case, &SIMPLE_COLD);
    std::hint::black_box(());
}

#[divan::bench(args = CASES, sample_count = 1000)]
#[inline(never)]
fn extended_cold(bencher: divan::Bencher, case: &Case) {
    bench(bencher, case, &EXTENDED_COLD);
    std::hint::black_box(());
}

#[divan::bench(args = CASES, sample_count = 1000)]
#[inline(never)]
fn extended_cached(bencher: divan::Bencher, case: &Case) {
    bench(bencher, case, &EXTENDED_CACHED);
    std::hint::black_box(());
}

#[divan::bench(args = CASES, sample_count = 1000)]
#[inline(never)]
fn extended_evicting(bencher: divan::Bencher, case: &Case) {
    bench(bencher, case, &EXTENDED_EVICTING);
    std::hint::black_box(());
}

#[divan::bench(args = CASES, sample_count = 1000)]
#[inline(never)]
fn full_simple(bencher: divan::Bencher, case: &Case) {
    bench(bencher, case, &FULL_SIMPLE);
    std::hint::black_box(());
}

#[divan::bench(args = CASES, sample_count = 1000)]
#[inline(never)]
fn full_extended_cached(bencher: divan::Bencher, case: &Case) {
    bench(bencher, case, &FULL_EXTENDED_CACHED);
    std::hint::black_box(());
}

#[divan::bench(args = CASES, sample_count = 1000)]
#[inline(never)]
fn full_named_cached(bencher: divan::Bencher, case: &Case) {
    bench(bencher, case, &FULL_NAMED_CACHED);
    std::hint::black_box(());
}

#[test]
#[cfg(all(feature = "bench", target_os = "linux"))]
fn count_instructions() {
    const WARMUP: usize = 100;
    const ITERATIONS: usize = 1000;

    fake_server::run_on_separate_thread();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let _guard = rt.enter();

    let module = module_path!().trim_start_matches("pgdog::");

    for group in GROUPS {
        for case in CASES {
            bench_config();
            let mut runner = rt.block_on(Runner::new(case, group));

            for _ in 0..WARMUP {
                let mut input = runner.input();
                rt.block_on(runner.request(&mut input));
            }

            let mut counter = instructions::Instructions::new().unwrap();
            let mut counts = Vec::with_capacity(ITERATIONS);
            let mut previous = 0;

            for _ in 0..ITERATIONS {
                let mut input = runner.input();

                counter.enable().unwrap();
                rt.block_on(runner.request(&mut input));
                counter.disable().unwrap();

                let total = counter.read().unwrap();
                counts.push(total - previous);
                previous = total;
            }

            counts.sort_unstable();

            println!(
                "instructions {module}::{}::{case} {}",
                group.name,
                counts[ITERATIONS / 2]
            );
        }
    }
}

#[tokio::test]
async fn test_bench_groups() {
    for group in GROUPS {
        for case in CASES {
            bench_config();
            let mut runner = Runner::new(case, group).await;
            let mut input = runner.input();
            let ok = Box::pin(runner.request(&mut input)).await;
            assert!(ok, "{}::{case}: error", group.name);
        }
    }
}

#[cfg(not(feature = "bench"))]
#[test]
fn test_bench_memory_steady() {
    const WARMUP: usize = 100;
    const REQUESTS: usize = 500;
    const MAX_RETAINED_BYTES: isize = 4 * 1024;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let _guard = rt.enter();

    for group in [&FULL_EXTENDED_CACHED, &FULL_NAMED_CACHED] {
        for case in CASES {
            bench_config();
            let mut runner = rt.block_on(Runner::new(case, group));
            let mut run = |requests| {
                for _ in 0..requests {
                    let mut input = runner.input();
                    rt.block_on(runner.request(&mut input));
                }
            };

            run(WARMUP);
            let region = stats_alloc::Region::new(crate::GLOBAL);

            run(REQUESTS);
            let change = region.change();
            let retained = change.bytes_allocated as isize - change.bytes_deallocated as isize;

            assert!(
                retained <= MAX_RETAINED_BYTES,
                "{}::{case}: {retained} bytes still allocated after {REQUESTS} requests",
                group.name
            );
        }
    }
}
