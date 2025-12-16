use std::{
    ops::Deref,
    sync::{Mutex, MutexGuard},
};

use crate::{
    config::{self, config, ConfigAndUsers, RewriteMode},
    net::{
        messages::{parse::Parse, Parameter},
        Close, Format, Sync,
    },
};
use bytes::Bytes;

use super::{super::Shard, *};
use crate::backend::Cluster;
use crate::config::ReadWriteStrategy;
use crate::frontend::{
    client::{Sticky, TransactionType},
    router::Ast,
    BufferedQuery, ClientRequest, PreparedStatements, RouterContext,
};
use crate::net::messages::Query;

struct ConfigModeGuard {
    original: ConfigAndUsers,
}

impl ConfigModeGuard {
    fn set(mode: RewriteMode) -> Self {
        let original = config().deref().clone();
        let mut updated = original.clone();
        updated.config.rewrite.shard_key = mode;
        updated.config.rewrite.enabled = true;
        config::set(updated).unwrap();
        Self { original }
    }
}

impl Drop for ConfigModeGuard {
    fn drop(&mut self) {
        config::set(self.original.clone()).unwrap();
    }
}

static CONFIG_MODE_LOCK: Mutex<()> = Mutex::new(());

fn lock_config_mode() -> MutexGuard<'static, ()> {
    CONFIG_MODE_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn parse_query(query: &str) -> Command {
    let mut query_parser = QueryParser::default();
    let cluster = Cluster::new_test();
    let ast = Ast::new(
        &BufferedQuery::Query(Query::new(query)),
        &cluster.sharding_schema(),
        &mut PreparedStatements::default(),
    )
    .unwrap();
    let mut client_request = ClientRequest::from(vec![Query::new(query).into()]);
    client_request.ast = Some(ast);

    let context =
        RouterContext::new(&client_request, &cluster, None, None, Sticky::new_test()).unwrap();
    let command = query_parser.parse(context).unwrap().clone();
    command
}

macro_rules! command {
    ($query:expr) => {{
        command!($query, false)
    }};

    ($query:expr, $in_transaction:expr) => {{
        let query = $query;
        let mut query_parser = QueryParser::default();
        let cluster = Cluster::new_test();
        let mut ast = Ast::new(
            &BufferedQuery::Query(Query::new($query)),
            &cluster.sharding_schema(),
            &mut PreparedStatements::default(),
        )
        .unwrap();
        ast.cached = false; // Simple protocol queries are not cached
        let mut client_request = ClientRequest::from(vec![Query::new(query).into()]);
        client_request.ast = Some(ast);
        let transaction = if $in_transaction {
            Some(TransactionType::ReadWrite)
        } else {
            None
        };
        let context =
            RouterContext::new(&client_request, &cluster, None, transaction, Sticky::new())
                .unwrap();
        let command = query_parser.parse(context).unwrap().clone();

        (command, query_parser)
    }};
}

macro_rules! query {
    ($query:expr) => {{
        query!($query, false)
    }};

    ($query:expr, $in_transaction:expr) => {{
        let query = $query;
        let (command, _) = command!(query, $in_transaction);

        match command {
            Command::Query(query) => query,

            _ => panic!("should be a query"),
        }
    }};
}

macro_rules! query_parser {
    ($qp:expr, $query:expr, $in_transaction:expr, $cluster:expr) => {{
        let cluster = $cluster;
        let mut client_request: ClientRequest = vec![$query.into()].into();

        let buffered_query = client_request
            .query()
            .expect("parsing query")
            .expect("no query in client request");

        let mut prep_stmts = PreparedStatements::default();

        let mut ast =
            Ast::new(&buffered_query, &cluster.sharding_schema(), &mut prep_stmts).unwrap();
        ast.cached = false; // Dry run test needs this.
        client_request.ast = Some(ast);

        let maybe_transaction = if $in_transaction {
            Some(TransactionType::ReadWrite)
        } else {
            None
        };

        let router_context = RouterContext::new(
            &client_request,
            &cluster,
            None,
            maybe_transaction,
            Sticky::new(),
        )
        .unwrap();

        $qp.parse(router_context).unwrap()
    }};

    ($qp:expr, $query:expr, $in_transaction:expr) => {
        query_parser!($qp, $query, $in_transaction, Cluster::new_test())
    };
}

macro_rules! parse {
    ($query: expr, $params: expr) => {
        parse!("", $query, $params)
    };

    ($name:expr, $query:expr, $params:expr, $codes:expr) => {{
        let parse = Parse::named($name, $query);
        let params = $params
            .into_iter()
            .map(|p| Parameter {
                len: p.len() as i32,
                data: Bytes::copy_from_slice(&p),
            })
            .collect::<Vec<_>>();
        let bind = Bind::new_params_codes($name, &params, $codes);
        let cluster = Cluster::new_test();
        let ast = Ast::new(
            &BufferedQuery::Prepared(Parse::new_anonymous($query)),
            &cluster.sharding_schema(),
            &mut PreparedStatements::default(),
        )
        .unwrap();
        let mut client_request = ClientRequest::from(vec![parse.into(), bind.into()]);
        client_request.ast = Some(ast);
        let route = QueryParser::default()
            .parse(
                RouterContext::new(&client_request, &cluster, None, None, Sticky::new()).unwrap(),
            )
            .unwrap()
            .clone();

        match route {
            Command::Query(query) => query,

            _ => panic!("should be a query"),
        }
    }};

    ($name:expr, $query:expr, $params: expr) => {
        parse!($name, $query, $params, &[])
    };
}

fn parse_with_parameters(query: &str) -> Result<Command, Error> {
    let cluster = Cluster::new_test();
    let mut ast = Ast::new(
        &BufferedQuery::Query(Query::new(query)),
        &cluster.sharding_schema(),
        &mut PreparedStatements::default(),
    )
    .unwrap();
    ast.cached = false; // Simple protocol queries are not cached
    let mut client_request: ClientRequest = vec![Query::new(query).into()].into();
    client_request.ast = Some(ast);
    let router_context =
        RouterContext::new(&client_request, &cluster, None, None, Sticky::new()).unwrap();
    QueryParser::default().parse(router_context)
}

fn parse_with_bind(query: &str, params: &[&[u8]]) -> Result<Command, Error> {
    let cluster = Cluster::new_test();
    let parse = Parse::new_anonymous(query);
    let params = params
        .iter()
        .map(|value| Parameter::new(value))
        .collect::<Vec<_>>();
    let bind = crate::net::messages::Bind::new_params("", &params);
    let ast = Ast::new(
        &BufferedQuery::Prepared(Parse::new_anonymous(query)),
        &cluster.sharding_schema(),
        &mut PreparedStatements::default(),
    )
    .unwrap();
    let mut client_request: ClientRequest = vec![parse.into(), bind.into()].into();
    client_request.ast = Some(ast);
    let router_context =
        RouterContext::new(&client_request, &cluster, None, None, Sticky::new()).unwrap();

    QueryParser::default().parse(router_context)
}

#[test]
fn test_insert() {
    let route = parse!(
        "INSERT INTO sharded (id, email) VALUES ($1, $2)",
        ["11".as_bytes(), "test@test.com".as_bytes()]
    );
    assert_eq!(route.shard(), &Shard::direct(1));
}

#[test]
fn test_order_by_vector() {
    let route = query!("SELECT * FROM embeddings ORDER BY embedding <-> '[1,2,3]'");
    let order_by = route.order_by().first().unwrap();
    assert!(order_by.asc());
    assert_eq!(
        order_by.vector().unwrap(),
        (
            &Vector::from(&[1.0, 2.0, 3.0][..]),
            &std::string::String::from("embedding")
        ),
    );

    let route = parse!(
        "SELECT * FROM embeddings ORDER BY embedding  <-> $1",
        ["[4.0,5.0,6.0]".as_bytes()]
    );
    let order_by = route.order_by().first().unwrap();
    assert!(order_by.asc());
    assert_eq!(
        order_by.vector().unwrap(),
        (
            &Vector::from(&[4.0, 5.0, 6.0][..]),
            &std::string::String::from("embedding")
        )
    );
}

#[test]
fn test_parse_with_cast() {
    let route = parse!(
        "test",
        r#"SELECT sharded.id, sharded.value
    FROM sharded
    WHERE sharded.id = $1::INTEGER ORDER BY sharded.id"#,
        [[0, 0, 0, 1]],
        &[Format::Binary]
    );
    assert!(route.is_read());
    assert_eq!(route.shard(), &Shard::Direct(0))
}

#[test]
fn test_select_for_update() {
    let route = query!("SELECT * FROM sharded WHERE id = $1 FOR UPDATE");
    assert!(route.is_write());
    assert!(matches!(route.shard(), Shard::All));
    let route = parse!(
        "SELECT * FROM sharded WHERE id = $1 FOR UPDATE",
        ["1".as_bytes()]
    );
    assert!(matches!(route.shard(), Shard::Direct(_)));
    assert!(route.is_write());
}

// #[test]
// fn test_prepared_avg_rewrite_plan() {
//     let route = parse!(
//         "avg_test",
//         "SELECT AVG(price) FROM menu",
//         Vec::<Vec<u8>>::new()
//     );

//     assert!(!route.rewrite_plan().is_noop());
//     assert_eq!(route.rewrite_plan().drop_columns(), &[1]);
//     let rewritten = route
//         .rewritten_sql()
//         .expect("rewrite should produce SQL for prepared average");
//     assert!(
//         rewritten.to_lowercase().contains("count"),
//         "helper COUNT should be injected"
//     );
// }

// #[test]
// fn test_prepared_stddev_rewrite_plan() {
//     let route = parse!(
//         "stddev_test",
//         "SELECT STDDEV(price) FROM menu",
//         Vec::<Vec<u8>>::new()
//     );

//     assert!(!route.rewrite_plan().is_noop());
//     assert_eq!(route.rewrite_plan().drop_columns(), &[1, 2, 3]);
//     let helpers = route.rewrite_plan().helpers();
//     assert_eq!(helpers.len(), 3);
//     let kinds: Vec<HelperKind> = helpers.iter().map(|h| h.kind).collect();
//     assert!(kinds.contains(&HelperKind::Count));
//     assert!(kinds.contains(&HelperKind::Sum));
//     assert!(kinds.contains(&HelperKind::SumSquares));

//     let rewritten = route
//         .rewritten_sql()
//         .expect("rewrite should produce SQL for prepared stddev");
//     assert!(rewritten.to_lowercase().contains("sum"));
//     assert!(rewritten.to_lowercase().contains("count"));
// }

#[test]
fn test_omni() {
    let mut omni_round_robin = HashSet::new();
    let q = "SELECT sharded_omni.* FROM sharded_omni WHERE sharded_omni.id = 1";

    for _ in 0..10 {
        let command = parse_query(q);
        match command {
            Command::Query(query) => {
                assert!(matches!(query.shard(), Shard::Direct(_)));
                omni_round_robin.insert(query.shard().clone());
            }

            _ => {}
        }
    }

    assert_eq!(omni_round_robin.len(), 2);

    // Test sticky routing
    let mut omni_sticky = HashSet::new();
    let q =
        "SELECT sharded_omni_sticky.* FROM sharded_omni_sticky WHERE sharded_omni_sticky.id = $1";

    for _ in 0..10 {
        let command = parse_query(q);
        match command {
            Command::Query(query) => {
                assert!(matches!(query.shard(), Shard::Direct(_)));
                omni_sticky.insert(query.shard().clone());
            }

            _ => {}
        }
    }

    assert_eq!(omni_sticky.len(), 1);

    // Test that sharded tables take priority.
    let q = "
        SELECT
            sharded_omni.*,
            sharded.*
        FROM
            sharded_omni
        INNER JOIN
            sharded
        ON sharded_omni.id = sharded.i
    WHERE sharded.id = 5";

    let route = query!(q);
    let shard = route.shard().clone();

    for _ in 0..5 {
        let route = query!(q);
        // Test that shard doesn't change (i.e. not round robin)
        assert_eq!(&shard, route.shard());
        assert!(matches!(shard, Shard::Direct(_)));
    }

    // Test that all tables have to be omnisharded.
    let q = "SELECT * FROM sharded_omni INNER JOIN not_sharded ON sharded_omni.id = not_sharded.id WHERE sharded_omni = $1";
    let route = query!(q);
    assert!(matches!(route.shard(), Shard::All));
}

#[test]
fn test_set() {
    let (command, _) = command!(r#"SET "pgdog.shard" TO 1"#, true);
    match command {
        Command::SetRoute(route) => assert_eq!(route.shard(), &Shard::Direct(1)),
        _ => panic!("not a set route"),
    }
    let (_, qp) = command!(r#"SET "pgdog.shard" TO 1"#, true);
    assert!(qp.in_transaction);

    let (command, _) = command!(r#"SET "pgdog.sharding_key" TO '11'"#, true);
    match command {
        Command::SetRoute(route) => assert_eq!(route.shard(), &Shard::Direct(1)),
        _ => panic!("not a set route"),
    }
    let (_, qp) = command!(r#"SET "pgdog.sharding_key" TO '11'"#, true);
    assert!(qp.in_transaction);

    for (command, qp) in [
        command!("SET TimeZone TO 'UTC'"),
        command!("SET TIME ZONE 'UTC'"),
    ] {
        match command {
            Command::Set { name, value, .. } => {
                assert_eq!(name, "timezone");
                assert_eq!(value, ParameterValue::from("UTC"));
            }
            _ => panic!("not a set"),
        };
        assert!(!qp.in_transaction);
    }

    let (command, qp) = command!("SET statement_timeout TO 3000");
    match command {
        Command::Set { name, value, .. } => {
            assert_eq!(name, "statement_timeout");
            assert_eq!(value, ParameterValue::from("3000"));
        }
        _ => panic!("not a set"),
    };
    assert!(!qp.in_transaction);

    // TODO: user shouldn't be able to set these.
    // The server will report an error on synchronization.
    let (command, qp) = command!("SET is_superuser TO true");
    match command {
        Command::Set { name, value, .. } => {
            assert_eq!(name, "is_superuser");
            assert_eq!(value, ParameterValue::from("true"));
        }
        _ => panic!("not a set"),
    };
    assert!(!qp.in_transaction);

    let (_, mut qp) = command!("BEGIN");
    assert!(qp.write_override);
    let command = query_parser!(qp, Query::new(r#"SET statement_timeout TO 3000"#), true);
    assert!(
        matches!(command, Command::Set { .. }),
        "set must be intercepted inside transactions"
    );

    let (command, _) = command!("SET search_path TO \"$user\", public, \"APPLES\"");
    match command {
        Command::Set { name, value, .. } => {
            assert_eq!(name, "search_path");
            assert_eq!(
                value,
                ParameterValue::Tuple(vec!["$user".into(), "public".into(), "APPLES".into()])
            )
        }
        _ => panic!("search path"),
    }

    let query_str = r#"SET statement_timeout TO 1"#;
    let cluster = Cluster::new_test();
    let mut prep_stmts = PreparedStatements::default();
    let buffered_query = BufferedQuery::Query(Query::new(query_str));
    let mut ast = Ast::new(&buffered_query, &cluster.sharding_schema(), &mut prep_stmts).unwrap();
    ast.cached = false;
    let mut buffer: ClientRequest = vec![Query::new(query_str).into()].into();
    buffer.ast = Some(ast);
    let transaction = Some(TransactionType::ReadWrite);
    let router_context =
        RouterContext::new(&buffer, &cluster, None, transaction, Sticky::new()).unwrap();
    let mut context = QueryParserContext::new(router_context);

    for read_only in [true, false] {
        context.read_only = read_only;
        // Overriding context above.
        let mut qp = QueryParser::default();
        qp.in_transaction = true;
        let route = qp.query(&mut context).unwrap();

        match route {
            Command::Set { .. } => {}
            _ => panic!("set must be intercepted"),
        }
    }

    let command = command!("SET LOCAL work_mem TO 1");
    match command.0 {
        Command::Set { local, .. } => assert!(local),
        _ => panic!("not a set"),
    }
}

#[test]
fn test_transaction() {
    let (command, mut qp) = command!("BEGIN");
    match command {
        Command::StartTransaction { query: q, .. } => assert_eq!(q.query(), "BEGIN"),
        _ => panic!("not a query"),
    };

    assert!(qp.in_transaction);
    assert!(qp.write_override);

    let route = query_parser!(qp, Parse::named("test", "SELECT $1"), true);
    match route {
        Command::Query(q) => assert!(q.is_write()),
        _ => panic!("not a select"),
    }

    let mut cluster = Cluster::new_test();
    cluster.set_read_write_strategy(ReadWriteStrategy::Aggressive);
    let command = query_parser!(
        QueryParser::default(),
        Query::new("BEGIN"),
        true,
        cluster.clone()
    );
    assert!(matches!(command, Command::StartTransaction { .. }));
    assert!(qp.in_transaction);

    qp.in_transaction = true;
    let route = query_parser!(
        qp,
        Query::new("SET application_name TO 'test'"),
        true,
        cluster.clone()
    );
    match route {
        Command::Set {
            name,
            value,
            extended,
            route,
            local,
        } => {
            assert!(!extended);
            assert_eq!(name, "application_name");
            assert_eq!(value.as_str().unwrap(), "test");
            assert!(!cluster.read_only());
            assert_eq!(route.shard(), &Shard::All);
            assert!(!local);
        }

        _ => panic!("not a query"),
    }
}

#[test]
fn test_insert_do_update() {
    let route = query!("INSERT INTO foo (id) VALUES ($1::UUID) ON CONFLICT (id) DO UPDATE SET id = excluded.id RETURNING id");
    assert!(route.is_write())
}

#[test]
fn update_sharding_key_errors_by_default() {
    let _lock = lock_config_mode();
    let _guard = ConfigModeGuard::set(RewriteMode::Error);

    let query = "UPDATE sharded SET id = id + 1 WHERE id = 1";
    let cluster = Cluster::new_test();
    let mut prep_stmts = PreparedStatements::default();
    let buffered_query = BufferedQuery::Query(Query::new(query));
    let mut ast = Ast::new(&buffered_query, &cluster.sharding_schema(), &mut prep_stmts).unwrap();
    ast.cached = false;
    let mut client_request: ClientRequest = vec![Query::new(query).into()].into();
    client_request.ast = Some(ast);
    let router_context =
        RouterContext::new(&client_request, &cluster, None, None, Sticky::new()).unwrap();

    let result = QueryParser::default().parse(router_context);
    assert!(
        matches!(result, Err(Error::ShardKeyUpdateViolation { .. })),
        "{result:?}"
    );
}

#[test]
fn update_sharding_key_ignore_mode_allows() {
    let _lock = lock_config_mode();
    let _guard = ConfigModeGuard::set(RewriteMode::Ignore);

    let query = "UPDATE sharded SET id = id + 1 WHERE id = 1";
    let cluster = Cluster::new_test();
    let mut prep_stmts = PreparedStatements::default();
    let buffered_query = BufferedQuery::Query(Query::new(query));
    let mut ast = Ast::new(&buffered_query, &cluster.sharding_schema(), &mut prep_stmts).unwrap();
    ast.cached = false;
    let mut client_request: ClientRequest = vec![Query::new(query).into()].into();
    client_request.ast = Some(ast);
    let router_context =
        RouterContext::new(&client_request, &cluster, None, None, Sticky::new()).unwrap();

    let command = QueryParser::default().parse(router_context).unwrap();
    assert!(matches!(command, Command::Query(_)));
}

#[test]
fn update_sharding_key_rewrite_mode_not_supported() {
    let _lock = lock_config_mode();
    let _guard = ConfigModeGuard::set(RewriteMode::Rewrite);

    let query = "UPDATE sharded SET id = id + 1 WHERE id = 1";
    let cluster = Cluster::new_test();
    let mut prep_stmts = PreparedStatements::default();
    let buffered_query = BufferedQuery::Query(Query::new(query));
    let mut ast = Ast::new(&buffered_query, &cluster.sharding_schema(), &mut prep_stmts).unwrap();
    ast.cached = false;
    let mut client_request: ClientRequest = vec![Query::new(query).into()].into();
    client_request.ast = Some(ast);
    let router_context =
        RouterContext::new(&client_request, &cluster, None, None, Sticky::new()).unwrap();

    let result = QueryParser::default().parse(router_context);
    assert!(
        matches!(result, Err(Error::ShardKeyRewriteNotSupported { .. })),
        "{result:?}"
    );
}

#[test]
fn update_sharding_key_rewrite_plan_detected() {
    let _lock = lock_config_mode();
    let _guard = ConfigModeGuard::set(RewriteMode::Rewrite);

    let query = "UPDATE sharded SET id = 11 WHERE id = 1";
    let cluster = Cluster::new_test();
    let mut prep_stmts = PreparedStatements::default();
    let buffered_query = BufferedQuery::Query(Query::new(query));
    let mut ast = Ast::new(&buffered_query, &cluster.sharding_schema(), &mut prep_stmts).unwrap();
    ast.cached = false;
    let mut client_request: ClientRequest = vec![Query::new(query).into()].into();
    client_request.ast = Some(ast);
    let router_context =
        RouterContext::new(&client_request, &cluster, None, None, Sticky::new()).unwrap();

    let command = QueryParser::default().parse(router_context).unwrap();
    match command {
        Command::ShardKeyRewrite(plan) => {
            assert_eq!(plan.table().name, "sharded");
            assert_eq!(plan.assignments().len(), 1);
            let assignment = &plan.assignments()[0];
            assert_eq!(assignment.column(), "id");
            assert!(matches!(assignment.value(), AssignmentValue::Integer(11)));
        }
        other => panic!("expected shard key rewrite plan, got {other:?}"),
    }
}

#[test]
fn update_sharding_key_rewrite_computes_new_shard() {
    let _lock = lock_config_mode();
    let _guard = ConfigModeGuard::set(RewriteMode::Rewrite);

    let command =
        parse_with_parameters("UPDATE sharded SET id = 11 WHERE id = 1").expect("expected command");

    let plan = match command {
        Command::ShardKeyRewrite(plan) => plan,
        other => panic!("expected shard key rewrite plan, got {other:?}"),
    };

    assert!(plan.new_shard().is_some(), "new shard should be computed");
}

#[test]
fn update_sharding_key_rewrite_requires_parameter_values() {
    let _lock = lock_config_mode();
    let _guard = ConfigModeGuard::set(RewriteMode::Rewrite);

    let result = parse_with_parameters("UPDATE sharded SET id = $1 WHERE id = 1");

    assert!(
        matches!(result, Err(Error::MissingParameter(1))),
        "{result:?}"
    );
}

#[test]
fn update_sharding_key_rewrite_parameter_assignment_succeeds() {
    let _lock = lock_config_mode();
    let _guard = ConfigModeGuard::set(RewriteMode::Rewrite);

    let command = parse_with_bind("UPDATE sharded SET id = $1 WHERE id = 1", &[b"11"])
        .expect("expected rewrite command");

    match command {
        Command::ShardKeyRewrite(plan) => {
            assert!(
                plan.new_shard().is_some(),
                "expected computed destination shard"
            );
            assert_eq!(plan.assignments().len(), 1);
            assert!(matches!(
                plan.assignments()[0].value(),
                AssignmentValue::Parameter(1)
            ));
        }
        other => panic!("expected shard key rewrite plan, got {other:?}"),
    }
}

#[test]
fn update_sharding_key_rewrite_self_assignment_falls_back() {
    let _lock = lock_config_mode();
    let _guard = ConfigModeGuard::set(RewriteMode::Rewrite);

    let command =
        parse_with_parameters("UPDATE sharded SET id = id WHERE id = 1").expect("expected command");

    match command {
        Command::Query(route) => {
            assert!(matches!(route.shard(), Shard::Direct(_)));
        }
        other => panic!("expected standard update route, got {other:?}"),
    }
}

#[test]
fn update_sharding_key_rewrite_null_assignment_not_supported() {
    let _lock = lock_config_mode();
    let _guard = ConfigModeGuard::set(RewriteMode::Rewrite);

    let result = parse_with_parameters("UPDATE sharded SET id = NULL WHERE id = 1");

    assert!(
        matches!(result, Err(Error::ShardKeyRewriteNotSupported { .. })),
        "{result:?}"
    );
}

#[test]
fn test_begin_extended() {
    let command = query_parser!(QueryParser::default(), Parse::new_anonymous("BEGIN"), false);
    match command {
        Command::StartTransaction { extended, .. } => assert!(extended),
        _ => panic!("not a transaction"),
    }
}

#[test]
fn test_show_shards() {
    let (cmd, qp) = command!("SHOW pgdog.shards");
    assert!(matches!(cmd, Command::InternalField { .. }));
    assert!(!qp.in_transaction);
}

#[test]
fn test_write_functions() {
    let route = query!("SELECT pg_advisory_lock($1)");
    assert!(route.is_write());
    assert!(route.lock_session());
}

#[test]
fn test_write_nolock() {
    let route = query!("SELECT nextval('234')");
    assert!(route.is_write());
    assert!(!route.lock_session());
}

#[test]
fn test_cte() {
    let route = query!("WITH s AS (SELECT 1) SELECT 2");
    assert!(route.is_read());

    let route = query!("WITH s AS (SELECT 1), s2 AS (INSERT INTO test VALUES ($1) RETURNING *), s3 AS (SELECT 123) SELECT * FROM s");
    assert!(route.is_write());
}

#[test]
fn test_function_begin() {
    let (cmd, mut qp) = command!("BEGIN");
    assert!(matches!(cmd, Command::StartTransaction { .. }));
    assert!(qp.in_transaction);
    let cluster = Cluster::new_test();
    let mut prep_stmts = PreparedStatements::default();
    let query_str = "SELECT
	ROW(t1.*) AS tt1,
	ROW(t2.*) AS tt2
FROM t1
LEFT JOIN t2 ON t1.id = t2.t1_id
WHERE t2.account = (
	SELECT
		account
	FROM
		t2
	WHERE
		t2.id = $1
	)
	";
    let buffered_query = BufferedQuery::Query(Query::new(query_str));
    let mut ast = Ast::new(&buffered_query, &cluster.sharding_schema(), &mut prep_stmts).unwrap();
    ast.cached = false;
    let mut buffer: ClientRequest = vec![Query::new(query_str).into()].into();
    buffer.ast = Some(ast);
    let transaction = Some(TransactionType::ReadWrite);
    let router_context =
        RouterContext::new(&buffer, &cluster, None, transaction, Sticky::new()).unwrap();
    let mut context = QueryParserContext::new(router_context);
    let route = qp.query(&mut context).unwrap();
    match route {
        Command::Query(query) => assert!(query.is_write()),
        _ => panic!("not a select"),
    }
    assert!(qp.in_transaction);
}

#[test]
fn test_comment() {
    let query = "/* pgdog_role: primary */ SELECT 1";
    let route = query!(query);
    assert!(route.is_write());

    let query = "/* pgdog_shard: 1234 */ SELECT 1234";
    let route = query!(query);
    assert_eq!(route.shard(), &Shard::Direct(1234));

    // Comment is ignored.
    let command = query_parser!(
        QueryParser::default(),
        Parse::named(
            "test",
            "/* pgdog_shard: 1234 */ SELECT * FROM sharded WHERE id = $1"
        ),
        false
    );

    match command {
        Command::Query(query) => assert_eq!(query.shard(), &Shard::Direct(1234)),
        _ => panic!("not a query"),
    }
}

#[test]
fn test_limit_offset() {
    let route = query!("SELECT * FROM users LIMIT 25 OFFSET 5");
    assert_eq!(route.limit().offset, Some(5));
    assert_eq!(route.limit().limit, Some(25));

    let cmd = parse!(
        "SELECT * FROM users LIMIT $1 OFFSET $2",
        &["1".as_bytes(), "25".as_bytes(),]
    );

    assert_eq!(cmd.limit().limit, Some(1));
    assert_eq!(cmd.limit().offset, Some(25));
}

#[test]
fn test_close_direct_one_shard() {
    let cluster = Cluster::new_test_single_shard();
    let mut qp = QueryParser::default();

    let buf: ClientRequest = vec![Close::named("test").into(), Sync.into()].into();
    let transaction = None;

    let context = RouterContext::new(&buf, &cluster, None, transaction, Sticky::new()).unwrap();

    let cmd = qp.parse(context).unwrap();

    match cmd {
        Command::Query(route) => assert_eq!(route.shard(), &Shard::Direct(0)),
        _ => panic!("not a query"),
    }
}

#[test]
fn test_distinct() {
    let route = query!("SELECT DISTINCT * FROM users");
    let distinct = route.distinct().as_ref().unwrap();
    assert_eq!(distinct, &DistinctBy::Row);

    let route = query!("SELECT DISTINCT ON(1, email) * FROM users");
    let distinct = route.distinct().as_ref().unwrap();
    assert_eq!(
        distinct,
        &DistinctBy::Columns(vec![
            DistinctColumn::Index(0),
            DistinctColumn::Name(std::string::String::from("email"))
        ])
    );
}

#[test]
fn test_any() {
    let route = query!("SELECT * FROM sharded WHERE id = ANY('{1, 2, 3}')");
    assert_eq!(route.shard(), &Shard::All);

    let route = parse!(
        "SELECT * FROM sharded WHERE id = ANY($1)",
        &["{1, 2, 3}".as_bytes()]
    );

    assert_eq!(route.shard(), &Shard::All);
}

#[test]
fn test_commit_prepared() {
    let stmt = pg_query::parse("COMMIT PREPARED 'test'").unwrap();
    println!("{:?}", stmt);
}

#[test]
fn test_dry_run_simple() {
    let mut config = config().deref().clone();
    config.config.general.dry_run = true;
    config::set(config).unwrap();

    let cluster = Cluster::new_test_single_shard();
    let command = query_parser!(
        QueryParser::default(),
        Query::new("/* pgdog_sharding_key: 1234 */ SELECT * FROM sharded"),
        false,
        cluster
    );
    let cache = Cache::queries();
    let stmt = cache.values().next().unwrap();
    assert_eq!(stmt.stats.lock().direct, 1);
    assert_eq!(stmt.stats.lock().multi, 0);
    assert_eq!(command.route().shard(), &Shard::Direct(0));
}

#[test]
fn test_set_comments() {
    let command = query_parser!(
        QueryParser::default(),
        Query::new("/* pgdog_sharding_key: 1234 */ SET statement_timeout TO 1"),
        true
    );
    assert_eq!(command.route().shard(), &Shard::Direct(0));

    let command = query_parser!(
        QueryParser::default(),
        Query::new("SET statement_timeout TO 1"),
        true
    );
    assert!(matches!(command, Command::Set { .. }));
}

#[test]
fn test_subqueries() {
    println!(
        "{:#?}",
        pg_query::parse(r#"
        SELECT
            count(*) AS "count"
        FROM
        (
            SELECT "companies".* FROM
            (
                 SELECT "companies".*
                 FROM "companies"
                 INNER JOIN "organizations_relevant_companies" ON
                 ("organizations_relevant_companies"."company_id" = "companies"."id")
                 WHERE
                 (
                     ("organizations_relevant_companies"."org_id" = 1)
                     AND NOT
                     (
                        EXISTS
                        (
                           SELECT * FROM "hidden_globals"
                           WHERE
                           (
                               ("hidden_globals"."org_id" = 1)
                               AND ("hidden_globals"."global_company_id" = "organizations_relevant_companies"."company_id")
                           )
                     )
                )
            )
        ) AS "companies" OFFSET 0) AS "t1" LIMIT 1"#).unwrap()
    );
}
