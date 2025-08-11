use super::{super::Shard, *};

use crate::backend::Cluster;
use crate::config::ReadWriteStrategy;
use crate::frontend::{
    logical_transaction::LogicalTransaction, Buffer, PreparedStatements, RouterContext,
};
use crate::net::messages::Query;
use crate::net::Parameters;
use crate::net::{
    messages::{parse::Parse, Parameter},
    Close, Format, Sync,
};

macro_rules! command {
    ($query:expr) => {{
        let query = $query;
        let mut query_parser = QueryParser::default();
        let buffer = Buffer::from(vec![Query::new(query).into()]);
        let cluster = Cluster::new_test();
        let mut stmt = PreparedStatements::default();
        let params = Parameters::default();
        let logical_transaction = LogicalTransaction::new();
        let context =
            RouterContext::new(&buffer, &cluster, &mut stmt, &params, &logical_transaction)
                .unwrap();
        let command = query_parser.parse(context).unwrap().clone();

        (command, query_parser)
    }};
}

macro_rules! query {
    ($query:expr) => {{
        let query = $query;
        let (command, _) = command!(query);

        match command {
            Command::Query(query) => query,

            _ => panic!("should be a query"),
        }
    }};
}

macro_rules! query_parser {
    ($qp:expr, $query:expr, $in_transaction:expr, $cluster:expr) => {{
        let cluster = $cluster;
        let mut prep_stmts = PreparedStatements::default();
        let params = Parameters::default();
        let buffer: Buffer = vec![$query.into()].into();
        let mut logical_transaction = LogicalTransaction::new();

        if $in_transaction {
            logical_transaction.soft_begin().unwrap();
        }

        let router_context = RouterContext::new(
            &buffer,
            &cluster,
            &mut prep_stmts,
            &params,
            &logical_transaction,
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
                data: p.to_vec(),
            })
            .collect::<Vec<_>>();
        let logical_transaction = LogicalTransaction::new();
        let bind = Bind::new_params_codes($name, &params, $codes);
        let route = QueryParser::default()
            .parse(
                RouterContext::new(
                    &Buffer::from(vec![parse.into(), bind.into()]),
                    &Cluster::new_test(),
                    &mut PreparedStatements::default(),
                    &Parameters::default(),
                    &logical_transaction,
                )
                .unwrap(),
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

#[test]
fn test_omni() {
    let q = "SELECT sharded_omni.* FROM sharded_omni WHERE sharded_omni.id = $1";
    let route = query!(q);
    assert!(matches!(route.shard(), Shard::Direct(_)));
    let (_, _qp) = command!(q);
}

#[test]
fn test_set() {
    let route = query!(r#"SET "pgdog.shard" TO 1"#);
    assert_eq!(route.shard(), &Shard::Direct(1));
    let (_, _qp) = command!(r#"SET "pgdog.shard" TO 1"#);

    let route = query!(r#"SET "pgdog.sharding_key" TO '11'"#);
    assert_eq!(route.shard(), &Shard::Direct(1));
    let (_, _qp) = command!(r#"SET "pgdog.sharding_key" TO '11'"#);

    for (command, _qp) in [
        command!("SET TimeZone TO 'UTC'"),
        command!("SET TIME ZONE 'UTC'"),
    ] {
        match command {
            Command::Set { name, value } => {
                assert_eq!(name, "timezone");
                assert_eq!(value, ParameterValue::from("UTC"));
            }
            _ => panic!("not a set"),
        };
    }

    let (command, _qp) = command!("SET statement_timeout TO 3000");
    match command {
        Command::Set { name, value } => {
            assert_eq!(name, "statement_timeout");
            assert_eq!(value, ParameterValue::from("3000"));
        }
        _ => panic!("not a set"),
    };

    // TODO: user shouldn't be able to set these.
    // The server will report an error on synchronization.
    let (command, _qp) = command!("SET is_superuser TO true");
    match command {
        Command::Set { name, value } => {
            assert_eq!(name, "is_superuser");
            assert_eq!(value, ParameterValue::from("true"));
        }
        _ => panic!("not a set"),
    };

    let (_, mut qp) = command!("BEGIN");
    assert!(qp.write_override);
    let command = query_parser!(qp, Query::new(r#"SET statement_timeout TO 3000"#), true);
    match command {
        Command::Query(q) => assert!(q.is_write()),
        _ => panic!("set should trigger binding"),
    }

    let (command, _) = command!("SET search_path TO \"$user\", public, \"APPLES\"");
    match command {
        Command::Set { name, value } => {
            assert_eq!(name, "search_path");
            assert_eq!(
                value,
                ParameterValue::Tuple(vec!["$user".into(), "public".into(), "APPLES".into()])
            )
        }
        _ => panic!("search path"),
    }

    let buffer: Buffer = vec![Query::new(r#"SET statement_timeout TO 1"#).into()].into();
    let cluster = Cluster::new_test();
    let mut prep_stmts = PreparedStatements::default();
    let params = Parameters::default();

    let mut logical_transaction = LogicalTransaction::new();
    logical_transaction.soft_begin().unwrap();

    let router_context = RouterContext::new(
        &buffer,
        &cluster,
        &mut prep_stmts,
        &params,
        &logical_transaction,
    )
    .unwrap();
    let mut context = QueryParserContext::new(router_context);

    for read_only in [true, false] {
        context.read_only = read_only;
        // Overriding context above.
        let mut qp = QueryParser::default();
        let route = qp.query(&mut context).unwrap();

        match route {
            Command::Query(route) => {
                assert_eq!(route.is_read(), read_only);
            }
            cmd => panic!("not a query: {:?}", cmd),
        }
    }
}

#[test]
fn test_transaction() {
    let (command, mut qp) = command!("BEGIN");
    match command {
        Command::StartTransaction(q) => assert_eq!(q.query(), "BEGIN"),
        _ => panic!("not a query"),
    };

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
    assert!(matches!(
        command,
        Command::StartTransaction(BufferedQuery::Query(_))
    ));

    let route = query_parser!(
        qp,
        Query::new("SET application_name TO 'test'"),
        true,
        cluster.clone()
    );
    match route {
        Command::Query(q) => {
            assert!(q.is_write());
            assert!(!cluster.read_only());
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
fn test_begin_extended() {
    let command = query_parser!(QueryParser::default(), Parse::new_anonymous("BEGIN"), false);
    assert!(matches!(command, Command::Query(_)));
}

#[test]
fn test_show_shards() {
    let (cmd, _qp) = command!("SHOW pgdog.shards");
    assert!(matches!(cmd, Command::Shards(2)));
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
    assert!(matches!(cmd, Command::StartTransaction(_)));
    let cluster = Cluster::new_test();
    let mut prep_stmts = PreparedStatements::default();
    let params = Parameters::default();

    let mut logical_transaction = LogicalTransaction::new();
    logical_transaction.soft_begin().unwrap();

    let buffer: Buffer = vec![Query::new(
        "SELECT
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
	",
    )
    .into()]
    .into();

    let router_context = RouterContext::new(
        &buffer,
        &cluster,
        &mut prep_stmts,
        &params,
        &logical_transaction,
    )
    .unwrap();

    let mut context = QueryParserContext::new(router_context);
    let route = qp.query(&mut context).unwrap();
    match route {
        Command::Query(query) => assert!(query.is_write()),
        _ => panic!("not a select"),
    }
}

#[test]
fn test_comment() {
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
        Command::Query(query) => assert_eq!(query.shard(), &Shard::All),
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

    let buf: Buffer = vec![Close::named("test").into(), Sync.into()].into();
    let mut pp = PreparedStatements::default();
    let params = Parameters::default();
    let logical_transaction = LogicalTransaction::new();

    let context =
        RouterContext::new(&buf, &cluster, &mut pp, &params, &logical_transaction).unwrap();

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
