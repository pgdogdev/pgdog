use std::collections::HashSet;
use std::ops::Deref;

use crate::config::{self, config};
use crate::frontend::router::parser::{DistinctBy, DistinctColumn, Shard};
use crate::net::messages::Parameter;

use super::setup::*;

#[test]
fn test_order_by_vector_simple() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("SELECT * FROM embeddings ORDER BY embedding <-> '[1,2,3]'").into(),
    ]);

    let route = command.route();
    let order_by = route.order_by().first().unwrap();
    assert!(order_by.asc());
}

#[test]
fn test_order_by_vector_with_params() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Parse::named(
            "__test_order",
            "SELECT * FROM embeddings ORDER BY embedding <-> $1",
        )
        .into(),
        Bind::new_params("__test_order", &[Parameter::new(b"[4.0,5.0,6.0]")]).into(),
        Execute::new().into(),
        Sync.into(),
    ]);

    let route = command.route();
    let order_by = route.order_by().first().unwrap();
    assert!(order_by.asc());
}

#[test]
fn test_limit_offset_simple() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("SELECT * FROM users LIMIT 25 OFFSET 5").into(),
    ]);

    let route = command.route();
    assert_eq!(route.limit().offset, Some(5));
    assert_eq!(route.limit().limit, Some(25));
}

#[test]
fn test_limit_offset_with_params() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Parse::named("__test_limit", "SELECT * FROM users LIMIT $1 OFFSET $2").into(),
        Bind::new_params(
            "__test_limit",
            &[Parameter::new(b"1"), Parameter::new(b"25")],
        )
        .into(),
        Execute::new().into(),
        Sync.into(),
    ]);

    let route = command.route();
    assert_eq!(route.limit().limit, Some(1));
    assert_eq!(route.limit().offset, Some(25));
}

#[test]
fn test_limit_offset_with_bad_params() {
    let mut test = QueryParserTest::new();

    let command = test.try_execute(vec![
        Parse::named("__test_limit", "SELECT * FROM users LIMIT $1 OFFSET $2").into(),
        Bind::new_params(
            "__test_limit",
            &[Parameter::new(b"apples"), Parameter::new(b"25")],
        )
        .into(),
        Execute::new().into(),
        Sync.into(),
    ]);

    let err = command.expect_err("limit should fail");
    assert_eq!(
        err.to_string(),
        "expected parameter $1 to be an integer, got 'apples' instead"
    );

    let command = test.try_execute(vec![
        Parse::named("__test_limit", "SELECT * FROM users LIMIT $1 OFFSET $2").into(),
        Bind::new_params(
            "__test_limit",
            &[Parameter::new(b"25"), Parameter::new(b"oranges")],
        )
        .into(),
        Execute::new().into(),
        Sync.into(),
    ]);

    let err = command.expect_err("offset should fail");
    assert_eq!(
        err.to_string(),
        "expected parameter $2 to be an integer, got 'oranges' instead"
    );
}

#[test]
fn test_distinct_row() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("SELECT DISTINCT * FROM users").into()]);

    let route = command.route();
    let distinct = route.distinct().as_ref().unwrap();
    assert_eq!(distinct, &DistinctBy::Row);
}

#[test]
fn test_distinct_on_columns() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("SELECT DISTINCT ON(1, email) * FROM users").into(),
    ]);

    let route = command.route();
    let distinct = route.distinct().as_ref().unwrap();
    assert_eq!(
        distinct,
        &DistinctBy::Columns(vec![
            DistinctColumn::Index(0),
            DistinctColumn::Name(String::from("email"))
        ])
    );
}

#[test]
fn test_any_literal() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("SELECT * FROM sharded WHERE id = ANY('{1, 2, 3}')").into(),
    ]);

    assert_eq!(command.route().shard(), &Shard::All);
}

#[test]
fn test_any_with_param() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Parse::named("__test_any", "SELECT * FROM sharded WHERE id = ANY($1)").into(),
        Bind::new_params("__test_any", &[Parameter::new(b"{1, 2, 3}")]).into(),
        Execute::new().into(),
        Sync.into(),
    ]);

    assert_eq!(command.route().shard(), &Shard::All);
}

#[test]
fn test_cte_read() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("WITH s AS (SELECT 1) SELECT 2").into()]);

    assert!(command.route().is_read());
}

#[test]
fn test_cte_write() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "WITH s AS (SELECT 1), s2 AS (INSERT INTO test VALUES ($1) RETURNING *), s3 AS (SELECT 123) SELECT * FROM s",
    )
    .into()]);

    assert!(command.route().is_write());
}

#[test]
fn test_omnisharded_sticky_config_enabled() {
    let mut updated = config().deref().clone();
    updated.config.general.omnisharded_sticky = true;
    config::set(updated).unwrap();

    let mut test = QueryParserTest::new_with_config(&config());

    let mut shards_seen = HashSet::new();
    let q = "SELECT sharded_omni.* FROM sharded_omni WHERE sharded_omni.id = 1";

    for _ in 0..10 {
        let command = test.execute(vec![Query::new(q).into()]);
        assert!(matches!(command.route().shard(), Shard::Direct(_)));
        shards_seen.insert(command.route().shard().clone());
    }

    assert_eq!(
        shards_seen.len(),
        1,
        "with omnisharded_sticky=true, all queries to sharded_omni should go to the same shard"
    );

    let mut updated = config().deref().clone();
    updated.config.general.omnisharded_sticky = false;
    config::set(updated).unwrap();
}

#[test]
fn test_omnisharded_sticky_config_disabled() {
    let mut updated = config().deref().clone();
    updated.config.general.omnisharded_sticky = false;
    config::set(updated).unwrap();

    let mut test = QueryParserTest::new_with_config(&config());

    let mut shards_seen = HashSet::new();
    let q = "SELECT sharded_omni.* FROM sharded_omni WHERE sharded_omni.id = 1";

    for _ in 0..10 {
        let command = test.execute(vec![Query::new(q).into()]);
        assert!(matches!(command.route().shard(), Shard::Direct(_)));
        assert!(command.route().is_omnisharded());
        shards_seen.insert(command.route().shard().clone());
    }

    assert_eq!(
        shards_seen.len(),
        2,
        "with omnisharded_sticky=false, queries should be load-balanced across shards"
    );
}

#[test]
fn test_system_catalog_sharded() {
    use pgdog_config::SystemCatalogsBehavior;

    let mut updated = config().deref().clone();
    updated.config.general.system_catalogs = SystemCatalogsBehavior::Sharded;
    config::set(updated).unwrap();

    let mut test = QueryParserTest::new_with_config(&config());

    let command = test.execute(vec![Query::new("SELECT * FROM pg_class").into()]);
    assert_eq!(
        command.route().shard(),
        &Shard::All,
        "system catalog query with Sharded behavior should go to all shards"
    );

    let command = test.execute(vec![
        Query::new("SELECT * FROM pg_type WHERE typname = 'int4'").into(),
    ]);
    assert_eq!(
        command.route().shard(),
        &Shard::All,
        "system catalog query with WHERE clause should still go to all shards"
    );
    assert!(!command.route().is_omnisharded());

    // Reset to default
    let mut updated = config().deref().clone();
    updated.config.general.system_catalogs = SystemCatalogsBehavior::default();
    config::set(updated).unwrap();
}

#[test]
fn test_system_catalog_omnisharded_default() {
    use pgdog_config::SystemCatalogsBehavior;

    let mut updated = config().deref().clone();
    updated.config.general.system_catalogs = SystemCatalogsBehavior::OmnishardedSticky;
    config::set(updated).unwrap();

    let mut test = QueryParserTest::new_with_config(&config());

    // Without Sharded mode, system catalog queries use omnisharded routing
    // (goes to a single shard, not all shards)
    let command = test.execute(vec![Query::new("SELECT * FROM pg_class").into()]);
    assert!(
        matches!(command.route().shard(), Shard::Direct(_)),
        "system catalog query with OmnishardedSticky should go to a single shard, not Shard::All"
    );
    assert!(command.route().is_omnisharded());

    // Reset to default
    let mut updated = config().deref().clone();
    updated.config.general.system_catalogs = SystemCatalogsBehavior::default();
    config::set(updated).unwrap();
}

/// A SELECT against a table explicitly configured as omnisharded routes to a
/// single shard and is flagged as omnisharded.
#[test]
fn test_omnisharded_configured_table() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("SELECT * FROM sharded_omni").into()]);
    assert!(matches!(command.route().shard(), Shard::Direct(_)));
    assert!(command.route().is_omnisharded());
}

/// A SELECT against a sticky omnisharded table routes to a single shard and is
/// flagged as omnisharded.
#[test]
fn test_omnisharded_configured_sticky_table() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("SELECT * FROM sharded_omni_sticky").into()]);
    assert!(matches!(command.route().shard(), Shard::Direct(_)));
    assert!(command.route().is_omnisharded());
}

/// A SELECT against a table that is neither sharded nor explicitly omnisharded
/// falls back to the omnisharded-by-default path: single shard, flagged as
/// omnisharded.
#[test]
fn test_omnisharded_by_default_unknown_table() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("SELECT * FROM unknown_table").into()]);
    assert!(matches!(command.route().shard(), Shard::Direct(_)));
    assert!(command.route().is_omnisharded());
}

/// A join of two omnisharded tables stays on a single shard and remains flagged
/// as omnisharded.
#[test]
fn test_omnisharded_join_of_omni_tables() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("SELECT * FROM sharded_omni a JOIN sharded_omni_sticky b ON a.id = b.id").into(),
    ]);
    assert!(matches!(command.route().shard(), Shard::Direct(_)));
    assert!(command.route().is_omnisharded());
}

/// A join of an omnisharded table with an unknown (omnisharded-by-default) table
/// stays on a single shard and remains flagged as omnisharded.
#[test]
fn test_omnisharded_join_omni_and_unknown() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("SELECT * FROM sharded_omni a JOIN unknown_table b ON a.id = b.id").into(),
    ]);
    assert!(matches!(command.route().shard(), Shard::Direct(_)));
    assert!(command.route().is_omnisharded());
}

/// A join that mixes an omnisharded table with a sharded table is NOT
/// omnisharded: it must fan out to all shards.
#[test]
fn test_omnisharded_join_omni_and_sharded_is_not_omni() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("SELECT * FROM sharded_omni a JOIN sharded b ON a.id = b.id").into(),
    ]);
    assert_eq!(command.route().shard(), &Shard::All);
    assert!(!command.route().is_omnisharded());
}

/// A sharded table queried without a sharding key fans out to all shards and is
/// NOT omnisharded.
#[test]
fn test_sharded_no_key_is_not_omnisharded() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("SELECT * FROM sharded").into()]);
    assert_eq!(command.route().shard(), &Shard::All);
    assert!(!command.route().is_omnisharded());
}

/// A sharded table queried with a sharding key routes direct-to-shard and is NOT
/// omnisharded.
#[test]
fn test_sharded_with_key_is_not_omnisharded() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("SELECT * FROM sharded WHERE id = 1").into(),
    ]);
    assert!(matches!(command.route().shard(), Shard::Direct(_)));
    assert!(!command.route().is_omnisharded());
}

/// A SELECT with no FROM clause (e.g. `SELECT 1`) round-robins to a single shard
/// but is NOT flagged as omnisharded (it touches no tables).
#[test]
fn test_no_table_select_is_not_omnisharded() {
    let mut test = QueryParserTest::new();

    for q in ["SELECT 1", "SELECT NOW()"] {
        let command = test.execute(vec![Query::new(q).into()]);
        assert!(matches!(command.route().shard(), Shard::Direct(_)));
        assert!(!command.route().is_omnisharded(), "query: {}", q);
    }
}
