//! Tests that test what the query parser is disabled
//! and we have only one shard (but we have replicas).
//!
//! QueryParser::query_parser_bypass.
//!
use pgdog_config::{QueryParserLevel, ReadWriteSplit};

use crate::{
    backend::pool::RoleTarget,
    config::config,
    frontend::router::parser::{Error, Shard},
    net::Query,
};

use super::setup::QueryParserTest;

fn setup() -> QueryParserTest {
    let mut config = (*config()).clone();
    config.config.general.query_parser = QueryParserLevel::Off;
    QueryParserTest::new_single_shard(&config)
}

fn setup_single_primary() -> QueryParserTest {
    let mut config = (*config()).clone();
    config.config.general.query_parser = QueryParserLevel::Off;
    QueryParserTest::new_single_primary(&config)
}

fn setup_single_replica() -> QueryParserTest {
    let mut config = (*config()).clone();
    config.config.general.query_parser = QueryParserLevel::Off;
    QueryParserTest::new_single_replica(&config)
}

fn setup_sharded() -> QueryParserTest {
    let mut config = (*config()).clone();
    config.config.general.query_parser = QueryParserLevel::Off;
    QueryParserTest::new_with_config(&config)
}

const QUERIES: &[&str] = &[
    "SELECT 1",
    "CREATE TABLE test (id BIGINT)",
    "SELECT * FROM test",
    "INSERT INTO test (id) VALUES (1)",
];

#[tokio::test]
async fn test_replica() {
    let mut test = setup().with_param("pgdog.role", "replica");

    for query in QUERIES {
        let result = test.try_execute(vec![Query::new(query).into()]).unwrap();
        assert_eq!(result.route().role_target(), RoleTarget::PreferReplica);
        assert_eq!(result.route().shard(), &Shard::Direct(0))
    }
}

#[tokio::test]
async fn test_primary() {
    let mut test = setup().with_param("pgdog.role", "primary");

    for query in QUERIES {
        let result = test.try_execute(vec![Query::new(query).into()]).unwrap();
        assert_eq!(result.route().role_target(), RoleTarget::Primary);
        assert_eq!(result.route().shard(), &Shard::Direct(0));
        assert_eq!(result.route().statement_type(), None);
    }
}

#[tokio::test]
async fn test_no_hints() {
    let mut test = setup();

    for query in QUERIES {
        let result = test.try_execute(vec![Query::new(query).into()]).unwrap();
        assert_eq!(result.route().role_target(), RoleTarget::Primary);
        assert_eq!(result.route().shard(), &Shard::Direct(0))
    }
}

/// An explicit role applies on every topology, even without a primary.
#[tokio::test]
async fn test_read_only_cluster() {
    let mut test = setup_single_replica().with_param("pgdog.role", "primary");

    for query in QUERIES {
        let result = test.try_execute(vec![Query::new(query).into()]).unwrap();
        assert_eq!(result.route().role_target(), RoleTarget::Primary);
        assert_eq!(result.route().shard(), &Shard::Direct(0))
    }
}

/// Without a role, a cluster without a primary uses its replicas.
#[tokio::test]
async fn test_read_only_cluster_no_hints() {
    let mut test = setup_single_replica();

    for query in QUERIES {
        let result = test.try_execute(vec![Query::new(query).into()]).unwrap();
        assert_eq!(result.route().role_target(), RoleTarget::Any);
        assert_eq!(result.route().shard(), &Shard::Direct(0))
    }
}

#[tokio::test]
async fn test_write_only_cluster() {
    let mut test = setup_single_primary().with_param("pgdog.role", "replica");

    for query in QUERIES {
        let result = test.try_execute(vec![Query::new(query).into()]).unwrap();
        assert_eq!(result.route().role_target(), RoleTarget::PreferReplica);
        assert_eq!(result.route().shard(), &Shard::Direct(0))
    }
}

#[tokio::test]
async fn test_role_auto() {
    let mut test = setup().with_param("pgdog.role", "auto");

    for query in QUERIES {
        let result = test.try_execute(vec![Query::new(query).into()]).unwrap();
        assert_eq!(result.route().role_target(), RoleTarget::Primary);
        assert_eq!(result.route().shard(), &Shard::Direct(0))
    }
}

#[tokio::test]
async fn test_prefer_primary() {
    let mut test = setup().with_rw_split(ReadWriteSplit::PreferPrimary);

    for query in QUERIES {
        let result = test.try_execute(vec![Query::new(query).into()]).unwrap();
        assert_eq!(result.route().role_target(), RoleTarget::Primary);
        assert_eq!(result.route().shard(), &Shard::Direct(0))
    }
}

#[tokio::test]
async fn test_prefer_replica() {
    let mut test = setup().with_rw_split(ReadWriteSplit::ExcludePrimary);

    for query in QUERIES {
        let result = test.try_execute(vec![Query::new(query).into()]).unwrap();
        assert_eq!(result.route().role_target(), RoleTarget::Any);
        assert_eq!(result.route().shard(), &Shard::Direct(0))
    }
}

#[tokio::test]
async fn test_sharded_with_shard() {
    let mut test = setup_sharded().with_param("pgdog.shard", "1");

    for query in QUERIES {
        let result = test.try_execute(vec![Query::new(query).into()]).unwrap();
        assert_eq!(result.route().role_target(), RoleTarget::Primary);
        assert_eq!(result.route().shard(), &Shard::Direct(1))
    }
}

#[tokio::test]
async fn test_sharded_with_shard_and_replica() {
    let mut test = setup_sharded()
        .with_param("pgdog.shard", "1")
        .with_param("pgdog.role", "replica");

    for query in QUERIES {
        let result = test.try_execute(vec![Query::new(query).into()]).unwrap();
        assert_eq!(result.route().role_target(), RoleTarget::PreferReplica);
        assert_eq!(result.route().shard(), &Shard::Direct(1))
    }
}

#[tokio::test]
async fn test_sharded_no_hints() {
    let mut test = setup_sharded();

    for query in QUERIES {
        let result = test
            .try_execute(vec![Query::new(query).into()])
            .unwrap_err();
        assert!(matches!(result, Error::QueryParserRequired));
    }
}
