//! Tests that test what the query parser is disabled
//! and we have only one shard (but we have replicas).
//!
//! QueryParser::query_parser_bypass.
//!
use pgdog_config::{QueryParserLevel, ReadWriteSplit, ReadWriteStrategy, Role};

use crate::{
    backend::pool::RoleTarget,
    config::config,
    frontend::{
        client::TransactionType,
        router::parser::{Error, Shard},
    },
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

fn assert_role_target(test: &mut QueryParserTest, expected: RoleTarget) {
    for query in QUERIES {
        let result = test.try_execute(vec![Query::new(query).into()]).unwrap();
        assert_eq!(result.route().role_target(), expected, "{query}");
        assert_eq!(result.route().shard(), &Shard::Direct(0), "{query}");
    }
}

#[tokio::test]
async fn test_user_role() {
    for (role, expected) in [
        (Role::Replica, RoleTarget::PreferReplica),
        (Role::Primary, RoleTarget::Primary),
        (Role::Auto, RoleTarget::Primary),
    ] {
        let mut test = setup().with_user_role(role);
        assert_role_target(&mut test, expected);
    }
}

/// `auto` clears the user role; an invalid value falls back to it.
#[tokio::test]
async fn test_param_over_user_role() {
    let mut test = setup()
        .with_rw_split(ReadWriteSplit::ExcludePrimary)
        .with_user_role(Role::Primary)
        .with_param("pgdog.role", "auto");
    assert_role_target(&mut test, RoleTarget::Any);

    let mut test = setup()
        .with_user_role(Role::Replica)
        .with_param("pgdog.role", "invalid");
    assert_role_target(&mut test, RoleTarget::PreferReplica);
}

#[tokio::test]
async fn test_transaction() {
    use ReadWriteStrategy::{Aggressive, Conservative};
    use TransactionType::{ReadOnly, ReadWrite};

    for (strategy, transaction, expected) in [
        (Conservative, ReadWrite, RoleTarget::Primary),
        (Conservative, ReadOnly, RoleTarget::Any),
        (Aggressive, ReadWrite, RoleTarget::Any),
        (Aggressive, ReadOnly, RoleTarget::Any),
    ] {
        let mut test = setup()
            .with_rw_split(ReadWriteSplit::ExcludePrimary)
            .with_read_write_strategy(strategy)
            .with_transaction(transaction);
        assert_role_target(&mut test, expected);
    }
}

/// The session role wins over the transaction.
#[tokio::test]
async fn test_transaction_with_role() {
    let mut test = setup()
        .with_transaction(TransactionType::ReadWrite)
        .with_param("pgdog.role", "replica");
    assert_role_target(&mut test, RoleTarget::PreferReplica);

    let mut test = setup()
        .with_transaction(TransactionType::ReadOnly)
        .with_user_role(Role::Primary);
    assert_role_target(&mut test, RoleTarget::Primary);
}

/// Without a primary, a transaction does not force one, like `main`.
#[tokio::test]
async fn test_read_only_cluster_transaction() {
    let mut test = setup_single_replica().with_transaction(TransactionType::ReadWrite);
    assert_role_target(&mut test, RoleTarget::Any);
}
