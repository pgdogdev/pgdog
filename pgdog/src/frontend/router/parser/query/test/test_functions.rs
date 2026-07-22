use bytes::Bytes;
#[cfg(feature = "new_parser")]
use pgdog_config::WriteFunctions;
#[cfg(feature = "new_parser")]
use std::ops::Deref;

use super::setup::*;
#[cfg(feature = "new_parser")]
use crate::config::config;

#[test]
fn test_write_function_advisory_lock() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("SELECT pg_advisory_lock(123)").into()]);

    assert!(command.route().is_write());
    assert!(command.route().is_lock_session());
}

#[test]
fn test_write_functions_prepared() {
    let mut test = QueryParserTest::new();
    let command = test.execute(vec![
        Parse::named("test", "SELECT pg_advisory_lock($1) IS NOT NULL").into(),
        Bind::new_params(
            "test",
            &[crate::net::bind::Parameter {
                len: 4,
                data: Bytes::from(b"1234".to_vec()),
            }],
        )
        .into(),
    ]);
    assert!(command.route().is_write());
    assert!(command.route().is_lock_session());
}

#[test]
fn test_write_function_nextval() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("SELECT nextval('234')").into()]);

    assert!(command.route().is_write());
    assert!(!command.route().is_lock_session());
}

#[test]
fn test_cross_shard_install_sharded_sequence() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("SELECT pgdog.install_sharded_sequence('foo', 'id')").into(),
    ]);

    assert!(command.route().is_cross_shard());
}

#[test]
fn test_install_sharded_sequence_without_schema_not_cross_shard() {
    // Without the `pgdog.` schema qualifier we should not flag the call
    // as a cross-shard function — it could be any user-defined function.
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("SELECT install_sharded_sequence('foo', 'id')").into(),
    ]);

    assert!(!command.route().is_cross_shard());
}

#[test]
#[cfg(feature = "new_parser")]
fn test_configured_write_function_routes_to_primary() {
    let mut updated = config().deref().clone();
    updated.config.write_functions = vec![WriteFunctions {
        database: "pgdog".into(),
        functions: vec!["my_write_fn".into()],
    }];

    let mut test = QueryParserTest::new_with_config(&updated);
    let command = test.execute(vec![Query::new("SELECT my_write_fn(1)").into()]);

    assert!(command.route().is_write());
}

#[test]
#[cfg(feature = "new_parser")]
fn test_configured_write_function_pg_identifier_semantics() {
    let mut updated = config().deref().clone();
    updated.config.write_functions = vec![WriteFunctions {
        database: "pgdog".into(),
        functions: vec!["my_write_fn".into()],
    }];

    let mut test = QueryParserTest::new_with_config(&updated);
    let command = test.execute(vec![Query::new("SELECT now(), My_Write_Fn(1)").into()]);

    assert!(command.route().is_write());

    let command = test.execute(vec![Query::new(r#"SELECT "My_Write_Fn"(1)"#).into()]);
    assert!(command.route().is_read());

    updated.config.write_functions = vec![WriteFunctions {
        database: "pgdog".into(),
        functions: vec![r#""My_Write_Fn""#.into()],
    }];
    let mut test = QueryParserTest::new_with_config(&updated);
    let command = test.execute(vec![Query::new(r#"SELECT "My_Write_Fn"(1)"#).into()]);
    assert!(command.route().is_write());
}

#[test]
#[cfg(feature = "new_parser")]
fn test_configured_write_function_with_schema() {
    let mut updated = config().deref().clone();
    updated.config.write_functions = vec![WriteFunctions {
        database: "pgdog".into(),
        functions: vec!["partman.create_partition".into()],
    }];

    let mut test = QueryParserTest::new_with_config(&updated);
    let command = test.execute(vec![
        Query::new("SELECT partman.create_partition(1)").into(),
    ]);
    assert!(command.route().is_write());

    let command = test.execute(vec![Query::new("SELECT other.create_partition(1)").into()]);
    assert!(command.route().is_read());
}
