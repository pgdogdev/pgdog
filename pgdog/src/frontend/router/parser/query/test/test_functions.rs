use bytes::Bytes;

use super::setup::*;
use crate::frontend::router::parser::Shard;

#[test]
fn test_write_function_advisory_lock() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("SELECT pg_advisory_lock(123)").into()]);

    assert!(command.route().is_write());
    assert!(command.route().is_lock_session());
}

/// Test every variant of pg_advisory "class" functions to ensure that they
/// all correctly deterministically hash to a Shard # based on the
/// lock ID specified.
#[test]
fn test_advisory_lock_routes_by_lock_id() {
    use crate::frontend::router::parser::route::{OverrideReason, ShardSource};

    let lock_on_shard_0 = 606;
    let lock_on_shard_1 = 505;

    let functions = [
        "pg_advisory_lock",
        "pg_advisory_lock_shared",
        "pg_try_advisory_lock",
        "pg_try_advisory_lock_shared",
        "pg_advisory_xact_lock",
        "pg_advisory_xact_lock_shared",
        "pg_try_advisory_xact_lock",
        "pg_try_advisory_xact_lock_shared",
        "pg_advisory_unlock",
    ];

    for function in functions {
        for (lock, shard) in [(lock_on_shard_0, 0), (lock_on_shard_1, 1)] {
            let mut test = QueryParserTest::new();
            let command = test.execute(vec![
                Query::new(format!("SELECT {function}({lock})")).into(),
            ]);
            let route = command.route();

            assert_eq!(route.shard(), &Shard::Direct(shard));
            assert_eq!(
                route.shard_with_priority().source(),
                &ShardSource::Override(OverrideReason::AdvisoryLock)
            );
        }
    }
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
    assert_eq!(command.route().shard(), &Shard::Direct(0));
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
