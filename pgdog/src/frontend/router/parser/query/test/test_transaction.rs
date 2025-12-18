use crate::config::ReadWriteStrategy;
use crate::frontend::Command;
use crate::net::parameter::ParameterValue;

use super::setup::*;

#[test]
fn test_begin_simple() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("BEGIN").into()]);

    match command {
        Command::StartTransaction { query: q, .. } => assert_eq!(q.query(), "BEGIN"),
        _ => panic!("expected StartTransaction, got {command:?}"),
    }
}

#[test]
fn test_begin_extended() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Parse::new_anonymous("BEGIN").into(),
        Bind::new_statement("").into(),
        Execute::new().into(),
        Sync.into(),
    ]);

    match command {
        Command::StartTransaction { extended, .. } => assert!(extended),
        _ => panic!("expected StartTransaction, got {command:?}"),
    }
}

#[test]
fn test_begin_sets_write_override() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("BEGIN").into()]);
    assert!(matches!(command, Command::StartTransaction { .. }));
    assert!(test.parser.write_override);
}

// NOTE: The test for "SELECT after BEGIN is treated as write" requires passing
// in_transaction=true to the router context for the subsequent query, plus reusing
// the same QueryParser instance. This is better tested by the original
// test_transaction test in mod.rs using the query_parser! macro.

#[test]
fn test_begin_with_aggressive_strategy() {
    let mut test = QueryParserTest::new().with_read_write_strategy(ReadWriteStrategy::Aggressive);

    let command = test.execute(vec![Query::new("BEGIN").into()]);

    assert!(matches!(command, Command::StartTransaction { .. }));
}

#[test]
fn test_set_in_transaction() {
    let mut test = QueryParserTest::new().in_transaction(true);

    let command = test.execute(vec![Query::new("SET statement_timeout TO 3000").into()]);

    match command {
        Command::Set { name, value, .. } => {
            assert_eq!(name, "statement_timeout");
            assert_eq!(value, ParameterValue::from("3000"));
        }
        _ => panic!("expected Set, got {command:?}"),
    }
}

#[test]
fn test_set_application_name_in_transaction() {
    let mut test = QueryParserTest::new()
        .in_transaction(true)
        .with_read_write_strategy(ReadWriteStrategy::Aggressive);

    let command = test.execute(vec![Query::new("SET application_name TO 'test'").into()]);

    match command.clone() {
        Command::Set {
            name,
            value,
            local,
            route,
        } => {
            assert_eq!(name, "application_name");
            assert_eq!(value.as_str().unwrap(), "test");
            assert!(!local);
            assert!(!test.cluster().read_only());
            assert!(route.is_write());
            assert!(route.shard().is_all());
        }
        _ => panic!("expected Set, got {command:?}"),
    }

    assert!(command.route().shard().is_all());
}
