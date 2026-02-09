use crate::{frontend::Command, net::parameter::ParameterValue};

use super::setup::*;

#[test]
fn test_set_comment() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "/* pgdog_sharding_key: 1234 */ SET statement_timeout TO 1",
    )
    .into()]);

    assert!(
        matches!(command.clone(), Command::Set { ref params, ref route } if params.len() == 1 && params[0].name == "statement_timeout" && !params[0].local && params[0].value == ParameterValue::String("1".into()) && route.shard().is_direct()),
        "expected Command::Set, got {:#?}",
        command,
    );
}

#[test]
fn test_set_multi_statement() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "SET statement_timeout TO 1; SET work_mem TO '64MB'",
    )
    .into()]);

    match command {
        Command::Set { ref params, .. } => {
            assert_eq!(params.len(), 2);
            assert_eq!(params[0].name, "statement_timeout");
            assert_eq!(params[0].value, ParameterValue::String("1".into()));
            assert!(!params[0].local);
            assert_eq!(params[1].name, "work_mem");
            assert_eq!(params[1].value, ParameterValue::String("64MB".into()));
            assert!(!params[1].local);
        }
        _ => panic!("expected Command::Set, got {command:#?}"),
    }
}

#[test]
fn test_set_multi_statement_mixed_local() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "SET statement_timeout TO 1; SET LOCAL work_mem TO '64MB'",
    )
    .into()]);

    match command {
        Command::Set { ref params, .. } => {
            assert_eq!(params.len(), 2);
            assert!(!params[0].local);
            assert!(params[1].local);
        }
        _ => panic!("expected Command::Set, got {command:#?}"),
    }
}

#[test]
fn test_set_multi_statement_mixed_returns_error() {
    let mut test = QueryParserTest::new();

    let result = test.try_execute(vec![
        Query::new("SET statement_timeout TO 1; SELECT 1").into()
    ]);
    assert!(result.is_err());
}

#[test]
fn test_multi_statement_no_set_falls_through() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("SELECT 1; SELECT 2").into()]);
    assert!(
        matches!(command, Command::Query(_)),
        "multi-statement without SET should fall through, got {command:#?}",
    );
}

#[test]
fn test_set_transaction_level() {
    let mut test = QueryParserTest::new();

    for query in [
        "SET TRANSACTION SNAPSHOT '00000003-0000001B-1'",
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        "set transaction isolation level repeatable read",
        "set transaction snapshot '00000003-0000001B-1'",
        "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE",
    ] {
        let command = test.execute(vec![Query::new(query).into()]);
        assert!(
            matches!(command.clone(), Command::Query(_)),
            "{:#?}",
            command
        );
    }
}
