use crate::{
    config::config,
    frontend::{
        Command,
        router::parser::{
            Shard,
            route::{OverrideReason, ShardSource},
        },
    },
    net::parameter::ParameterValue,
};

use super::setup::*;

#[test]
fn test_mixed_set_passthrough_in_session_mode() {
    let mut test = QueryParserTest::new_session_mode(&config());

    // In session mode, mixed SET + non-SET multi-statement queries must not be rejected.
    // This is the exact batch psqlODBC sends during connection startup (issue #1087).
    let command = test.execute(vec![
        Query::new("SET DateStyle='ISO';SET extra_float_digits = 2;show transaction_isolation")
            .into(),
    ]);
    assert!(
        matches!(command, Command::Query(_)),
        "expected Command::Query passthrough in session mode, got {command:#?}",
    );
}

#[test]
fn test_mixed_set_rejected_in_transaction_mode() {
    let mut test = QueryParserTest::new();

    let result = test.try_execute(vec![
        Query::new("SET DateStyle='ISO'; show transaction_isolation").into(),
    ]);
    assert!(
        result.is_err(),
        "expected error for mixed SET in transaction mode, got {result:#?}",
    );
}

#[test]
fn test_set_comment() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("/* pgdog_sharding_key: 1234 */ SET statement_timeout TO 1").into(),
    ]);

    assert!(
        matches!(command.clone(), Command::Set { ref params, ref route, ..} if params.len() == 1 && params[0].name == "statement_timeout" && !params[0].local && params[0].value == Some(ParameterValue::String("1".into())) && route.shard().is_direct()),
        "expected Command::Set, got {:#?}",
        command,
    );
}

#[test]
fn test_set_multi_statement() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("SET statement_timeout TO 1; SET work_mem TO '64MB'").into(),
    ]);

    match command {
        Command::Set { ref params, .. } => {
            assert_eq!(params.len(), 2);
            assert_eq!(params[0].name, "statement_timeout");
            assert_eq!(params[0].value, Some(ParameterValue::String("1".into())));
            assert!(!params[0].local);
            assert_eq!(params[1].name, "work_mem");
            assert_eq!(params[1].value, Some(ParameterValue::String("64MB".into())));
            assert!(!params[1].local);
        }
        _ => panic!("expected Command::Set, got {command:#?}"),
    }
}

#[test]
fn test_set_multi_statement_mixed_local() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("SET statement_timeout TO 1; SET LOCAL work_mem TO '64MB'").into(),
    ]);

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
        Query::new("SET statement_timeout TO 1; SELECT 1").into(),
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
fn test_set_multi_statement_with_timezone_interval() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new(
            "SET client_min_messages TO warning;SET TIME ZONE INTERVAL '+00:00' HOUR TO MINUTE",
        )
        .into(),
    ]);

    match command {
        Command::Set { ref params, .. } => {
            assert_eq!(params.len(), 2);
            assert_eq!(params[0].name, "client_min_messages");
            assert_eq!(
                params[0].value,
                Some(ParameterValue::String("warning".into()))
            );
            assert_eq!(params[1].name, "timezone");
            assert_eq!(
                params[1].value,
                Some(ParameterValue::String("+00:00".into()))
            );
        }
        _ => panic!("expected Command::Set, got {command:#?}"),
    }
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
        "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY",
        "SET SESSION CHARACTERISTICS AS TRANSACTION DEFERRABLE",
    ] {
        let command = test.execute(vec![Query::new(query).into()]);
        match &command {
            Command::Query(route) => {
                assert_eq!(
                    route.shard(),
                    &Shard::All,
                    "SET TRANSACTION should route to all shards for '{query}'"
                );
                assert!(
                    route.is_write(),
                    "SET TRANSACTION should be a write for '{query}'"
                );
            }
            _ => panic!("expected Command::Query for '{query}', got {command:#?}"),
        }
    }
}

#[test]
fn test_reset() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("RESET statement_timeout").into()]);
    match &command {
        Command::Set { params, .. } => {
            assert_eq!(params.len(), 1);
            assert_eq!(params[0].name, "statement_timeout");
            assert_eq!(params[0].value, None);
        }
        _ => panic!("expected Command::Set, got {command:#?}"),
    }

    let command = test.execute(vec![Query::new("RESET ALL").into()]);
    assert!(
        matches!(command, Command::ResetAll),
        "expected Command::ResetAll, got {command:#?}",
    );
}

#[test]
fn test_set_single_primary() {
    let mut test = QueryParserTest::new_single_primary(&config());
    let command = test.execute(vec![Query::new("SET statement_timeout TO 1").into()]);
    assert!(matches!(command, Command::Set { .. }));

    let mut config = (*config()).clone();
    config.config.general.query_parser = pgdog_config::QueryParserLevel::Off;

    let mut test = QueryParserTest::new_single_primary(&config);
    let command = test.execute(vec![Query::new("SET statement_timeout TO 1").into()]);
    match command {
        Command::Query(query) => assert_eq!(
            query.shard_with_priority().source(),
            &ShardSource::Override(OverrideReason::ParserDisabled)
        ),
        _ => panic!("expected Query, got {:?}", command),
    };
}

#[test]
fn test_single_shard_set() {
    let mut test = QueryParserTest::new_single_shard(&config());
    let command = test.execute(vec![Query::new("SET lock_timeout TO '1s'").into()]);

    match command {
        Command::Set { route, .. } => assert!(!route.is_cross_shard()),
        _ => panic!("not a set"),
    }
}
