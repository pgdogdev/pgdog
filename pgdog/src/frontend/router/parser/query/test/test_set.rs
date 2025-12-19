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
        matches!(command.clone(), Command::Set { name, value, local, route } if name == "statement_timeout" && !local && value == ParameterValue::String("1".into()) && route.shard().is_direct()),
        "expected Command::Set, got {:#?}",
        command,
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
