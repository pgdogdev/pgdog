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
        matches!(command, Command::Set { name, value, local } if name == "statement_timeout" && !local && value == ParameterValue::String("1".into())),
        "expected Command::Set"
    );
}
