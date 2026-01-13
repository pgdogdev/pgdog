use super::setup::*;

#[test]
fn test_write_function_advisory_lock() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("SELECT pg_advisory_lock($1)").into()]);

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
