use pgdog_config::QueryParserLevel;

use crate::{config::config, frontend::Command};

use super::setup::*;

fn setup() -> QueryParserTest {
    let mut config = (*config()).clone();
    config.config.general.query_parser = QueryParserLevel::SessionControl;
    QueryParserTest::new_single_primary(&config)
}

#[test]
fn test_set() {
    let mut test = setup();
    let command = test.execute(vec![Query::new("SET statement_timeout TO 1").into()]);
    assert!(
        matches!(command, Command::Set { .. }),
        "expected Command::Set, got {command:#?}",
    );
}

#[test]
fn test_reset() {
    let mut test = setup();
    let command = test.execute(vec![Query::new("RESET statement_timeout").into()]);
    assert!(
        matches!(command, Command::Set { .. }),
        "expected Command::Set, got {command:#?}",
    );
}

#[test]
fn test_reset_all() {
    let mut test = setup();
    let command = test.execute(vec![Query::new("RESET ALL").into()]);
    assert!(
        matches!(command, Command::ResetAll),
        "expected Command::ResetAll, got {command:#?}",
    );
}

#[test]
fn test_begin() {
    let mut test = setup();
    let command = test.execute(vec![Query::new("BEGIN").into()]);
    assert!(
        matches!(command, Command::StartTransaction { .. }),
        "expected Command::StartTransaction, got {command:#?}",
    );
}

#[test]
fn test_commit() {
    let mut test = setup();
    let command = test.execute(vec![Query::new("COMMIT").into()]);
    assert!(
        matches!(command, Command::CommitTransaction { .. }),
        "expected Command::CommitTransaction, got {command:#?}",
    );
}

#[test]
fn test_rollback() {
    let mut test = setup();
    let command = test.execute(vec![Query::new("ROLLBACK").into()]);
    assert!(
        matches!(command, Command::RollbackTransaction { .. }),
        "expected Command::RollbackTransaction, got {command:#?}",
    );
}

#[test]
fn test_select_bypassed() {
    let mut test = setup();
    let command = test.execute(vec![Query::new("SELECT 1").into()]);
    assert!(
        matches!(command, Command::Query(_)),
        "expected Command::Query (bypass), got {command:#?}",
    );
}

#[test]
fn test_insert_bypassed() {
    let mut test = setup();
    let command = test.execute(vec![Query::new("INSERT INTO users VALUES (1)").into()]);
    assert!(
        matches!(command, Command::Query(_)),
        "expected Command::Query (bypass), got {command:#?}",
    );
}
