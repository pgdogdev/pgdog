use pgdog_config::QueryParserLevel;

use crate::{config::config, frontend::Command};

use super::setup::*;

/// Pool impersonating a fixed role. The query parser is switched off to
/// check that `server_role` forces full parsing on its own.
fn setup() -> QueryParserTest {
    let mut config = (*config()).clone();
    config.config.general.query_parser = QueryParserLevel::Off;
    QueryParserTest::new_single_primary(&config).with_server_role("analytics")
}

#[test]
fn test_rejects_role_changes() {
    for (query, expected) in [
        ("SET ROLE postgres", "role"),
        ("SET LOCAL ROLE postgres", "role"),
        ("RESET ROLE", "role"),
        (
            "SET SESSION AUTHORIZATION postgres",
            "session_authorization",
        ),
        ("RESET SESSION AUTHORIZATION", "session_authorization"),
        ("SELECT 1; SET ROLE postgres", "role"),
        ("SET statement_timeout TO 1; RESET ROLE", "role"),
        ("SELECT set_config('role', 'postgres', false)", "role"),
        (
            "SELECT pg_catalog.set_config('role', 'postgres', true)",
            "role",
        ),
        (
            "SELECT set_config('session_authorization', 'postgres', false)",
            "session_authorization",
        ),
    ] {
        let mut test = setup();
        let command = test.execute(vec![Query::new(query).into()]);
        assert!(
            matches!(&command, Command::RoleLocked { name } if name == expected),
            "{query}: expected RoleLocked({expected}), got {command:#?}",
        );
    }
}

#[test]
fn test_rejects_role_changes_extended() {
    let mut test = setup();
    let command = test.execute(vec![
        Parse::new_anonymous("SET ROLE postgres").into(),
        Sync.into(),
    ]);
    assert!(
        matches!(&command, Command::RoleLocked { name } if name == "role"),
        "got {command:#?}",
    );
}

#[test]
fn test_allows_other_session_state() {
    for query in [
        "RESET ALL",
        "DISCARD ALL",
        "SET statement_timeout TO 1",
        "RESET statement_timeout",
        "SELECT set_config('work_mem', '8MB', false)",
        "SELECT current_user",
    ] {
        let mut test = setup();
        let command = test.execute(vec![Query::new(query).into()]);
        assert!(
            !matches!(command, Command::RoleLocked { .. }),
            "{query}: got {command:#?}",
        );
    }
}

#[test]
fn test_role_changes_allowed_without_server_role() {
    let mut config = (*config()).clone();
    config.config.general.query_parser = QueryParserLevel::On;
    let mut test = QueryParserTest::new_single_primary(&config);

    let command = test.execute(vec![Query::new("SET ROLE postgres").into()]);
    assert!(
        matches!(command, Command::Set { .. }),
        "expected Command::Set, got {command:#?}",
    );
}
