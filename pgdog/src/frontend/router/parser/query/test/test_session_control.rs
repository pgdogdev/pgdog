use pgdog_config::QueryParserLevel;

use crate::{
    config::config,
    frontend::{Command, DiscardTarget},
};

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
fn test_discard_variants() {
    let mut config = (*config()).clone();
    config.config.general.query_parser = QueryParserLevel::On;

    for (query, expected) in [
        ("DISCARD ALL", DiscardTarget::All),
        ("DISCARD PLANS", DiscardTarget::Plans),
        ("DISCARD SEQUENCES", DiscardTarget::Sequences),
        ("DISCARD TEMP", DiscardTarget::Temp),
        ("DISCARD TEMPORARY", DiscardTarget::Temp),
    ] {
        let mut test = QueryParserTest::new_single_primary(&config);
        let command = test.execute(vec![Query::new(query).into()]);
        assert!(
            matches!(command, Command::Discard { target, .. } if target == expected),
            "expected {expected:?} for {query}, got {command:#?}",
        );
    }
}

#[test]
fn test_begin() {
    for query in [
        "BEGIN",
        "BEGIN READ WRITE",
        "BEGIN READ ONLY",
        "BEGIN ISOLATION LEVEL SERIALIZABLE",
        "BEGIN ISOLATION LEVEL REPEATABLE READ",
        "BEGIN ISOLATION LEVEL READ COMMITTED",
        "BEGIN ISOLATION LEVEL READ UNCOMMITTED",
        "BEGIN DEFERRABLE",
        "BEGIN NOT DEFERRABLE",
        "BEGIN ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE",
        "BEGIN ISOLATION LEVEL REPEATABLE READ READ WRITE NOT DEFERRABLE",
        "BEGIN READ ONLY, ISOLATION LEVEL SERIALIZABLE, NOT DEFERRABLE",
        "BEGIN WORK",
        "BEGIN WORK READ WRITE",
        "BEGIN WORK READ ONLY",
        "BEGIN WORK ISOLATION LEVEL SERIALIZABLE",
        "BEGIN WORK ISOLATION LEVEL REPEATABLE READ",
        "BEGIN WORK ISOLATION LEVEL READ COMMITTED",
        "BEGIN WORK ISOLATION LEVEL READ UNCOMMITTED",
        "BEGIN WORK DEFERRABLE",
        "BEGIN WORK NOT DEFERRABLE",
        "BEGIN WORK ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE",
        "BEGIN WORK ISOLATION LEVEL REPEATABLE READ READ WRITE NOT DEFERRABLE",
        "BEGIN WORK READ ONLY, ISOLATION LEVEL SERIALIZABLE, NOT DEFERRABLE",
        "BEGIN TRANSACTION",
        "BEGIN TRANSACTION READ WRITE",
        "BEGIN TRANSACTION READ ONLY",
        "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        "BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED",
        "BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
        "BEGIN TRANSACTION DEFERRABLE",
        "BEGIN TRANSACTION NOT DEFERRABLE",
        "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE",
        "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ WRITE NOT DEFERRABLE",
        "BEGIN TRANSACTION READ ONLY, ISOLATION LEVEL SERIALIZABLE, NOT DEFERRABLE",
        "START TRANSACTION",
        "START TRANSACTION READ WRITE",
        "START TRANSACTION READ ONLY",
        "START TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        "START TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        "START TRANSACTION ISOLATION LEVEL READ COMMITTED",
        "START TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
        "START TRANSACTION DEFERRABLE",
        "START TRANSACTION NOT DEFERRABLE",
        "START TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE",
        "START TRANSACTION ISOLATION LEVEL REPEATABLE READ READ WRITE NOT DEFERRABLE",
        "START TRANSACTION READ ONLY, ISOLATION LEVEL SERIALIZABLE, NOT DEFERRABLE",
        "begin;",
        "bEgIn WoRk;",
        "start transaction;",
        "  BEGIN  ",
        "/* transaction */ BEGIN",
    ] {
        let mut test = setup();
        let command = test.execute(vec![Query::new(query).into()]);
        assert!(
            matches!(command, Command::StartTransaction { .. }),
            "expected Command::StartTransaction, for {query:?}, got {command:#?}",
        );
    }
}

#[test]
fn test_commit() {
    for query in [
        "COMMIT",
        "COMMIT AND CHAIN",
        "COMMIT AND NO CHAIN",
        "COMMIT WORK",
        "COMMIT WORK AND CHAIN",
        "COMMIT WORK AND NO CHAIN",
        "COMMIT TRANSACTION",
        "COMMIT TRANSACTION AND CHAIN",
        "COMMIT TRANSACTION AND NO CHAIN",
        "commit;",
        "  COMMIT  ",
        "/* transaction */ COMMIT",
        "END",
        "END AND CHAIN",
        "END AND NO CHAIN",
        "END WORK",
        "END WORK AND CHAIN",
        "END WORK AND NO CHAIN",
        "END TRANSACTION",
        "END TRANSACTION AND CHAIN",
        "END TRANSACTION AND NO CHAIN",
        "end;",
        "  END  ",
        "/* transaction */ END",
    ] {
        let mut test = setup();
        let command = test.execute(vec![Query::new(query).into()]);
        assert!(
            matches!(command, Command::CommitTransaction { .. }),
            "expected Command::CommitTransaction, for {query:?}, got {command:#?}",
        );
    }
}

#[test]
fn test_rollback() {
    for query in [
        "ROLLBACK",
        "ROLLBACK AND CHAIN",
        "ROLLBACK AND NO CHAIN",
        "ROLLBACK WORK",
        "ROLLBACK WORK AND CHAIN",
        "ROLLBACK WORK AND NO CHAIN",
        "ROLLBACK TRANSACTION",
        "ROLLBACK TRANSACTION AND CHAIN",
        "ROLLBACK TRANSACTION AND NO CHAIN",
        "rollback;",
        "  ROLLBACK  ",
        "/* transaction */ ROLLBACK",
        "ABORT",
        "ABORT AND CHAIN",
        "ABORT AND NO CHAIN",
        "ABORT WORK",
        "ABORT WORK AND CHAIN",
        "ABORT WORK AND NO CHAIN",
        "ABORT TRANSACTION",
        "ABORT TRANSACTION AND CHAIN",
        "ABORT TRANSACTION AND NO CHAIN",
        "abort;",
        "  ABORT  ",
        "/* transaction */ ABORT",
    ] {
        let mut test = setup();
        let command = test.execute(vec![Query::new(query).into()]);
        assert!(
            matches!(command, Command::RollbackTransaction { .. }),
            "expected Command::RollbackTransaction, for {query:?}, got {command:#?}",
        );
    }
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

fn setup_with_locks() -> QueryParserTest {
    let mut config = (*config()).clone();
    config.config.general.query_parser = QueryParserLevel::SessionControlAndLocks;
    QueryParserTest::new_single_primary(&config)
}

#[test]
fn test_advisory_lock_detected() {
    let lock_queries = [
        "SELECT pg_advisory_lock(1)",
        "SELECT pg_advisory_lock_shared(1)",
        "SELECT pg_try_advisory_lock(1)",
        "SELECT pg_try_advisory_lock_shared(1)",
    ];

    for query in lock_queries {
        let mut test = setup_with_locks();
        let command = test.execute(vec![Query::new(query).into()]);
        match command {
            Command::Query(route) => assert!(!route.advisory_locks().is_empty()),
            _ => panic!("expected Command::Query for '{query}', got {command:#?}"),
        }
    }

    let unlock_queries = [
        "SELECT pg_advisory_unlock(1)",
        "SELECT pg_advisory_unlock_all()",
    ];

    for query in unlock_queries {
        let mut test = setup_with_locks();
        let command = test.execute(vec![Query::new(query).into()]);
        match command {
            Command::Query(route) => assert!(!route.advisory_locks().is_empty()),
            _ => panic!("expected Command::Query for '{query}', got {command:#?}"),
        }
    }

    // xact variants pin the backend for the life of the transaction, but the
    // query engine drops them at COMMIT/ROLLBACK rather than treating them as
    // session-persistent locks.
    let xact_queries = [
        "SELECT pg_advisory_xact_lock(1)",
        "SELECT pg_advisory_xact_lock_shared(1)",
        "SELECT pg_try_advisory_xact_lock(1)",
        "SELECT pg_try_advisory_xact_lock_shared(1)",
    ];

    for query in xact_queries {
        let mut test = setup_with_locks();
        let command = test.execute(vec![Query::new(query).into()]);
        match command {
            Command::Query(route) => {
                assert!(!route.advisory_locks().is_empty());
            }
            _ => panic!("expected Command::Query for '{query}', got {command:#?}"),
        }
    }
}

#[test]
fn test_advisory_lock_not_detected_without_locks_level() {
    use crate::frontend::router::parser::route::{OverrideReason, ShardSource};

    let mut test = setup();
    let command = test.execute(vec![Query::new("SELECT pg_advisory_lock(1)").into()]);
    match command {
        Command::Query(route) => {
            assert_eq!(
                route.shard_with_priority().source(),
                &ShardSource::Override(OverrideReason::ParserDisabled),
                "advisory lock should bypass the parser at SessionControl level"
            );
        }
        _ => panic!("expected Command::Query, got {command:#?}"),
    }
}
