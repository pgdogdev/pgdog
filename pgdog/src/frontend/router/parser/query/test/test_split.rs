use crate::{config::config, frontend::Command};

use super::{Error, setup::*};

fn execute(query: &str) -> Command {
    QueryParserTest::new().execute(vec![Query::new(query).into()])
}

fn assert_split(query: &str, expected: &[&str]) {
    let command = execute(query);

    match command {
        Command::Split(queries) => assert_eq!(
            queries, expected,
            "unexpected statements produced by splitting `{query}`",
        ),
        _ => panic!("expected Command::Split for `{query}`, got {command:#?}"),
    }
}

fn assert_unsafe(query: &str) {
    let result = QueryParserTest::new()
        .try_execute(vec![Query::new(query).into()])
        .expect_err("query should not be safe to execute without a transaction");

    assert!(
        matches!(result, Error::MultiStatementSafety),
        "expected MultiStatementSafety for `{query}`, got {result:#?}",
    );
}

#[test]
fn test_single_statement_is_not_split() {
    let command = execute("SELECT 1");

    assert!(
        matches!(command, Command::Query(_)),
        "expected Command::Query, got {command:#?}",
    );
}

#[test]
fn test_session_mode_multi_statement_is_passed_through_as_write() {
    let mut test = QueryParserTest::new_session_mode(&config());
    let command = test.execute(vec![Query::new("SELECT 1; SELECT 2").into()]);

    assert!(
        matches!(command, Command::Query(ref route) if route.is_write()),
        "expected a write Command::Query, got {command:#?}",
    );
}

#[test]
fn test_extended_protocol_multi_statement_is_not_split() {
    let mut test = QueryParserTest::new();
    let command = test.execute(vec![Parse::new_anonymous("SELECT 1; SELECT 2").into()]);

    assert!(
        matches!(command, Command::Query(_)),
        "expected Command::Query, got {command:#?}",
    );
}

#[test]
fn test_direct_shard_multi_statement_is_not_split() {
    let command = execute("/* pgdog_shard: 1 */ SELECT 1; SELECT 2");

    assert!(
        matches!(command, Command::Query(ref route) if route.shard().is_direct()),
        "expected a direct-shard Command::Query, got {command:#?}",
    );
}

#[test]
fn test_ddl_only_multi_statement_is_not_split() {
    let command = execute("CREATE TABLE split_test (id bigint); DROP TABLE split_test");

    assert!(
        matches!(command, Command::Query(ref route) if route.is_write()),
        "expected a write Command::Query, got {command:#?}",
    );
}

#[test]
fn test_one_dml_with_non_mutating_statements_is_split() {
    for (query, expected) in [
        (
            "SHOW application_name; SELECT 1",
            &["SHOW application_name", "SELECT 1"] as &[_],
        ),
        (
            "DEALLOCATE ALL; SELECT 1",
            &["DEALLOCATE ALL", "SELECT 1"] as &[_],
        ),
        (
            "VACUUM split_test; SELECT 1",
            &["VACUUM split_test", "SELECT 1"] as &[_],
        ),
        (
            "PREPARE split_stmt AS SELECT 1; SELECT 2",
            &["PREPARE split_stmt AS SELECT 1", "SELECT 2"] as &[_],
        ),
    ] {
        assert_split(query, expected);
    }
}

#[test]
fn test_each_dml_kind_counts_toward_the_safety_limit() {
    for query in [
        "SELECT 1; SELECT 2",
        "INSERT INTO split_test VALUES (1); SELECT 1",
        "UPDATE split_test SET id = 2; SELECT 1",
        "DELETE FROM split_test; SELECT 1",
        "EXECUTE split_stmt; SELECT 1",
    ] {
        assert_unsafe(query);
    }
}

#[test]
fn test_dml_and_ddl_outside_transaction_is_unsafe() {
    assert_unsafe("SELECT 1; CREATE TABLE split_test (id bigint)");
}

#[test]
fn test_complete_transactions_are_split() {
    for (query, expected) in [
        (
            "BEGIN; SELECT 1; SELECT 2; COMMIT",
            &["BEGIN", "SELECT 1", "SELECT 2", "COMMIT"] as &[_],
        ),
        (
            "START TRANSACTION; INSERT INTO split_test VALUES (1); ROLLBACK",
            &[
                "START TRANSACTION",
                "INSERT INTO split_test VALUES (1)",
                "ROLLBACK",
            ] as &[_],
        ),
    ] {
        assert_split(query, expected);
    }
}

#[test]
fn test_open_transaction_is_unsafe() {
    assert_unsafe("BEGIN; SELECT 1");
}

#[test]
fn test_multiple_statements_after_transaction_are_unsafe() {
    assert_unsafe("BEGIN; SELECT 1; COMMIT; SELECT 2; SELECT 3");
}

#[test]
fn test_one_statement_after_transaction_are_unsafe() {
    assert_unsafe("BEGIN; SELECT 1; COMMIT; SELECT 2;");
}

#[test]
fn test_ddl_in_explicit_transaction_is_split() {
    assert_split(
        "BEGIN; CREATE TABLE split_test (id bigint); DROP TABLE split_test; COMMIT",
        &[
            "BEGIN",
            "CREATE TABLE split_test (id bigint)",
            "DROP TABLE split_test",
            "COMMIT",
        ],
    );
}

#[test]
fn test_mixed_set_and_multiple_ddl_statements_are_unsafe() {
    assert_unsafe(
        "SET application_name TO 'split_test'; CREATE TABLE split_test (id bigint); DROP TABLE split_test",
    );
}
