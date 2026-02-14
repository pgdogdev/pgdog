use crate::frontend::router::parser::Shard;
use crate::frontend::Command;
use crate::net::messages::Parameter;

use super::setup::{QueryParserTest, *};

// --- NULL handling ---

#[test]
fn test_where_is_null() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("SELECT * FROM sharded WHERE id IS NULL").into()
    ]);

    // NULL can be on any shard
    assert_eq!(command.route().shard(), &Shard::All);
}

#[test]
fn test_where_is_not_null() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "SELECT * FROM sharded WHERE id IS NOT NULL",
    )
    .into()]);

    assert_eq!(command.route().shard(), &Shard::All);
}

// --- OR conditions ---

#[test]
fn test_where_or_same_key() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "SELECT * FROM sharded WHERE id = 1 OR id = 2",
    )
    .into()]);

    // OR with different values goes to all shards
    assert_eq!(command.route().shard(), &Shard::All);
}

#[test]
fn test_where_in_list() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "SELECT * FROM sharded WHERE id IN (1, 2, 3)",
    )
    .into()]);

    // IN list with multiple values routes to Multi shard covering those specific shards
    assert!(matches!(command.route().shard(), Shard::Multi(_)));
}

// --- CASE expressions ---

#[test]
fn test_case_expression() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "SELECT CASE WHEN id = 1 THEN 'one' ELSE 'other' END FROM sharded WHERE id = 1",
    )
    .into()]);

    assert!(matches!(command.route().shard(), Shard::Direct(_)));
}

// --- UNION queries ---

#[test]
fn test_union() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "SELECT id FROM sharded WHERE id = 1 UNION SELECT id FROM sharded WHERE id = 2",
    )
    .into()]);

    assert!(command.route().is_read());
    // UNION finds the first matching shard key - both sides have literals so it picks one
    assert!(matches!(command.route().shard(), Shard::Direct(_)));
}

#[test]
fn test_union_all() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "SELECT id FROM sharded WHERE id = 1 UNION ALL SELECT id FROM sharded WHERE id = 2",
    )
    .into()]);

    assert!(command.route().is_read());
}

// --- Aggregate functions ---

#[test]
fn test_count_all() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("SELECT COUNT(*) FROM sharded").into()]);

    assert!(command.route().is_read());
    assert_eq!(command.route().shard(), &Shard::All);
}

#[test]
fn test_sum_with_group_by() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "SELECT email, SUM(amount) FROM sharded GROUP BY email",
    )
    .into()]);

    assert!(command.route().is_read());
    assert_eq!(command.route().shard(), &Shard::All);
}

// --- Window functions ---

#[test]
fn test_window_function() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM sharded WHERE id = 1",
    )
    .into()]);

    assert!(command.route().is_read());
}

// --- RETURNING clause ---

#[test]
fn test_insert_returning() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Parse::named(
            "__test_ins_ret",
            "INSERT INTO sharded (id, email) VALUES ($1, $2) RETURNING id, email",
        )
        .into(),
        Bind::new_params(
            "__test_ins_ret",
            &[Parameter::new(b"5"), Parameter::new(b"test@test.com")],
        )
        .into(),
        Execute::new().into(),
        Sync.into(),
    ]);

    assert!(command.route().is_write());
}

#[test]
fn test_update_returning() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Parse::named(
            "__test_upd_ret",
            "UPDATE sharded SET email = $1 WHERE id = $2 RETURNING *",
        )
        .into(),
        Bind::new_params(
            "__test_upd_ret",
            &[Parameter::new(b"new@test.com"), Parameter::new(b"5")],
        )
        .into(),
        Execute::new().into(),
        Sync.into(),
    ]);

    assert!(command.route().is_write());
}

// --- COALESCE and NULL functions ---

#[test]
fn test_coalesce_in_where() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "SELECT * FROM sharded WHERE COALESCE(id, 0) = 1",
    )
    .into()]);

    assert!(command.route().is_read());
}

// --- BETWEEN ---

#[test]
fn test_between_on_sharding_key() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "SELECT * FROM sharded WHERE id BETWEEN 1 AND 10",
    )
    .into()]);

    // BETWEEN can span multiple shards
    assert_eq!(command.route().shard(), &Shard::All);
}

// --- LIKE/ILIKE ---

#[test]
fn test_like_on_non_sharding_column() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "SELECT * FROM sharded WHERE email LIKE '%test%'",
    )
    .into()]);

    assert_eq!(command.route().shard(), &Shard::All);
}

// --- SHOW commands ---

#[test]
fn test_show_tables() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("SHOW TABLES").into()]);

    match command {
        Command::Query(_) | Command::InternalField { .. } => {}
        _ => panic!("expected Query or InternalField, got {command:?}"),
    }
}

#[test]
fn test_show_server_version() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("SHOW server_version").into()]);

    match command {
        Command::Query(_) | Command::InternalField { .. } => {}
        _ => panic!("expected Query or InternalField, got {command:?}"),
    }
}

// --- Locking ---

#[test]
fn test_select_for_share() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Parse::named(
            "__test_share",
            "SELECT * FROM sharded WHERE id = $1 FOR SHARE",
        )
        .into(),
        Bind::new_params("__test_share", &[Parameter::new(b"1")]).into(),
        Execute::new().into(),
        Sync.into(),
    ]);

    assert!(command.route().is_write());
}

#[test]
fn test_select_for_no_key_update() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Parse::named(
            "__test_nku",
            "SELECT * FROM sharded WHERE id = $1 FOR NO KEY UPDATE",
        )
        .into(),
        Bind::new_params("__test_nku", &[Parameter::new(b"1")]).into(),
        Execute::new().into(),
        Sync.into(),
    ]);

    assert!(command.route().is_write());
}

// --- Cast expressions ---

#[test]
fn test_cast_sharding_key() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Parse::named(
            "__test_cast",
            "SELECT * FROM sharded WHERE id = $1::INTEGER",
        )
        .into(),
        Bind::new_params("__test_cast", &[Parameter::new(b"5")]).into(),
        Execute::new().into(),
        Sync.into(),
    ]);

    assert!(matches!(command.route().shard(), Shard::Direct(_)));
}

// --- No-query message combinations ---

#[test]
fn test_sync_only() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Sync.into()]);

    match &command {
        Command::Query(route) => {
            assert_eq!(route.shard(), &Shard::All);
            assert!(route.is_write());
        }
        _ => panic!("expected Command::Query, got {command:#?}"),
    }
}

#[test]
fn test_close_and_flush() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Close::named("test").into(), Flush.into()]);

    match &command {
        Command::Query(route) => {
            assert_eq!(route.shard(), &Shard::All);
            assert!(route.is_write());
        }
        _ => panic!("expected Command::Query, got {command:#?}"),
    }
}

#[test]
fn test_close_only() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Close::named("test").into()]);

    match &command {
        Command::Query(route) => {
            assert_eq!(route.shard(), &Shard::All);
            assert!(route.is_write());
        }
        _ => panic!("expected Command::Query, got {command:#?}"),
    }
}

// --- Empty/edge parameter values ---

#[test]
fn test_empty_string_param() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Parse::named("__test_empty", "SELECT * FROM sharded WHERE email = $1").into(),
        Bind::new_params("__test_empty", &[Parameter::new(b"")]).into(),
        Execute::new().into(),
        Sync.into(),
    ]);

    assert!(command.route().is_read());
}
