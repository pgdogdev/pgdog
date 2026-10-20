use crate::frontend::Command;
use crate::frontend::router::parser::Shard;

use super::setup::{QueryParserTest, *};

#[test]
fn test_create_table() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("CREATE TABLE test_table (id SERIAL PRIMARY KEY, name TEXT)").into(),
    ]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::All);
}

#[test]
fn test_drop_table() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("DROP TABLE IF EXISTS test_table").into()]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::All);
}

#[test]
fn test_alter_table() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("ALTER TABLE sharded ADD COLUMN new_col TEXT").into(),
    ]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::All);
}

#[test]
fn test_create_index() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("CREATE INDEX idx_test ON sharded (email)").into(),
    ]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::All);
}

#[test]
fn test_drop_index() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("DROP INDEX IF EXISTS idx_test").into()]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::All);
}

#[test]
fn test_truncate() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("TRUNCATE TABLE sharded").into()]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::All);
}

#[test]
fn test_create_sequence() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("CREATE SEQUENCE test_seq").into()]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::All);
}

#[test]
fn test_vacuum() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("VACUUM sharded").into()]);

    assert!(command.route().is_write());
}

#[test]
fn test_analyze() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("ANALYZE sharded").into()]);

    assert!(command.route().is_write());
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
        let mut test = QueryParserTest::new();

        let command = test.execute(vec![Query::new(query).into()]);

        match command {
            Command::CommitTransaction { .. } => {}
            _ => panic!("expected CommitTransaction, for {query:?}, got {command:?}"),
        }
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
        let mut test = QueryParserTest::new();

        let command = test.execute(vec![Query::new(query).into()]);

        match command {
            Command::RollbackTransaction { .. } => {}
            _ => panic!("expected RollbackTransaction, for {query:?}, got {command:?}"),
        }
    }
}

// --- Schema-based sharding tests ---

#[test]
fn test_create_table_shard_0() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("CREATE TABLE shard_0.test_table (id SERIAL PRIMARY KEY, name TEXT)").into(),
    ]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(0));
}

#[test]
fn test_create_table_shard_1() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("CREATE TABLE shard_1.test_table (id SERIAL PRIMARY KEY, name TEXT)").into(),
    ]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(1));
}

#[test]
fn test_drop_table_shard_0() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("DROP TABLE IF EXISTS shard_0.test_table").into(),
    ]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(0));
}

#[test]
fn test_drop_table_shard_1() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("DROP TABLE IF EXISTS shard_1.test_table").into(),
    ]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(1));
}

#[test]
fn test_alter_table_shard_0() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("ALTER TABLE shard_0.sharded ADD COLUMN new_col TEXT").into(),
    ]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(0));
}

#[test]
fn test_alter_table_shard_1() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("ALTER TABLE shard_1.sharded ADD COLUMN new_col TEXT").into(),
    ]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(1));
}

#[test]
fn test_create_index_shard_0() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("CREATE INDEX idx_test ON shard_0.sharded (email)").into(),
    ]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(0));
}

#[test]
fn test_create_index_shard_1() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("CREATE INDEX idx_test ON shard_1.sharded (email)").into(),
    ]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(1));
}

#[test]
fn test_drop_index_shard_0() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("DROP INDEX IF EXISTS shard_0.idx_test").into(),
    ]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(0));
}

#[test]
fn test_drop_index_shard_1() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("DROP INDEX IF EXISTS shard_1.idx_test").into(),
    ]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(1));
}

#[test]
fn test_truncate_shard_0() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("TRUNCATE TABLE shard_0.sharded").into()]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(0));
}

#[test]
fn test_truncate_shard_1() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("TRUNCATE TABLE shard_1.sharded").into()]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(1));
}

#[test]
fn test_create_sequence_shard_0() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("CREATE SEQUENCE shard_0.test_seq").into()]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(0));
}

#[test]
fn test_create_sequence_shard_1() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("CREATE SEQUENCE shard_1.test_seq").into()]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(1));
}

#[test]
fn test_vacuum_shard_0() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("VACUUM shard_0.sharded").into()]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(0));
}

#[test]
fn test_vacuum_shard_1() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("VACUUM shard_1.sharded").into()]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(1));
}

#[test]
fn test_analyze_shard_0() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("ANALYZE shard_0.sharded").into()]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(0));
}

#[test]
fn test_analyze_shard_1() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("ANALYZE shard_1.sharded").into()]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(1));
}
