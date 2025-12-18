use crate::config::RewriteMode;
use crate::frontend::router::parser::{Error, Shard};
use crate::frontend::Command;
use crate::net::messages::Parameter;

use super::setup::{QueryParserTest, *};

#[test]
fn test_update_sharding_key_errors_by_default() {
    let mut test = QueryParserTest::new().with_rewrite_mode(RewriteMode::Error);

    let result = test.try_execute(vec![Query::new(
        "UPDATE sharded SET id = id + 1 WHERE id = 1",
    )
    .into()]);

    assert!(
        matches!(result, Err(Error::ShardKeyUpdateViolation { .. })),
        "{result:?}"
    );
}

#[test]
fn test_update_sharding_key_ignore_mode_allows() {
    let mut test = QueryParserTest::new().with_rewrite_mode(RewriteMode::Ignore);

    let command = test.execute(vec![Query::new(
        "UPDATE sharded SET id = id + 1 WHERE id = 1",
    )
    .into()]);

    assert!(matches!(command, Command::Query(_)));
}

#[test]
fn test_update_sharding_key_rewrite_mode_not_supported() {
    let mut test = QueryParserTest::new().with_rewrite_mode(RewriteMode::Rewrite);

    let result = test.try_execute(vec![Query::new(
        "UPDATE sharded SET id = id + 1 WHERE id = 1",
    )
    .into()]);

    assert!(
        matches!(result, Err(Error::ShardKeyRewriteNotSupported { .. })),
        "{result:?}"
    );
}

#[test]
fn test_update_sharding_key_rewrite_plan_detected() {
    let mut test = QueryParserTest::new().with_rewrite_mode(RewriteMode::Rewrite);

    let command = test.execute(vec![
        Query::new("UPDATE sharded SET id = 11 WHERE id = 1").into()
    ]);

    match command {
        Command::ShardKeyRewrite(plan) => {
            assert_eq!(plan.table().name, "sharded");
            assert_eq!(plan.assignments().len(), 1);
            let assignment = &plan.assignments()[0];
            assert_eq!(assignment.column(), "id");
        }
        other => panic!("expected shard key rewrite plan, got {other:?}"),
    }
}

#[test]
fn test_update_sharding_key_rewrite_computes_new_shard() {
    let mut test = QueryParserTest::new().with_rewrite_mode(RewriteMode::Rewrite);

    let command = test.execute(vec![
        Query::new("UPDATE sharded SET id = 11 WHERE id = 1").into()
    ]);

    let plan = match command {
        Command::ShardKeyRewrite(plan) => plan,
        other => panic!("expected shard key rewrite plan, got {other:?}"),
    };

    assert!(plan.new_shard().is_some(), "new shard should be computed");
}

#[test]
fn test_update_sharding_key_rewrite_requires_parameter_values() {
    let mut test = QueryParserTest::new().with_rewrite_mode(RewriteMode::Rewrite);

    let result = test.try_execute(vec![
        Query::new("UPDATE sharded SET id = $1 WHERE id = 1").into()
    ]);

    assert!(
        matches!(result, Err(Error::MissingParameter(1))),
        "{result:?}"
    );
}

#[test]
fn test_update_sharding_key_rewrite_parameter_assignment_succeeds() {
    let mut test = QueryParserTest::new().with_rewrite_mode(RewriteMode::Rewrite);

    let command = test.execute(vec![
        Parse::named("__test_rewrite", "UPDATE sharded SET id = $1 WHERE id = 1").into(),
        Bind::new_params("__test_rewrite", &[Parameter::new(b"11")]).into(),
        Execute::new().into(),
        Sync.into(),
    ]);

    match command {
        Command::ShardKeyRewrite(plan) => {
            assert!(
                plan.new_shard().is_some(),
                "expected computed destination shard"
            );
            assert_eq!(plan.assignments().len(), 1);
        }
        other => panic!("expected shard key rewrite plan, got {other:?}"),
    }
}

#[test]
fn test_update_sharding_key_rewrite_self_assignment_falls_back() {
    let mut test = QueryParserTest::new().with_rewrite_mode(RewriteMode::Rewrite);

    let command = test.execute(vec![
        Query::new("UPDATE sharded SET id = id WHERE id = 1").into()
    ]);

    match command {
        Command::Query(route) => {
            assert!(matches!(route.shard(), Shard::Direct(_)));
        }
        other => panic!("expected standard update route, got {other:?}"),
    }
}

#[test]
fn test_update_sharding_key_rewrite_null_assignment_not_supported() {
    let mut test = QueryParserTest::new().with_rewrite_mode(RewriteMode::Rewrite);

    let result = test.try_execute(vec![Query::new(
        "UPDATE sharded SET id = NULL WHERE id = 1",
    )
    .into()]);

    assert!(
        matches!(result, Err(Error::ShardKeyRewriteNotSupported { .. })),
        "{result:?}"
    );
}
