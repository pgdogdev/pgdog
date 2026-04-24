use crate::frontend::router::parser::{Cache, Shard};

use super::setup::*;

#[test]
fn test_comment_pgdog_role_primary() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("/* pgdog_role: primary */ SELECT 1").into()]);

    assert!(command.route().is_write());
}

#[test]
fn test_comment_pgdog_shard() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("/* pgdog_shard: 1234 */ SELECT 1234").into()
    ]);

    assert_eq!(command.route().shard(), &Shard::Direct(1234));
}

#[test]
fn test_comment_pgdog_shard_extended() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Parse::named(
            "__test_comment",
            "/* pgdog_shard: 1234 */ SELECT * FROM sharded WHERE id = $1",
        )
        .into(),
        Bind::new_statement("__test_comment").into(),
        Execute::new().into(),
        Sync.into(),
    ]);

    assert_eq!(command.route().shard(), &Shard::Direct(1234));
}

/// A shard-commented query that requires a rewrite must not be stored in
/// the cache under the comment-stripped key. If it were, a subsequent
/// uncommented query with the same body would cache-hit and inherit the
/// rewrite plan built against the direct-shard variant.
///
/// `pgdog.unique_id()` is a reliable rewrite trigger: the rewriter replaces
/// the call with a `$N::bigint` parameter in extended protocol.
#[test]
fn test_shard_comment_with_rewrite_not_cached() {
    Cache::reset();
    let mut test = QueryParserTest::new();

    let stripped = "SELECT pgdog.unique_id()";
    let commented = "/* pgdog_shard: 0 */ SELECT pgdog.unique_id()";

    test.execute(vec![
        Parse::named("__shard_comment_rewrite", commented).into(),
        Bind::new_statement("__shard_comment_rewrite").into(),
        Execute::new().into(),
        Sync.into(),
    ]);

    let cached = Cache::queries();
    let poisoned = cached.keys().any(|k| k.as_str() == stripped);
    assert!(
        !poisoned,
        "shard-commented query with a non-empty rewrite plan must not be \
         cached under the stripped key; a subsequent uncommented lookup \
         would otherwise receive the direct-shard plan"
    );
}

/// Complement to the previous test: a shard-commented query whose rewrite
/// plan is empty can safely share a cache entry with its uncommented
/// counterpart, since there's no plan to poison. Verifies the cacheable
/// check isn't blanket-dropping every shard-commented query.
#[test]
fn test_shard_comment_without_rewrite_is_cached() {
    Cache::reset();
    let mut test = QueryParserTest::new();

    let stripped = "SELECT 1";
    let commented = "/* pgdog_shard: 0 */ SELECT 1";

    test.execute(vec![
        Parse::named("__shard_comment_no_rewrite", commented).into(),
        Bind::new_statement("__shard_comment_no_rewrite").into(),
        Execute::new().into(),
        Sync.into(),
    ]);

    let cached = Cache::queries();
    assert!(
        cached.keys().any(|k| k.as_str() == stripped),
        "shard-commented query with an empty rewrite plan must still be \
         cached under the stripped key"
    );
}
