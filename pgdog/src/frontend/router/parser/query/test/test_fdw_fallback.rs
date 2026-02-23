use crate::net::messages::Query;

use super::setup::*;

#[test]
fn test_fdw_fallback_comment() {
    let mut test = QueryParserTest::new().with_fdw_fallback();

    let command = test.execute(vec![Query::new(
        "/* pgdog_cross_shard_backend: fdw */ SELECT * FROM sharded ORDER BY id LIMIT 10 OFFSET 5",
    )
    .into()]);

    let route = command.route();
    assert!(route.is_fdw_fallback(),);
}

#[test]
fn test_fdw_fallback_comment_if_direct() {
    let mut test = QueryParserTest::new().with_fdw_fallback();

    let command = test.execute(vec![Query::new(
        "/* pgdog_cross_shard_backend: fdw */ SELECT * FROM sharded WHERE id = 1",
    )
    .into()]);

    let route = command.route();
    assert!(route.is_fdw_fallback(),);
}

// =============================================================================
// Config verification tests
// =============================================================================

/// FDW fallback should NOT be triggered when cross_shard_backend is not
/// configured for FDW (default is Pgdog).
#[test]
#[ignore]
fn test_fdw_fallback_requires_config() {
    // Without with_fdw_fallback(), cross_shard_backend defaults to Pgdog
    let mut test = QueryParserTest::new();

    // This query would normally trigger FDW fallback (OFFSET > 0)
    let command = test.execute(vec![Query::new(
        "SELECT * FROM sharded ORDER BY id LIMIT 10 OFFSET 5",
    )
    .into()]);

    let route = command.route();
    assert!(
        !route.is_fdw_fallback(),
        "FDW fallback should NOT be triggered when cross_shard_backend is Pgdog (default)"
    );
}

// =============================================================================
// OFFSET tests
// =============================================================================

/// Cross-shard SELECT with OFFSET > 0 should trigger FDW fallback
/// because OFFSET cannot be correctly applied across shards without
/// fetching all rows first.
#[test]
#[ignore]
fn test_cross_shard_offset_triggers_fdw_fallback() {
    let mut test = QueryParserTest::new().with_fdw_fallback();

    // This query goes to all shards (no sharding key) with OFFSET
    let command = test.execute(vec![Query::new(
        "SELECT * FROM sharded ORDER BY id LIMIT 10 OFFSET 5",
    )
    .into()]);

    let route = command.route();
    assert!(
        route.is_fdw_fallback(),
        "Cross-shard query with OFFSET > 0 should trigger FDW fallback"
    );
}

/// Cross-shard SELECT with OFFSET = 0 should NOT trigger FDW fallback
#[test]
#[ignore]
fn test_cross_shard_offset_zero_no_fdw_fallback() {
    let mut test = QueryParserTest::new().with_fdw_fallback();

    let command = test.execute(vec![Query::new(
        "SELECT * FROM sharded ORDER BY id LIMIT 10 OFFSET 0",
    )
    .into()]);

    let route = command.route();
    assert!(
        !route.is_fdw_fallback(),
        "Cross-shard query with OFFSET = 0 should NOT trigger FDW fallback"
    );
}

/// Direct-to-shard SELECT with OFFSET should NOT trigger FDW fallback
#[test]
#[ignore]
fn test_direct_shard_offset_no_fdw_fallback() {
    let mut test = QueryParserTest::new().with_fdw_fallback();

    // This query goes to a specific shard (has sharding key)
    let command = test.execute(vec![Query::new(
        "SELECT * FROM sharded WHERE id = 1 ORDER BY id LIMIT 10 OFFSET 5",
    )
    .into()]);

    let route = command.route();
    assert!(
        !route.is_fdw_fallback(),
        "Direct-to-shard query with OFFSET should NOT trigger FDW fallback"
    );
}

// =============================================================================
// CTE tests
// =============================================================================

/// CTE that references an unsharded table without a sharding key should trigger
/// FDW fallback when the main query is cross-shard.
#[test]
#[ignore]
fn test_cte_unsharded_table_triggers_fdw_fallback() {
    let mut test = QueryParserTest::new().with_fdw_fallback();

    // `users` is not in the sharded tables config, making it "unsharded"
    // The CTE has no sharding key, so this should trigger FDW fallback
    let command = test.execute(vec![Query::new(
        "WITH user_data AS (SELECT * FROM users WHERE email = 'test@test.com')
         SELECT s.* FROM sharded s JOIN user_data u ON s.value = u.id",
    )
    .into()]);

    let route = command.route();
    assert!(
        route.is_fdw_fallback(),
        "CTE with unsharded table and no sharding key should trigger FDW fallback"
    );
}

/// CTE that only references sharded tables should NOT trigger FDW fallback
#[test]
#[ignore]
fn test_cte_sharded_table_no_fdw_fallback() {
    let mut test = QueryParserTest::new().with_fdw_fallback();

    // CTE references only the sharded table
    let command = test.execute(vec![Query::new(
        "WITH shard_data AS (SELECT * FROM sharded WHERE id = 5)
         SELECT * FROM shard_data",
    )
    .into()]);

    let route = command.route();
    assert!(
        !route.is_fdw_fallback(),
        "CTE with sharded table and sharding key should NOT trigger FDW fallback"
    );
}

/// CTE that only references omnisharded tables should NOT trigger FDW fallback
#[test]
#[ignore]
fn test_cte_omnisharded_table_no_fdw_fallback() {
    let mut test = QueryParserTest::new().with_fdw_fallback();

    // CTE references only omnisharded table
    let command = test.execute(vec![Query::new(
        "WITH omni_data AS (SELECT * FROM sharded_omni WHERE id = 1)
         SELECT * FROM omni_data",
    )
    .into()]);

    let route = command.route();
    assert!(
        !route.is_fdw_fallback(),
        "CTE with omnisharded table should NOT trigger FDW fallback"
    );
}

// =============================================================================
// Subquery tests
// =============================================================================

/// Subquery in FROM that references unsharded table without sharding key
/// should trigger FDW fallback when main query is cross-shard.
#[test]
#[ignore]
fn test_subquery_unsharded_table_triggers_fdw_fallback() {
    let mut test = QueryParserTest::new().with_fdw_fallback();

    // Subquery references unsharded table without sharding key
    let command = test.execute(vec![Query::new(
        "SELECT s.* FROM sharded s
         JOIN (SELECT * FROM users WHERE active = true) u ON s.value = u.id",
    )
    .into()]);

    let route = command.route();
    assert!(
        route.is_fdw_fallback(),
        "Subquery with unsharded table should trigger FDW fallback"
    );
}

/// Subquery with correlated reference to outer sharding key should NOT trigger
/// FDW fallback (inherits sharding context from outer query).
#[test]
#[ignore]
fn test_subquery_correlated_no_fdw_fallback() {
    let mut test = QueryParserTest::new().with_fdw_fallback();

    // Correlated subquery references outer query's sharded column
    let command = test.execute(vec![Query::new(
        "SELECT * FROM sharded s WHERE s.id = 5 AND EXISTS (
            SELECT 1 FROM sharded_omni o WHERE o.id = s.id
        )",
    )
    .into()]);

    let route = command.route();
    assert!(
        !route.is_fdw_fallback(),
        "Correlated subquery with sharding key in outer query should NOT trigger FDW fallback"
    );
}

// =============================================================================
// Edge case tests
// =============================================================================

/// Multiple CTEs where one is safe (sharded table) and one is unsafe (unsharded
/// table) should trigger FDW fallback when there's no sharding key.
#[test]
#[ignore]
fn test_multiple_ctes_mixed_safe_unsafe_triggers_fallback() {
    let mut test = QueryParserTest::new().with_fdw_fallback();

    // First CTE uses sharded table (safe), second CTE uses unsharded table (unsafe)
    // No sharding key in either CTE, so unsafe CTE triggers FDW fallback
    let command = test.execute(vec![Query::new(
        "WITH safe_data AS (SELECT * FROM sharded),
              unsafe_data AS (SELECT * FROM users WHERE active = true)
         SELECT s.*, u.* FROM safe_data s, unsafe_data u",
    )
    .into()]);

    let route = command.route();
    assert!(
        route.is_fdw_fallback(),
        "Multiple CTEs with one unsafe table should trigger FDW fallback"
    );
}

/// Nested subqueries where the innermost references an unsharded table
/// should trigger FDW fallback.
#[test]
#[ignore]
fn test_deeply_nested_subquery_unsharded_triggers_fallback() {
    let mut test = QueryParserTest::new().with_fdw_fallback();

    // Three levels deep: outer query -> subquery -> subquery with unsharded table
    let command = test.execute(vec![Query::new(
        "SELECT * FROM sharded WHERE value IN (
            SELECT id FROM sharded_omni WHERE id IN (
                SELECT id FROM users WHERE active = true
            )
        )",
    )
    .into()]);

    let route = command.route();
    assert!(
        route.is_fdw_fallback(),
        "Deeply nested subquery with unsharded table should trigger FDW fallback"
    );
}

/// JOIN inside a subquery mixing sharded and unsharded tables should trigger
/// FDW fallback.
#[test]
#[ignore]
fn test_subquery_join_mixed_tables_triggers_fallback() {
    let mut test = QueryParserTest::new().with_fdw_fallback();

    // Subquery JOINs sharded and unsharded tables
    let command = test.execute(vec![Query::new(
        "SELECT * FROM sharded WHERE id IN (
            SELECT s.id FROM sharded s
            JOIN users u ON s.value = u.id
            WHERE u.active = true
        )",
    )
    .into()]);

    let route = command.route();
    assert!(
        route.is_fdw_fallback(),
        "Subquery JOIN mixing sharded and unsharded tables should trigger FDW fallback"
    );
}

/// OFFSET with bind parameter should trigger FDW fallback when value > 0.
#[test]
#[ignore]
fn test_offset_bind_parameter_triggers_fallback() {
    use crate::net::messages::Parameter;

    let mut test = QueryParserTest::new().with_fdw_fallback();

    // OFFSET using $1 bind parameter with value 5
    let command = test.execute(vec![
        Parse::named(
            "__offset_test",
            "SELECT * FROM sharded ORDER BY id LIMIT 10 OFFSET $1",
        )
        .into(),
        Bind::new_params("__offset_test", &[Parameter::new(b"5")]).into(),
        Execute::new().into(),
        Sync.into(),
    ]);

    let route = command.route();
    assert!(
        route.is_fdw_fallback(),
        "Cross-shard query with OFFSET bind parameter > 0 should trigger FDW fallback"
    );
}

/// Schema-qualified unsharded table should still trigger FDW fallback.
#[test]
#[ignore]
fn test_schema_qualified_unsharded_triggers_fallback() {
    let mut test = QueryParserTest::new().with_fdw_fallback();

    // Using schema-qualified name for unsharded table
    let command = test.execute(vec![Query::new(
        "WITH user_data AS (SELECT * FROM public.users WHERE email = 'test@test.com')
         SELECT s.* FROM sharded s JOIN user_data u ON s.value = u.id",
    )
    .into()]);

    let route = command.route();
    assert!(
        route.is_fdw_fallback(),
        "Schema-qualified unsharded table should trigger FDW fallback"
    );
}

// =============================================================================
// Window function tests
// =============================================================================

/// Cross-shard query with window function should trigger FDW fallback
/// because window functions can't be correctly merged across shards.
#[test]
#[ignore]
fn test_window_function_cross_shard_triggers_fallback() {
    let mut test = QueryParserTest::new().with_fdw_fallback();

    // ROW_NUMBER() without sharding key = cross-shard
    let command = test.execute(vec![Query::new(
        "SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM sharded",
    )
    .into()]);

    let route = command.route();
    assert!(
        route.is_fdw_fallback(),
        "Cross-shard query with window function should trigger FDW fallback"
    );
}

/// Direct-to-shard query with window function should NOT trigger FDW fallback.
#[test]
#[ignore]
fn test_window_function_single_shard_no_fallback() {
    let mut test = QueryParserTest::new().with_fdw_fallback();

    // ROW_NUMBER() with sharding key = single shard, no fallback needed
    let command = test.execute(vec![Query::new(
        "SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM sharded WHERE id = 1",
    )
    .into()]);

    let route = command.route();
    assert!(
        !route.is_fdw_fallback(),
        "Single-shard query with window function should NOT trigger FDW fallback"
    );
}

/// Multiple window functions in cross-shard query should trigger FDW fallback.
#[test]
#[ignore]
fn test_multiple_window_functions_triggers_fallback() {
    let mut test = QueryParserTest::new().with_fdw_fallback();

    let command = test.execute(vec![Query::new(
        "SELECT id,
                ROW_NUMBER() OVER (ORDER BY id) as rn,
                RANK() OVER (PARTITION BY email ORDER BY id) as rnk
         FROM sharded",
    )
    .into()]);

    let route = command.route();
    assert!(
        route.is_fdw_fallback(),
        "Cross-shard query with multiple window functions should trigger FDW fallback"
    );
}

/// Window function in subquery should trigger FDW fallback for cross-shard.
#[test]
#[ignore]
fn test_window_function_in_subquery_triggers_fallback() {
    let mut test = QueryParserTest::new().with_fdw_fallback();

    let command = test.execute(vec![Query::new(
        "SELECT * FROM (
            SELECT id, ROW_NUMBER() OVER (ORDER BY id) as rn FROM sharded
        ) sub WHERE rn <= 10",
    )
    .into()]);

    let route = command.route();
    assert!(
        route.is_fdw_fallback(),
        "Cross-shard subquery with window function should trigger FDW fallback"
    );
}
