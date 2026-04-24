use pgdog_config::SystemCatalogsBehavior;

use crate::backend::ShardedTables;
use crate::backend::ShardingSchema;
use crate::config::database::Role;

use super::super::Shard;
use super::directive::{get_matched_value, SHARDING_KEY};
use super::parse_edge_comment;

fn test_schema() -> ShardingSchema {
    ShardingSchema {
        shards: 2,
        tables: ShardedTables::new(vec![], vec![], false, SystemCatalogsBehavior::default()),
        ..Default::default()
    }
}

#[test]
fn test_sharding_key_regex() {
    // Test unquoted integer
    let comment = "/* pgdog_sharding_key: 123 */";
    let caps = SHARDING_KEY.captures(comment);
    assert!(caps.is_some());
    assert_eq!(get_matched_value(&caps.unwrap()).unwrap(), "123");

    // Test unquoted string
    let comment = "/* pgdog_sharding_key: user123 */";
    let caps = SHARDING_KEY.captures(comment);
    assert!(caps.is_some());
    assert_eq!(get_matched_value(&caps.unwrap()).unwrap(), "user123");

    // Test unquoted UUID
    let comment = "/* pgdog_sharding_key: 550e8400-e29b-41d4-a716-446655440000 */";
    let caps = SHARDING_KEY.captures(comment);
    assert!(caps.is_some());
    assert_eq!(
        get_matched_value(&caps.unwrap()).unwrap(),
        "550e8400-e29b-41d4-a716-446655440000"
    );

    // Test double quoted string
    let comment = r#"/* pgdog_sharding_key: "user with spaces" */"#;
    let caps = SHARDING_KEY.captures(comment);
    assert!(caps.is_some());
    assert_eq!(
        get_matched_value(&caps.unwrap()).unwrap(),
        "user with spaces"
    );

    // Test single quoted string
    let comment = "/* pgdog_sharding_key: 'another user' */";
    let caps = SHARDING_KEY.captures(comment);
    assert!(caps.is_some());
    assert_eq!(get_matched_value(&caps.unwrap()).unwrap(), "another user");

    // Test double quoted UUID
    let comment = r#"/* pgdog_sharding_key: "550e8400-e29b-41d4-a716-446655440000" */"#;
    let caps = SHARDING_KEY.captures(comment);
    assert!(caps.is_some());
    assert_eq!(
        get_matched_value(&caps.unwrap()).unwrap(),
        "550e8400-e29b-41d4-a716-446655440000"
    );

    // Test single quoted UUID
    let comment = "/* pgdog_sharding_key: '550e8400-e29b-41d4-a716-446655440000' */";
    let caps = SHARDING_KEY.captures(comment);
    assert!(caps.is_some());
    assert_eq!(
        get_matched_value(&caps.unwrap()).unwrap(),
        "550e8400-e29b-41d4-a716-446655440000"
    );

    // Test with spaces around key
    let comment = "/* pgdog_sharding_key:   abc-123   */";
    let caps = SHARDING_KEY.captures(comment);
    assert!(caps.is_some());
    assert_eq!(get_matched_value(&caps.unwrap()).unwrap(), "abc-123");
}

#[test]
fn test_primary_role_detection() {
    let schema = test_schema();
    let query = "SELECT * FROM users /* pgdog_role: primary */";
    let result = parse_edge_comment(query, &schema).unwrap();
    assert_eq!(result.role, Some(Role::Primary));
}

#[test]
fn test_role_and_shard_detection() {
    let schema = ShardingSchema {
        shards: 3,
        tables: ShardedTables::new(vec![], vec![], false, SystemCatalogsBehavior::default()),
        ..Default::default()
    };

    let query = "SELECT * FROM users /* pgdog_role: replica pgdog_shard: 2 */";
    let result = parse_edge_comment(query, &schema).unwrap();
    assert_eq!(result.shard, Some(Shard::Direct(2)));
    assert_eq!(result.role, Some(Role::Replica));
}

#[test]
fn test_replica_role_detection() {
    let schema = test_schema();
    let query = "SELECT * FROM users /* pgdog_role: replica */";
    let result = parse_edge_comment(query, &schema).unwrap();
    assert_eq!(result.role, Some(Role::Replica));
}

#[test]
fn test_invalid_role_detection() {
    let schema = test_schema();
    let query = "SELECT * FROM users /* pgdog_role: invalid */";
    let result = parse_edge_comment(query, &schema).unwrap();
    assert_eq!(result.role, None);
}

#[test]
fn test_no_role_comment() {
    let schema = test_schema();
    let query = "SELECT * FROM users";
    let result = parse_edge_comment(query, &schema).unwrap();
    assert_eq!(result.role, None);
}

#[test]
fn test_remove_comment_leading() {
    let schema = test_schema();
    let qac = parse_edge_comment("/* hello */ SELECT 1", &schema).unwrap();
    assert_eq!(qac.query, "SELECT 1");
    assert_eq!(qac.comment, "/* hello */");
}

#[test]
fn test_remove_comment_trailing() {
    let schema = test_schema();
    let qac = parse_edge_comment("SELECT 1 /* hello */", &schema).unwrap();
    assert_eq!(qac.query, "SELECT 1");
    assert_eq!(qac.comment, "/* hello */");
}

#[test]
fn test_remove_comment_no_surrounding_whitespace() {
    let schema = test_schema();
    let qac = parse_edge_comment("/*a*/SELECT 1", &schema).unwrap();
    assert_eq!(qac.query, "SELECT 1");
    assert_eq!(qac.comment, "/*a*/");

    let qac = parse_edge_comment("SELECT 1/*b*/", &schema).unwrap();
    assert_eq!(qac.query, "SELECT 1");
    assert_eq!(qac.comment, "/*b*/");
}

#[test]
fn test_remove_comment_none() {
    let schema = test_schema();
    assert_eq!(parse_edge_comment("SELECT 1", &schema).unwrap().comment, "");
    assert_eq!(parse_edge_comment("", &schema).unwrap().comment, "");
    assert_eq!(parse_edge_comment("   ", &schema).unwrap().comment, "");
}

#[test]
fn test_remove_comment_in_middle_not_matched() {
    let schema = test_schema();
    // Comment sandwiched between query content is not at the start or end
    // so the regex must not match it.
    assert_eq!(
        parse_edge_comment("SELECT /* inline */ 1", &schema)
            .unwrap()
            .comment,
        ""
    );
    assert_eq!(
        parse_edge_comment("SELECT 1 /* a */ FROM t", &schema)
            .unwrap()
            .comment,
        ""
    );
}

#[test]
fn test_remove_comment_multiline_block() {
    let schema = test_schema();
    let query = "/* line1\n line2\n line3 */ SELECT 1";
    let qac = parse_edge_comment(query, &schema).unwrap();
    assert_eq!(qac.query, "SELECT 1");
    assert_eq!(qac.comment, "/* line1\n line2\n line3 */");
}

#[test]
fn test_remove_comment_with_surrounding_newlines() {
    let schema = test_schema();
    let qac = parse_edge_comment("\n\t/* hi */\n SELECT 1", &schema).unwrap();
    assert_eq!(qac.query, "SELECT 1");
    assert_eq!(qac.comment, "/* hi */");

    let qac = parse_edge_comment("SELECT 1\n /* hi */\n\t", &schema).unwrap();
    assert_eq!(qac.query, "SELECT 1");
    assert_eq!(qac.comment, "/* hi */");
}

#[test]
fn test_remove_comment_only_comment() {
    let schema = test_schema();
    let qac = parse_edge_comment("/* only */", &schema).unwrap();
    assert_eq!(qac.query, "");
    assert_eq!(qac.comment, "/* only */");
}

#[test]
fn test_remove_comment_preserves_inner_content() {
    let schema = test_schema();
    let qac = parse_edge_comment("SELECT 'a/*b*/c' /* pgdog_shard: 1 */", &schema).unwrap();
    assert_eq!(qac.query, "SELECT 'a/*b*/c'");
    assert_eq!(qac.comment, "/* pgdog_shard: 1 */");
    assert_eq!(qac.shard, Some(Shard::Direct(1)));
}

#[test]
fn test_remove_comment_non_greedy() {
    let schema = test_schema();
    // Ensure the regex doesn't greedily span across two separate comments.
    let qac = parse_edge_comment("/* first */ SELECT /* mid */ 1", &schema).unwrap();
    assert_eq!(qac.query, "SELECT /* mid */ 1");
    assert_eq!(qac.comment, "/* first */");

    let qac = parse_edge_comment("SELECT /* mid */ 1 /* last */", &schema).unwrap();
    assert_eq!(qac.query, "SELECT /* mid */ 1");
    assert_eq!(qac.comment, "/* last */");
}

#[test]
fn test_remove_comment_empty_body() {
    let schema = test_schema();
    let qac = parse_edge_comment("/**/ SELECT 1", &schema).unwrap();
    assert_eq!(qac.query, "SELECT 1");
    assert_eq!(qac.comment, "/**/");
}

#[test]
fn test_remove_comment_multiple_leading() {
    let schema = test_schema();
    let qac = parse_edge_comment("/* a */ /* b */ SELECT 1", &schema).unwrap();
    assert_eq!(qac.query, "SELECT 1");
    assert_eq!(qac.comment, "/* a */ /* b */");

    let qac = parse_edge_comment("/* a *//* b */SELECT 1", &schema).unwrap();
    assert_eq!(qac.query, "SELECT 1");
    assert_eq!(qac.comment, "/* a *//* b */");

    let qac = parse_edge_comment("/* a */ /* pgdog_shard: 1 */ SELECT 1", &schema).unwrap();
    assert_eq!(qac.query, "SELECT 1");
    assert_eq!(qac.comment, "/* a */ /* pgdog_shard: 1 */");
    assert_eq!(qac.shard, Some(Shard::Direct(1)));
}

#[test]
fn test_remove_comment_multiple_trailing() {
    let schema = test_schema();
    let qac = parse_edge_comment("SELECT 1 /* a */ /* b */", &schema).unwrap();
    assert_eq!(qac.query, "SELECT 1");
    assert_eq!(qac.comment, "/* a */ /* b */");

    let qac = parse_edge_comment(
        "SELECT 1 /* pgdog_role: primary *//* pgdog_shard: 1 */",
        &schema,
    )
    .unwrap();
    assert_eq!(qac.query, "SELECT 1");
    assert_eq!(qac.comment, "/* pgdog_role: primary *//* pgdog_shard: 1 */");
    assert_eq!(qac.role, Some(Role::Primary));
    assert_eq!(qac.shard, Some(Shard::Direct(1)));
}

#[test]
fn test_remove_comment_unterminated_leading() {
    let schema = test_schema();
    let qac = parse_edge_comment("/* no end here SELECT 1", &schema).unwrap();
    assert_eq!(qac.query, "/* no end here SELECT 1");
    assert_eq!(qac.comment, "");
}

#[test]
fn test_remove_comment_valid_then_unterminated() {
    let schema = test_schema();
    let qac = parse_edge_comment("/* ok */ /* oops SELECT 1", &schema).unwrap();
    assert_eq!(qac.query, "/* oops SELECT 1");
    assert_eq!(qac.comment, "/* ok */");
}

#[test]
fn test_remove_comment_trailing_interior_poison() {
    let schema = test_schema();
    // The final `*/` has no matching `/*` — the body of a would-be trailing
    // comment already contains a `*/`, so nothing is extracted.
    let qac = parse_edge_comment("SELECT /* a */ b */", &schema).unwrap();
    assert_eq!(qac.query, "SELECT /* a */ b */");
    assert_eq!(qac.comment, "");
}

#[test]
fn test_remove_comment_trailing_clean_after_poison() {
    let schema = test_schema();
    // A clean trailing comment should still be consumed even if an earlier
    // part of the query looks poisoned.
    let qac = parse_edge_comment("SELECT /* a */ b */ /* c */", &schema).unwrap();
    assert_eq!(qac.query, "SELECT /* a */ b */");
    assert_eq!(qac.comment, "/* c */");
}

#[test]
fn test_remove_comment_three_leading() {
    let schema = test_schema();
    let qac = parse_edge_comment("/* a */ /* b */ /* c */ SELECT 1", &schema).unwrap();
    assert_eq!(qac.query, "SELECT 1");
    assert_eq!(qac.comment, "/* a */ /* b */ /* c */");
}

#[test]
fn test_remove_comment_three_trailing() {
    let schema = test_schema();
    let qac = parse_edge_comment("SELECT 1 /* a */ /* b */ /* c */", &schema).unwrap();
    assert_eq!(qac.query, "SELECT 1");
    assert_eq!(qac.comment, "/* a */ /* b */ /* c */");
}

#[test]
fn test_strips_both_leading_and_trailing() {
    let schema = test_schema();
    let qac = parse_edge_comment("/* lead */ SELECT 1 /* trail */", &schema).unwrap();
    assert_eq!(qac.query, "SELECT 1");
    assert_eq!(qac.comment, "/* lead */ /* trail */");
}

#[test]
fn test_strips_datadog_trailing_with_pgdog_leading() {
    let schema = test_schema();
    let qac = parse_edge_comment(
        "/* pgdog_shard: 0 */ SELECT 1 /*dddbs='postgres',dde='prod',traceparent='00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'*/",
        &schema,
    )
    .unwrap();
    assert_eq!(qac.query, "SELECT 1");
    assert_eq!(qac.shard, Some(Shard::Direct(0)));
}

#[test]
fn test_strips_directive_in_trailing_with_leading_noise() {
    let schema = test_schema();
    let qac = parse_edge_comment(
        "/*traceparent='00-abc-01'*/ SELECT 1 /* pgdog_shard: 1 */",
        &schema,
    )
    .unwrap();
    assert_eq!(qac.query, "SELECT 1");
    assert_eq!(qac.shard, Some(Shard::Direct(1)));
}

#[test]
fn test_leading_wins_on_conflicting_shard() {
    let schema = test_schema();
    let qac = parse_edge_comment(
        "/* pgdog_shard: 0 */ SELECT 1 /* pgdog_shard: 1 */",
        &schema,
    )
    .unwrap();
    assert_eq!(qac.query, "SELECT 1");
    assert_eq!(qac.shard, Some(Shard::Direct(0)));
}

#[test]
fn test_role_and_shard_split_across_sides() {
    let schema = test_schema();
    let qac = parse_edge_comment(
        "/* pgdog_role: primary */ SELECT 1 /* pgdog_shard: 1 */",
        &schema,
    )
    .unwrap();
    assert_eq!(qac.query, "SELECT 1");
    assert_eq!(qac.role, Some(Role::Primary));
    assert_eq!(qac.shard, Some(Shard::Direct(1)));
}

#[test]
fn test_strips_stacked_comments_on_both_sides() {
    let schema = test_schema();
    let qac = parse_edge_comment("/* a */ /* b */ SELECT 1 /* c */ /* d */", &schema).unwrap();
    assert_eq!(qac.query, "SELECT 1");
    assert_eq!(qac.comment, "/* a */ /* b */ /* c */ /* d */");
}

#[test]
fn test_remove_comment_delimiter_only_input() {
    let schema = test_schema();
    let qac = parse_edge_comment("/*", &schema).unwrap();
    assert_eq!(qac.query, "/*");
    assert_eq!(qac.comment, "");

    let qac = parse_edge_comment("*/", &schema).unwrap();
    assert_eq!(qac.query, "*/");
    assert_eq!(qac.comment, "");
}

#[test]
fn test_remove_comment_directive_in_first_of_stack() {
    let schema = test_schema();
    let qac = parse_edge_comment("/* pgdog_shard: 1 */ /* noise */ SELECT 1", &schema).unwrap();
    assert_eq!(qac.query, "SELECT 1");
    assert_eq!(qac.comment, "/* pgdog_shard: 1 */ /* noise */");
    assert_eq!(qac.shard, Some(Shard::Direct(1)));
}

#[test]
fn test_remove_comment_pgdog_directive() {
    let schema = test_schema();
    let qac = parse_edge_comment("SELECT * FROM users /* pgdog_role: primary */", &schema).unwrap();
    assert_eq!(qac.query, "SELECT * FROM users");
    assert_eq!(qac.comment, "/* pgdog_role: primary */");
    assert_eq!(qac.role, Some(Role::Primary));
}

#[test]
fn test_sharding_key_with_schema_name() {
    use crate::backend::replication::ShardedSchemas;
    use pgdog_config::sharding::ShardedSchema;

    let sales_schema = ShardedSchema {
        database: "test".to_string(),
        name: Some("sales".to_string()),
        shard: 1,
        all: false,
    };

    let schema = ShardingSchema {
        shards: 2,
        tables: ShardedTables::new(vec![], vec![], false, SystemCatalogsBehavior::default()),
        schemas: ShardedSchemas::new(vec![sales_schema]),
        ..Default::default()
    };

    let query = "SELECT * FROM users /* pgdog_sharding_key: sales */";
    let result = parse_edge_comment(query, &schema).unwrap();
    assert_eq!(result.shard, Some(Shard::Direct(1)));
}
