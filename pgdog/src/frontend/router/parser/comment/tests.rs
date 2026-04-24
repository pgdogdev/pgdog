use pgdog_config::SystemCatalogsBehavior;

use crate::backend::ShardedTables;
use crate::backend::ShardingSchema;
use crate::config::database::Role;

use super::super::Shard;
use super::directive::{comment, get_matched_value, SHARDING_KEY};
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
    let result = comment(query, &schema).unwrap();
    assert_eq!(result.1, Some(Role::Primary));
}

#[test]
fn test_role_and_shard_detection() {
    let schema = ShardingSchema {
        shards: 3,
        tables: ShardedTables::new(vec![], vec![], false, SystemCatalogsBehavior::default()),
        ..Default::default()
    };

    let query = "SELECT * FROM users /* pgdog_role: replica pgdog_shard: 2 */";
    let result = comment(query, &schema).unwrap();
    assert_eq!(result.0, Some(Shard::Direct(2)));
    assert_eq!(result.1, Some(Role::Replica));
}

#[test]
fn test_replica_role_detection() {
    let schema = test_schema();
    let query = "SELECT * FROM users /* pgdog_role: replica */";
    let result = comment(query, &schema).unwrap();
    assert_eq!(result.1, Some(Role::Replica));
}

#[test]
fn test_invalid_role_detection() {
    let schema = test_schema();
    let query = "SELECT * FROM users /* pgdog_role: invalid */";
    let result = comment(query, &schema).unwrap();
    assert_eq!(result.1, None);
}

#[test]
fn test_no_role_comment() {
    let schema = test_schema();
    let query = "SELECT * FROM users";
    let result = comment(query, &schema).unwrap();
    assert_eq!(result.1, None);
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
    let result = comment(query, &schema).unwrap();
    assert_eq!(result.0, Some(Shard::Direct(1)));
}
