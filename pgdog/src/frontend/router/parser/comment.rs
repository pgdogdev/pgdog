use memchr::memmem;
use once_cell::sync::Lazy;
use pg_query::scan_raw;
use pg_query::{protobuf::Token, scan};
use pgdog_config::QueryParserEngine;
use regex::Regex;

use crate::backend::ShardingSchema;
use crate::config::database::Role;
use crate::frontend::router::sharding::ContextBuilder;

use super::super::parser::Shard;
use super::Error;

static SHARD: Lazy<Regex> = Lazy::new(|| Regex::new(r#"pgdog_shard: *([0-9]+)"#).unwrap());
static SHARDING_KEY: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"pgdog_sharding_key: *(?:"([^"]*)"|'([^']*)'|([0-9a-zA-Z-]+))"#).unwrap()
});
static ROLE: Lazy<Regex> = Lazy::new(|| Regex::new(r#"pgdog_role: *(primary|replica)"#).unwrap());

pub(crate) struct QueryAndComment {
    pub(crate) query: String,
    #[cfg(test)]
    pub(crate) comment: String,
    pub(crate) role: Option<Role>,
    pub(crate) shard: Option<Shard>,
}

fn is_edge_whitespace(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\r')
}

/// Strip a leading `/* ... */` block comment, allowing whitespace on either
/// side. Returns `(rest, comment)`.
fn leading_block_comment(q: &str) -> Option<(&str, &str)> {
    let trimmed = q.trim_start_matches(is_edge_whitespace);
    if !trimmed.starts_with("/*") {
        return None;
    }
    // Match the nearest `*/` after the opening `/*`; that's the correct
    // (shortest) comment. Searching from byte 2 avoids matching a `*/` that
    // overlaps with the opening `/*`, e.g. in `/*/abc*/`.
    let body_end = memmem::find(&trimmed.as_bytes()[2..], b"*/")?;
    let comment_end = 2 + body_end + 2;
    let comment = &trimmed[..comment_end];
    let rest = trimmed[comment_end..].trim_start_matches(is_edge_whitespace);
    Some((rest, comment))
}

/// Strip a trailing `/* ... */` block comment, allowing whitespace on either
/// side. Returns `(rest, comment)`.
fn trailing_block_comment(q: &str) -> Option<(&str, &str)> {
    let trimmed = q.trim_end_matches(is_edge_whitespace);
    if !trimmed.ends_with("*/") {
        return None;
    }
    let inner = &trimmed[..trimmed.len() - 2];
    let start = memmem::rfind(inner.as_bytes(), b"/*")?;
    // If the body contains `*/`, the trailing `*/` pairs with an earlier
    // `/*` we can't see from here — reject.
    if memmem::find(inner[start + 2..].as_bytes(), b"*/").is_some() {
        return None;
    }
    let comment = &trimmed[start..];
    let rest = q[..start].trim_end_matches(is_edge_whitespace);
    Some((rest, comment))
}

pub(crate) fn parse_edge_comment(
    query: &str,
    schema: &ShardingSchema,
) -> Result<Option<QueryAndComment>, Error> {
    let Some((stripped, comment)) =
        leading_block_comment(query).or_else(|| trailing_block_comment(query))
    else {
        return Ok(None);
    };

    let (shard, role) = shard_role_from_comment(comment, schema)?;

    Ok(Some(QueryAndComment {
        query: stripped.to_string(),
        #[cfg(test)]
        comment: comment.to_string(),
        shard,
        role,
    }))
}

fn get_matched_value<'a>(caps: &'a regex::Captures<'a>) -> Option<&'a str> {
    caps.get(1)
        .or_else(|| caps.get(2))
        .or_else(|| caps.get(3))
        .map(|m| m.as_str())
}

/// Extract shard number from a comment.
///
/// Comment style uses the C-style comments (not SQL comments!)
/// as to allow the comment to appear anywhere in the query.
///
/// See [`SHARD`] and [`SHARDING_KEY`] for the style of comment we expect.
///
pub fn comment(
    query: &str,
    schema: &ShardingSchema,
) -> Result<(Option<Shard>, Option<Role>), Error> {
    let tokens = match schema.query_parser_engine {
        QueryParserEngine::PgQueryProtobuf => scan(query),
        QueryParserEngine::PgQueryRaw => scan_raw(query),
    }
    .map_err(Error::PgQuery)?;
    let mut role = None;

    for token in tokens.tokens.iter() {
        if token.token == Token::CComment as i32 {
            let comment = &query[token.start as usize..token.end as usize];
            let (shard, comment_role) = shard_role_from_comment(comment, schema)?;
            role = comment_role;

            if shard.is_some() {
                return Ok((shard, role));
            }
        }
    }

    Ok((None, role))
}

fn shard_role_from_comment(
    comment: &str,
    schema: &ShardingSchema,
) -> Result<(Option<Shard>, Option<Role>), Error> {
    let mut role = None;

    if let Some(cap) = ROLE.captures(comment) {
        if let Some(r) = cap.get(1) {
            match r.as_str() {
                "primary" => role = Some(Role::Primary),
                "replica" => role = Some(Role::Replica),
                _ => return Err(Error::RegexError),
            }
        }
    }
    if let Some(cap) = SHARDING_KEY.captures(comment) {
        if let Some(sharding_key) = get_matched_value(&cap) {
            if let Some(schema) = schema.schemas.get(Some(sharding_key.into())) {
                return Ok((Some(schema.shard().into()), role));
            }
            let ctx = ContextBuilder::infer_from_from_and_config(sharding_key, schema)?
                .shards(schema.shards)
                .build()?;
            return Ok((Some(ctx.apply()?), role));
        }
    }
    if let Some(cap) = SHARD.captures(comment) {
        if let Some(shard) = cap.get(1) {
            return Ok((
                Some(
                    shard
                        .as_str()
                        .parse::<usize>()
                        .ok()
                        .map(Shard::Direct)
                        .unwrap_or(Shard::All),
                ),
                role,
            ));
        }
    }

    Ok((None, role))
}

#[cfg(test)]
mod tests {
    use pgdog_config::SystemCatalogsBehavior;

    use super::*;

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
        use crate::backend::ShardedTables;

        let schema = ShardingSchema {
            shards: 2,
            tables: ShardedTables::new(vec![], vec![], false, SystemCatalogsBehavior::default()),
            ..Default::default()
        };

        let query = "SELECT * FROM users /* pgdog_role: primary */";
        let result = comment(query, &schema).unwrap();
        assert_eq!(result.1, Some(Role::Primary));
    }

    #[test]
    fn test_role_and_shard_detection() {
        use crate::backend::ShardedTables;

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
        use crate::backend::ShardedTables;

        let schema = ShardingSchema {
            shards: 2,
            tables: ShardedTables::new(vec![], vec![], false, SystemCatalogsBehavior::default()),
            ..Default::default()
        };

        let query = "SELECT * FROM users /* pgdog_role: replica */";
        let result = comment(query, &schema).unwrap();
        assert_eq!(result.1, Some(Role::Replica));
    }

    #[test]
    fn test_invalid_role_detection() {
        use crate::backend::ShardedTables;

        let schema = ShardingSchema {
            shards: 2,
            tables: ShardedTables::new(vec![], vec![], false, SystemCatalogsBehavior::default()),
            ..Default::default()
        };

        let query = "SELECT * FROM users /* pgdog_role: invalid */";
        let result = comment(query, &schema).unwrap();
        assert_eq!(result.1, None);
    }

    #[test]
    fn test_no_role_comment() {
        use crate::backend::ShardedTables;

        let schema = ShardingSchema {
            shards: 2,
            tables: ShardedTables::new(vec![], vec![], false, SystemCatalogsBehavior::default()),
            ..Default::default()
        };

        let query = "SELECT * FROM users";
        let result = comment(query, &schema).unwrap();
        assert_eq!(result.1, None);
    }

    fn test_schema() -> ShardingSchema {
        use crate::backend::ShardedTables;

        ShardingSchema {
            shards: 2,
            tables: ShardedTables::new(vec![], vec![], false, SystemCatalogsBehavior::default()),
            ..Default::default()
        }
    }

    #[test]
    fn test_remove_comment_leading() {
        let schema = test_schema();
        let qac = parse_edge_comment("/* hello */ SELECT 1", &schema)
            .unwrap()
            .unwrap();
        assert_eq!(qac.query, "SELECT 1");
        assert_eq!(qac.comment, "/* hello */");
    }

    #[test]
    fn test_remove_comment_trailing() {
        let schema = test_schema();
        let qac = parse_edge_comment("SELECT 1 /* hello */", &schema)
            .unwrap()
            .unwrap();
        assert_eq!(qac.query, "SELECT 1");
        assert_eq!(qac.comment, "/* hello */");
    }

    #[test]
    fn test_remove_comment_no_surrounding_whitespace() {
        let schema = test_schema();
        let qac = parse_edge_comment("/*a*/SELECT 1", &schema)
            .unwrap()
            .unwrap();
        assert_eq!(qac.query, "SELECT 1");
        assert_eq!(qac.comment, "/*a*/");

        let qac = parse_edge_comment("SELECT 1/*b*/", &schema)
            .unwrap()
            .unwrap();
        assert_eq!(qac.query, "SELECT 1");
        assert_eq!(qac.comment, "/*b*/");
    }

    #[test]
    fn test_remove_comment_none() {
        let schema = test_schema();
        assert!(parse_edge_comment("SELECT 1", &schema).unwrap().is_none());
        assert!(parse_edge_comment("", &schema).unwrap().is_none());
        assert!(parse_edge_comment("   ", &schema).unwrap().is_none());
    }

    #[test]
    fn test_remove_comment_in_middle_not_matched() {
        let schema = test_schema();
        // Comment sandwiched between query content is not at the start or end
        // so the regex must not match it.
        assert!(parse_edge_comment("SELECT /* inline */ 1", &schema)
            .unwrap()
            .is_none());
        assert!(parse_edge_comment("SELECT 1 /* a */ FROM t", &schema)
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_remove_comment_multiline_block() {
        let schema = test_schema();
        let query = "/* line1\n line2\n line3 */ SELECT 1";
        let qac = parse_edge_comment(query, &schema).unwrap().unwrap();
        assert_eq!(qac.query, "SELECT 1");
        assert_eq!(qac.comment, "/* line1\n line2\n line3 */");
    }

    #[test]
    fn test_remove_comment_with_surrounding_newlines() {
        let schema = test_schema();
        let qac = parse_edge_comment("\n\t/* hi */\n SELECT 1", &schema)
            .unwrap()
            .unwrap();
        assert_eq!(qac.query, "SELECT 1");
        assert_eq!(qac.comment, "/* hi */");

        let qac = parse_edge_comment("SELECT 1\n /* hi */\n\t", &schema)
            .unwrap()
            .unwrap();
        assert_eq!(qac.query, "SELECT 1");
        assert_eq!(qac.comment, "/* hi */");
    }

    #[test]
    fn test_remove_comment_only_comment() {
        let schema = test_schema();
        let qac = parse_edge_comment("/* only */", &schema).unwrap().unwrap();
        assert_eq!(qac.query, "");
        assert_eq!(qac.comment, "/* only */");
    }

    #[test]
    fn test_remove_comment_preserves_inner_content() {
        let schema = test_schema();
        let qac = parse_edge_comment("SELECT 'a/*b*/c' /* pgdog_shard: 1 */", &schema)
            .unwrap()
            .unwrap();
        assert_eq!(qac.query, "SELECT 'a/*b*/c'");
        assert_eq!(qac.comment, "/* pgdog_shard: 1 */");
        assert_eq!(qac.shard, Some(Shard::Direct(1)));
    }

    #[test]
    fn test_remove_comment_non_greedy() {
        let schema = test_schema();
        // Ensure the regex doesn't greedily span across two separate comments.
        let qac = parse_edge_comment("/* first */ SELECT /* mid */ 1", &schema)
            .unwrap()
            .unwrap();
        assert_eq!(qac.query, "SELECT /* mid */ 1");
        assert_eq!(qac.comment, "/* first */");

        let qac = parse_edge_comment("SELECT /* mid */ 1 /* last */", &schema)
            .unwrap()
            .unwrap();
        assert_eq!(qac.query, "SELECT /* mid */ 1");
        assert_eq!(qac.comment, "/* last */");
    }

    #[test]
    fn test_remove_comment_empty_body() {
        let schema = test_schema();
        let qac = parse_edge_comment("/**/ SELECT 1", &schema)
            .unwrap()
            .unwrap();
        assert_eq!(qac.query, "SELECT 1");
        assert_eq!(qac.comment, "/**/");
    }

    #[test]
    fn test_remove_comment_pgdog_directive() {
        let schema = test_schema();
        let qac = parse_edge_comment("SELECT * FROM users /* pgdog_role: primary */", &schema)
            .unwrap()
            .unwrap();
        assert_eq!(qac.query, "SELECT * FROM users");
        assert_eq!(qac.comment, "/* pgdog_role: primary */");
        assert_eq!(qac.role, Some(Role::Primary));
    }

    #[test]
    fn test_sharding_key_with_schema_name() {
        use crate::backend::replication::ShardedSchemas;
        use crate::backend::ShardedTables;
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
}
