use once_cell::sync::Lazy;
use pg_query::scan_raw;
use pg_query::{protobuf::Token, scan};
use pgdog_config::{CrossShardBackend, QueryParserEngine};
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
static BACKEND: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"pgdog_cross_shard_backend: fdw"#).unwrap());

fn get_matched_value<'a>(caps: &'a regex::Captures<'a>) -> Option<&'a str> {
    caps.get(1)
        .or_else(|| caps.get(2))
        .or_else(|| caps.get(3))
        .map(|m| m.as_str())
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct CommentRoute {
    pub shard: Option<Shard>,
    pub role: Option<Role>,
    pub cross_shard_backend: Option<CrossShardBackend>,
}

/// Extract shard number from a comment.
///
/// Comment style uses the C-style comments (not SQL comments!)
/// as to allow the comment to appear anywhere in the query.
///
/// See [`SHARD`] and [`SHARDING_KEY`] for the style of comment we expect.
///
pub fn comment(query: &str, schema: &ShardingSchema) -> Result<CommentRoute, Error> {
    let tokens = match schema.query_parser_engine {
        QueryParserEngine::PgQueryProtobuf => scan(query),
        QueryParserEngine::PgQueryRaw => scan_raw(query),
    }
    .map_err(Error::PgQuery)?;
    let mut comment_route = CommentRoute::default();

    for token in tokens.tokens.iter() {
        if token.token == Token::CComment as i32 {
            let comment = &query[token.start as usize..token.end as usize];
            if let Some(cap) = ROLE.captures(comment) {
                if let Some(r) = cap.get(1) {
                    match r.as_str() {
                        "primary" => comment_route.role = Some(Role::Primary),
                        "replica" => comment_route.role = Some(Role::Replica),
                        _ => return Err(Error::RegexError),
                    }
                }
            }
            if let Some(cap) = SHARDING_KEY.captures(comment) {
                if let Some(sharding_key) = get_matched_value(&cap) {
                    if let Some(schema) = schema.schemas.get(Some(sharding_key.into())) {
                        comment_route.shard = Some(schema.shard().into());
                    } else {
                        let ctx = ContextBuilder::infer_from_from_and_config(sharding_key, schema)?
                            .shards(schema.shards)
                            .build()?;
                        comment_route.shard = Some(ctx.apply()?);
                    }
                }
            } else if let Some(cap) = SHARD.captures(comment) {
                if let Some(shard) = cap.get(1) {
                    comment_route.shard = Some(
                        shard
                            .as_str()
                            .parse::<usize>()
                            .ok()
                            .map(Shard::Direct)
                            .unwrap_or(Shard::All),
                    );
                }
            }
            if let Some(_) = BACKEND.captures(comment) {
                comment_route.cross_shard_backend = Some(CrossShardBackend::Fdw);
            }
        }
    }

    Ok(comment_route)
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
        assert_eq!(result.role, Some(Role::Primary));
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
        assert_eq!(result.shard, Some(Shard::Direct(2)));
        assert_eq!(result.role, Some(Role::Replica));
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
        assert_eq!(result.role, Some(Role::Replica));
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
        assert_eq!(result.role, None);
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
        assert_eq!(result.role, None);
    }

    #[test]
    fn test_fdw_fallback() {
        let query = "/* pgdog_cross_shard_backend: fdw */ SELECT * FROM users";
        let result = comment(query, &ShardingSchema::default()).unwrap();
        assert_eq!(result.cross_shard_backend, Some(CrossShardBackend::Fdw));
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
        assert_eq!(result.shard, Some(Shard::Direct(1)));
    }
}
