use once_cell::sync::Lazy;
use pg_query::protobuf::ScanToken;
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

fn get_matched_value<'a>(caps: &'a regex::Captures<'a>) -> Option<&'a str> {
    caps.get(1)
        .or_else(|| caps.get(2))
        .or_else(|| caps.get(3))
        .map(|m| m.as_str())
}

/// Extract shard number from a comment. Additionally returns the entire
/// comment string if it exists.
///
/// Comment style for the shard metadata uses the C-style comments (not SQL comments!)
/// as to allow the comment to appear anywhere in the query.
///
/// See [`SHARD`] and [`SHARDING_KEY`] for the style of comment we expect.
///
pub fn parse_comment(
    query: &str,
    schema: &ShardingSchema,
) -> Result<(Option<Shard>, Option<Role>, Option<String>), Error> {
    let tokens = match schema.query_parser_engine {
        QueryParserEngine::PgQueryProtobuf => scan(query),
        QueryParserEngine::PgQueryRaw => scan_raw(query),
    }
    .map_err(Error::PgQuery)?;
    let mut shard = None;
    let mut role = None;
    let mut filtered_query = None;

    for token in tokens.tokens.iter() {
        if token.token == Token::CComment as i32 {
            let comment = &query[token.start as usize..token.end as usize];
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
                        shard = Some(schema.shard().into());
                    } else {
                        let ctx = ContextBuilder::infer_from_from_and_config(sharding_key, schema)?
                            .shards(schema.shards)
                            .build()?;
                        shard = Some(ctx.apply()?);
                    }
                }
            }
            if let Some(cap) = SHARD.captures(comment) {
                if let Some(s) = cap.get(1) {
                    shard = Some(
                        s.as_str()
                            .parse::<usize>()
                            .ok()
                            .map(Shard::Direct)
                            .unwrap_or(Shard::All),
                    );
                }
            }
        }
    }

    if has_comments(&tokens.tokens) {
        filtered_query = Some(remove_comments(
            query,
            &tokens.tokens,
            Some(&[&SHARD, &*SHARDING_KEY, &ROLE]),
        )?);
    }

    Ok((shard, role, filtered_query))
}

pub fn has_comments(tokenized_query: &Vec<ScanToken>) -> bool {
    tokenized_query
        .iter()
        .any(|st| st.token == Token::CComment as i32 || st.token == Token::SqlComment as i32)
}

pub fn remove_comments(
    query: &str,
    tokenized_query: &Vec<ScanToken>,
    except: Option<&[&Regex]>,
) -> Result<String, Error> {
    let mut cursor = 0;
    let mut out = String::with_capacity(query.len());
    let mut metadata = Vec::with_capacity(3);

    for st in tokenized_query {
        let start = st.start as usize;
        let end = st.end as usize;

        out.push_str(&query[cursor..start]);

        match st.token {
            t if t == Token::CComment as i32 => {
                let comment = &query[start..end];

                if let Some(except) = except {
                    let m = keep_only_matching(comment, except);

                    metadata.push(m.to_string());
                }
            }
            _ => {
                out.push_str(&query[start..end]);
            }
        }

        cursor = end;
    }

    if cursor < query.len() {
        out.push_str(&query[cursor..]);
    }

    metadata.sort_unstable();
    out.insert_str(0, &metadata.join(""));

    Ok(out)
}

fn keep_only_matching(comment: &str, regs: &[&Regex]) -> String {
    let mut out = String::new();

    for reg in regs {
        for m in reg.find_iter(comment) {
            out.push_str(m.as_str());
        }
    }

    out
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
        let result = parse_comment(query, &schema).unwrap();
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
        let result = parse_comment(query, &schema).unwrap();
        assert_eq!(result.0, Some(Shard::Direct(2)));
        assert_eq!(result.1, Some(Role::Replica));
    }

    #[test]
    fn test_remove_comments_no_exceptions() {
        let query = "SELECT * FROM table /* comment */ WHERE id = 1";
        let tokens = scan(query).unwrap().tokens;

        let result = remove_comments(query, &tokens, None).unwrap();

        assert_eq!(result, "SELECT * FROM table  WHERE id = 1");
    }

    #[test]
    fn test_remove_comments_with_exceptions() {
        let query = "SELECT /* comment */ * FROM table /* pgdog_shard: 4 comment */ WHERE id = 1";
        let tokens = scan(query).unwrap().tokens;

        let result = remove_comments(query, &tokens, Some(&[&SHARD])).unwrap();

        assert_eq!(result, "pgdog_shard: 4SELECT  * FROM table  WHERE id = 1");
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
        let result = parse_comment(query, &schema).unwrap();
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
        let result = parse_comment(query, &schema).unwrap();
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
        let result = parse_comment(query, &schema).unwrap();
        assert_eq!(result.1, None);
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
        let result = parse_comment(query, &schema).unwrap();
        assert_eq!(result.0, Some(Shard::Direct(1)));
    }
}
