use once_cell::sync::Lazy;
use pg_query::{protobuf::Token, scan};
use regex::Regex;

use crate::backend::ShardingSchema;
use crate::frontend::router::sharding::ContextBuilder;

use super::super::parser::Shard;
use super::Error;

static SHARD: Lazy<Regex> = Lazy::new(|| Regex::new(r#"pgdog_shard: *([0-9]+)"#).unwrap());
static SHARDING_KEY: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"pgdog_sharding_key: *(?:"([^"]*)"|'([^']*)'|([0-9a-zA-Z-]+))"#).unwrap()
});

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
pub fn shard(query: &str, schema: &ShardingSchema) -> Result<Shard, Error> {
    let tokens = scan(query).map_err(Error::PgQuery)?;

    for token in tokens.tokens.iter() {
        if token.token == Token::CComment as i32 {
            let comment = &query[token.start as usize..token.end as usize];
            if let Some(cap) = SHARDING_KEY.captures(comment) {
                if let Some(sharding_key) = get_matched_value(&cap) {
                    let ctx = ContextBuilder::infer_from_from_and_config(sharding_key, schema)?
                        .shards(schema.shards)
                        .build()?;
                    return Ok(ctx.apply()?);
                }
            }
            if let Some(cap) = SHARD.captures(comment) {
                if let Some(shard) = cap.get(1) {
                    return Ok(shard
                        .as_str()
                        .parse::<usize>()
                        .ok()
                        .map(Shard::Direct)
                        .unwrap_or(Shard::All));
                }
            }
        }
    }

    Ok(Shard::All)
}

#[cfg(test)]
mod tests {
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
}
