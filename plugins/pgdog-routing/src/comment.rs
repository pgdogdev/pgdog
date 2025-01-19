//! Parse shards/sharding keys from comments.

use once_cell::sync::Lazy;
use pg_query::{protobuf::Token, scan, Error};
use regex::Regex;
use uuid::Uuid;

use crate::sharding_function;

static SHARD: Lazy<Regex> = Lazy::new(|| Regex::new(r#"pgdog_shard: *([0-9]+)"#).unwrap());
static SHARDING_KEY: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"pgdog_sharding_key: *([0-9a-zA-Z]+)"#).unwrap());

/// Extract shard number from a comment.
///
/// Comment style uses the C-style comments (not SQL comments!)
/// as to allow the comment to appear anywhere in the query.
///
/// See [`SHARD`] and [`SHARDING_KEY`] for the style of comment we expect.
///
pub fn shard(query: &str, shards: usize) -> Result<Option<usize>, Error> {
    let tokens = scan(query)?;

    for token in tokens.tokens.iter() {
        if token.token == Token::CComment as i32 {
            let comment = &query[token.start as usize..token.end as usize];
            if let Some(cap) = SHARDING_KEY.captures(comment) {
                if let Some(sharding_key) = cap.get(1) {
                    if let Ok(value) = sharding_key.as_str().parse::<i64>() {
                        return Ok(Some(sharding_function::bigint(value, shards)));
                    }
                    if let Ok(value) = sharding_key.as_str().parse::<Uuid>() {
                        return Ok(Some(sharding_function::uuid(value, shards)));
                    }
                }
            }
            if let Some(cap) = SHARD.captures(comment) {
                if let Some(shard) = cap.get(1) {
                    return Ok(shard.as_str().parse::<usize>().ok());
                }
            }
        }
    }

    Ok(None)
}
