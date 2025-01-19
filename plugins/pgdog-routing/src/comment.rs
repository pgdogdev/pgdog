//! Parse shards/sharding keys from comments.

use once_cell::sync::Lazy;
use pg_query::{protobuf::Token, scan, Error};
use regex::Regex;

static SHARD_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r#"pgdog_shard: *([0-9]+)"#).unwrap());

/// Extract shard number from a comment.
///
/// Comment style uses the C-style comments (not SQL comments!)
/// as to allow the comment to appear anywhere in the query.
///
/// See [`SHARD_REGEX`] for the style of comment we expect.
///
pub fn shard(query: &str) -> Result<Option<usize>, Error> {
    let tokens = scan(query)?;

    for token in tokens.tokens.iter() {
        if token.token == Token::CComment as i32 {
            let comment = &query[token.start as usize..token.end as usize];
            if let Some(cap) = SHARD_REGEX.captures(comment) {
                if let Some(shard) = cap.get(1) {
                    return Ok(shard.as_str().parse::<usize>().ok());
                }
            }
        }
    }

    Ok(None)
}
