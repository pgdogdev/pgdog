use once_cell::sync::Lazy;
use pg_query::scan_raw;
use pg_query::{protobuf::Token, scan};
use pgdog_config::QueryParserEngine;
use regex::Regex;

use crate::backend::ShardingSchema;
use crate::config::database::Role;
use crate::frontend::router::sharding::ContextBuilder;

use super::super::Error;
use super::super::Shard;

pub(super) static SHARD: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"pgdog_shard: *([0-9]+)"#).unwrap());
pub(super) static SHARDING_KEY: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"pgdog_sharding_key: *(?:"([^"]*)"|'([^']*)'|([0-9a-zA-Z-]+))"#).unwrap()
});
pub(super) static ROLE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"pgdog_role: *(primary|replica)"#).unwrap());

pub(super) fn get_matched_value<'a>(caps: &'a regex::Captures<'a>) -> Option<&'a str> {
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

pub(super) fn shard_role_from_comment(
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
