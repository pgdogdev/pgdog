mod directive;
mod query;
mod strip;

#[cfg(test)]
mod tests;

pub use query::QueryWithoutComment;

use crate::backend::ShardingSchema;
use crate::config::database::Role;

use super::Error;
use super::Shard;
use strip::{leading_block_comment, trailing_block_comment};

#[derive(Default, Debug, Clone)]
pub struct QueryAndComment<'a> {
    pub query: QueryWithoutComment<'a>,
    #[cfg(test)]
    pub comment: String,
    pub role: Option<Role>,
    pub shard: Option<Shard>,
}

/// Extract SQL C-style block comments from both the beginning and the end
/// of the query, returning the stripped query string and directives found
/// in either side. Leading takes precedence when both sides carry the same
/// directive (e.g. shard or role).
///
/// This algorithm uses a heuristic, and not the real Postgres parser, because the heuristic
/// is 2x faster and will work most of the time.
///
pub fn parse_edge_comment<'a>(
    query: &'a str,
    schema: &ShardingSchema,
) -> Result<QueryAndComment<'a>, Error> {
    let (after_leading, leading) = match leading_block_comment(query) {
        Some((rest, c)) => (rest, Some(c)),
        None => (query, None),
    };
    let (stripped, trailing) = match trailing_block_comment(after_leading) {
        Some((rest, c)) => (rest, Some(c)),
        None => (after_leading, None),
    };

    if leading.is_none() && trailing.is_none() {
        return Ok(QueryAndComment {
            query: QueryWithoutComment::Original(query),
            #[cfg(test)]
            comment: String::new(),
            ..Default::default()
        });
    }

    // Leading wins per-field: extract from leading first, then fill in any
    // fields the leading didn't provide from trailing.
    let (mut shard, mut role) = match leading {
        Some(c) => directive::shard_role_from_comment(c, schema)?,
        None => (None, None),
    };
    if let Some(c) = trailing {
        let (t_shard, t_role) = directive::shard_role_from_comment(c, schema)?;
        if shard.is_none() {
            shard = t_shard;
        }
        if role.is_none() {
            role = t_role;
        }
    }

    Ok(QueryAndComment {
        query: QueryWithoutComment::Stripped(stripped.to_string()),
        #[cfg(test)]
        comment: match (leading, trailing) {
            (Some(l), Some(t)) => format!("{} {}", l, t),
            (Some(l), None) => l.to_string(),
            (None, Some(t)) => t.to_string(),
            (None, None) => String::new(),
        },
        shard,
        role,
    })
}
