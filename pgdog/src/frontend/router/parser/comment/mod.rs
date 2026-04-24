mod directive;
mod query;
mod strip;

#[cfg(test)]
mod tests;

pub use directive::comment;
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

pub fn parse_edge_comment<'a>(
    query: &'a str,
    schema: &ShardingSchema,
) -> Result<QueryAndComment<'a>, Error> {
    let Some((stripped, comment)) =
        leading_block_comment(query).or_else(|| trailing_block_comment(query))
    else {
        return Ok(QueryAndComment {
            query: QueryWithoutComment::Original(query),
            #[cfg(test)]
            comment: String::new(),
            ..Default::default()
        });
    };

    let (shard, role) = directive::shard_role_from_comment(comment, schema)?;

    Ok(QueryAndComment {
        query: QueryWithoutComment::Stripped(stripped.to_string()),
        #[cfg(test)]
        comment: comment.to_string(),
        shard,
        role,
    })
}
