mod directive;
mod strip;

#[cfg(test)]
mod tests;

use std::ops::Range;

use crate::backend::ShardingSchema;

use super::Error;
use crate::frontend::RoutingComment;
use crate::frontend::router::parser::comment::directive::Directive;
pub(crate) use strip::{leading_block_comment, trailing_block_comment};

#[derive(Default, Debug, Clone)]
pub(crate) struct QueryAndComment<'a> {
    pub(crate) query: &'a str,
    #[cfg(test)]
    pub(crate) raw_comment: String,
    pub(in crate::frontend) comment: RoutingComment,
    /// Where the edge comments sit in the original query.
    pub(crate) spans: CommentSpans,
}

/// Byte ranges of the leading and trailing block comments in a query, so
/// their text can be read later without scanning the query again.
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommentSpans {
    leading: Option<Range<usize>>,
    trailing: Option<Range<usize>>,
}

#[allow(dead_code)] // used by ee
impl CommentSpans {
    /// Leading comments, read from `query`: the string the spans were taken from.
    pub(crate) fn leading_comment<'a>(&self, query: &'a str) -> Option<&'a str> {
        query.get(self.leading.clone()?)
    }

    /// Trailing comments, read from `query`: the string the spans were taken from.
    pub(crate) fn trailing_comment<'a>(&self, query: &'a str) -> Option<&'a str> {
        query.get(self.trailing.clone()?)
    }
}

/// Byte range of `part` inside `query`. `part` must be a sub-slice of `query`,
/// as returned by the strip helpers.
fn span_of(query: &str, part: &str) -> Range<usize> {
    let start = part.as_ptr() as usize - query.as_ptr() as usize;
    debug_assert!(
        start + part.len() <= query.len(),
        "part is not inside query"
    );
    start..start + part.len()
}

/// Extract SQL C-style block comments from both the beginning and the end
/// of the query, returning the stripped query string and directives found
/// in either side. Leading takes precedence when both sides carry the same
/// directive (e.g. shard or role).
///
/// This algorithm uses a heuristic, and not the real Postgres parser, because the heuristic
/// is 2x faster and will work most of the time.
///
pub(crate) fn parse_edge_comment<'a>(
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
            query,
            ..Default::default()
        });
    }

    let spans = CommentSpans {
        leading: leading.map(|comment| span_of(query, comment)),
        trailing: trailing.map(|comment| span_of(query, comment)),
    };

    // Leading wins per-field: extract from leading first, then fill in any
    // fields the leading didn't provide from trailing.
    let mut directive = match leading {
        Some(c) => directive::shard_role_from_comment(c, schema)?,
        None => Directive {
            shard_or_lookup: None,
            role: None,
            label: None,
            sharding_key: None,
            lsn: None,
        },
    };
    if let Some(c) = trailing {
        let t_directive = directive::shard_role_from_comment(c, schema)?;
        if directive.shard_or_lookup.is_none() {
            directive.shard_or_lookup = t_directive.shard_or_lookup;
        }
        if directive.role.is_none() {
            directive.role = t_directive.role;
        }
        if directive.label.is_none() {
            directive.label = t_directive.label;
        }
        if directive.sharding_key.is_none() {
            directive.sharding_key = t_directive.sharding_key;
        }
        if directive.lsn.is_none() {
            directive.lsn = t_directive.lsn;
        }
    }

    Ok(QueryAndComment {
        query: stripped,
        #[cfg(test)]
        raw_comment: match (leading, trailing) {
            (Some(l), Some(t)) => format!("{} {}", l, t),
            (Some(l), None) => l.to_string(),
            (None, Some(t)) => t.to_string(),
            (None, None) => String::new(),
        },
        comment: RoutingComment {
            sharding_key: directive.sharding_key,
            shard: directive.shard_or_lookup,
            role: directive.role,
            lsn: directive.lsn,
            label: directive.label,
        },
        spans,
    })
}
