use crate::{
    frontend::router::parser::rewrite::statement::offset::OffsetPlan, stats::memory::MemoryUsage,
};

use super::prelude::*;

/// Prepared statements cache key.
///
/// If two `Extended` keys match, it's effectively the same statement.
/// If they don't, e.g. client sent the same query but
/// with different data types, we can't re-use it and
/// need to plan a new one.
///
/// A `Simple` key comes from SQL `PREPARE` and matches nothing but itself.
/// Its declared argument types are not captured, so two of those
/// statements are never known to be the same.
///
/// If a Prepare contains a limit + offset query, then we also
/// include that as part of the [`CacheKey::Simple`],
/// considering that we have to store the `A_Const` nodes that we re-wrote
/// with `ParamRefs` (so we can replace them later if we need to for multi-shard)
///
#[derive(Debug, Clone, PartialEq, Hash, Eq)]
pub(crate) enum CacheKey {
    Extended {
        query: Bytes,
        data_types: Bytes,
    },
    Simple {
        query: Bytes,
        offset_plan: Option<OffsetPlan>,
    },
}

impl MemoryUsage for CacheKey {
    #[inline]
    fn memory_usage(&self) -> usize {
        // The Bytes alias the Parse in Statement, which counts them via Parse::len.
        std::mem::size_of::<Self>()
    }
}

impl CacheKey {
    /// Get a UTF-8 encoded query string
    /// stored in the cache.
    pub(crate) fn query(&self) -> Result<&str, crate::net::Error> {
        match self {
            Self::Extended { query, .. } => Ok(from_utf8(&query[0..query.len() - 1])?),
            // TODO: When `OffsetPlan` is Some(..) should the limit / offset *CONSTANTS* be included
            //       in this output to the admin command? As in, we resolve the relevant params in the query string.
            Self::Simple { query, .. } => Ok(from_utf8(query)?), // Simple queries are regular Rust strings.
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    impl CacheKey {
        pub(crate) fn query_ref(&self) -> &Bytes {
            match self {
                Self::Extended { query, .. } => query,
                Self::Simple { query, .. } => query,
            }
        }
    }
}
