//! AST parsing context.

use crate::backend::ShardingSchema;
use crate::backend::pool::Cluster;
use crate::backend::schema::Schema;
use crate::frontend::BufferedQuery;
use crate::frontend::client::QueryTimestamps;
use crate::parameters::{ParameterStore, ParameterValue};

/// Context for AST parsing and rewriting.
#[derive(Debug)]
pub(crate) struct AstContext<'a> {
    /// Sharding schema configuration.
    pub(crate) sharding_schema: &'a ShardingSchema,
    /// Database schema with table/column info.
    pub(crate) db_schema: &'a Schema,
    /// User name for search_path resolution.
    pub(crate) user: &'a str,
    /// Search path for table lookups.
    pub(crate) search_path: Option<&'a ParameterValue>,
    /// Allows `timestamp` types to use the Client's local time when excecuting a `TimeFunction`
    pub(crate) timezone: Option<&'a ParameterValue>,
    /// Statement, and transaction DateTime<Utc> relevant to the current Query (if not being cached)
    pub(crate) query_timestamps: QueryTimestamps,
}

impl<'a> AstContext<'a> {
    /// Create AstContext using the client's effective transaction parameters.
    pub(crate) fn from_cluster(
        cluster: &'a Cluster,
        params: &'a ParameterStore,
        query_timestamps: QueryTimestamps,
    ) -> Self {
        Self {
            sharding_schema: cluster.sharding_schema(),
            db_schema: cluster.loaded_schema(),
            user: cluster.user(),
            search_path: params.get("search_path"),
            timezone: params
                .get("timezone")
                .or_else(|| cluster.default_timezone()),
            query_timestamps,
        }
    }
}

#[cfg(test)]
impl Default for AstContext<'static> {
    fn default() -> Self {
        use once_cell::sync::Lazy;

        static EMPTY_SHARDING_SCHEMA: Lazy<ShardingSchema> = Lazy::new(ShardingSchema::default);
        static EMPTY_SCHEMA: Lazy<Schema> = Lazy::new(Schema::default);

        Self {
            sharding_schema: &EMPTY_SHARDING_SCHEMA,
            db_schema: &EMPTY_SCHEMA,
            user: Default::default(),
            search_path: Default::default(),
            timezone: Default::default(),
            query_timestamps: Default::default(),
        }
    }
}

/// Query passed to the parser.
pub(crate) struct AstQuery<'a> {
    /// The original request.
    pub(crate) original_query: &'a BufferedQuery,
    /// Query without comments and other noise.
    pub(crate) query_without_comment: &'a str,
}

impl<'a> AstQuery<'a> {
    /// Return the first `sample_len` characters of the original query, including any comment.
    pub(crate) fn truncated_query(&self, sample_len: usize) -> &str {
        let query = self.original_query.query();
        let end = query
            .char_indices()
            .nth(sample_len)
            .map_or(query.len(), |(i, _)| i);
        &query[..end]
    }
}
