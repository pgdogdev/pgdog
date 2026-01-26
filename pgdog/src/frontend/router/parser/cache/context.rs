//! AST parsing context.

use crate::backend::pool::Cluster;
use crate::backend::schema::Schema;
use crate::backend::ShardingSchema;
use crate::net::parameter::ParameterValue;
use crate::net::Parameters;

/// Context for AST parsing and rewriting.
///
/// This struct owns the sharding schema and db schema since they are
/// typically computed from the cluster. The user and search_path are
/// borrowed references.
#[derive(Debug)]
pub struct AstContext<'a> {
    /// Sharding schema configuration.
    pub sharding_schema: ShardingSchema,
    /// Database schema with table/column info.
    pub db_schema: Schema,
    /// User name for search_path resolution.
    pub user: &'a str,
    /// Search path for table lookups.
    pub search_path: Option<&'a ParameterValue>,
}

impl<'a> AstContext<'a> {
    /// Create AstContext from a Cluster and Parameters.
    pub fn from_cluster(cluster: &'a Cluster, params: &'a Parameters) -> Self {
        Self {
            sharding_schema: cluster.sharding_schema(),
            db_schema: cluster.schema(),
            user: cluster.user(),
            search_path: params.get("search_path"),
        }
    }

    /// Create a default/empty AstContext for tests.
    pub fn empty() -> AstContext<'static> {
        AstContext {
            sharding_schema: ShardingSchema::default(),
            db_schema: Schema::default(),
            user: "",
            search_path: None,
        }
    }
}
