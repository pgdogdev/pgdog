//! Query router.

pub mod context;
pub mod copy;
pub mod error;
pub mod parser;
pub mod request;
pub mod round_robin;
pub mod search_path;
pub mod sharding;

pub use copy::CopyRow;
pub use error::Error;
pub use parser::{Command, QueryParser, Route};

use super::Buffer;
pub use context::RouterContext;
pub use search_path::SearchPath;

/// Query router.
#[derive(Debug)]
pub struct Router {
    query_parser: QueryParser,
}

impl Default for Router {
    fn default() -> Self {
        Self::new()
    }
}

impl Router {
    /// Create new router.
    pub fn new() -> Router {
        Self {
            query_parser: QueryParser::default(),
        }
    }

    /// Set into replication mode.
    pub fn replication_mode(&mut self) {
        self.query_parser.replication_mode();
    }

    /// Route a query to a shard.
    ///
    /// If the router can't determine the route for the query to take,
    /// previous route is preserved. This is useful in case the client
    /// doesn't supply enough information in the buffer, e.g. just issued
    /// a Describe request to a previously submitted Parse.
    pub fn query(&mut self, context: RouterContext) -> Result<&Command, Error> {
        Ok(self.query_parser.parse(context)?)
    }

    /// Parse CopyData messages and shard them.
    pub fn copy_data(&mut self, buffer: &Buffer) -> Result<Vec<CopyRow>, Error> {
        Ok(self.query_parser.copy_data(buffer.copy_data()?)?)
    }

    /// Get current route.
    pub fn route(&self) -> Route {
        self.query_parser.route()
    }

    /// Reset sharding context.
    pub fn reset(&mut self) {
        self.query_parser.reset()
    }
}
