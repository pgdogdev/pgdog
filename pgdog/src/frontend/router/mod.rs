//! Query router.

pub mod context;
pub mod copy;
pub mod error;
pub mod parser;
pub mod round_robin;
pub mod search_path;
pub mod sharding;

pub(crate) use copy::CopyRow;
pub(crate) use error::Error;
use lazy_static::lazy_static;
use parser::Shard;
pub(crate) use parser::{Command, QueryParser, Route};

use super::ClientRequest;
pub(crate) use context::RouterContext;
pub(crate) use search_path::SearchPath;
pub(crate) use sharding::Ranges;

/// Query router.
#[derive(Debug)]
pub(crate) struct Router {
    query_parser: QueryParser,
    latest_command: Command,
}

impl Default for Router {
    fn default() -> Self {
        Self::new()
    }
}

impl Router {
    /// Create new router.
    pub(crate) fn new() -> Router {
        Self {
            query_parser: QueryParser::default(),
            latest_command: Command::default(),
        }
    }

    /// Route a query to a shard.
    ///
    /// If the router can't determine the route for the query to take,
    /// previous route is preserved. This is useful in case the client
    /// doesn't supply enough information in the buffer, e.g. just issued
    /// a Describe request to a previously submitted Parse.
    pub(crate) fn query(&mut self, context: RouterContext) -> Result<&Command, Error> {
        // Don't invoke parser in copy mode until we're done.
        if context.copy_mode {
            return Ok(&self.latest_command);
        }

        let command = self.query_parser.parse(context)?;
        self.latest_command = command;
        Ok(&self.latest_command)
    }

    /// Parse CopyData messages and shard them.
    pub(crate) fn copy_data(&mut self, buffer: &ClientRequest) -> Result<Vec<CopyRow>, Error> {
        match self.latest_command {
            Command::Copy(ref mut copy) => Ok(copy.shard(&buffer.copy_data()?)?),
            _ => Ok(buffer
                .copy_data()?
                .into_iter()
                .map(CopyRow::omnishard)
                .collect()),
        }
    }

    /// Get current route.
    pub(crate) fn route(&self) -> &Route {
        lazy_static! {
            static ref DEFAULT_ROUTE: Route = Route::write(Shard::All);
        }

        match self.command() {
            Command::Query(route) => route,
            _ => &DEFAULT_ROUTE,
        }
    }

    /// Reset query routing state.
    pub(crate) fn reset(&mut self) {
        self.query_parser = QueryParser::default();
        self.latest_command = Command::default();
    }

    /// Query parser is inside a transaction.
    pub(crate) fn in_transaction(&self) -> bool {
        self.query_parser.in_transaction()
    }

    /// Get last commmand computed by the query parser.
    pub(crate) fn command(&self) -> &Command {
        &self.latest_command
    }
}
