//! Query router.

pub mod cli;
pub mod context;
pub mod copy;
pub mod error;
pub mod parameter_hints;
pub mod parser;
pub mod round_robin;
pub mod search_path;
pub mod sharding;

pub use copy::CopyRow;
pub use error::Error;
use lazy_static::lazy_static;
use parser::Shard;
pub use parser::{Ast, Command, QueryParser, Route};

use crate::frontend::router::parser::ShardWithPriority;

use super::ClientRequest;
pub use context::RouterContext;
pub use parameter_hints::ParameterHints;
pub use search_path::SearchPath;
pub use sharding::{Lists, Ranges};

/// Query router.
#[derive(Debug)]
pub struct Router {
    query_parser: QueryParser,
    latest_command: Command,
    schema_changed: bool,
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
            latest_command: Command::default(),
            schema_changed: false,
        }
    }

    /// Route a query to a shard.
    ///
    /// If the router can't determine the route for the query to take,
    /// previous route is preserved. This is useful in case the client
    /// doesn't supply enough information in the buffer, e.g. just issued
    /// a Describe request to a previously submitted Parse.
    pub fn query(&mut self, context: RouterContext) -> Result<&Command, Error> {
        // Don't invoke parser in copy mode until we're done.
        if context.copy_mode {
            return Ok(&self.latest_command);
        }

        let command = self.query_parser.parse(context)?;
        self.latest_command = command;

        if let Command::Query(ref route) = self.latest_command {
            if route.is_schema_changed() {
                self.schema_changed = true;
            }
        }

        Ok(&self.latest_command)
    }

    /// Parse CopyData messages and shard them.
    pub fn copy_data(&mut self, buffer: &ClientRequest) -> Result<Vec<CopyRow>, Error> {
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
    pub fn route(&self) -> &Route {
        lazy_static! {
            static ref DEFAULT_ROUTE: Route =
                Route::write(ShardWithPriority::new_default_unset(Shard::All));
        }

        match self.command() {
            Command::Query(route) => route,
            _ => &DEFAULT_ROUTE,
        }
    }

    /// Reset query routing state.
    pub fn reset(&mut self) {
        self.query_parser = QueryParser::default();
        self.latest_command = Command::default();
        self.schema_changed = false;
    }

    /// Get last commmand computed by the query parser.
    pub fn command(&self) -> &Command {
        &self.latest_command
    }

    /// Has the schema been altered?
    pub fn schema_changed(&self) -> bool {
        self.schema_changed
    }
}
