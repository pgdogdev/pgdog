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
use lazy_static::lazy_static;
use parser::Shard;
pub use parser::{Command, QueryParser, Route};

use super::Buffer;
pub use context::RouterContext;
pub use search_path::SearchPath;
pub use sharding::{Lists, Ranges};

/// Query router.
#[derive(Debug)]
pub struct Router {
    query_parser: QueryParser,
    active_command: Option<Command>,
    latest_command: Command,
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
            active_command: None,
            latest_command: Command::default(),
        }
    }

    /// Route a query to a shard.
    ///
    /// If the router can't determine the route for the query to take,
    /// previous route is preserved. This is useful in case the client
    /// doesn't supply enough information in the buffer, e.g. just issued
    /// a Describe request to a previously submitted Parse.
    pub fn query(&mut self, context: RouterContext) -> Result<&Command, Error> {
        let command = self.query_parser.parse(context)?;
        if self.active_command.is_none() {
            self.active_command = Some(command);
            Ok(self.command())
        } else {
            self.latest_command = command;
            Ok(&self.latest_command)
        }
    }

    /// Parse CopyData messages and shard them.
    pub fn copy_data(&mut self, buffer: &Buffer) -> Result<Vec<CopyRow>, Error> {
        match self.active_command {
            Some(Command::Copy(ref mut copy)) => Ok(copy.shard(&buffer.copy_data()?)?),
            _ => Ok(vec![]),
        }
    }

    /// Get current route.
    pub fn route(&self) -> &Route {
        lazy_static! {
            static ref DEFAULT_ROUTE: Route = Route::write(Shard::All);
        }

        match self.command() {
            Command::Query(route) => route,
            _ => &DEFAULT_ROUTE,
        }
    }

    /// Reset query routing state.
    pub fn reset(&mut self) {
        self.query_parser = QueryParser::default();
        self.active_command = None;
        self.latest_command = Command::default();
    }

    /// The router is configured.
    pub fn routed(&self) -> bool {
        self.active_command.is_some()
    }

    /// Query parser is inside a transaction.
    pub fn in_transaction(&self) -> bool {
        self.query_parser.in_transaction()
    }

    /// Get last commmand computed by the query parser.
    pub fn command(&self) -> &Command {
        lazy_static! {
            static ref DEFAULT_COMMAND: Command = Command::Query(Route::write(Shard::All));
        }

        if let Some(ref command) = self.active_command {
            command
        } else {
            &DEFAULT_COMMAND
        }
    }
}
