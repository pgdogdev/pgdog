//! Query router.

pub(crate) mod cli;
mod comment;
pub(crate) mod context;
pub(crate) mod copy;
pub(crate) mod error;
pub(crate) mod parameter_hints;
pub(crate) mod parser;
pub(crate) mod round_robin;
pub(crate) mod search_path;
pub(crate) mod sharding;

pub(in crate::frontend) use comment::RoutingComment;
pub(crate) use copy::CopyRow;
pub(crate) use error::Error;
pub(crate) use parser::{Ast, Command, DiscardTarget, QueryParser, Route, SetParam};

use crate::frontend::router::parser::{Shard, route::ShardSource};

use super::ClientRequest;
pub(crate) use context::RouterContext;
pub(crate) use parameter_hints::ParameterHints;
pub(crate) use search_path::SearchPath;

/// Query router.
#[derive(Debug)]
pub(crate) struct Router {
    query_parser: QueryParser,
    latest_command: Command,
    schema_changed: bool,
    pinned_shard: Option<Shard>,
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
            schema_changed: false,
            pinned_shard: None,
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
        let executable = context.executable;

        let command = self.query_parser.parse(context)?;

        self.latest_command = command;

        if let Command::Query(ref route) = self.latest_command
            && route.is_schema_changed()
        {
            self.schema_changed = true;
        }

        // Check for shard pinning, e.g.,
        // SET pgdog.shard TO x;
        // The expectation here is the client cannot quietly change shards mid-transaction
        // without explicitely unpining, e.g., removing pgdog.shard or setting a different value.
        let pinned = self.latest_command.route().is_search_path_driven()
            || self.latest_command.route().shard_with_priority().source() == &ShardSource::Set;

        if pinned {
            self.pinned_shard = Some(self.latest_command.route().shard().clone());
        }

        if let Some(ref shard) = self.pinned_shard
            && let Command::Query(ref route) = self.latest_command
        {
            use crate::backend::Error as BackendError;
            if shard != route.shard() && executable {
                return Err(Error::Backend(BackendError::DirectShardMismatch));
            }
        }

        Ok(&self.latest_command)
    }

    /// Parse CopyData messages and shard them.
    pub(crate) async fn copy_data(
        &mut self,
        buffer: &ClientRequest,
    ) -> Result<Vec<CopyRow>, Error> {
        match self.latest_command {
            Command::Copy { ref mut copy, .. } => Ok(copy.shard(&buffer.copy_data()?).await?),
            _ => Ok(buffer
                .copy_data()?
                .into_iter()
                .map(CopyRow::omnishard)
                .collect()),
        }
    }

    /// Reset query routing state.
    pub(crate) fn reset(&mut self) {
        self.query_parser = QueryParser::default();
        self.latest_command = Command::default();
        self.pinned_shard = None;
        self.schema_changed = false;
    }

    /// Get last commmand computed by the query parser.
    pub(crate) fn command(&self) -> &Command {
        &self.latest_command
    }

    /// Has the schema been altered?
    pub(crate) fn schema_changed(&self) -> bool {
        self.schema_changed
    }
}
