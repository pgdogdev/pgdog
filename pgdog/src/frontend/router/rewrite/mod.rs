//! Query rewrite engine.
//!
//! It handles the following scenarios:
//!
//! 1. Sharding key UPDATE: rewrite to send a DELETE and INSERT
//! 2. Multi-tuple INSERT: rewrite to send multiple INSERTs
//! 3. pgdog.unique_id() call: inject a unique ID
//!
pub mod context;
pub mod error;
pub mod insert_split;
pub mod interface;
pub mod output;
pub mod plan;
pub mod prepared;
pub mod request;
pub mod state;
pub mod stats;
pub mod unique_id;

pub use context::Context;
pub use error::Error;
pub use interface::RewriteModule;
pub use output::{RewriteAction, StepOutput};
pub use plan::{ImmutableRewritePlan, RewritePlan, UniqueIdPlan};
pub use request::RewriteRequest;
pub use state::RewriteState;

use crate::frontend::PreparedStatements;

/// Combined rewrite engine that runs all rewrite modules.
pub struct Rewrite<'a> {
    prepared_statements: &'a mut PreparedStatements,
}

impl<'a> Rewrite<'a> {
    pub fn new(prepared_statements: &'a mut PreparedStatements) -> Self {
        Self {
            prepared_statements,
        }
    }
}

impl RewriteModule for Rewrite<'_> {
    fn rewrite(&mut self, input: &mut Context<'_>) -> Result<(), Error> {
        // N.B.: the ordering here matters!
        //
        // First, we need to inject the unique ID into the query. Once that's done,
        // we can proceed with additional rewrites.

        // Unique ID rewrites.
        unique_id::ExplainUniqueIdRewrite::default().rewrite(input)?;
        unique_id::InsertUniqueIdRewrite::default().rewrite(input)?;
        unique_id::UpdateUniqueIdRewrite::default().rewrite(input)?;
        unique_id::SelectUniqueIdRewrite::default().rewrite(input)?;

        // Prepared statement rewrites
        prepared::PreparedRewrite::new(self.prepared_statements).rewrite(input)?;

        Ok(())
    }
}
