//! Query rewrite engine.
//!
//! It handles the following scenarios:
//!
//! 1. Sharding key UPDATE: rewrite to send a DELETE and INSERT
//! 2. Multi-tuple INSERT: rewrite to send multiple INSERTs
//! 3. pgdog.unique_id() call: inject a unique ID
//!
pub mod error;
pub mod input;
pub mod interface;
pub mod prepared;
pub mod unique_id;

pub use error::Error;
pub use input::{Input, Output};
pub use interface::RewriteModule;

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
    fn rewrite(&mut self, input: &mut Input<'_>) -> Result<(), Error> {
        // N.B.: the ordering here matters!
        //
        // First, we need to inject the unique ID into the query. Once that's done,
        // we can proceed with additional rewrites.

        // Unique ID rewrites
        unique_id::insert::InsertUniqueIdRewrite::default().rewrite(input)?;
        unique_id::update::UpdateUniqueIdRewrite::default().rewrite(input)?;
        unique_id::select::SelectUniqueIdRewrite::default().rewrite(input)?;

        // Prepared statement rewrites
        prepared::PreparedRewrite::new(self.prepared_statements).rewrite(input)?;

        Ok(())
    }
}
