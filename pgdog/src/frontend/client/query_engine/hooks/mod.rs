//! Query hooks.
#![allow(unused_variables, dead_code)]
use super::*;

pub struct QueryEngineHooks;

impl QueryEngineHooks {
    pub(super) fn new() -> Self {
        Self {}
    }

    pub(super) fn before_execution(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<(), Error> {
        Ok(())
    }

    pub(super) fn after_execution(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<(), Error> {
        Ok(())
    }
}
