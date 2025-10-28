//! Query hooks.
#![allow(unused_variables, dead_code)]
use super::*;

#[derive(Debug)]
pub struct QueryEngineHooks;

impl Default for QueryEngineHooks {
    fn default() -> Self {
        Self::new()
    }
}

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

    pub(super) fn after_connected(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        backend: &Connection,
    ) -> Result<(), Error> {
        Ok(())
    }

    pub(super) fn after_execution(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<(), Error> {
        Ok(())
    }

    pub(super) fn on_server_message(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        message: &Message,
    ) -> Result<(), Error> {
        Ok(())
    }

    pub(super) fn on_engine_error(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        error: &ErrorResponse,
    ) -> Result<(), Error> {
        Ok(())
    }
}
