use super::*;

/// Set maintenance mode on the query engine
/// and automatically unset it when done.
pub(crate) struct EngineMaintenanceMode<'a> {
    engine: &'a mut QueryEngine,
    state: State,
}

impl<'a> EngineMaintenanceMode<'a> {
    /// Active maintenance mode on the engine immediately.
    ///
    /// The maintenance mode is disabled when the returned guard is dropped.
    pub(crate) fn new(engine: &'a mut QueryEngine) -> Self {
        let state = engine.stats.state;
        engine.set_state(State::Waiting);

        Self { engine, state }
    }
}

impl Drop for EngineMaintenanceMode<'_> {
    fn drop(&mut self) {
        self.engine.set_state(self.state);
    }
}

impl QueryEngine {
    /// Set the query engine into maintenance mode.
    pub(crate) fn set_maintenance_mode(&mut self) -> EngineMaintenanceMode<'_> {
        EngineMaintenanceMode::new(self)
    }
}
