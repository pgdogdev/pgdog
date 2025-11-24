use super::*;

impl QueryEngine {
    pub fn backend(&mut self) -> &mut Connection {
        &mut self.backend
    }

    pub fn router(&mut self) -> &mut Router {
        &mut self.router
    }

    pub fn stats(&mut self) -> &mut Stats {
        &mut self.stats
    }

    pub fn set_test_mode(&mut self, test_mode: bool) {
        self.test_mode = test_mode;
    }

    pub fn set_inflight(&mut self, inflight: bool) {
        self.inflight = inflight;
    }

    pub fn update_stats_for_test(&mut self, context: &mut QueryEngineContext<'_>) {
        self.update_stats(context);
    }
}
