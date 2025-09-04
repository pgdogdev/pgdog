use super::*;

impl QueryEngine {
    pub(crate) fn backend(&mut self) -> &mut Connection {
        &mut self.backend
    }

    pub(crate) fn router(&mut self) -> &mut Router {
        &mut self.router
    }

    pub(crate) fn stats(&mut self) -> &mut Stats {
        &mut self.stats
    }
}
