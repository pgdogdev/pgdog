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
}
