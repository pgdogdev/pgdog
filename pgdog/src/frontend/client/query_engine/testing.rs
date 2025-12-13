use super::*;

impl QueryEngine {
    pub fn router(&mut self) -> &mut Router {
        &mut self.router
    }

    pub fn stats(&mut self) -> &mut Stats {
        &mut self.stats
    }

    pub fn set_test_mode(&mut self, test_mode: bool) {
        self.test_mode = test_mode;
    }
}
