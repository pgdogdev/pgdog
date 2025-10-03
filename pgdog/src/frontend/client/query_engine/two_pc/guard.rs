use crate::frontend::client::query_engine::two_pc::{manager::Manager, TwoPcTransaction};

#[derive(Debug)]
pub struct TwoPcGuard {
    pub(super) transaction: TwoPcTransaction,
    pub(super) manager: Manager,
}

impl Drop for TwoPcGuard {
    fn drop(&mut self) {
        self.manager.return_guard(self);
    }
}
