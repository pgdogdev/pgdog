use std::sync::Arc;

use crate::{
    backend::databases::User,
    frontend::client::query_engine::{
        two_pc::{manager::Manager, TwoPcTransaction},
        TwoPcPhase,
    },
};

#[derive(Debug)]
pub struct TwoPcGuard {
    pub(super) phase: TwoPcPhase,
    pub(super) transaction: TwoPcTransaction,
    pub(super) identifier: Arc<User>,
    pub(super) manager: Manager,
}

impl Drop for TwoPcGuard {
    fn drop(&mut self) {
        self.manager.return_guard(&self);
    }
}
