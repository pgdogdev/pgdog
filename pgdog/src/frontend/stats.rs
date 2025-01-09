//! Frontend client statistics.

use crate::state::State;

/// Client statistics.
#[derive(Default)]
pub struct Stats {
    pub bytes_sent: usize,
    pub bytes_received: usize,
    pub transactions: usize,
    pub queries: usize,
    pub errors: usize,
    pub state: State,
}

impl Stats {
    pub(super) fn new_disconnected() -> Self {
        Self {
            state: State::Disconnected,
            ..Default::default()
        }
    }

    pub(super) fn disconnected(&self) -> bool {
        self.state == State::Disconnected
    }

    pub(super) fn disconnect(&mut self) {
        self.state = State::Disconnected;
    }

    pub(super) fn transaction(&mut self) {
        self.transactions += 1;
        self.state = State::Idle;
    }

    pub(super) fn error(&mut self) {
        self.errors += 1;
        self.state = State::Idle;
    }

    pub(super) fn query(&mut self) {
        self.queries += 1;
    }

    pub(super) fn waiting(&mut self) {
        self.state = State::Waiting;
    }

    pub(super) fn connected(&mut self) {
        self.state = State::Active;
    }
}
