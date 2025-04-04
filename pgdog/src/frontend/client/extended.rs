//! Extended protocol state machine.

use std::collections::VecDeque;

use crate::net::{Message, Protocol};

#[derive(Debug, Clone, PartialEq)]
enum Action {
    Forward,
    Drop,
}

#[derive(Default, Debug, Clone)]
pub struct Extended {
    received: VecDeque<char>,
    queue: VecDeque<Action>,
}

impl Extended {
    pub fn forward(&mut self) {
        self.queue.push_back(Action::Forward);
    }

    pub fn swallow(&mut self) {
        self.queue.push_back(Action::Drop);
    }

    pub fn should_forward(&mut self) -> bool {
        self.queue.pop_front().unwrap_or(Action::Forward) == Action::Forward
    }
}
