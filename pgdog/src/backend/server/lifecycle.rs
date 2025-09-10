//! Server connection lifecycle management.

use super::Server;
use crate::{
    net::{ProtocolMessage, Sync},
    state::State,
};

impl Server {
    /// Attempt to rollback the transaction on this server, if any has been started.
    pub async fn rollback(&mut self) {
        if self.in_transaction() {
            if let Err(_err) = self.execute("ROLLBACK").await {
                self.stats.state(State::Error);
            }
            self.stats.rollback();
        }

        if !self.done() {
            self.stats.state(State::Error);
        }
    }

    pub async fn drain(&mut self) {
        while self.has_more_messages() {
            if self.read().await.is_err() {
                self.stats.state(State::Error);
                break;
            }
        }

        if !self.in_sync() {
            if self
                .send(&vec![ProtocolMessage::Sync(Sync)].into())
                .await
                .is_err()
            {
                self.stats.state(State::Error);
                return;
            }
            while !self.in_sync() {
                if self.read().await.is_err() {
                    self.stats.state(State::Error);
                    break;
                }
            }

            self.re_synced = true;
        }
    }
}

#[cfg(test)]
pub mod test {
    // Lifecycle tests will be moved here
}
