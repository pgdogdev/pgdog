//! Handle server response.

use crate::{
    backend::pool::Connection,
    frontend::{Error, Stats},
    net::{Message, Protocol},
};

use super::engine_impl::Stream;

pub struct ServerMessage<'a> {
    backend: &'a mut Connection,
    stats: &'a mut Stats,
}

impl<'a> ServerMessage<'a> {
    pub fn new(backend: &'a mut Connection, stats: &'a mut Stats) -> Self {
        Self { backend, stats }
    }

    pub async fn handle(
        &mut self,
        message: Message,
        client_socket: &mut Stream,
    ) -> Result<bool, Error> {
        let code = message.code();
        let flush = matches!(code, 'Z' | 'G' | 'E' | 'N' | 'A')
            || !self.backend.has_more_messages()
            || message.streaming();
        let mut in_transaction = true;

        if code == 'Z' {
            self.stats.query();

            if message.transaction_finished() {
                self.backend.mirror_flush();
                in_transaction = false;
            }

            self.stats.idle(in_transaction);
        }

        // Keep track of how much data we sent to the client.
        self.stats.sent(message.len());

        if flush {
            client_socket.send_flush(&message).await?;
        } else {
            client_socket.send(&message).await?;
        }

        Ok(in_transaction)
    }
}
