//! Handle server response.

use crate::{
    backend::pool::Connection,
    frontend::{client::transaction::Transaction, Error, Stats},
    net::{Message, Protocol},
};

use super::engine_impl::Stream;

pub struct ServerMessage<'a> {
    backend: &'a mut Connection,
    transaction: &'a mut Transaction,
    stats: &'a mut Stats,
}

impl<'a> ServerMessage<'a> {
    pub fn new(
        backend: &'a mut Connection,
        transaction: &'a mut Transaction,
        stats: &'a mut Stats,
    ) -> Self {
        Self {
            backend,
            transaction,
            stats,
        }
    }

    pub async fn handle(
        &mut self,
        message: Message,
        client_socket: &mut Stream,
    ) -> Result<usize, Error> {
        let code = message.code();
        let flush = matches!(code, 'Z' | 'G' | 'E' | 'N' | 'A')
            || !self.backend.has_more_messages()
            || message.streaming();

        if code == 'Z' {
            // Update stats.
            self.stats.query();
            self.stats.idle(self.transaction.started());

            if message.transaction_finished() {
                self.backend.mirror_flush();
                self.transaction.finish();
                self.stats.transaction();
            }
        }

        if flush {
            client_socket.send_flush(&message).await?;
        } else {
            client_socket.send(&message).await?;
        }

        Ok(message.len())
    }
}
