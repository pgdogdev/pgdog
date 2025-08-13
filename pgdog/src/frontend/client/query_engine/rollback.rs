use crate::{
    backend::pool::Connection,
    frontend::{Error, Stats},
    net::{CommandComplete, ErrorResponse, NoticeResponse, Protocol, ReadyForQuery},
};

use super::engine_impl::Stream;

pub struct Rollback<'a> {
    in_transaction: bool,
    backend: &'a mut Connection,
    stats: &'a mut Stats,
}

impl<'a> Rollback<'a> {
    pub fn new(in_transaction: bool, backend: &'a mut Connection, stats: &'a mut Stats) -> Self {
        Self {
            in_transaction,
            backend,
            stats,
        }
    }

    pub async fn handle(&mut self, client_socket: &mut Stream) -> Result<(), Error> {
        if self.backend.connected() {
            self.backend.execute("ROLLBACK").await?;
        }

        let bytes_sent = if self.in_transaction {
            client_socket
                .send_many(&[
                    CommandComplete::new_rollback().message()?.backend(),
                    ReadyForQuery::in_transaction(false).message()?.backend(),
                ])
                .await?
        } else {
            client_socket
                .send_many(&[
                    NoticeResponse::from(ErrorResponse::no_transaction())
                        .message()?
                        .backend(),
                    CommandComplete::new_rollback().message()?.backend(),
                    ReadyForQuery::in_transaction(false).message()?.backend(),
                ])
                .await?
        };

        self.stats.sent(bytes_sent);

        Ok(())
    }
}
