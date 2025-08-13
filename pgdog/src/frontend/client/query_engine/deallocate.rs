//! Handle DEALLOCATE command.

use crate::{
    frontend::{Error, PreparedStatements, Stats},
    net::{CommandComplete, Protocol, ReadyForQuery},
};

use super::engine_impl::Stream;

pub struct Deallocate<'a> {
    // TODO: Remove statement from the client cache.
    #[allow(dead_code)]
    prepared_statements: &'a mut PreparedStatements,
    in_transaction: bool,
    stats: &'a mut Stats,
}

impl<'a> Deallocate<'a> {
    pub fn new(
        prepared_statements: &'a mut PreparedStatements,
        in_transaction: bool,
        stats: &'a mut Stats,
    ) -> Self {
        Self {
            prepared_statements,
            in_transaction,
            stats,
        }
    }

    pub async fn handle(&'a mut self, client_socket: &'a mut Stream) -> Result<(), Error> {
        let bytes_sent = client_socket
            .send_many(&[
                CommandComplete::new("DEALLOCATE").message()?,
                ReadyForQuery::in_transaction(self.in_transaction).message()?,
            ])
            .await?;

        self.stats.sent(bytes_sent);

        Ok(())
    }
}
