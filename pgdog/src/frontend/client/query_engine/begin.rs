use crate::{
    frontend::{Error, Stats},
    net::{CommandComplete, Protocol, ReadyForQuery},
};

use super::engine_impl::Stream;

pub struct Begin<'a> {
    // TODO: Send notice if transaction already started.
    #[allow(dead_code)]
    in_transaction: bool,
    stats: &'a mut Stats,
}

impl<'a> Begin<'a> {
    pub fn new(in_transaction: bool, stats: &'a mut Stats) -> Self {
        Self {
            in_transaction,
            stats,
        }
    }

    pub async fn handle(&mut self, client_socket: &mut Stream) -> Result<(), Error> {
        let bytes_sent = client_socket
            .send_many(&[
                CommandComplete::new_begin().message()?.backend(),
                ReadyForQuery::in_transaction(true).message()?.backend(),
            ])
            .await?;

        self.stats.sent(bytes_sent);

        Ok(())
    }
}
