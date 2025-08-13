use crate::{
    backend::pool::Connection,
    frontend::{Command, Error, Stats},
    net::{CommandComplete, Protocol, ReadyForQuery},
};

use super::engine_impl::Stream;

pub struct PubSub<'a> {
    backend: &'a mut Connection,
    stats: &'a mut Stats,
    in_transaction: bool,
}

impl<'a> PubSub<'a> {
    pub fn new(backend: &'a mut Connection, stats: &'a mut Stats, in_transaction: bool) -> Self {
        Self {
            backend,
            stats,
            in_transaction,
        }
    }

    pub async fn handle(
        &'a mut self,
        client_socket: &mut Stream,
        command: &Command,
    ) -> Result<(), Error> {
        let command = match command {
            Command::Listen { channel, shard } => {
                self.backend.listen(channel, shard.clone()).await?;
                CommandComplete::new("LISTEN")
            }

            Command::Notify {
                channel,
                payload,
                shard,
            } => {
                self.backend.notify(channel, payload, shard.clone()).await?;
                CommandComplete::new("NOTIFY")
            }

            Command::Unlisten(channel) => {
                self.backend.unlisten(channel);
                CommandComplete::new("UNLISTEN")
            }

            _ => unreachable!("query_engine/pub_sub"),
        };

        let bytes_sent = client_socket
            .send_many(&[
                command.message()?,
                ReadyForQuery::in_transaction(self.in_transaction).message()?,
            ])
            .await?;
        self.stats.sent(bytes_sent);

        Ok(())
    }
}
