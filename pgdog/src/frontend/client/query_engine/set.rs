use super::engine_impl::Stream;
use crate::{
    frontend::{client::transaction::Transaction, Error, Stats},
    net::{parameter::ParameterValue, CommandComplete, Parameters, Protocol, ReadyForQuery},
};

pub struct Set<'a> {
    params: &'a mut Parameters,
    stats: &'a mut Stats,
    transaction: &'a mut Transaction,
}

impl<'a> Set<'a> {
    pub fn new(
        params: &'a mut Parameters,
        stats: &'a mut Stats,
        transaction: &'a mut Transaction,
    ) -> Self {
        Self {
            params,
            stats,
            transaction,
        }
    }

    pub async fn handle(
        &'a mut self,
        name: &str,
        value: &ParameterValue,
        client_socket: &mut Stream,
    ) -> Result<(), Error> {
        self.params.insert(name, value.clone());

        if self.transaction.started() {
            self.transaction.params().insert(name, value.clone());
        }

        let bytes_sent = client_socket
            .send_many(&[
                CommandComplete::new("SET").message()?,
                ReadyForQuery::in_transaction(false).message()?,
            ])
            .await?;

        self.stats.sent(bytes_sent);

        Ok(())
    }
}
