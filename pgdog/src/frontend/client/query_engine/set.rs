use super::engine_impl::Stream;
use crate::{
    frontend::{Error, Stats},
    net::{parameter::ParameterValue, CommandComplete, Parameters, Protocol, ReadyForQuery},
};

pub struct Set<'a> {
    params: &'a mut Parameters,
    stats: &'a mut Stats,
}

impl<'a> Set<'a> {
    pub fn new(params: &'a mut Parameters, stats: &'a mut Stats) -> Self {
        Self { params, stats }
    }

    pub async fn handle(
        &'a mut self,
        name: &str,
        value: &ParameterValue,
        client_socket: &mut Stream,
    ) -> Result<(), Error> {
        self.params.insert(name, value.clone());

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
