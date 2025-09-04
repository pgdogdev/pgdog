use tokio::time::{timeout, Duration, Instant};
use url::Url;

use crate::{
    backend::{pool::Address, Server, ServerOptions},
    config::config,
};

use super::prelude::*;

#[derive(Debug, Clone)]
pub(crate) struct Probe {
    url: Url,
}

#[async_trait]
impl Command for Probe {
    fn name(&self) -> String {
        "PROBE".into()
    }

    fn parse(sql: &str) -> Result<Self, Error> {
        let url = Url::parse(sql.split(" ").last().ok_or(Error::Empty)?)?;
        Ok(Self { url })
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let mut conn = timeout(
            Duration::from_millis(config().config.general.connect_timeout),
            Server::connect(
                &Address::try_from(self.url.clone()).map_err(|_| Error::InvalidAddress)?,
                ServerOptions::default(),
            ),
        )
        .await?
        .map_err(|err| Error::Backend(Box::new(err)))?;

        let start = Instant::now();
        conn.execute("SELECT 1")
            .await
            .map_err(|err| Error::Backend(Box::new(err)))?;
        let duration = start.elapsed();
        let rd = RowDescription::new(&[Field::bigint("latency")]);
        let mut dr = DataRow::new();
        dr.add(duration.as_millis() as i64);
        Ok(vec![rd.message()?, dr.message()?])
    }
}
