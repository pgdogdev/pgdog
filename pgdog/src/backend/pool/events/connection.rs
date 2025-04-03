use crate::{
    backend::{pool::Address, Error, Pool, Server},
    net::{Bind, Execute, Flush, Message, Parse, Protocol, Sync},
};

#[derive(Debug)]
pub struct Connection {
    server: Option<Server>,
    pool: Pool,
}

impl Connection {
    pub fn new(pool: &Pool) -> Self {
        Self {
            server: None,
            pool: pool.clone(),
        }
    }

    pub async fn read(&mut self) -> Result<Message, Error> {
        loop {
            if let Some(ref mut server) = self.server {
                return server.read().await;
            } else {
                self.reconnect().await?;
            }
        }
    }

    pub async fn execute(&mut self, name: &str) -> Result<(), Error> {
        loop {
            if let Some(ref mut server) = self.server {
                return server
                    .send(vec![
                        Bind::new(name).message()?,
                        Execute::new().message()?,
                        Sync.message()?,
                    ])
                    .await;
            } else {
                self.reconnect().await?;
            }
        }
    }

    async fn connect(&mut self) -> Result<(), Error> {
        self.server =
            Some(Server::connect(self.pool.addr(), self.pool.startup_parameters()).await?);
        Ok(())
    }

    async fn prepare(&mut self) -> Result<(), Error> {
        self.server
            .as_mut()
            .unwrap()
            .send(vec![
                Parse::named("lsn_primary", "SELECT pg_current_wal_lsn()").message()?,
                Parse::named("lsn_replica", "SELECT pg_last_wal_replay_lsn()").message()?,
                Parse::named("is_replica", "SELECT pg_is_in_recovery()").message()?,
                Flush.message()?,
            ])
            .await?;
        self.server.as_mut().unwrap().flush().await?;

        for _ in 0..3 {
            let reply = self.server.as_mut().unwrap().read().await?;
            if reply.code() != '1' {
                return Err(Error::NotInSync);
            }
        }

        Ok(())
    }

    pub async fn reconnect(&mut self) -> Result<(), Error> {
        self.connect().await?;
        self.prepare().await
    }

    pub fn addr(&self) -> Result<&Address, Error> {
        Ok(self.server.as_ref().ok_or(Error::NotInSync)?.addr())
    }
}
