use pg_query::NodeEnum;

use crate::{
    backend::{Cluster, Server},
    config::Role,
    frontend::router::parser::{CopyParser, Shard},
    net::{CopyData, CopyDone, Protocol, Query},
};

use super::super::{CopyStatement, Error};

static BUFFER_SIZE: usize = 10;

#[derive(Debug)]
pub struct Subscriber {
    copy: CopyParser,
    cluster: Cluster,
    buffer: Vec<CopyData>,
    connections: Vec<Server>,
    stmt: CopyStatement,
}

impl Subscriber {
    pub fn new(copy_stmt: &CopyStatement, cluster: &Cluster) -> Result<Self, Error> {
        let stmt = pg_query::parse(copy_stmt.clone().copy_in().as_str())?;
        let stmt = stmt
            .protobuf
            .stmts
            .first()
            .ok_or(Error::MissingData)?
            .stmt
            .as_ref()
            .ok_or(Error::MissingData)?
            .node
            .as_ref()
            .ok_or(Error::MissingData)?;
        let copy = if let NodeEnum::CopyStmt(stmt) = stmt {
            CopyParser::new(stmt, cluster)
                .map_err(|_| Error::MissingData)?
                .ok_or(Error::MissingData)?
        } else {
            return Err(Error::MissingData);
        };

        Ok(Self {
            copy,
            cluster: cluster.clone(),
            buffer: vec![],
            connections: vec![],
            stmt: copy_stmt.clone(),
        })
    }

    /// Connect to all shards.
    pub async fn connect(&mut self) -> Result<(), Error> {
        let mut servers = vec![];
        for shard in self.cluster.shards() {
            let primary = shard
                .pools_with_roles()
                .iter()
                .filter(|(role, _)| role == &Role::Primary)
                .next()
                .ok_or(Error::NoPrimary)?
                .1
                .standalone()
                .await?;
            servers.push(primary);
        }

        self.connections = servers;

        Ok(())
    }

    pub fn disconnect(&mut self) -> Result<(), Error> {
        self.connections.clear();
        Ok(())
    }

    /// Start copy on the subscriber table.
    pub async fn start_copy(&mut self) -> Result<(), Error> {
        let stmt = Query::new(self.stmt.copy_in());

        if self.connections.is_empty() {
            self.connect().await?;
        }

        for server in &mut self.connections {
            server.send_one(&stmt.clone().into()).await?;
            server.flush().await?;

            let msg = server.read().await?;
            if msg.code() != 'G' {
                return Err(Error::OutOfSync(msg.code()));
            }
        }

        Ok(())
    }

    /// Finish copy
    pub async fn copy_done(&mut self) -> Result<(), Error> {
        self.flush().await?;

        for server in &mut self.connections {
            server.send_one(&CopyDone.into()).await?;
            server.flush().await?;

            let command_complete = server.read().await?;
            if command_complete.code() != 'C' {
                return Err(Error::OutOfSync(command_complete.code()));
            }
            let rfq = server.read().await?;
            if rfq.code() != 'Z' {
                return Err(Error::OutOfSync(rfq.code()));
            }
        }

        Ok(())
    }

    /// Send data to subscriber, buffered.
    pub async fn copy_data(&mut self, data: CopyData) -> Result<(), Error> {
        self.buffer.push(data);
        if self.buffer.len() == BUFFER_SIZE {
            self.flush().await?
        }

        Ok(())
    }

    async fn flush(&mut self) -> Result<(), Error> {
        let result = self.copy.shard(&self.buffer)?;
        self.buffer.clear();

        for row in result {
            for (shard, server) in self.connections.iter_mut().enumerate() {
                match row.shard() {
                    Shard::All => server.send_one(&row.message().into()).await?,
                    Shard::Direct(destination) => {
                        if *destination == shard {
                            server.send_one(&row.message().into()).await?;
                        }
                    }
                    Shard::Multi(multi) => {
                        if multi.contains(&shard) {
                            server.send_one(&row.message().into()).await?;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_subscriber() {
        crate::logger();

        let copy = CopyStatement::new("pgdog", "sharded", &["id".into(), "value".into()]);
        let cluster = Cluster::new_test();
        cluster.launch();

        cluster
            .execute("TRUNCATE TABLE pgdog.sharded")
            .await
            .unwrap();

        let mut subscriber = Subscriber::new(&copy, &cluster).unwrap();
        subscriber.start_copy().await.unwrap();

        for i in 0..25 {
            let copy_data = CopyData::new(format!("{}\ttest@test.com\n", i).as_bytes());
            subscriber.copy_data(copy_data).await.unwrap();
        }

        subscriber.copy_done().await.unwrap();
        cluster
            .execute("TRUNCATE TABLE pgdog.sharded")
            .await
            .unwrap();

        cluster.shutdown();
    }
}
