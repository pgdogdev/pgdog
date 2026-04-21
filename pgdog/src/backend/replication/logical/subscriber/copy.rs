//! Shard COPY stream from one source
//! between N shards.

use futures::future::try_join_all;
use pg_query::{parse_raw, NodeEnum};
use pgdog_config::QueryParserEngine;
use tracing::debug;

use crate::{
    backend::{Cluster, ConnectReason, Server},
    config::Role,
    frontend::router::parser::{CopyParser, Shard},
    net::{CopyData, CopyDone, ErrorResponse, FromBytes, Protocol, Query, ToBytes},
};

use super::super::{CopyStatement, Error};

// Not really needed, but we're currently
// sharding 3 CopyData messages at a time.
// This reduces memory allocations.
static BUFFER_SIZE: usize = 3;

#[derive(Debug)]
pub struct CopySubscriber {
    copy: CopyParser,
    cluster: Cluster,
    buffer: Vec<CopyData>,
    connections: Vec<Server>,
    stmt: CopyStatement,
    bytes_sharded: usize,
}

impl CopySubscriber {
    /// COPY statement determines:
    ///
    /// 1. What kind of encoding we use.
    /// 2. Which column is used for sharding.
    ///
    pub fn new(
        copy_stmt: &CopyStatement,
        cluster: &Cluster,
        query_parser_engine: QueryParserEngine,
    ) -> Result<Self, Error> {
        let stmt = match query_parser_engine {
            QueryParserEngine::PgQueryProtobuf => {
                pg_query::parse(copy_stmt.clone().copy_in().as_str())
            }
            QueryParserEngine::PgQueryRaw => parse_raw(copy_stmt.clone().copy_in().as_str()),
        }?;
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
            CopyParser::new(stmt, cluster).map_err(|_| Error::MissingData)?
        } else {
            return Err(Error::MissingData);
        };

        Ok(Self {
            copy,
            cluster: cluster.clone(),
            buffer: vec![],
            connections: vec![],
            stmt: copy_stmt.clone(),
            bytes_sharded: 0,
        })
    }

    /// Connect to all shards. One connection per primary.
    pub async fn connect(&mut self) -> Result<(), Error> {
        let mut servers = vec![];
        for shard in self.cluster.shards() {
            let primary = shard
                .pools_with_roles()
                .iter()
                .find(|(role, _)| role == &Role::Primary)
                .ok_or(Error::NoPrimary)?
                .1
                .standalone(ConnectReason::Replication)
                .await?;
            servers.push(primary);
        }

        self.connections = servers;

        Ok(())
    }

    /// Disconnect from all shards.
    pub async fn disconnect(&mut self) -> Result<(), Error> {
        self.connections.clear();

        Ok(())
    }

    /// Start COPY on all shards.
    pub async fn start_copy(&mut self) -> Result<(), Error> {
        if self.connections.is_empty() {
            self.connect().await?;
        }

        let stmt = Query::new(self.stmt.copy_in());

        // Start COPY IN on all shards concurrently.
        try_join_all(self.connections.iter_mut().map(|server| {
            let msg: crate::net::ProtocolMessage = stmt.clone().into();
            debug!("{} [{}]", stmt.query(), server.addr());

            async move {
                server.send_one(&msg).await?;
                server.flush().await?;
                let reply = server.read().await?;
                match reply.code() {
                    'G' => Ok(()),
                    'E' => Err(Error::PgError(Box::new(ErrorResponse::from_bytes(
                        reply.to_bytes()?,
                    )?))),
                    c => Err(Error::OutOfSync(c)),
                }
            }
        }))
        .await?;

        Ok(())
    }

    /// Finish COPY on all shards.
    pub async fn copy_done(&mut self) -> Result<(), Error> {
        self.flush().await?;

        // Finalise COPY on all shards concurrently.
        try_join_all(self.connections.iter_mut().map(|server| async move {
            server.send_one(&CopyDone.into()).await?;
            server.flush().await?;

            let cc = server.read().await?;
            match cc.code() {
                'E' => {
                    let error = ErrorResponse::from_bytes(cc.to_bytes()?)?;
                    if error.code == "08P01" && error.message == "insufficient data left in message"
                    {
                        return Err(Error::BinaryFormatMismatch(Box::new(error)));
                    }
                    return Err(Error::PgError(Box::new(error)));
                }
                'C' => (),
                c => return Err(Error::OutOfSync(c)),
            }

            let rfq = server.read().await?;
            if rfq.code() != 'Z' {
                return Err(Error::OutOfSync(rfq.code()));
            }
            Ok(())
        }))
        .await?;

        Ok(())
    }

    /// Send data to subscriber, buffered.
    pub async fn copy_data(&mut self, data: CopyData) -> Result<(usize, usize), Error> {
        self.buffer.push(data);
        if self.buffer.len() == BUFFER_SIZE {
            return self.flush().await;
        }

        Ok((0, 0))
    }

    async fn flush(&mut self) -> Result<(usize, usize), Error> {
        if self.buffer.is_empty() {
            return Ok((0, 0));
        }

        let result = self.copy.shard(&self.buffer)?;
        self.buffer.clear();

        let rows = result.len();
        let bytes = result.iter().map(|row| row.len()).sum::<usize>();
        self.bytes_sharded += bytes;

        // Route each row to the right shard(s). send_one is a buffered write
        // so this loop does no I/O — no concurrency needed here.
        for row in &result {
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

        // Flush all shards concurrently — this is the actual socket write.
        try_join_all(self.connections.iter_mut().map(|s| s.flush())).await?;

        Ok((rows, bytes))
    }

    /// Total amount of bytes shaded.
    pub fn bytes_sharded(&self) -> usize {
        self.bytes_sharded
    }
}

#[cfg(test)]
mod test {
    use bytes::Bytes;

    use crate::{
        backend::{pool::Request, replication::publisher::PublicationTable},
        config::config,
        frontend::router::parser::binary::{header::Header, Data, Tuple},
    };

    use super::*;

    #[tokio::test]
    async fn test_subscriber() {
        crate::logger();

        let table = PublicationTable {
            schema: "pgdog".into(),
            name: "sharded".into(),
            ..Default::default()
        };

        let copy = CopyStatement::new(
            &table,
            &["id".into(), "value".into()],
            pgdog_config::CopyFormat::Binary,
        );
        let cluster = Cluster::new_test(&config());
        cluster.launch();

        cluster
            .execute("CREATE TABLE IF NOT EXISTS pgdog.sharded (id BIGINT, value TEXT)")
            .await
            .unwrap();

        cluster
            .execute("TRUNCATE TABLE pgdog.sharded")
            .await
            .unwrap();

        let mut subscriber =
            CopySubscriber::new(&copy, &cluster, config().config.general.query_parser_engine)
                .unwrap();
        subscriber.start_copy().await.unwrap();

        let header = CopyData::new(&Header::new().to_bytes().unwrap());
        subscriber.copy_data(header).await.unwrap();

        for i in 0..25_i64 {
            let id = Data::Column(Bytes::copy_from_slice(&i.to_be_bytes()));
            let email = Data::Column(Bytes::copy_from_slice("test@test.com".as_bytes()));
            let tuple = Tuple::new(&[id, email]);
            subscriber
                .copy_data(CopyData::new(&tuple.to_bytes().unwrap()))
                .await
                .unwrap();
        }

        subscriber
            .copy_data(CopyData::new(&Tuple::new_end().to_bytes().unwrap()))
            .await
            .unwrap();

        subscriber.copy_done().await.unwrap();
        let mut server = cluster.primary(0, &Request::default()).await.unwrap();
        let count = server
            .fetch_all::<i64>("SELECT COUNT(*)::BIGINT FROM pgdog.sharded")
            .await
            .unwrap();
        // Test shards point to the same database.
        // Otherwise, it would of been 50 if this didn't work (all shard).
        assert_eq!(count.first().unwrap().clone(), 25);

        cluster
            .execute("TRUNCATE TABLE pgdog.sharded")
            .await
            .unwrap();

        cluster.shutdown();
    }
}
