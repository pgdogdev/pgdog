//! Shard COPY stream from one source
//! between N shards.

use futures::future::join_all;
use pg_query::{NodeEnum, parse_raw};
use pgdog_config::QueryParserEngine;
use tracing::debug;

use crate::frontend::client::query_engine::TwoPcPhase;
use crate::frontend::client::query_engine::two_pc::{Manager, TwoPcTransaction};

use crate::{
    backend::{Cluster, ConnectReason, replication::subscriber::ParallelConnection},
    config::Role,
    frontend::router::parser::{CopyParser, Shard},
    net::{
        CopyData, CopyDone, ErrorResponse, FromBytes, Message, Protocol, ProtocolMessage, Query,
        ToBytes,
    },
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
    connections: Vec<ParallelConnection>,
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
            servers.push(ParallelConnection::new(primary)?);
        }

        self.connections = servers;

        Ok(())
    }

    /// Disconnect from all shards.
    pub async fn disconnect(&mut self) -> Result<(), Error> {
        for conn in std::mem::take(&mut self.connections) {
            conn.reattach().await?;
        }

        Ok(())
    }

    /// Send a single protocol message on a shard connection and confirm the backend replied
    /// with `CommandComplete` followed by `ReadyForQuery`. On an `ErrorResponse` the connection
    /// is drained through the trailing `ReadyForQuery` and the error is surfaced as
    /// `Error::PgError`.
    async fn send_and_confirm(
        server: &mut ParallelConnection,
        message: ProtocolMessage,
    ) -> Result<(), Error> {
        server.send_one(&message).await?;
        server.flush().await?;

        let command_complete = Self::read_skipping_async(server).await?;
        match command_complete.code() {
            'C' => (),
            'E' => {
                let error = ErrorResponse::from_bytes(command_complete.to_bytes())?;
                // Drain through the trailing ReadyForQuery so the connection isn't left
                // mid-protocol.
                Self::drain_to_ready(server).await;
                return Err(error.into());
            }
            c => return Err(Error::OutOfSync(c)),
        }

        let rfq = Self::read_skipping_async(server).await?;
        if rfq.code() != 'Z' {
            return Err(Error::OutOfSync(rfq.code()));
        }

        Ok(())
    }

    /// Read the next message from the server, skipping asynchronous protocol messages that
    /// Postgres may interleave at any time: `NoticeResponse` (`'N'`), `ParameterStatus`
    /// (`'S'`), and `NotificationResponse` (`'A'`). The underlying `Server::read` surfaces these
    /// unfiltered, so without skipping them a trigger-emitted `NOTICE` (e.g. fired during
    /// `CopyDone` finalisation or at `COMMIT`) would be misread as an out-of-sync reply and turn
    /// a successful copy into a spurious failure.
    async fn read_skipping_async(server: &mut ParallelConnection) -> Result<Message, Error> {
        loop {
            let message = server.read().await?;
            if !matches!(message.code(), 'N' | 'S' | 'A') {
                return Ok(message);
            }
        }
    }

    /// Drain messages until the trailing `ReadyForQuery`, leaving the connection at a clean
    /// protocol boundary after an `ErrorResponse`. Best-effort: stops on the first read error
    /// (e.g. the backend closed the connection), so it cannot block indefinitely.
    async fn drain_to_ready(server: &mut ParallelConnection) {
        while let Ok(message) = server.read().await {
            if message.code() == 'Z' {
                break;
            }
        }
    }

    /// Start COPY on all shards.
    pub async fn start_copy(&mut self) -> Result<(), Error> {
        let stmt = Query::new(self.stmt.copy_in());

        if self.connections.is_empty() {
            self.connect().await?;
        }

        for server in &mut self.connections {
            // Open an explicit transaction so the COPY is not autocommit: the commit is
            // deferred to copy_done(), so a CopyDone failure on any shard rolls back every
            // shard (none commit). The commit pass itself is per-shard, not atomic across shards.
            // TODO: implement complete 2pc to copy data to minimize chances of partial commits even more
            Self::send_and_confirm(server, Query::new("BEGIN").into()).await?;

            debug!("{} [{}]", stmt.query(), server.addr());
            server.send_one(&stmt.clone().into()).await?;
            server.flush().await?;

            let msg = server.read().await?;
            match msg.code() {
                'G' => (),
                'E' => {
                    return Err(Error::PgError(Box::new(ErrorResponse::from_bytes(
                        msg.to_bytes(),
                    )?)));
                }
                c => return Err(Error::OutOfSync(c)),
            }
        }

        Ok(())
    }

    /// Finish COPY on all shards.
    pub async fn copy_done(&mut self) -> Result<(), Error> {
        self.flush().await?;

        // Stage pass: send CopyDone to every shard and read CommandComplete + ReadyForQuery.
        // Because each shard is inside an explicit transaction, CopyDone only finalises the
        // COPY (writes pages, builds indexes, checks constraints) — it does NOT commit.
        // The ReadyForQuery status byte is 'T' (in-transaction).
        // A failure here leaves every shard in an open transaction that rolls back when its
        // connection closes → no partial commit.
        for server in &mut self.connections {
            // A binary COPY width mismatch (e.g. an int column copied into a bigint) surfaces as
            // protocol_violation "insufficient data left in message"; reclassify it so the
            // operator gets actionable guidance instead of a generic protocol error.
            Self::send_and_confirm(server, CopyDone.into())
                .await
                .map_err(|error| match error {
                    Error::PgError(pg)
                        if pg.code == "08P01"
                            && pg.message == "insufficient data left in message" =>
                    {
                        Error::BinaryFormatMismatch(pg)
                    }
                    other => other,
                })?;
        }

        // Commit pass: every shard has staged its rows and is sitting in an open transaction.
        // Commit them. The data is already written, so COMMIT is cheap and very likely to
        // succeed. Sequential and NOT atomic across shards: if a COMMIT fails after one or more
        // earlier shards have already committed, those shards stay committed — the only residual
        // partial-commit window (full cross-shard atomicity via 2PC is intentionally out of
        // scope). Shards not yet committed roll back on connection close. The
        // destination_has_rows() guard in parallel_sync.rs prevents a doomed retry if this
        // window is ever hit.
        if self.cluster.two_pc_enabled() {
            self.commit_two_pc().await?;
        } else {
            for (shard, server) in self.connections.iter_mut().enumerate() {
                if let Err(error) =
                    Self::send_and_confirm(server, Query::new("COMMIT").into()).await
                {
                    tracing::error!(
                        "COMMIT failed on destination shard {shard} during copy_done: {error}; \
                         shards committed before it stay committed, the rest roll back on \
                         connection close"
                    );
                    return Err(error);
                }
            }
        }

        Ok(())
    }

    async fn commit_two_pc(&mut self) -> Result<(), Error> {
        let manager = Manager::get();
        let txn = TwoPcTransaction::new();
        let identifier = self.cluster.identifier();

        let _guard_phase_1 = manager
            .transaction_state(&txn, &identifier, TwoPcPhase::Phase1)
            .await?;
        self.two_pc_on_shards(&txn, TwoPcPhase::Phase1).await?;

        let _guard_phase_2 = manager
            .transaction_state(&txn, &identifier, TwoPcPhase::Phase2)
            .await?;
        self.two_pc_on_shards(&txn, TwoPcPhase::Phase2).await?;

        manager.done(&txn).await?;

        Ok(())
    }

    async fn two_pc_on_shards(
        &mut self,
        txn: &TwoPcTransaction,
        phase: TwoPcPhase,
    ) -> Result<(), Error> {
        let mut futures = Vec::new();

        for (shard, server) in self.connections.iter_mut().enumerate() {
            let query = match phase {
                TwoPcPhase::Phase1 => format!("PREPARE TRANSACTION '{txn}_{shard}'"),
                TwoPcPhase::Phase2 => format!("COMMIT PREPARED '{txn}_{shard}'"),
                // Rollback is not issued here. If this path fails, the TwoPcGuards in
                // commit_two_pc() are dropped without manager.done(), and the 2PC Manager
                // cleanup task issues ROLLBACK PREPARED (Phase1) or COMMIT PREPARED (Phase2).
                TwoPcPhase::Rollback => unreachable!(),
            };
            futures.push(Self::send_and_confirm(server, Query::new(query).into()));
        }

        for result in join_all(futures).await {
            result?;
        }

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
        let result = self.copy.shard(&self.buffer)?;
        self.buffer.clear();

        let rows = result.len();
        let bytes = result.iter().map(|row| row.len()).sum::<usize>();

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

        self.bytes_sharded += result.iter().map(|c| c.len()).sum::<usize>();

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
        backend::{
            pool::Request, replication::publisher::PublicationTable, server::test::test_server,
        },
        config::config,
        frontend::router::parser::binary::{Data, Tuple, header::Header},
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

        let header = CopyData::new(&Header::new().to_bytes());
        subscriber.copy_data(header).await.unwrap();

        for i in 0..25_i64 {
            let id = Data::Column(Bytes::copy_from_slice(&i.to_be_bytes()));
            let email = Data::Column(Bytes::copy_from_slice("test@test.com".as_bytes()));
            let tuple = Tuple::new(&[id, email]);
            subscriber
                .copy_data(CopyData::new(&tuple.to_bytes()))
                .await
                .unwrap();
        }

        subscriber
            .copy_data(CopyData::new(&Tuple::new_end().to_bytes()))
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

    #[tokio::test]
    async fn send_and_confirm_skips_async_messages() {
        crate::logger();

        let server = test_server().await;
        let mut conn = ParallelConnection::new(server).unwrap();

        // RAISE WARNING emits a NoticeResponse ('N') before the statement's
        // CommandComplete. Without async-message skipping this was misread as
        // OutOfSync('N'); it must now be transparently skipped.
        CopySubscriber::send_and_confirm(
            &mut conn,
            Query::new("DO $$ BEGIN RAISE WARNING 'noise'; END $$").into(),
        )
        .await
        .unwrap();

        // The connection is left at a clean ReadyForQuery boundary, so it can be
        // handed back to the pool.
        let server = conn.reattach().await.unwrap();
        assert!(server.in_sync());
    }

    #[tokio::test]
    async fn send_and_confirm_drains_to_ready_after_error_with_notice() {
        crate::logger();

        let server = test_server().await;
        let mut conn = ParallelConnection::new(server).unwrap();

        // A NoticeResponse precedes the ErrorResponse. The notice is skipped, the
        // error is surfaced, and drain_to_ready consumes the trailing ReadyForQuery
        // so the connection is not left mid-protocol.
        let result = CopySubscriber::send_and_confirm(
            &mut conn,
            Query::new("DO $$ BEGIN RAISE WARNING 'noise'; RAISE EXCEPTION 'boom'; END $$").into(),
        )
        .await;

        assert!(matches!(result, Err(Error::PgError(_))));

        // drain_to_ready left the connection at a ReadyForQuery boundary.
        let server = conn.reattach().await.unwrap();
        assert!(server.in_sync());
    }
}
