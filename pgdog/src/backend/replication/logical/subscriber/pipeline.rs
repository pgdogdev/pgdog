use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::select;
use tokio::spawn;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tracing::trace;

use crate::backend::Server;
use crate::backend::pool::Address;
use crate::net::{
    Bind, CommandComplete, ErrorResponse, Execute, Flush, FromBytes, Message, Parse, Protocol,
    ProtocolMessage, Sync, ToBytes,
};
use pgdog_stats::MissedRows;

use super::super::Error;
use super::connect_address;
use super::replication_origin::ReplicationOrigin;
use crate::util::retry::{Retry, RetryConfig};
use crate::util::safe_interval;

/// We flush the buffer when hitting this so that we can batch that number of operations together,
/// instead of doing them individually.
///
/// If it doesn't hit this number beforehand, it's ran when we have a Sync.
const ROWS_PER_FLUSH: u32 = 100;

/// This represents backpressure (if the Shard can't keep up)
const COMMAND_CHANNEL_SIZE: usize = 4096;

const PROGRESS_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Debug, Clone, Copy, bon::Builder)]
pub(crate) struct ProgressCheck {
    #[builder(default = PROGRESS_INTERVAL)]
    interval: Duration,
    retry_attempts: usize,
    retry_delay: Duration,
}

// State shared between the handle and its background listener and progress tasks.
#[derive(Debug, Default)]
struct Shared {
    // First error observed on this connection. Sticky until taken.
    error: Option<Error>,
    // Rows a direct-to-shard DML expected to touch but didn't (0 rows affected).
    missed: MissedRows,
    /// Missed rows of acknowledged commits, keyed by the commit's source LSN,
    /// counted once the commit is confirmed to the source.
    pending_missed: VecDeque<(i64, MissedRows)>,
    /// Source LSN of the last commit acknowledged with ReadyForQuery.
    /// It's only acknowledged by backend, but could be not yet durable.
    source_lsn_acked: i64,
    /// Source LSN that should be committed on this shard and should
    /// become durable eventually. It's higher than or equal
    /// to [`Self::source_lsn_durable`]
    source_lsn_changed: i64,
    /// Source LSN that is marked durable by replication origin tracker.
    source_lsn_durable: i64,
}

/// How a sync point completes.
enum SyncPointKind {
    /// Wait for a single ReadyForQuery (`Sync`: commit, or out-of-transaction prepare).
    ReadyForQuery,
    /// In-transaction prepare (`Flush`), there's nothing to track, no RFQ is sent back
    Flush,
}

// One entry per command sent to Postgres, in send order. Popped as acks arrive.
enum OpSyncPoint {
    // Bind/Execute/Flush: resolved by CommandComplete ('C').
    DirectDml { is_direct: bool },
    // Commit or out-of-transaction prepare (Sync): resolved by ReadyForQuery ('Z').
    // `source_lsn` is the source LSN of the commit.
    ReadyForQuery { source_lsn: Option<i64> },
}

// Work sent from the handle to the listener task.
enum Command {
    // Fire-and-forget DML: Bind/Execute/Flush. `is_direct` drives missed-row counting.
    Execute {
        bind: Bind,
        is_direct: bool,
    },
    // A synchronization point: write `messages`, then resolve `done` per `kind`.
    SyncPoint {
        messages: Vec<ProtocolMessage>,
        kind: SyncPointKind,
        source_lsn: Option<i64>,
    },
}

/// Pipelined destination connection: a handle over a background task that
/// owns the `Server` and reconciles responses.
#[derive(Debug)]
pub(crate) struct PipelinedConnection {
    tx: Sender<Command>,
    shared: Arc<Mutex<Shared>>,
    address: Address,
    progress: JoinHandle<()>,
}

impl PipelinedConnection {
    /// This moves `server` into a background task and returns a handle to it.
    /// A second background task reads the progress of `origin`, as set by `check`.
    pub(crate) fn new(
        server: Server,
        origin: ReplicationOrigin,
        check: ProgressCheck,
    ) -> Result<Self, Error> {
        let (tx, rx) = channel(COMMAND_CHANNEL_SIZE);
        let shared = Arc::new(Mutex::new(Shared::default()));
        let address = server.addr().clone();

        let listener = Listener {
            rx,
            server,
            shared: shared.clone(),
            queue: VecDeque::new(),
            flushed: 0,
        };

        spawn(listener.run());

        let progress = Progress {
            origin,
            address: address.clone(),
            shared: shared.clone(),
            check,
        };

        Ok(Self {
            tx,
            shared,
            address,
            progress: spawn(progress.run()),
        })
    }

    /// Server address.
    pub(crate) fn addr(&self) -> &Address {
        &self.address
    }

    /// Identify if the progress should be limited with upper bound by
    /// the durable_lsn on this destination.
    /// Some(durable_lsn) - on shard, if there are commits in progress that
    /// are not yet durable. Returns the last durable lsn.
    /// None - all pushed commits already durable
    pub(crate) fn durable_lsn_progress(&self) -> Option<i64> {
        let shared = self.shared.lock();
        (shared.source_lsn_durable < shared.source_lsn_changed).then_some(shared.source_lsn_durable)
    }

    /// Missed rows of the commits confirmed up to `source_lsn_confirmed`.
    pub(crate) fn take_confirmed_missed(&self, source_lsn_confirmed: i64) -> MissedRows {
        let mut shared = self.shared.lock();
        let mut missed = MissedRows::default();
        while let Some((_, rows)) = shared
            .pending_missed
            .pop_front_if(|(source_lsn, _)| *source_lsn <= source_lsn_confirmed)
        {
            missed.merge(rows);
        }
        missed
    }

    /// Enqueue a DML statement (`Bind/Execute/Flush`) without waiting for its
    /// response. `is_direct` marks a single-shard write whose 0-row result
    /// counts as a missed row.
    pub(crate) async fn execute(&self, bind: Bind, is_direct: bool) -> Result<(), Error> {
        self.tx
            .send(Command::Execute { bind, is_direct })
            .await
            .map_err(|_| Error::PipelineClosed)
    }

    /// Prepare `parses` and wait for the acknowledgments. Inside a transaction
    /// uses `Flush` (must not commit the open implicit transaction); otherwise
    /// uses `Sync`.
    pub(crate) async fn prepare(
        &self,
        parses: &[Parse],
        in_transaction: bool,
    ) -> Result<(), Error> {
        if parses.is_empty() {
            return Ok(());
        }
        let mut messages: Vec<ProtocolMessage> = parses.iter().map(|p| p.clone().into()).collect();
        let kind = if in_transaction {
            messages.push(Flush.into());
            SyncPointKind::Flush
        } else {
            messages.push(Sync.into());
            SyncPointKind::ReadyForQuery
        };
        self.send_command(messages, kind, None).await
    }

    /// Commit the open implicit transaction on this shard as the source
    /// commit at `source_lsn`, recording it in the replication origin first.
    pub(crate) async fn commit(&self, origin: Bind, source_lsn: i64) -> Result<(), Error> {
        self.shared.lock().source_lsn_changed = source_lsn;
        self.execute(origin, false).await?;
        self.sync(Some(source_lsn)).await
    }

    /// Send `Sync` and wait for `ReadyForQuery` (commits the open implicit
    /// transaction on this shard). `source_lsn` is the source LSN of the commit.
    async fn sync(&self, source_lsn: Option<i64>) -> Result<(), Error> {
        self.send_command(vec![Sync.into()], SyncPointKind::ReadyForQuery, source_lsn)
            .await
    }

    /// Non-blocking peek + take of the latched error. `Some` means the shard
    /// has errored and the transaction must be rolled back.
    pub(crate) fn take_error(&self) -> Option<Error> {
        self.shared.lock().error.take()
    }

    /// This function send the prepared command along with the type of sync point we are waiting for.
    async fn send_command(
        &self,
        messages: Vec<ProtocolMessage>,
        kind: SyncPointKind,
        source_lsn: Option<i64>,
    ) -> Result<(), Error> {
        self.tx
            .send(Command::SyncPoint {
                messages,
                kind,
                source_lsn,
            })
            .await
            .map_err(|_| Error::PipelineClosed)
    }
}

/// Background task: owns the `Server`, writes queued messages, and reconciles
/// responses (counts acks, records missed rows, latches the first error).
struct Listener {
    rx: Receiver<Command>,
    server: Server,
    shared: Arc<Mutex<Shared>>,
    queue: VecDeque<OpSyncPoint>,
    flushed: u32,
}

impl Listener {
    async fn run(mut self) {
        loop {
            select! {
                // Read-first, biased: drain responses before issuing more
                // writes. A fair select can pick the write branch while acks
                // sit unread, filling the socket buffers in both directions
                // and deadlocking the full-duplex pipeline. Draining reads
                // first keeps Postgres's send buffer clear so it keeps reading
                // our commands, so our writes never block.
                biased;

                message = self.server.read() => {
                    match message {
                        Ok(message) => self.handle_response(message),
                        Err(err) => {
                            self.latch_error(err.into());
                            self.wake_all();
                            break;
                        }
                    }
                }

                command = self.rx.recv() => {
                    match command {
                        Some(command) => {
                            if self.handle_command(command).await.is_err() {
                                break;
                            }
                        }
                        None => break,
                    }
                }
            }
        }
    }

    async fn handle_command(&mut self, command: Command) -> Result<(), Error> {
        if self.shared.lock().error.is_some() {
            return Ok(());
        }

        match command {
            Command::Execute { bind, is_direct } => {
                // If our command fails to be executed, we latch the error to the shared state.
                // we also wake up all the sync point and resolve all the sync point waiters to
                // resolve with the error.
                if let Err(err) = self.write_dml(bind).await {
                    self.latch_error(err);
                    self.wake_all();
                    return Err(Error::PipelineClosed);
                }
                self.queue.push_back(OpSyncPoint::DirectDml { is_direct });
            }
            Command::SyncPoint {
                messages,
                kind,
                source_lsn,
            } => {
                if let Err(err) = self.write(&messages).await {
                    self.latch_error(err);
                    self.wake_all();
                    return Err(Error::PipelineClosed);
                }
                self.flushed = 0;

                match kind {
                    SyncPointKind::ReadyForQuery => {
                        self.queue
                            .push_back(OpSyncPoint::ReadyForQuery { source_lsn });
                    }
                    SyncPointKind::Flush => {}
                }
            }
        }

        Ok(())
    }

    fn handle_response(&mut self, message: Message) {
        let code = message.code();
        trace!("[{}] --> {}", self.address(), code);

        match code {
            'E' => {
                let err = ErrorResponse::from_bytes(message.to_bytes())
                    .map(|resp| Error::PgError(Box::new(resp)))
                    .unwrap_or(Error::PipelineClosed);
                self.latch_error(err);
                // After an error Postgres skips until Sync; abandon tracking and
                // resolve every waiter so the handle can roll back.
                self.wake_all();
            }
            // BindComplete: nothing to account for.
            '2' => {}
            // CommandComplete: match to the DML op and count missed rows.
            'C' => {
                let is_direct = match self.queue.pop_front() {
                    Some(OpSyncPoint::DirectDml { is_direct }) => is_direct,
                    _ => false,
                };
                if is_direct
                    && let Ok(complete) = CommandComplete::try_from(message)
                    && matches!(complete.rows(), Ok(Some(0)))
                {
                    let mut shared = self.shared.lock();
                    match complete.tag() {
                        "INSERT" => shared.missed.inserts += 1,
                        "UPDATE" => shared.missed.updates += 1,
                        "DELETE" => shared.missed.deletes += 1,
                        _ => (),
                    }
                }
            }
            // ReadyForQuery: resolve the front ReadySync waiter.
            'Z' => {
                if let Some(OpSyncPoint::ReadyForQuery {
                    source_lsn: Some(source_lsn),
                }) = self.queue.pop_front()
                {
                    let mut shared = self.shared.lock();
                    shared.source_lsn_acked = source_lsn;
                    if shared.missed.non_zero() {
                        let missed = std::mem::take(&mut shared.missed);
                        shared.pending_missed.push_back((source_lsn, missed));
                    }
                }
            }
            // NoticeResponse / ParameterStatus / NotificationResponse / etc.
            _ => {}
        }
    }

    /// Write messages to the socket and flush if hitting `ROWS_PER_FLUSH` rows in the buffer.
    /// Write one DML: Bind/Execute/Flush
    async fn write_dml(&mut self, bind: Bind) -> Result<(), Error> {
        let bind: ProtocolMessage = bind.into();
        self.server.send_one(&bind).await?;
        self.server.send_one(&Execute::new().into()).await?;

        self.flushed += 1;
        if self.flushed == ROWS_PER_FLUSH {
            self.flushed = 0;

            self.server.send_one(&Flush.into()).await?;
            self.server.flush().await?;
        }
        Ok(())
    }

    async fn write(&mut self, messages: &[ProtocolMessage]) -> Result<(), Error> {
        for message in messages {
            self.server.send_one(message).await?;
        }
        self.server.flush().await?;
        Ok(())
    }

    fn latch_error(&self, err: Error) {
        self.shared.lock().latch_error(err);
    }

    fn wake_all(&mut self) {
        self.queue.clear();
    }

    fn address(&self) -> &Address {
        self.server.addr()
    }
}

impl Shared {
    fn latch_error(&mut self, err: Error) {
        if self.error.is_none() {
            self.error = Some(err);
        }
    }
}

impl Drop for PipelinedConnection {
    fn drop(&mut self) {
        self.progress.abort();
    }
}

/// Background task: reads the replication origin progress on a separate
/// connection and updates the durable LSN.
struct Progress {
    origin: ReplicationOrigin,
    address: Address,
    shared: Arc<Mutex<Shared>>,
    check: ProgressCheck,
}

impl Progress {
    async fn run(self) {
        let mut server = None;
        let mut interval = safe_interval(self.check.interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut retry = Retry::new(
            RetryConfig::builder()
                .name(format!("[replication] origin progress {}", self.address))
                .max_attempts(self.check.retry_attempts)
                .delay(self.check.retry_delay)
                .build(),
        );

        loop {
            interval.tick().await;
            let Err(err) = self.refresh(&mut server).await else {
                retry.reset();
                continue;
            };

            server = None;
            if !err.is_retryable() || !retry.delay_retry(&err).await {
                self.shared.lock().latch_error(err);
                break;
            }
        }
    }

    /// The progress is capped by the acknowledged LSN: an aborted commit also
    /// moves the origin, but it is never acknowledged.
    async fn refresh(&self, server: &mut Option<Server>) -> Result<(), Error> {
        {
            let shared = self.shared.lock();
            if shared.source_lsn_durable >= shared.source_lsn_changed {
                return Ok(());
            }
        }

        let server = match server {
            Some(server) => server,
            None => server.insert(connect_address(&self.address).await?),
        };
        // PERF: the progress will cause actual flush on destination, that could
        // lower the performance in theory. But the effect is unknown and it's
        // not very simple to get the what is actually flushed otherwise.
        let progress = self.origin.progress(server, true).await?;
        let mut shared = self.shared.lock();
        shared.source_lsn_durable = shared.source_lsn_acked.min(progress.lsn);

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        backend::server::test::test_server,
        net::{Parse, messages::bind::Parameter},
    };
    use std::time::{Duration, Instant};
    use tokio::time::sleep;

    fn origin(name: &str, address: &Address) -> ReplicationOrigin {
        ReplicationOrigin::builder()
            .name(name)
            .address(address)
            .shard(0)
            .build()
    }

    fn pipelined(server: Server) -> PipelinedConnection {
        let origin = origin("test_pipeline", server.addr());
        let check = ProgressCheck::builder()
            .retry_attempts(0)
            .retry_delay(Duration::from_millis(100))
            .build();
        PipelinedConnection::new(server, origin, check).unwrap()
    }

    async fn commit_and_wait(conn: &PipelinedConnection, lsn: i64) -> bool {
        conn.sync(Some(lsn)).await.unwrap();

        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if conn.shared.lock().source_lsn_acked == lsn {
                return true;
            }
            sleep(Duration::from_millis(10)).await;
        }
        false
    }

    async fn wait_for_error(conn: &PipelinedConnection) -> Option<Error> {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if let Some(err) = conn.take_error() {
                return Some(err);
            }
            sleep(Duration::from_millis(10)).await;
        }
        None
    }

    #[tokio::test]
    async fn prepare_execute_drain_commit() {
        let server = test_server().await;
        let conn = pipelined(server);

        // Prepare + create a temp table (out of transaction: uses Sync).
        conn.prepare(
            &[Parse::named(
                "__pipe_create",
                "CREATE TEMP TABLE __pipe_t (id bigint)",
            )],
            false,
        )
        .await
        .unwrap();
        conn.execute(Bind::new_statement("__pipe_create"), false)
            .await
            .unwrap();

        // Prepare an insert (Sync) and enqueue a single row.
        conn.prepare(
            &[Parse::named(
                "__pipe_insert",
                "INSERT INTO __pipe_t (id) VALUES ($1)",
            )],
            false,
        )
        .await
        .unwrap();
        conn.execute(
            Bind::new_params("__pipe_insert", &[Parameter::new(b"42")]),
            false,
        )
        .await
        .unwrap();

        assert!(commit_and_wait(&conn, 1).await);
        assert!(conn.take_error().is_none());
    }

    #[tokio::test]
    async fn in_transaction_prepare_uses_flush() {
        let server = test_server().await;
        let conn = pipelined(server);

        conn.prepare(&[Parse::named("__pipe_flush", "SELECT $1::bigint")], true)
            .await
            .unwrap();

        conn.execute(
            Bind::new_params("__pipe_flush", &[Parameter::new(b"42")]),
            false,
        )
        .await
        .unwrap();
        assert!(commit_and_wait(&conn, 1).await);
        assert!(conn.take_error().is_none());
    }

    #[tokio::test]
    async fn prepare_invalid_sql_sync_returns_error() {
        let server = test_server().await;
        let conn = pipelined(server);

        conn.prepare(&[Parse::named("__pipe_bad", "NOT VALID SQL")], false)
            .await
            .unwrap();
        let err = wait_for_error(&conn).await.unwrap();
        assert!(
            matches!(err, Error::PgError(_)),
            "unexpected error: {err:?}"
        );
        assert!(conn.take_error().is_none());
    }

    #[tokio::test]
    async fn prepare_invalid_sql_flush_returns_error() {
        let server = test_server().await;
        let conn = pipelined(server);

        conn.prepare(&[Parse::named("__pipe_bad", "NOT VALID SQL")], true)
            .await
            .unwrap();
        let err = wait_for_error(&conn).await.unwrap();
        assert!(
            matches!(err, Error::PgError(_)),
            "unexpected error: {err:?}"
        );
        assert!(conn.take_error().is_none());
    }

    #[tokio::test]
    async fn execute_runtime_error_latches_and_does_not_block() {
        use tokio::time::timeout;

        let server = test_server().await;
        let conn = pipelined(server);

        // Valid prepare (succeeds), then a fire-and-forget execute that errors
        // only at execution time: division by zero. The ErrorResponse arrives
        // asynchronously as 'E'.
        conn.prepare(&[Parse::named("__pipe_div", "SELECT 1 / $1::int")], false)
            .await
            .unwrap();
        conn.execute(
            Bind::new_params("__pipe_div", &[Parameter::new(b"0")]),
            false,
        )
        .await
        .unwrap();

        conn.sync(None).await.unwrap();
        let err = wait_for_error(&conn).await.unwrap();
        assert!(
            matches!(err, Error::PgError(_)),
            "unexpected error: {err:?}"
        );

        for _ in 0..3 {
            let _ = timeout(
                Duration::from_secs(5),
                conn.execute(
                    Bind::new_params("__pipe_div", &[Parameter::new(b"1")]),
                    false,
                ),
            )
            .await
            .expect("execute blocked after error");
        }
        let _ = timeout(Duration::from_secs(5), conn.sync(None))
            .await
            .expect("sync blocked after error");
    }

    #[tokio::test]
    async fn errored_connection_never_completes_commit() {
        let server = test_server().await;
        let conn = pipelined(server);

        // Fire-and-forget DML that fails at execution time (division by zero).
        conn.prepare(&[Parse::named("__drain_div", "SELECT 1 / $1::int")], false)
            .await
            .unwrap();
        conn.execute(
            Bind::new_params("__drain_div", &[Parameter::new(b"0")]),
            true,
        )
        .await
        .unwrap();

        conn.sync(Some(1)).await.unwrap();
        let err = wait_for_error(&conn).await.unwrap();
        assert!(
            matches!(err, Error::PgError(_)),
            "unexpected error: {err:?}"
        );
        sleep(Duration::from_millis(300)).await;
        assert_eq!(conn.shared.lock().source_lsn_acked, 0);
    }

    #[tokio::test]
    async fn direct_dml_zero_rows_counts_missed() {
        let server = test_server().await;
        let conn = pipelined(server);

        // Scratch table with one row (id = 1).
        conn.prepare(
            &[Parse::named(
                "__miss_create",
                "CREATE TEMP TABLE __miss (id bigint)",
            )],
            false,
        )
        .await
        .unwrap();
        conn.execute(Bind::new_statement("__miss_create"), false)
            .await
            .unwrap();
        conn.prepare(
            &[Parse::named(
                "__miss_seed",
                "INSERT INTO __miss (id) VALUES (1)",
            )],
            false,
        )
        .await
        .unwrap();
        conn.execute(Bind::new_statement("__miss_seed"), false)
            .await
            .unwrap();

        // Direct DELETE matching nothing -> "DELETE 0" -> counted.
        conn.prepare(
            &[Parse::named(
                "__miss_del",
                "DELETE FROM __miss WHERE id = $1",
            )],
            false,
        )
        .await
        .unwrap();
        conn.execute(
            Bind::new_params("__miss_del", &[Parameter::new(b"999")]),
            true,
        )
        .await
        .unwrap();

        // Direct UPDATE matching nothing -> "UPDATE 0" -> counted.
        conn.prepare(
            &[Parse::named(
                "__miss_upd",
                "UPDATE __miss SET id = id WHERE id = $1",
            )],
            false,
        )
        .await
        .unwrap();
        conn.execute(
            Bind::new_params("__miss_upd", &[Parameter::new(b"999")]),
            true,
        )
        .await
        .unwrap();

        // Negative control 1: same 0-row DELETE but not direct -> not counted.
        conn.execute(
            Bind::new_params("__miss_del", &[Parameter::new(b"999")]),
            false,
        )
        .await
        .unwrap();

        // Negative control 2: direct DELETE that hits the seeded row (1 row) ->
        // rows != 0 -> not counted.
        conn.execute(
            Bind::new_params("__miss_del", &[Parameter::new(b"1")]),
            true,
        )
        .await
        .unwrap();

        assert!(commit_and_wait(&conn, 1).await);
        assert!(conn.take_error().is_none());

        // Counted only once the commit is confirmed to the source.
        assert!(!conn.take_confirmed_missed(0).non_zero());
        let missed = conn.take_confirmed_missed(1);
        // (insert, update, delete): one 0-row direct UPDATE and one 0-row direct
        // DELETE counted; the non-direct DELETE and the 1-row DELETE are not.
        // Insert never missed here, so it must stay 0 (no spurious counter).
        assert_eq!(missed.inserts, 0);
        assert_eq!(missed.updates, 1);
        assert_eq!(missed.deletes, 1);
        assert!(!conn.take_confirmed_missed(1).non_zero());
    }

    fn spawn_progress(
        origin: ReplicationOrigin,
        address: Address,
        check: ProgressCheck,
        changed: i64,
        acked: i64,
    ) -> (Arc<Mutex<Shared>>, JoinHandle<()>) {
        let shared = Arc::new(Mutex::new(Shared {
            source_lsn_changed: changed,
            source_lsn_acked: acked,
            ..Default::default()
        }));
        let progress = Progress {
            origin,
            address,
            shared: shared.clone(),
            check,
        };
        (shared, spawn(progress.run()))
    }

    async fn wait_until(shared: &Mutex<Shared>, done: impl Fn(&Shared) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !done(&shared.lock()) {
            assert!(Instant::now() < deadline, "condition not reached");
            sleep(Duration::from_millis(10)).await;
        }
    }

    fn unreachable() -> Address {
        Address {
            port: 1,
            ..Address::new_test()
        }
    }

    #[tokio::test]
    async fn progress_durable_follows_origin_capped_by_acked() {
        let address = Address::new_test();
        let origin = origin("test_progress_durable", &address);
        origin.drop_origin().await.unwrap();
        origin.create_origin().await.unwrap();

        let mut session = test_server().await;
        origin.setup_session(&mut session).await.unwrap();
        session
            .execute_checked(
                "BEGIN; \
                 SELECT pg_replication_origin_xact_setup('0/1000', now()); \
                 SELECT pg_current_xact_id(); \
                 COMMIT",
            )
            .await
            .unwrap();
        drop(session);

        let check = ProgressCheck::builder()
            .interval(Duration::from_millis(10))
            .retry_attempts(0)
            .retry_delay(Duration::from_millis(10))
            .build();
        let (shared, task) = spawn_progress(origin.clone(), address, check, 0x1000, 0x800);

        wait_until(&shared, |shared| shared.source_lsn_durable == 0x800).await;
        sleep(Duration::from_millis(50)).await;
        assert_eq!(shared.lock().source_lsn_durable, 0x800);

        shared.lock().source_lsn_acked = 0x1000;
        wait_until(&shared, |shared| shared.source_lsn_durable == 0x1000).await;
        assert!(shared.lock().error.is_none());

        task.abort();
        origin.drop_origin().await.unwrap();
    }

    #[tokio::test]
    async fn progress_skips_shard_without_pending_commits() {
        let address = unreachable();
        let check = ProgressCheck::builder()
            .interval(Duration::from_millis(10))
            .retry_attempts(1)
            .retry_delay(Duration::from_millis(10))
            .build();
        let (shared, task) =
            spawn_progress(origin("test_progress_idle", &address), address, check, 0, 0);

        sleep(Duration::from_millis(300)).await;
        assert!(shared.lock().error.is_none());

        task.abort();
    }

    #[tokio::test]
    async fn progress_retries_retryable_errors_then_latches() {
        let address = unreachable();
        let check = ProgressCheck::builder()
            .interval(Duration::from_millis(10))
            .retry_attempts(2)
            .retry_delay(Duration::from_millis(100))
            .build();
        let started = Instant::now();
        let (shared, task) = spawn_progress(
            origin("test_progress_retry", &address),
            address,
            check,
            100,
            100,
        );

        wait_until(&shared, |shared| shared.error.is_some()).await;
        assert!(started.elapsed() >= Duration::from_millis(200));
        {
            let shared = shared.lock();
            assert!(shared.error.as_ref().unwrap().is_retryable());
            assert_eq!(shared.source_lsn_durable, 0);
        }

        assert!(task.await.is_ok());
    }

    #[tokio::test]
    async fn progress_latches_non_retryable_error_at_once() {
        let address = Address::new_test();
        let origin = origin("test_progress_missing", &address);
        origin.drop_origin().await.unwrap();
        let check = ProgressCheck::builder()
            .interval(Duration::from_millis(10))
            .retry_attempts(5)
            .retry_delay(Duration::from_secs(10))
            .build();
        let (shared, task) = spawn_progress(origin, address, check, 100, 100);

        wait_until(&shared, |shared| shared.error.is_some()).await;
        assert!(!shared.lock().error.as_ref().unwrap().is_retryable());

        assert!(task.await.is_ok());
    }
}
