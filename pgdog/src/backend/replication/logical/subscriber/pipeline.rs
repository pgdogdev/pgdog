use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;

use parking_lot::Mutex;
use pgdog_postgres_types::Oid;
use tokio::select;
use tokio::spawn;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tracing::trace;

use crate::backend::Server;
use crate::backend::pool::Address;
use crate::net::{
    Bind, CommandComplete, ErrorResponse, Execute, Flush, FromBytes, Message, Parse, Protocol,
    ProtocolMessage, Sync, ToBytes,
};
use pgdog_stats::MissedRows;

use super::super::Error;

/// The current state of a transaction that is either:
/// (1) sent to the shard, but we have not heard back with a RFQ
/// (2) we've heard back, but it hasn't been confirmed flushed
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct TransactionAwaitingCommit {
    pub(crate) transaction_lsn: i64,
    pub(crate) current_lsn: i64,
    pub(crate) changed_tables: HashSet<Oid>,
    /// Set when we've heard back, and ran `set_durable_bound_if_not_set` stemming from `check_for_committed_transaction`
    /// After which, we await the `wal_flush_lsn` to advance past, so we know, with certainty, this transaction has flushed.
    pub(crate) durable_bound: Option<i64>,
    pub(crate) missed: MissedRows,
}

/// We flush the buffer when hitting this so that we can batch that number of operations together,
/// instead of doing them individually.
///
/// If it doesn't hit this number beforehand, it's ran when we have a Sync.
const ROWS_PER_FLUSH: u32 = 100;

/// This represents backpressure (if the Shard can't keep up)
const COMMAND_CHANNEL_SIZE: usize = 4096;

// State shared between the handle and its background listener task.
#[derive(Debug, Default)]
struct Shared {
    // First error observed on this connection. Sticky until taken.
    error: Option<Error>,
    // Rows a direct-to-shard DML expected to touch but didn't (0 rows affected).
    missed: MissedRows,

    /// A queue of not fully flushed transactions.
    finished_commit: VecDeque<TransactionAwaitingCommit>,

    /// These two fields below are saved state from `refresh_wal_positions` for
    /// transaction bookkeeping.
    last_flushed_lsn: i64,
    last_insert_lsn: i64,
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
    DirectDml {
        is_direct: bool,
    },
    // Commit or out-of-transaction prepare (Sync): resolved by ReadyForQuery ('Z').
    ReadyForQuery {
        transaction: Option<TransactionAwaitingCommit>,
    },
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
        transaction: Option<TransactionAwaitingCommit>,
    },
}

/// Pipelined destination connection: a handle over a background task that
/// owns the `Server` and reconciles responses.
#[derive(Debug)]
pub(crate) struct PipelinedConnection {
    tx: Sender<Command>,
    shared: Arc<Mutex<Shared>>,
    address: Address,
}

impl PipelinedConnection {
    /// This moves `server` into a background task and returns a handle to it.
    pub(crate) fn new(server: Server) -> Result<Self, Error> {
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

        Ok(Self {
            tx,
            shared,
            address,
        })
    }

    /// Server address.
    pub(crate) fn addr(&self) -> &Address {
        &self.address
    }

    /// Fetches the `last_flushed_lsn` and `last_insert_lsn` for this shard.
    pub(crate) fn get_flushed_and_insert_lsn(&self) -> (i64, i64) {
        let lock = self.shared.lock();
        (lock.last_flushed_lsn, lock.last_insert_lsn)
    }

    /// Sets the `last_flushed_lsn` and `last_insert_lsn` for this shard.
    pub(crate) fn set_wal_positions(&self, insert_lsn: i64, flush_lsn: i64) {
        let mut lock = self.shared.lock();
        lock.last_insert_lsn = insert_lsn;
        lock.last_flushed_lsn = flush_lsn;
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

    /// Send `Sync` and wait for `ReadyForQuery` (commits the open implicit
    /// transaction on this shard).
    pub(crate) async fn sync(
        &self,
        transaction: Option<TransactionAwaitingCommit>,
    ) -> Result<(), Error> {
        self.send_command(vec![Sync.into()], SyncPointKind::ReadyForQuery, transaction)
            .await
    }

    /// Checks the front of the `finished_commit` queue, to see what the
    /// `current_lsn` and `durable_bound` are for that transaction.
    pub(crate) fn peek_finished_commits_lsn(&self) -> Option<(i64, Option<i64>)> {
        self.shared
            .lock()
            .finished_commit
            .front()
            .map(|trans| (trans.current_lsn, trans.durable_bound))
    }

    /// If any transactions in the `finished_commit` queue do not have
    /// their `durable_bound` set, set it to `dur`.
    pub(crate) fn set_durable_bound_if_not_set(&self, dur: i64) {
        let mut lock = self.shared.lock();
        for x in &mut lock.finished_commit {
            if x.durable_bound.is_none() {
                x.durable_bound = Some(dur);
            }
        }
    }

    /// Pop the front of `finished_commit`
    pub(crate) fn pop_finished_commit(&self) -> Option<TransactionAwaitingCommit> {
        self.shared.lock().finished_commit.pop_front()
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
        transaction: Option<TransactionAwaitingCommit>,
    ) -> Result<(), Error> {
        self.tx
            .send(Command::SyncPoint {
                messages,
                kind,
                transaction,
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
                transaction,
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
                            .push_back(OpSyncPoint::ReadyForQuery { transaction });
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
                if let Some(OpSyncPoint::ReadyForQuery { transaction }) = self.queue.pop_front()
                    && let Some(mut waiting_transaction) = transaction
                {
                    let mut shared = self.shared.lock();
                    waiting_transaction.missed = std::mem::take(&mut shared.missed);
                    shared.finished_commit.push_back(waiting_transaction);
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
        let mut shared = self.shared.lock();
        if shared.error.is_none() {
            shared.error = Some(err);
        }
    }

    fn wake_all(&mut self) {
        self.queue.clear();
    }

    fn address(&self) -> &Address {
        self.server.addr()
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

    async fn commit_and_wait(
        conn: &PipelinedConnection,
        lsn: i64,
    ) -> Option<TransactionAwaitingCommit> {
        conn.sync(Some(TransactionAwaitingCommit {
            transaction_lsn: lsn,
            current_lsn: lsn,
            changed_tables: HashSet::new(),
            durable_bound: None,
            missed: MissedRows::default(),
        }))
        .await
        .unwrap();

        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if conn.peek_finished_commits_lsn().map(|(front, _)| front) == Some(lsn) {
                return conn.pop_finished_commit();
            }
            sleep(Duration::from_millis(10)).await;
        }
        None
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
        let conn = PipelinedConnection::new(server).unwrap();

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

        let committed = commit_and_wait(&conn, 1).await;
        assert!(committed.is_some());
        assert!(conn.take_error().is_none());
    }

    #[tokio::test]
    async fn in_transaction_prepare_uses_flush() {
        let server = test_server().await;
        let conn = PipelinedConnection::new(server).unwrap();

        conn.prepare(&[Parse::named("__pipe_flush", "SELECT $1::bigint")], true)
            .await
            .unwrap();

        conn.execute(
            Bind::new_params("__pipe_flush", &[Parameter::new(b"42")]),
            false,
        )
        .await
        .unwrap();
        let committed = commit_and_wait(&conn, 1).await;
        assert!(committed.is_some());
        assert!(conn.take_error().is_none());
    }

    #[tokio::test]
    async fn prepare_invalid_sql_sync_returns_error() {
        let server = test_server().await;
        let conn = PipelinedConnection::new(server).unwrap();

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
        let conn = PipelinedConnection::new(server).unwrap();

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
        let conn = PipelinedConnection::new(server).unwrap();

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
        let conn = PipelinedConnection::new(server).unwrap();

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

        conn.sync(Some(TransactionAwaitingCommit {
            transaction_lsn: 1,
            current_lsn: 1,
            changed_tables: HashSet::new(),
            durable_bound: None,
            missed: MissedRows::default(),
        }))
        .await
        .unwrap();
        let err = wait_for_error(&conn).await.unwrap();
        assert!(
            matches!(err, Error::PgError(_)),
            "unexpected error: {err:?}"
        );
        sleep(Duration::from_millis(300)).await;
        let finished = conn.peek_finished_commits_lsn();
        assert!(finished.is_none());
    }

    #[tokio::test]
    async fn direct_dml_zero_rows_counts_missed() {
        let server = test_server().await;
        let conn = PipelinedConnection::new(server).unwrap();

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

        let committed = commit_and_wait(&conn, 1).await;
        assert!(committed.is_some());
        assert!(conn.take_error().is_none());

        let missed = committed.unwrap().missed;
        // (insert, update, delete): one 0-row direct UPDATE and one 0-row direct
        // DELETE counted; the non-direct DELETE and the 1-row DELETE are not.
        // Insert never missed here, so it must stay 0 (no spurious counter).
        assert_eq!(missed.inserts, 0);
        assert_eq!(missed.updates, 1);
        assert_eq!(missed.deletes, 1);
    }
}
