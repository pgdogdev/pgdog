use std::collections::VecDeque;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::select;
use tokio::spawn;
use tokio::sync::{
    mpsc::{Receiver, Sender, channel},
    oneshot,
};
use tracing::trace;

use super::stream::MissedRows;
use crate::backend::Server;
use crate::backend::pool::Address;
use crate::net::{
    Bind, CommandComplete, ErrorResponse, Execute, Flush, FromBytes, Message, Parse, Protocol,
    ProtocolMessage, Sync, ToBytes,
};

use super::super::Error;

// State shared between the handle and its background listener task.
#[derive(Debug, Default)]
struct Shared {
    // First error observed on this connection. Sticky until taken.
    error: Option<Error>,
    // Rows a direct-to-shard DML expected to touch but didn't (0 rows affected).
    missed: MissedRows,
}

// How a sync point completes.
enum SyncPointKind {
    // Wait for `n` ParseComplete responses (in-transaction prepare, `Flush`).
    ParseAcks(usize),
    // Wait for a single ReadyForQuery (`Sync`: commit, or out-of-transaction prepare).
    ReadyForQuery,
}

// One entry per command sent to Postgres, in send order. Popped as acks arrive.
enum OpSyncPoint {
    // Bind/Execute/Flush: resolved by CommandComplete ('C').
    DirectDml {
        is_direct: bool,
    },
    // In-transaction prepare (Flush): resolved after `remaining` ParseComplete ('1') acks.
    ParseAcks {
        remaining: usize,
        done: oneshot::Sender<()>,
    },
    // Commit or out-of-transaction prepare (Sync): resolved by ReadyForQuery ('Z').
    ReadyForQuery {
        done: oneshot::Sender<()>,
    },
}

// Work sent from the handle to the listener task.
enum Command {
    // Fire-and-forget DML: Bind/Execute/Flush. `is_direct` drives missed-row counting.
    Execute {
        messages: Vec<ProtocolMessage>,
        is_direct: bool,
    },
    // A synchronization point: write `messages`, then resolve `done` per `kind`.
    SyncPoint {
        messages: Vec<ProtocolMessage>,
        kind: SyncPointKind,
        done: oneshot::Sender<()>,
    },
    // Wait until every outstanding DML ack has been read (no messages sent).
    DrainAcks {
        done: oneshot::Sender<()>,
    },
}

/// Pipelined destination connection: a handle over a background task that
/// owns the `Server` and reconciles responses.
#[derive(Debug)]
pub struct PipelinedConnection {
    tx: Sender<Command>,
    shared: Arc<Mutex<Shared>>,
    address: Address,
}

impl PipelinedConnection {
    /// This moves `server` into a background task and returns a handle to it.
    pub fn new(server: Server) -> Result<Self, Error> {
        let (tx, rx) = channel(4096);
        let shared = Arc::new(Mutex::new(Shared::default()));
        let address = server.addr().clone();

        let listener = Listener {
            rx,
            server,
            shared: shared.clone(),
            queue: VecDeque::new(),
            drain_waiter: None,
        };

        spawn(listener.run());

        Ok(Self {
            tx,
            shared,
            address,
        })
    }

    /// Server address.
    pub fn addr(&self) -> &Address {
        &self.address
    }

    /// Enqueue a DML statement (`Bind/Execute/Flush`) without waiting for its
    /// response. `is_direct` marks a single-shard write whose 0-row result
    /// counts as a missed row.
    pub async fn execute(&self, bind: &Bind, is_direct: bool) -> Result<(), Error> {
        let messages = vec![bind.clone().into(), Execute::new().into(), Flush.into()];
        self.tx
            .send(Command::Execute {
                messages,
                is_direct,
            })
            .await
            .map_err(|_| Error::PipelineClosed)
    }

    /// Prepare `parses` and wait for the acknowledgments. Inside a transaction
    /// uses `Flush` (must not commit the open implicit transaction); otherwise
    /// uses `Sync`.
    pub async fn prepare(&self, parses: &[Parse], in_transaction: bool) -> Result<(), Error> {
        if parses.is_empty() {
            return Ok(());
        }
        let mut messages: Vec<ProtocolMessage> = parses.iter().map(|p| p.clone().into()).collect();
        let kind = if in_transaction {
            // If in transaction we send flush and wait for the acknowledgements
            // since we donot want to commit the open transaction.
            messages.push(Flush.into());
            SyncPointKind::ParseAcks(parses.len())
        } else {
            // Since we are not in a transaction we send Sync and wait for postgres
            // to becomes ready for the next command.
            messages.push(Sync.into());
            SyncPointKind::ReadyForQuery
        };
        self.send_command_and_wait_for_sync_point(messages, kind)
            .await
    }

    /// Send `Sync` and wait for `ReadyForQuery` (commits the open implicit
    /// transaction on this shard).
    pub async fn sync_and_drain(&self) -> Result<(), Error> {
        self.send_command_and_wait_for_sync_point(vec![Sync.into()], SyncPointKind::ReadyForQuery)
            .await
    }

    /// Wait until every outstanding DML acknowledgment has been read. Used at
    /// commit to confirm the shard is error-free before any shard is `Sync`ed.
    pub async fn drain_acks(&self) -> Result<(), Error> {
        let (done, rx) = oneshot::channel();
        self.tx
            .send(Command::DrainAcks { done })
            .await
            .map_err(|_| Error::PipelineClosed)?;
        self.resolve_sync_point(rx).await
    }

    /// Non-blocking peek + take of the latched error. `Some` means the shard
    /// has errored and the transaction must be rolled back.
    pub fn take_error(&self) -> Option<Error> {
        self.shared.lock().error.take()
    }

    /// Drain the missed-row counters accumulated by the listener.
    pub(crate) fn take_missed_rows(&self) -> MissedRows {
        std::mem::take(&mut self.shared.lock().missed)
    }

    // This function send the prepared command along with the type of sync point we are waiting for.
    async fn send_command_and_wait_for_sync_point(
        &self,
        messages: Vec<ProtocolMessage>,
        kind: SyncPointKind,
    ) -> Result<(), Error> {
        let (done, rx) = oneshot::channel();
        self.tx
            .send(Command::SyncPoint {
                messages,
                kind,
                done,
            })
            .await
            .map_err(|_| Error::PipelineClosed)?;
        self.resolve_sync_point(rx).await
    }

    // Resolve a sync point (signaled by the listener task).
    // If the task died before signaling, surface the latched error.
    async fn resolve_sync_point(&self, rx: oneshot::Receiver<()>) -> Result<(), Error> {
        rx.await
            .map_err(|_| self.take_error().unwrap_or(Error::PipelineClosed))
    }
}

// Background task: owns the `Server`, writes queued messages, and reconciles
// responses (counts acks, records missed rows, latches the first error).
struct Listener {
    rx: Receiver<Command>,
    server: Server,
    shared: Arc<Mutex<Shared>>,
    queue: VecDeque<OpSyncPoint>,
    // Waiter that resolves once every DML ack has been read. At most one is
    // outstanding: `commit()` issues drain_acks sequentially, one per connection.
    drain_waiter: Option<oneshot::Sender<()>>,
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
            Command::Execute {
                messages,
                is_direct,
            } => {
                // If our command fails to be executed, we latch the error to the shared state.
                // we also wake up all the sync point and resolve all the sync point waiters to
                // resolve with the error.
                if let Err(err) = self.write(&messages).await {
                    self.latch_error(err);
                    self.wake_all();
                    return Err(Error::PipelineClosed);
                }
                self.queue.push_back(OpSyncPoint::DirectDml { is_direct });
            }
            Command::SyncPoint {
                messages,
                kind,
                done,
            } => {
                if let Err(err) = self.write(&messages).await {
                    self.latch_error(err);
                    // Drop the sender (do not send): rx errors, so resolve_sync_point
                    // surfaces the latched error instead of a false Ok.
                    drop(done);
                    self.wake_all();
                    return Err(Error::PipelineClosed);
                }
                match kind {
                    SyncPointKind::ParseAcks(remaining) => {
                        self.queue
                            .push_back(OpSyncPoint::ParseAcks { remaining, done });
                    }
                    SyncPointKind::ReadyForQuery => {
                        self.queue.push_back(OpSyncPoint::ReadyForQuery { done });
                    }
                }
            }
            Command::DrainAcks { done } => {
                let has_dml = self
                    .queue
                    .iter()
                    .any(|op| matches!(op, OpSyncPoint::DirectDml { .. }));
                if !has_dml {
                    let _ = done.send(());
                } else {
                    self.drain_waiter = Some(done);
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
            // ParseComplete: decrement the front ParseSync counter; resolve when it hits 0.
            '1' => {
                let resolved = if let Some(OpSyncPoint::ParseAcks { remaining, .. }) =
                    self.queue.front_mut()
                {
                    *remaining -= 1;
                    *remaining == 0
                } else {
                    false
                };
                if resolved
                    && let Some(OpSyncPoint::ParseAcks { done, .. }) = self.queue.pop_front()
                {
                    let _ = done.send(());
                }
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
                    self.shared.lock().missed.record(complete.tag());
                }
                if !self
                    .queue
                    .iter()
                    .any(|op| matches!(op, OpSyncPoint::DirectDml { .. }))
                {
                    self.wake_drain();
                }
            }
            // ReadyForQuery: resolve the front ReadySync waiter.
            'Z' => {
                if let Some(OpSyncPoint::ReadyForQuery { done }) = self.queue.pop_front() {
                    let _ = done.send(());
                }
            }
            // NoticeResponse / ParameterStatus / NotificationResponse / etc.
            _ => {}
        }
    }

    // Write messages to the socket and flush so the bytes reach Postgres.
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
        drop(self.drain_waiter.take());
    }

    fn wake_drain(&mut self) {
        if let Some(done) = self.drain_waiter.take() {
            let _ = done.send(());
        }
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
        net::{Parse, messages::Parameter},
    };

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
        conn.execute(&Bind::new_statement("__pipe_create"), false)
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
            &Bind::new_params("__pipe_insert", &[Parameter::new(b"42")]),
            false,
        )
        .await
        .unwrap();

        // Drain the outstanding DML ack, then commit.
        conn.drain_acks().await.unwrap();
        conn.sync_and_drain().await.unwrap();
        assert!(conn.take_error().is_none());
    }

    #[tokio::test]
    async fn in_transaction_prepare_uses_flush() {
        let server = test_server().await;
        let conn = PipelinedConnection::new(server).unwrap();

        // In-transaction prepare sends Flush and waits for ParseComplete acks
        // (ParseAcks path) rather than committing with Sync.
        conn.prepare(&[Parse::named("__pipe_flush", "SELECT $1::bigint")], true)
            .await
            .unwrap();

        // Execute against the just-prepared statement, then commit.
        conn.execute(
            &Bind::new_params("__pipe_flush", &[Parameter::new(b"42")]),
            false,
        )
        .await
        .unwrap();
        conn.sync_and_drain().await.unwrap();
        assert!(conn.take_error().is_none());
    }

    #[tokio::test]
    async fn prepare_invalid_sql_sync_returns_error() {
        let server = test_server().await;
        let conn = PipelinedConnection::new(server).unwrap();

        // Out-of-transaction prepare of invalid SQL: Postgres replies with an
        // ErrorResponse, which is latched and surfaced through the Sync path.
        let err = conn
            .prepare(&[Parse::named("__pipe_bad", "NOT VALID SQL")], false)
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::PgError(_)),
            "unexpected error: {err:?}"
        );
        // The error was taken while surfacing, so nothing remains latched.
        assert!(conn.take_error().is_none());
    }

    #[tokio::test]
    async fn prepare_invalid_sql_flush_returns_error() {
        let server = test_server().await;
        let conn = PipelinedConnection::new(server).unwrap();

        // In-transaction prepare uses Flush, so Postgres sends no ReadyForQuery
        // on error. The parked ParseAcks waiter can only be released by the
        // 'E' handler's wake_all(): this proves the prepare does not hang.
        let err = conn
            .prepare(&[Parse::named("__pipe_bad", "NOT VALID SQL")], true)
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::PgError(_)),
            "unexpected error: {err:?}"
        );
        assert!(conn.take_error().is_none());
    }

    #[tokio::test]
    async fn execute_runtime_error_latches_and_does_not_block() {
        use std::time::Duration;
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
            &Bind::new_params("__pipe_div", &[Parameter::new(b"0")]),
            false,
        )
        .await
        .unwrap();

        // Barrier that observes the error. Two benign races:
        //  - sync point queued before 'E': wake_all drops its `done`, the sync
        //    resolves as Err(PgError).
        //  - 'E' latched first: the sync hits the `errored` branch and returns
        //    Ok, leaving the error latched.
        // Either way the error is observable; it is never lost.
        let barrier = conn.sync_and_drain().await;
        assert!(
            barrier.is_err() || conn.take_error().is_some(),
            "error was neither surfaced nor latched: {barrier:?}"
        );

        // Future calls must not block. Depending on the race the connection is
        // either still errored (commands short-circuit) or already recovered by
        // the barrier's Sync (commands run again); either way each call must
        // resolve promptly. A hang (queue never drained, `done` never resolved)
        // would elapse the timeout. The Ok/Err of each call is irrelevant here.
        for _ in 0..3 {
            let _ = timeout(
                Duration::from_secs(5),
                conn.execute(
                    &Bind::new_params("__pipe_div", &[Parameter::new(b"1")]),
                    false,
                ),
            )
            .await
            .expect("execute blocked after error");
        }
        let _ = timeout(Duration::from_secs(5), conn.sync_and_drain())
            .await
            .expect("sync_and_drain blocked after error");
    }

    #[tokio::test]
    async fn errored_connection_drain_acks_returns_error() {
        use std::time::Duration;
        use tokio::time::sleep;

        let server = test_server().await;
        let conn = PipelinedConnection::new(server).unwrap();

        // Fire-and-forget DML that fails at execution time (division by zero).
        conn.prepare(&[Parse::named("__drain_div", "SELECT 1 / $1::int")], false)
            .await
            .unwrap();
        conn.execute(
            &Bind::new_params("__drain_div", &[Parameter::new(b"0")]),
            true,
        )
        .await
        .unwrap();

        // Let the listener read the ErrorResponse and latch it, so the following
        // drain_acks hits the `errored` branch (the path that used to send `done`
        // and return a false Ok, swallowing the error at commit time).
        sleep(Duration::from_millis(300)).await;

        // drain_acks must surface the latched error, not report a clean drain.
        // This mirrors commit()'s phase 1 `?`, which aborts before any Sync.
        let err = conn.drain_acks().await.unwrap_err();
        assert!(
            matches!(err, Error::PgError(_)),
            "unexpected error: {err:?}"
        );
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
        conn.execute(&Bind::new_statement("__miss_create"), false)
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
        conn.execute(&Bind::new_statement("__miss_seed"), false)
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
            &Bind::new_params("__miss_del", &[Parameter::new(b"999")]),
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
            &Bind::new_params("__miss_upd", &[Parameter::new(b"999")]),
            true,
        )
        .await
        .unwrap();

        // Negative control 1: same 0-row DELETE but not direct -> not counted.
        conn.execute(
            &Bind::new_params("__miss_del", &[Parameter::new(b"999")]),
            false,
        )
        .await
        .unwrap();

        // Negative control 2: direct DELETE that hits the seeded row (1 row) ->
        // rows != 0 -> not counted.
        conn.execute(
            &Bind::new_params("__miss_del", &[Parameter::new(b"1")]),
            true,
        )
        .await
        .unwrap();

        // Drain acks so every 'C' (and thus every record()) is processed before
        // we read the stats, then commit.
        conn.drain_acks().await.unwrap();
        conn.sync_and_drain().await.unwrap();
        assert!(conn.take_error().is_none());

        let missed = conn.take_missed_rows();
        // (insert, update, delete): one 0-row direct UPDATE and one 0-row direct
        // DELETE counted; the non-direct DELETE and the 1-row DELETE are not.
        // Insert never missed here, so it must stay 0 (no spurious counter).
        assert_eq!(missed.counts(), (0, 1, 1));
    }
}
