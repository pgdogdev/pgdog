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
            direct: VecDeque::new(),
            parse_ack_sync_points: VecDeque::new(),
            ready_to_commit_sync_points: VecDeque::new(),
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
        self.sync_point(messages, kind).await
    }

    /// Send `Sync` and wait for `ReadyForQuery` (commits the open implicit
    /// transaction on this shard).
    pub async fn sync_and_drain(&self) -> Result<(), Error> {
        self.sync_point(vec![Sync.into()], SyncPointKind::ReadyForQuery)
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
        self.await_done(rx).await
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

    async fn sync_point(
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
        self.await_done(rx).await
    }

    // Wait for a sync point to resolve, then report any latched error. If the
    // task died before signaling, surface the latched error or a generic one.
    async fn await_done(&self, rx: oneshot::Receiver<()>) -> Result<(), Error> {
        match rx.await {
            Ok(()) => match self.take_error() {
                Some(err) => Err(err),
                None => Ok(()),
            },
            Err(_) => Err(self.take_error().unwrap_or(Error::PipelineClosed)),
        }
    }
}

// Background task: owns the `Server`, writes queued messages, and reconciles
// responses (counts acks, records missed rows, latches the first error).
struct Listener {
    rx: Receiver<Command>,
    server: Server,
    shared: Arc<Mutex<Shared>>,
    // One entry per outstanding DML op, in send order: `is_direct`. Popped on
    // each CommandComplete.
    direct: VecDeque<bool>,
    // In-transaction prepare sync points: (remaining ParseComplete acks, waiter).
    parse_ack_sync_points: VecDeque<(usize, oneshot::Sender<()>)>,
    // Commit / out-of-transaction prepare sync points, resolved on ReadyForQuery.
    ready_to_commit_sync_points: VecDeque<oneshot::Sender<()>>,
    // Waiter that resolves once every DML ack has been read. At most one is
    // outstanding: `commit()` issues drain_acks sequentially, one per connection.
    drain_waiter: Option<oneshot::Sender<()>>,
}

impl Listener {
    async fn run(mut self) {
        loop {
            select! {
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
            }
        }
    }

    async fn handle_command(&mut self, command: Command) -> Result<(), Error> {
        let errored = self.shared.lock().error.is_some();

        match command {
            Command::Execute {
                messages,
                is_direct,
            } => {
                // If any connection has already reported and error in the shared state,
                // we do not need to enqueue the command as postgres will ignore the command.
                if errored {
                    return Ok(());
                }
                // If our command fails to be executed, we latch the error to the shared state.
                // we also wake up all the sync point and resolve all the sync point waiters to
                // resolve with the error.
                if let Err(err) = self.write(&messages).await {
                    self.latch_error(err);
                    self.wake_all();
                    return Err(Error::PipelineClosed);
                }
                self.direct.push_back(is_direct);
            }
            Command::SyncPoint {
                messages,
                kind,
                done,
            } => {
                if errored {
                    let _ = done.send(());
                    return Ok(());
                }
                if let Err(err) = self.write(&messages).await {
                    self.latch_error(err);
                    let _ = done.send(());
                    self.wake_all();
                    return Err(Error::PipelineClosed);
                }
                match kind {
                    // We enqueue the number of acknowledgements we are waiting for and the waiter to resolve.
                    SyncPointKind::ParseAcks(remaining) => {
                        self.parse_ack_sync_points.push_back((remaining, done));
                    }
                    // We enqueue the waiter to resolve when postgres is ready to commit.
                    SyncPointKind::ReadyForQuery => {
                        self.ready_to_commit_sync_points.push_back(done);
                    }
                }
            }
            Command::DrainAcks { done } => {
                if errored || self.direct.is_empty() {
                    let _ = done.send(());
                } else {
                    // At most one drain is outstanding (issued one-at-a-time by
                    // commit()). Overwriting would drop a prior sender, which
                    // surfaces as an error on its receiver rather than a hang.
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
                self.direct.clear();
                self.wake_all();
            }
            // as sync points arrive we decrement the remaining count and resolve th waiter
            // when the remaining count becomes 0.
            '1' => {
                let resolved = if let Some((remaining, _)) = self.parse_ack_sync_points.front_mut()
                {
                    *remaining -= 1;
                    *remaining == 0
                } else {
                    false
                };
                if resolved && let Some((_, done)) = self.parse_ack_sync_points.pop_front() {
                    let _ = done.send(());
                }
            }
            // BindComplete: nothing to account for.
            '2' => {}
            // CommandComplete: match to the DML op and count missed rows.
            'C' => {
                let is_direct = self.direct.pop_front().unwrap_or(false);
                if is_direct
                    && let Ok(complete) = CommandComplete::try_from(message)
                    && matches!(complete.rows(), Ok(Some(0)))
                {
                    self.shared.lock().missed.record(complete.tag());
                }
                if self.direct.is_empty() {
                    self.wake_drain();
                }
            }
            // When Ready to commit sync point arrives we resolve the waiter.
            'Z' => {
                if let Some(done) = self.ready_to_commit_sync_points.pop_front() {
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
        while let Some((_, done)) = self.parse_ack_sync_points.pop_front() {
            let _ = done.send(());
        }
        while let Some(done) = self.ready_to_commit_sync_points.pop_front() {
            let _ = done.send(());
        }
        self.wake_drain();
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
    use crate::{backend::server::test::test_server, net::Parse};

    #[tokio::test]
    async fn prepare_and_commit_drain() {
        let server = test_server().await;
        let conn = PipelinedConnection::new(server).unwrap();

        // Prepare out of a transaction (uses Sync internally).
        conn.prepare(&[Parse::named("__pipe_test", "SELECT $1::bigint")], false)
            .await
            .unwrap();

        // A bare Sync must drain cleanly with no error.
        conn.sync_and_drain().await.unwrap();
        assert!(conn.take_error().is_none());
    }

    #[tokio::test]
    async fn drain_acks_with_no_outstanding_is_immediate() {
        let server = test_server().await;
        let conn = PipelinedConnection::new(server).unwrap();

        // No DML enqueued: drain_acks resolves immediately without error.
        conn.drain_acks().await.unwrap();
        assert!(conn.take_error().is_none());
    }
}
