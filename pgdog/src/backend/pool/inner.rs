//! Pool internals synchronized with a mutex.

use std::cmp::max;
use std::collections::VecDeque;
use std::fmt::Display;

use crate::backend::{ConnectReason, DisconnectReason};
use crate::backend::{Server, stats::Counts as BackendCounts};
use crate::net::messages::{BackendKeyData, FrontendPid};

use tokio::time::Instant;

use super::{Config, Error, Oids, Pool, Request, Stats, Taken, Waiter, lsn_monitor::ReplicaLag};

/// Pool internals protected by a mutex.
#[derive(Default)]
pub(super) struct Inner {
    /// Idle server connections.
    #[allow(clippy::vec_box)]
    idle_connections: Vec<Box<Server>>,
    /// Server connections currently checked out.
    taken: Taken,
    /// Pool configuration.
    pub(super) config: Config,
    /// Waiter queue. Cancelled waiters are removed lazily.
    pub(super) waiting: VecDeque<Waiter>,
    /// Number of clients still waiting for a connection.
    pub(super) live_waiters: usize,
    /// Pool is online and available to clients.
    pub(super) online: bool,
    /// Pool is paused.
    pub(super) paused: bool,
    /// Track out of sync terminations.
    pub(super) out_of_sync: usize,
    /// How many times servers had to be re-synced
    /// after back check-in.
    pub(super) re_synced: usize,
    /// Number of connections that were force closed.
    pub(super) force_close: usize,
    /// Track connections closed with errors.
    pub(super) errors: usize,
    /// Stats
    pub(super) stats: Stats,
    /// OIDs.
    pub(super) oids: Option<Oids>,
    /// The pool has been changed and connections should be returned
    /// to the new pool.
    moved: Option<Pool>,
    /// Unique pool identifier.
    id: u64,
    /// Replica lag.
    pub(super) replica_lag: ReplicaLag,
    /// Bumped each time Vault credentials rotate. Connections stamped with
    /// an older generation are closed on check-in rather than reused.
    pub(super) credentials_generation: u64,
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Inner")
            .field("paused", &self.paused)
            .field("taken", &self.taken.len())
            .field("idle_connections", &self.idle_connections.len())
            .field("waiting", &self.live_waiters)
            .field("online", &self.online)
            .finish()
    }
}

impl Inner {
    /// New inner structure.
    pub(super) fn new(config: Config, id: u64) -> Self {
        Self {
            idle_connections: Vec::new(),
            taken: Taken::default(),
            config,
            waiting: VecDeque::new(),
            live_waiters: 0,
            online: false,
            paused: false,
            force_close: 0,
            out_of_sync: 0,
            re_synced: 0,
            errors: 0,
            stats: Stats::default(),
            oids: None,
            moved: None,
            id,
            replica_lag: ReplicaLag::default(),
            credentials_generation: 0,
        }
    }
    /// Total number of connections managed by the pool.
    #[inline]
    pub(super) fn total(&self) -> usize {
        self.idle() + self.checked_out()
    }

    /// The pool is full and will not
    /// create any more connections.
    #[inline]
    pub(super) fn full(&self) -> bool {
        self.total() >= self.max()
    }

    /// Number of idle connections in the pool.
    #[inline]
    pub(super) fn idle(&self) -> usize {
        self.idle_connections.len()
    }

    #[cfg(test)]
    pub(super) fn idle_conns(&self) -> &[Box<Server>] {
        &self.idle_connections
    }

    /// Number of connections checked out of the pool
    /// by clients.
    #[inline]
    pub(super) fn checked_out(&self) -> usize {
        self.taken.len()
    }

    /// Cancel key for the server currently assigned to this client.
    #[inline]
    pub(super) fn cancel_key(&self, client: FrontendPid) -> Option<&BackendKeyData> {
        self.taken.cancel_key(client)
    }

    /// All cancel keys for currently checked-out server connections.
    pub(super) fn cancel_keys(&self) -> impl Iterator<Item = &BackendKeyData> {
        self.taken.cancel_keys()
    }

    /// How many connections can be removed from the pool
    /// without affecting the minimum connection requirement.
    #[inline]
    pub(super) fn can_remove(&self) -> usize {
        let total = self.total() as i64;
        let min = self.min() as i64;

        max(0, total - min) as usize
    }

    /// Minimum number of connections the pool should keep open.
    #[inline]
    pub(super) fn min(&self) -> usize {
        self.config.min
    }

    /// Maximum number of connections in the pool.
    #[inline]
    pub(super) fn max(&self) -> usize {
        self.config.max
    }

    /// The pool should create more connections now.
    #[inline]
    pub(super) fn should_create(&self) -> ShouldCreate {
        let below_min = self.total() < self.min();
        let below_max = self.total() < self.max();
        let maintain_min = below_min && below_max;
        let client_needs = below_max && self.live_waiters > 0 && self.idle_connections.is_empty();
        let maintenance_on = self.online && !self.paused;

        // Clients from banned pools won't be able to request connections
        // unless it's a primary.
        let reason = if client_needs {
            ConnectReason::ClientWaiting
        } else if maintenance_on && maintain_min {
            ConnectReason::BelowMin
        } else {
            return ShouldCreate::No;
        };

        ShouldCreate::Yes {
            reason,
            min: self.min(),
            max: self.max(),
            idle: self.idle(),
            taken: self.checked_out(),
            waiting: self.live_waiters,
        }
    }

    /// Register a waiter after it is pushed into the queue.
    #[inline]
    pub(super) fn add_waiter(&mut self) {
        self.live_waiters += 1;
    }

    /// Mark a waiter as no longer waiting.
    #[inline]
    pub(super) fn remove_live_waiter(&mut self) {
        self.live_waiters = self.live_waiters.saturating_sub(1);
    }

    /// Remove cancelled waiters from the physical queue.
    #[inline]
    pub(super) fn cleanup_waiters(&mut self) {
        if self.waiting.len() > 64 {
            self.waiting.retain(|waiter| !waiter.tx.is_closed());
        }
    }

    /// Close connections that have exceeded the max age.
    #[inline]
    pub(crate) fn close_old(&mut self, now: Instant) -> usize {
        let base_max_age = self.config.max_age;
        let mut removed = 0;

        self.idle_connections.retain_mut(|c| {
            let age = c.age(now);
            let keep = age < c.effective_max_age(base_max_age);
            if !keep {
                removed += 1;
            }
            if !keep {
                c.disconnect_reason(DisconnectReason::Old);
            }
            keep
        });

        removed
    }

    /// Close connections that have been idle for too long
    /// without affecting the minimum pool size requirement.
    #[inline]
    pub(crate) fn close_idle(&mut self, now: Instant) -> usize {
        let (mut remove, mut removed) = (self.can_remove(), 0);
        let idle_timeout = self.config.idle_timeout;

        self.idle_connections.retain_mut(|c| {
            let idle_for = c.idle_for(now);

            let keep = if remove > 0 && idle_for >= idle_timeout {
                remove -= 1;
                removed += 1;
                false
            } else {
                true
            };

            if !keep {
                c.disconnect_reason(DisconnectReason::Idle);
            }

            keep
        });

        removed
    }

    /// Pool configuration options.
    #[inline]
    pub(super) fn config(&self) -> &Config {
        &self.config
    }

    /// Take connection from the idle pool.
    #[inline(always)]
    pub(super) fn take(&mut self, request: &Request) -> Result<Option<Box<Server>>, Error> {
        match self.idle_connections.pop() {
            Some(conn) => {
                let cancel_key = conn.key().clone();
                self.taken.take(request.id, conn.id(), cancel_key);

                Ok(Some(conn))
            }
            _ => Ok(None),
        }
    }

    /// Place connection back into the pool
    /// or give it to a waiting client.
    #[inline]
    pub(super) fn put(&mut self, mut conn: Box<Server>, now: Instant) -> Result<(), Error> {
        // Try to give it to a client that's been waiting, if any.
        let cancel_key = conn.key().clone();
        let server_id = conn.id();
        while let Some(waiter) = self.waiting.pop_front() {
            match waiter.tx.send(Ok(conn)) {
                // The waiter is gone, which means it cancelled waiting, probably
                // due to checkout timeout.
                //
                // Try next waiter.
                //
                Err(conn_ret) => {
                    conn = conn_ret.unwrap(); // SAFETY: We sent Ok(conn), we'll get back Ok(conn) if channel is closed.
                }
                _ => {
                    self.taken.take(waiter.request.id, server_id, cancel_key);
                    self.stats.counts.server_assignment_count += 1;
                    self.stats.counts.wait_time += now.duration_since(waiter.request.created_at);
                    return Ok(());
                }
            }
        }

        // No waiters, put connection in idle list.
        self.idle_connections.push(conn);

        Ok(())
    }

    #[inline]
    pub(super) fn set_taken(&mut self, taken: Taken) {
        self.taken = taken;
    }

    #[inline]
    pub(super) fn credentials_generation(&self) -> u64 {
        self.credentials_generation
    }

    /// Rotate credentials generation: evict all idle connections and bump the
    /// counter so checked-out connections are closed on check-in.
    #[inline]
    pub(super) fn bump_credentials_generation(&mut self) {
        self.credentials_generation += 1;
        self.dump_idle();
    }

    /// Dump all idle connections.
    #[inline]
    pub(super) fn dump_idle(&mut self) {
        for conn in &mut self.idle_connections {
            conn.disconnect_reason(DisconnectReason::Offline);
        }
        self.idle_connections.clear();
    }

    /// Take all idle connections and tell active ones to
    /// be returned to a different pool instance.
    #[inline]
    #[allow(clippy::vec_box)] // Server is a very large struct, reading it when moving between containers is expensive.
    pub(super) fn move_conns_to(&mut self, destination: &Pool) -> (Vec<Box<Server>>, Taken) {
        self.moved = Some(destination.clone());
        let mut idle = std::mem::take(&mut self.idle_connections);
        let taken = std::mem::take(&mut self.taken);

        for conn in idle.iter_mut() {
            conn.stats_mut().set_pool_id(destination.id());
        }

        (idle, taken)
    }

    /// Check a connection back into the pool if it's ok to do so.
    /// Otherwise, drop the connection and close it.
    ///
    /// Return: true if the pool should be banned, false otherwise.
    #[inline(always)]
    pub(super) fn maybe_check_in(
        &mut self,
        mut server: Box<Server>,
        now: Instant,
        stats: BackendCounts,
        moving: bool,
    ) -> Result<CheckInResult, Error> {
        let mut result = CheckInResult {
            server_error: false,
            replenish: true,
        };

        if let Some(ref moved) = self.moved {
            result.replenish = false;
            // Prevents deadlocks.
            if moved.id() != self.id {
                server.stats_mut().set_pool_id(moved.id());
                server.stats().update();
                moved.lock().maybe_check_in(server, now, stats, true)?;
                return Ok(result);
            }
        }

        self.taken.check_in(server.id())?;

        // Update stats
        self.stats.counts = self.stats.counts + stats;

        // Ban the pool from serving more clients.
        if server.error() {
            self.errors += 1;
            result.server_error = true;
            server.disconnect_reason(DisconnectReason::Error);

            return Ok(result);
        }

        // Pool is offline or paused, connection should be closed.
        if !self.online && !moving || self.paused {
            result.replenish = false;
            return Ok(result);
        }

        // Close connections exceeding max age.
        if server.age(now) >= server.effective_max_age(self.config.max_age) {
            server.disconnect_reason(DisconnectReason::Old);
            return Ok(result);
        }

        // Force close the connection.
        if server.force_close() {
            self.force_close += 1;
            server.disconnect_reason(DisconnectReason::ForceClose);
            return Ok(result);
        }

        // Vault credentials rotated while this connection was checked out.
        // The old role will be revoked; close rather than reuse.
        if server.credentials_generation() != self.credentials_generation {
            server.disconnect_reason(DisconnectReason::CredentialsRefresh);
            return Ok(result);
        }

        // Close connections in replication mode,
        // they are generally not re-usable.
        if server.replication_mode() {
            server.disconnect_reason(DisconnectReason::ReplicationMode);
            return Ok(result);
        }

        if server.re_synced() {
            self.re_synced += 1;
            server.reset_re_synced();
        }

        // Finally, if the server is ok,
        // place the connection back into the idle list.
        if server.can_check_in() {
            self.put(server, now)?;
            result.replenish = false;
        } else {
            self.out_of_sync += 1;
            server.disconnect_reason(DisconnectReason::OutOfSync);
        }

        Ok(result)
    }

    #[inline]
    pub(super) fn close_waiters(&mut self, err: Error) {
        for waiter in self.waiting.drain(..) {
            let _ = waiter.tx.send(Err(err));
        }
    }
}

/// Result of connection check into the pool.
#[derive(Debug, Copy, Clone)]
pub(super) struct CheckInResult {
    pub(super) server_error: bool,
    pub(super) replenish: bool,
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub(super) enum ShouldCreate {
    No,
    Yes {
        reason: ConnectReason,
        min: usize,
        max: usize,
        idle: usize,
        taken: usize,
        waiting: usize,
    },
}

impl ShouldCreate {
    pub(super) fn yes(&self) -> bool {
        matches!(self, Self::Yes { .. })
    }
}

impl Display for ShouldCreate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::No => write!(f, "no"),
            Self::Yes {
                reason,
                min,
                max,
                idle,
                taken,
                waiting,
            } => {
                write!(
                    f,
                    "reason={}, min={}, max={}, idle={}, taken={}, waiting={}",
                    reason, min, max, idle, taken, waiting
                )
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use tokio::sync::oneshot::channel;

    use crate::net::messages::{BackendKeyData, BackendPid, FrontendPid};

    use super::*;

    #[test]
    fn test_default_state() {
        let inner = Inner::default();

        assert_eq!(inner.idle(), 0);
        assert_eq!(inner.checked_out(), 0);
        assert_eq!(inner.total(), 0);
        assert!(!inner.online);
        assert!(!inner.paused);
    }

    #[test]
    fn test_offline_pool_behavior() {
        let mut inner = Inner::default();

        let server = Box::new(Server::default());

        inner
            .taken
            .take(FrontendPid::new(), server.id(), server.key().clone());

        let result = inner
            .maybe_check_in(server, Instant::now(), BackendCounts::default(), false)
            .unwrap();

        assert!(!result.server_error);
        assert_eq!(inner.idle(), 0); // pool offline, connection not added
        assert_eq!(inner.total(), 0);
    }

    #[test]
    fn test_paused_pool_behavior() {
        let mut inner = Inner {
            online: true,
            paused: true,
            ..Default::default()
        };

        let server = Box::new(Server::default());

        inner
            .taken
            .take(FrontendPid::new(), server.id(), server.key().clone());

        inner
            .maybe_check_in(server, Instant::now(), BackendCounts::default(), false)
            .unwrap();

        assert_eq!(inner.total(), 0); // pool paused, connection not added
    }

    #[test]
    fn test_online_pool_accepts_connections() {
        let mut inner = Inner {
            online: true,
            paused: false,
            ..Default::default()
        };

        let server = Box::new(Server::default());

        inner
            .taken
            .take(FrontendPid::new(), server.id(), server.key().clone());

        let result = inner
            .maybe_check_in(server, Instant::now(), BackendCounts::default(), false)
            .unwrap();

        assert!(!result.server_error);
        assert_eq!(inner.idle(), 1);
        assert_eq!(inner.total(), 1);
    }

    #[test]
    fn test_server_error_handling() {
        let mut inner = Inner {
            online: true,
            ..Default::default()
        };

        let server = Box::new(Server::new_error());

        // Simulate server being checked out
        inner
            .taken
            .take(FrontendPid::new(), server.id(), server.key().clone());
        assert_eq!(inner.checked_out(), 1);

        let result = inner
            .maybe_check_in(server, Instant::now(), BackendCounts::default(), false)
            .unwrap();
        assert!(result.server_error);

        assert_eq!(inner.checked_out(), 0); // Error server removed from taken
        assert_eq!(inner.idle(), 0); // Error server not added to idle
    }

    #[test]
    fn test_should_create_with_waiting_clients() {
        let mut inner = Inner {
            online: true,
            ..Default::default()
        };
        inner.config.max = 5;
        inner.config.min = 1;

        inner.waiting.push_back(Waiter {
            request: Request::default(),
            tx: channel().0,
        });
        inner.add_waiter();

        assert_eq!(inner.idle(), 0);
        assert!(matches!(
            inner.should_create(),
            ShouldCreate::Yes {
                reason: ConnectReason::ClientWaiting,
                min: 1,
                max: 5,
                idle: 0,
                taken: 0,
                waiting: 1,
            }
        ));
    }

    #[test]
    fn test_should_create_below_minimum() {
        let mut inner = Inner {
            online: true,
            ..Default::default()
        };
        inner.config.min = 2;
        inner.config.max = 5;

        assert!(inner.total() < inner.min());
        assert!(inner.total() < inner.max());
        assert!(matches!(
            inner.should_create(),
            ShouldCreate::Yes {
                reason: ConnectReason::BelowMin,
                min: 2,
                max: 5,
                idle: 0,
                taken: 0,
                waiting: 0,
            }
        ));
    }

    #[test]
    fn test_should_not_create_at_max() {
        let mut inner = Inner {
            online: true,
            ..Default::default()
        };
        inner.config.max = 3;

        assert!(!inner.full());

        // Add 2 idle connections and 1 checked out connection to reach max
        inner.idle_connections.push(Box::new(Server::default()));
        inner.idle_connections.push(Box::new(Server::default()));
        inner.taken.take(
            FrontendPid::new(),
            BackendPid::for_test(1),
            BackendKeyData::random_legacy(),
        );

        assert_eq!(inner.idle(), 2);
        assert_eq!(inner.checked_out(), 1);
        assert_eq!(inner.total(), inner.config.max);
        assert!(inner.full());
        assert_eq!(inner.should_create(), ShouldCreate::No);
    }

    #[test]
    fn test_close_idle_respects_minimum() {
        let mut inner = Inner::default();
        inner.config.min = 2;
        inner.config.max = 3;
        inner.config.idle_timeout = Duration::from_millis(5_000);

        // Add connections to max
        inner.idle_connections.push(Box::new(Server::default()));
        inner.idle_connections.push(Box::new(Server::default()));
        inner.idle_connections.push(Box::new(Server::default()));

        // Close idle connections - shouldn't close any initially
        inner.close_idle(Instant::now());
        assert_eq!(inner.idle(), inner.config.max);

        // Close after timeout - should respect minimum
        for _ in 0..10 {
            inner.close_idle(Instant::now() + Duration::from_secs(6));
        }
        assert_eq!(inner.idle(), inner.config.min);

        // Further closing should still respect minimum
        inner.config.min = 1;
        inner.close_idle(Instant::now() + Duration::from_secs(6));
        assert_eq!(inner.idle(), inner.config.min);
    }

    #[test]
    fn test_close_old_ignores_minimum() {
        let mut inner = Inner {
            online: true,
            ..Default::default()
        };
        inner.config.min = 1;
        inner.config.max_age = Duration::from_millis(60_000);

        // Add a connection
        let server = Box::new(Server::default());

        inner
            .taken
            .take(FrontendPid::new(), server.id(), server.key().clone());

        inner
            .maybe_check_in(server, Instant::now(), BackendCounts::default(), false)
            .unwrap();
        assert_eq!(inner.idle(), 1);

        // Close old connections before max age - should keep connection
        inner.close_old(Instant::now() + Duration::from_secs(59));
        assert_eq!(inner.idle(), 1);

        // Close old connections after max age - ignores minimum
        inner.close_old(Instant::now() + Duration::from_secs(61));
        assert_eq!(inner.idle(), 0);
    }

    #[test]
    fn test_connection_lifecycle() {
        let mut inner = Inner::default();

        assert_eq!(inner.total(), 0);

        // Simulate taking a connection
        inner.taken.take(
            FrontendPid::new(),
            BackendPid::for_test(1),
            BackendKeyData::random_legacy(),
        );
        assert_eq!(inner.total(), 1);
        assert_eq!(inner.checked_out(), 1);

        // Clear taken connections
        inner.taken.clear();
        assert_eq!(inner.total(), 0);
        assert_eq!(inner.checked_out(), 0);
    }

    #[test]
    fn test_max_age_enforcement_on_checkin() {
        let mut inner = Inner {
            online: true,
            ..Default::default()
        };
        inner.config.max_age = Duration::from_millis(60_000);

        let server = Box::new(Server::default());

        inner
            .taken
            .take(FrontendPid::new(), server.id(), server.key().clone());

        inner
            .maybe_check_in(
                server,
                Instant::now() + Duration::from_secs(61), // Exceeds max age
                BackendCounts::default(),
                false,
            )
            .unwrap();

        assert_eq!(inner.total(), 0); // Connection not added due to max age
    }

    #[test]
    fn test_peer_lookup() {
        let mut inner = Inner::default();
        let client_id = FrontendPid::new();
        let server_id = BackendPid::for_test(1);
        let cancel_key = BackendKeyData::legacy(server_id.pid(), 0);

        assert_eq!(inner.cancel_key(client_id), None);

        inner.taken.take(client_id, server_id, cancel_key.clone());

        assert_eq!(inner.cancel_key(client_id), Some(&cancel_key));
    }

    #[test]
    fn test_taken_server_returns_server_when_mapped() {
        let mut taken = Taken::default();
        let client_id = FrontendPid::new();
        let server_id = BackendPid::for_test(1);
        let cancel_key = BackendKeyData::legacy(server_id.pid(), 0);

        // No mapping yet
        assert_eq!(taken.cancel_key(client_id), None);

        // Add mapping
        taken.take(client_id, server_id, cancel_key.clone());

        // Cancel key should be returned for mapped client, pid matches server_id
        let stored = taken.cancel_key(client_id).unwrap();
        assert_eq!(stored.pid(), server_id.pid());
        assert_eq!(stored, &cancel_key);

        // Different client should return None
        let other_client = FrontendPid::new();
        assert_eq!(taken.cancel_key(other_client), None);
    }

    #[test]
    fn test_can_remove() {
        let mut inner = Inner::default();
        inner.config.min = 2;
        inner.config.max = 5;

        assert_eq!(inner.can_remove(), 0); // total=0, min=2

        inner.idle_connections.push(Box::new(Server::default()));
        assert_eq!(inner.can_remove(), 0); // total=1, min=2

        inner.idle_connections.push(Box::new(Server::default()));
        assert_eq!(inner.can_remove(), 0); // total=2, min=2

        inner.idle_connections.push(Box::new(Server::default()));
        assert_eq!(inner.can_remove(), 1); // total=3, min=2
    }

    #[test]
    fn test_take_connection() {
        let mut inner = Inner::default();
        let request = Request::default();

        assert!(inner.take(&request).unwrap().is_none());

        inner.idle_connections.push(Box::new(Server::default()));
        let server = inner.take(&request);
        assert!(server.unwrap().is_some());
        assert_eq!(inner.idle(), 0);
        assert_eq!(inner.checked_out(), 1);
    }

    #[test]
    fn test_put_connection_with_waiter() {
        let mut inner = Inner::default();
        let (tx, mut rx) = channel();
        let waiter_request = Request::default();

        inner.waiting.push_back(Waiter {
            request: waiter_request,
            tx,
        });

        let server = Box::new(Server::default());
        inner.put(server, Instant::now()).unwrap();

        assert_eq!(inner.idle(), 0); // Connection given to waiter, not idle
        assert_eq!(inner.checked_out(), 1); // Connection now checked out to waiter
        assert!(inner.waiting.is_empty()); // Waiter was served

        // Verify waiter received the connection
        assert!(rx.try_recv().is_ok());
    }

    #[test]
    fn test_put_connection_no_waiters() {
        let mut inner = Inner::default();
        let server = Box::new(Server::default());

        inner.put(server, Instant::now()).unwrap();

        assert_eq!(inner.idle(), 1); // Connection added to idle pool
        assert_eq!(inner.checked_out(), 0);
        assert!(inner.waiting.is_empty());
    }

    #[test]
    fn test_dump_idle() {
        let mut inner = Inner::default();
        inner.idle_connections.push(Box::new(Server::default()));
        inner.idle_connections.push(Box::new(Server::default()));

        assert_eq!(inner.idle(), 2);
        inner.dump_idle();
        assert_eq!(inner.idle(), 0);
    }

    #[test]
    fn test_close_waiters() {
        let mut inner = Inner::default();
        let (tx1, mut rx1) = channel();
        let (tx2, mut rx2) = channel();

        inner.waiting.push_back(Waiter {
            request: Request::default(),
            tx: tx1,
        });
        inner.waiting.push_back(Waiter {
            request: Request::default(),
            tx: tx2,
        });

        assert_eq!(inner.waiting.len(), 2);
        inner.close_waiters(Error::CheckoutTimeout);
        assert_eq!(inner.waiting.len(), 0);

        // Verify waiters received the correct error
        assert_eq!(rx1.try_recv().unwrap().unwrap_err(), Error::CheckoutTimeout);
        assert_eq!(rx2.try_recv().unwrap().unwrap_err(), Error::CheckoutTimeout);
    }

    #[test]
    fn test_should_create_for_waiting_clients_even_above_minimum() {
        let mut inner = Inner {
            online: true,
            ..Default::default()
        };
        inner.config.min = 1;
        inner.config.max = 5;

        // Add connections above minimum but all are checked out (no idle)
        inner.taken.take(
            FrontendPid::new(),
            BackendPid::for_test(1),
            BackendKeyData::random_legacy(),
        );
        inner.taken.take(
            FrontendPid::new(),
            BackendPid::for_test(2),
            BackendKeyData::random_legacy(),
        );

        // Add a waiting client
        inner.waiting.push_back(Waiter {
            request: Request::default(),
            tx: channel().0,
        });
        inner.add_waiter();

        assert!(inner.total() > inner.min()); // Above minimum
        assert!(inner.total() < inner.max()); // Below maximum
        assert_eq!(inner.idle(), 0); // No idle connections
        assert!(inner.live_waiters > 0); // Has waiting clients
        assert!(matches!(
            inner.should_create(),
            ShouldCreate::Yes {
                reason: ConnectReason::ClientWaiting,
                min: 1,
                max: 5,
                idle: 0,
                taken: 2,
                waiting: 1,
            }
        ));
    }

    #[test]
    fn test_should_not_create_offline() {
        let mut inner = Inner {
            online: false,
            ..Default::default()
        };
        inner.config.min = 2;

        assert!(inner.total() < inner.min());
        assert_eq!(inner.should_create(), ShouldCreate::No);
    }

    #[test]
    fn test_set_taken() {
        let mut inner = Inner::default();
        let client = FrontendPid::new();
        let server = BackendPid::for_test(1);

        assert_eq!(inner.checked_out(), 0);

        let mut taken = Taken::default();
        taken.take(client, server, BackendKeyData::legacy(server.pid(), 0));

        inner.set_taken(taken);
        assert_eq!(inner.checked_out(), 1);
    }

    #[test]
    fn test_put_connection_skips_dropped_waiters() {
        let mut inner = Inner::default();
        let (tx1, _rx1) = channel(); // Will be dropped
        let (tx2, _rx2) = channel(); // Will be dropped
        let (tx3, mut rx3) = channel(); // Will remain active

        let req1 = Request::default();
        let req2 = Request::default();
        let req3 = Request::default();

        // Add three waiters to the queue
        inner.waiting.push_back(Waiter {
            request: req1,
            tx: tx1,
        });
        inner.waiting.push_back(Waiter {
            request: req2,
            tx: tx2,
        });
        inner.waiting.push_back(Waiter {
            request: req3,
            tx: tx3,
        });

        // Drop the first two receivers to simulate cancelled waiters
        drop(_rx1);
        drop(_rx2);

        assert_eq!(inner.waiting.len(), 3);

        let server = Box::new(Server::default());
        inner.put(server, Instant::now()).unwrap();

        // All waiters should be removed from queue since we tried each one
        assert_eq!(inner.waiting.len(), 0);
        // Connection should be given to the third waiter (the only one still listening)
        assert_eq!(inner.checked_out(), 1);
        assert_eq!(inner.idle(), 0);

        // Verify the third waiter received the connection
        assert!(rx3.try_recv().is_ok());
    }

    #[test]
    fn test_put_connection_all_waiters_dropped() {
        let mut inner = Inner::default();
        let (tx1, _rx1) = channel();
        let (tx2, _rx2) = channel();

        let req1 = Request::default();
        let req2 = Request::default();

        inner.waiting.push_back(Waiter {
            request: req1,
            tx: tx1,
        });
        inner.waiting.push_back(Waiter {
            request: req2,
            tx: tx2,
        });

        // Drop all receivers
        drop(_rx1);
        drop(_rx2);

        assert_eq!(inner.waiting.len(), 2);

        let server = Box::new(Server::default());
        inner.put(server, Instant::now()).unwrap();

        // All waiters should be removed since they were all dropped
        assert_eq!(inner.waiting.len(), 0);
        // Connection should go to idle pool since no waiters could receive it
        assert_eq!(inner.idle(), 1);
        assert_eq!(inner.checked_out(), 0);
    }

    #[test]
    fn test_cleanup_waiters_after_threshold() {
        let mut inner = Inner::default();

        for _ in 0..64 {
            let (tx, rx) = channel();
            inner.waiting.push_back(Waiter {
                request: Request::default(),
                tx,
            });
            drop(rx);
        }

        inner.cleanup_waiters();
        assert_eq!(inner.waiting.len(), 64);

        let (tx, rx) = channel();
        inner.waiting.push_back(Waiter {
            request: Request::default(),
            tx,
        });
        drop(rx);

        inner.cleanup_waiters();
        assert_eq!(inner.waiting.len(), 0);
        assert_eq!(inner.live_waiters, 0);
    }

    #[test]
    fn test_same_client_checks_out_two_connections() {
        let mut inner = Inner {
            online: true,
            ..Default::default()
        };
        inner.config.max = 2;
        inner.config.min = 0;

        // Add two idle connections to the pool
        let server1 = Box::new(Server::default());
        let server1_id = server1.id();
        let server2 = Box::new(Server::default());
        let server2_id = server2.id();
        inner.idle_connections.push(server1);
        inner.idle_connections.push(server2);

        assert_eq!(inner.idle(), 2);
        assert_eq!(inner.checked_out(), 0);
        assert_eq!(inner.total(), 2);

        // Same client ID for both requests
        let client_id = FrontendPid::new();
        let request = Request::unrouted(client_id);

        // Check out first connection
        let conn1 = inner
            .take(&request)
            .unwrap()
            .expect("should get connection");
        assert_eq!(inner.idle(), 1);
        assert_eq!(inner.checked_out(), 1);
        assert_eq!(inner.total(), 2);

        // Check out second connection with the same client ID
        let conn2 = inner
            .take(&request)
            .unwrap()
            .expect("should get connection");
        assert_eq!(inner.idle(), 0);
        assert_eq!(inner.checked_out(), 2);
        assert_eq!(inner.total(), 2);

        // Verify the connections are different
        assert_ne!(conn1.id(), conn2.id());

        // Check in both connections
        let now = Instant::now();
        inner
            .maybe_check_in(conn1, now, BackendCounts::default(), false)
            .unwrap();
        assert_eq!(inner.idle(), 1);
        assert_eq!(inner.checked_out(), 1);
        assert_eq!(inner.total(), 2);

        inner
            .maybe_check_in(conn2, now, BackendCounts::default(), false)
            .unwrap();
        assert_eq!(inner.idle(), 2);
        assert_eq!(inner.checked_out(), 0);
        assert_eq!(inner.total(), 2);

        // Verify the specific servers are back in the idle pool
        let idle_ids: Vec<_> = inner.idle_conns().iter().map(|s| s.id()).collect();
        assert!(idle_ids.contains(&server1_id));
        assert!(idle_ids.contains(&server2_id));
    }
}
