//! Pool internals synchronized with a mutex.

use std::cmp::max;
use std::collections::VecDeque;

use crate::backend::{stats::Counts as BackendCounts, Server};
use crate::net::messages::BackendKeyData;

use tokio::time::Instant;

use super::{Ban, Config, Error, Mapping, Oids, Pool, Request, Stats, Taken, Waiter};

/// Pool internals protected by a mutex.
#[derive(Default)]
pub(super) struct Inner {
    /// Idle server connections.
    #[allow(clippy::vec_box)]
    conns: Vec<Box<Server>>,
    /// Server connections currently checked out.
    taken: Taken,
    /// Pool configuration.
    pub(super) config: Config,
    /// Number of clients waiting for a connection.
    pub(super) waiting: VecDeque<Waiter>,
    /// Pool ban status.
    pub(super) ban: Option<Ban>,
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
    id: u64,
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Inner")
            .field("paused", &self.paused)
            .field("taken", &self.taken.len())
            .field("conns", &self.conns.len())
            .field("waiting", &self.waiting.len())
            .field("online", &self.online)
            .finish()
    }
}

impl Inner {
    /// New inner structure.
    pub(super) fn new(config: Config, id: u64) -> Self {
        Self {
            conns: Vec::new(),
            taken: Taken::default(),
            config,
            waiting: VecDeque::new(),
            ban: None,
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
        }
    }
    /// Total number of connections managed by the pool.
    #[inline]
    pub(super) fn total(&self) -> usize {
        self.idle() + self.checked_out()
    }

    /// Number of idle connections in the pool.
    #[inline]
    pub(super) fn idle(&self) -> usize {
        self.conns.len()
    }

    /// Number of connections checked out of the pool
    /// by clients.
    #[inline]
    pub(super) fn checked_out(&self) -> usize {
        self.taken.len()
    }

    /// Find the server currently linked to this client, if any.
    #[inline]
    pub(super) fn peer(&self, id: &BackendKeyData) -> Option<BackendKeyData> {
        self.taken.server(id)
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
    pub(super) fn should_create(&self) -> bool {
        let below_min = self.total() < self.min();
        let below_max = self.total() < self.max();
        let maintain_min = below_min && below_max;
        let client_needs = below_max && !self.waiting.is_empty() && self.conns.is_empty();
        let maintenance_on = self.online && !self.paused;

        !self.banned() && (client_needs || maintenance_on && maintain_min)
    }

    /// Check if the pool ban should be removed.
    #[inline]
    pub(super) fn check_ban(&mut self, now: Instant) -> bool {
        if self.ban.is_none() {
            return false;
        }

        let mut unbanned = false;
        if let Some(ban) = self.ban.take() {
            if !ban.expired(now) {
                self.ban = Some(ban);
            } else {
                unbanned = true;
            }
        }

        unbanned
    }

    /// Close connections that have exceeded the max age.
    #[inline]
    pub(crate) fn close_old(&mut self, now: Instant) -> usize {
        let max_age = self.config.max_age;
        let mut removed = 0;

        self.conns.retain(|c| {
            let age = c.age(now);
            let keep = age < max_age;
            if !keep {
                removed += 1;
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

        self.conns.retain(|c| {
            let idle_for = c.idle_for(now);

            if remove > 0 && idle_for >= idle_timeout {
                remove -= 1;
                removed += 1;
                false
            } else {
                true
            }
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
    pub(super) fn take(&mut self, request: &Request) -> Option<Box<Server>> {
        if let Some(conn) = self.conns.pop() {
            self.taken.take(&Mapping {
                client: request.id,
                server: *(conn.id()),
            });

            Some(conn)
        } else {
            None
        }
    }

    /// Place connection back into the pool
    /// or give it to a waiting client.
    #[inline]
    pub(super) fn put(&mut self, conn: Box<Server>) {
        // Try to give it to a client that's been waiting, if any.
        let id = *conn.id();
        if let Some(waiter) = self.waiting.pop_front() {
            if let Err(conn) = waiter.tx.send(Ok(conn)) {
                self.conns.push(conn.unwrap());
            } else {
                self.taken.take(&Mapping {
                    server: id,
                    client: waiter.request.id,
                });
            }
        } else {
            self.conns.push(conn);
        }
    }

    #[inline]
    pub(super) fn set_taken(&mut self, taken: Taken) {
        self.taken = taken;
    }

    /// Dump all idle connections.
    #[inline]
    pub(super) fn dump_idle(&mut self) {
        self.conns.clear();
    }

    /// Take all idle connections and tell active ones to
    /// be returned to a different pool instance.
    #[inline]
    #[allow(clippy::vec_box)] // Server is a very large struct, reading it when moving between contains is expensive.
    pub(super) fn move_conns_to(&mut self, destination: &Pool) -> (Vec<Box<Server>>, Taken) {
        self.moved = Some(destination.clone());
        let idle = std::mem::take(&mut self.conns).into_iter().collect();
        let taken = std::mem::take(&mut self.taken);

        (idle, taken)
    }

    #[inline(always)]
    /// Check a connection back into the pool if it's ok to do so.
    /// Otherwise, drop the connection and close it.
    ///
    /// Return: true if the pool should be banned, false otherwise.
    pub(super) fn maybe_check_in(
        &mut self,
        mut server: Box<Server>,
        now: Instant,
        stats: BackendCounts,
    ) -> CheckInResult {
        let mut result = CheckInResult {
            banned: false,
            replenish: true,
        };

        if let Some(ref moved) = self.moved {
            result.replenish = false;
            // Prevents deadlocks.
            if moved.id() != self.id {
                moved.lock().maybe_check_in(server, now, stats);
                return result;
            }
        }

        self.taken.check_in(server.id());

        // Update stats
        self.stats.counts = self.stats.counts + stats;

        // Ban the pool from serving more clients.
        if server.error() {
            self.errors += 1;
            result.banned = self.maybe_ban(now, Error::ServerError);
            return result;
        }

        // Pool is offline or paused, connection should be closed.
        if !self.online || self.paused {
            result.replenish = false;
            return result;
        }

        // Close connections exceeding max age.
        if server.age(now) >= self.config.max_age {
            return result;
        }

        // Force close the connection.
        if server.force_close() {
            self.force_close += 1;
            return result;
        }

        if server.re_synced() {
            self.re_synced += 1;
            server.reset_re_synced();
        }

        // Finally, if the server is ok,
        // place the connection back into the idle list.
        if server.can_check_in() {
            self.put(server);
        } else {
            self.out_of_sync += 1;
        }

        result
    }

    #[inline]
    pub(super) fn remove_waiter(&mut self, id: &BackendKeyData) {
        if let Some(waiter) = self.waiting.pop_front() {
            if waiter.request.id != *id {
                // Put me back.
                self.waiting.push_front(waiter);

                // Slow search, but we should be somewhere towards the front
                // if the runtime is doing scheduling correctly.
                for (i, waiter) in self.waiting.iter().enumerate() {
                    if waiter.request.id == *id {
                        self.waiting.remove(i);
                        break;
                    }
                }
            }
        }
    }

    /// Ban the pool from serving traffic if that's allowed
    /// per configuration.
    #[inline]
    pub fn maybe_ban(&mut self, now: Instant, reason: Error) -> bool {
        if self.config.bannable || reason == Error::ManualBan {
            let ban = Ban {
                created_at: now,
                reason,
                ban_timeout: self.config.ban_timeout(),
            };
            self.ban = Some(ban);

            // Tell every waiting client that this pool is busted.
            self.close_waiters(Error::Banned);
            true
        } else {
            false
        }
    }

    #[inline]
    pub(super) fn close_waiters(&mut self, err: Error) {
        for waiter in self.waiting.drain(..) {
            let _ = waiter.tx.send(Err(err));
        }
    }

    /// Remove the pool ban unless it' been manually banned.
    #[inline(always)]
    pub fn maybe_unban(&mut self) -> bool {
        let mut unbanned = false;
        if let Some(ban) = self.ban.take() {
            if ban.reason == Error::ManualBan {
                self.ban = Some(ban);
            } else {
                unbanned = true;
            }
        }

        unbanned
    }

    #[inline(always)]
    pub fn banned(&self) -> bool {
        self.ban.is_some()
    }
}

#[derive(Debug, Copy, Clone)]
pub(super) struct CheckInResult {
    pub(super) banned: bool,
    pub(super) replenish: bool,
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use tokio::sync::oneshot::channel;

    use crate::net::messages::BackendKeyData;

    use super::*;

    #[test]
    fn test_invariants() {
        let mut inner = Inner::default();

        // Defaults.
        assert!(!inner.banned());
        assert!(inner.idle() == 0);
        assert_eq!(inner.idle(), 0);
        assert!(!inner.online);
        assert!(!inner.paused);

        // The ban list.
        let banned = inner.maybe_ban(Instant::now(), Error::CheckoutTimeout);
        assert!(banned);
        let unbanned = inner.check_ban(Instant::now() + Duration::from_secs(100));
        assert!(!unbanned);
        assert!(inner.banned());
        let unbanned = inner.check_ban(Instant::now() + Duration::from_secs(301));
        assert!(unbanned);
        assert!(!inner.banned());
        let unbanned = inner.maybe_unban();
        assert!(!unbanned);
        assert!(!inner.banned());
        let banned = inner.maybe_ban(Instant::now(), Error::ManualBan);
        assert!(banned);
        assert!(!inner.maybe_unban());
        assert!(inner.banned());
        let banned = inner.maybe_ban(Instant::now(), Error::ServerError);
        assert!(banned);

        // Testing check-in server.
        let result = inner.maybe_check_in(
            Box::new(Server::default()),
            Instant::now(),
            BackendCounts::default(),
        );
        assert!(!result.banned);
        assert_eq!(inner.idle(), 0); // pool offline

        inner.online = true;
        inner.paused = true;
        inner.maybe_check_in(
            Box::new(Server::default()),
            Instant::now(),
            BackendCounts::default(),
        );
        assert_eq!(inner.total(), 0); // pool paused;
        inner.paused = false;
        assert!(
            !inner
                .maybe_check_in(
                    Box::new(Server::default()),
                    Instant::now(),
                    BackendCounts::default()
                )
                .banned
        );
        assert!(inner.idle() > 0);
        assert_eq!(inner.idle(), 1);

        let server = Box::new(Server::new_error());

        assert_eq!(inner.checked_out(), 0);
        inner.taken.take(&Mapping {
            client: BackendKeyData::new(),
            server: *server.id(),
        });
        assert_eq!(inner.checked_out(), 1);

        let result = inner.maybe_check_in(server, Instant::now(), BackendCounts::default());
        assert!(result.banned);
        assert_eq!(inner.ban.unwrap().reason, Error::ServerError);
        assert!(inner.taken.is_empty());
        inner.ban = None;

        inner.config.max = 5;
        inner.waiting.push_back(Waiter {
            request: Request::default(),
            tx: channel().0,
        });
        assert_eq!(inner.idle(), 1);
        assert!(!inner.should_create());

        assert_eq!(inner.config.min, 1);
        assert_eq!(inner.idle(), 1);
        assert!(!inner.should_create());

        inner.config.min = 2;
        assert_eq!(inner.config.max, 5);
        assert!(inner.total() < inner.min());
        assert!(inner.total() < inner.max());
        assert!(!inner.banned() && inner.online);
        assert!(inner.should_create());

        inner.config.max = 1;
        assert!(!inner.should_create());

        inner.config.max = 3;

        assert!(inner.should_create());

        inner.conns.push(Box::new(Server::default()));
        inner.conns.push(Box::new(Server::default()));
        assert!(!inner.should_create());

        // Close idle connections.
        inner.config.idle_timeout = Duration::from_millis(5_000); // 5 seconds.
        inner.close_idle(Instant::now());
        assert_eq!(inner.idle(), inner.config.max); // Didn't close any.
        for _ in 0..10 {
            inner.close_idle(Instant::now() + Duration::from_secs(6));
        }
        assert_eq!(inner.idle(), inner.config.min);
        inner.config.min = 1;
        inner.close_idle(Instant::now() + Duration::from_secs(6));
        assert_eq!(inner.idle(), inner.config.min);

        // Close old connections.
        inner.config.max_age = Duration::from_millis(60_000);
        inner.close_old(Instant::now() + Duration::from_secs(59));
        assert_eq!(inner.idle(), 1);
        inner.close_old(Instant::now() + Duration::from_secs(61));
        assert_eq!(inner.idle(), 0); // This ignores the min setting!

        assert!(inner.should_create());

        assert_eq!(inner.total(), 0);
        inner.taken.take(&Mapping::default());
        assert_eq!(inner.total(), 1);
        inner.taken.clear();
        assert_eq!(inner.total(), 0);

        let server = Box::new(Server::default());
        let result = inner.maybe_check_in(
            server,
            Instant::now() + Duration::from_secs(61),
            BackendCounts::default(),
        );

        assert!(!result.banned);
        // Not checked in because of max age.
        assert_eq!(inner.total(), 0);
    }
}
