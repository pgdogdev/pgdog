//! Pool internals synchronized with a mutex.

use std::collections::VecDeque;
use std::{cmp::max, time::Instant};

use crate::backend::Server;

use super::{Ban, Config, Error, Mapping};

/// Pool internals protected by a mutex.
#[derive(Default)]
pub(super) struct Inner {
    /// Idle server connections.
    pub(super) conns: VecDeque<Server>,
    /// Server connectios currently checked out.
    pub(super) taken: Vec<Mapping>,
    /// Pool configuration.
    pub(super) config: Config,
    /// Number of clients waiting for a connection.
    pub(super) waiting: usize,
    /// Pool ban status.
    pub(super) ban: Option<Ban>,
    /// Pool is online and availble to clients.
    pub(super) online: bool,
    /// Pool is paused.
    pub(super) paused: bool,
    /// Connections being created.
    pub(super) creating: usize,
    /// Creating connections as we speak.
    pub(super) create: usize,
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Inner")
            .field("creating", &self.creating)
            .field("paused", &self.paused)
            .field("taken", &self.taken.len())
            .field("conns", &self.conns.len())
            .field("waiting", &self.waiting)
            .field("online", &self.online)
            .finish()
    }
}

impl Inner {
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

    /// The pool is currently empty of idle connections.
    #[inline]
    #[allow(dead_code)]
    pub(super) fn empty(&self) -> bool {
        self.idle() == 0
    }

    /// The pool can create more connections if they are needed
    /// without breaking the maximum number of connections requirement.
    #[inline]
    pub(super) fn can_create(&self) -> bool {
        println!("{} < {}", self.total(), self.config.max);
        self.total() < self.config.max
    }

    /// Number of connections checked out of the pool
    /// by clients.
    #[inline]
    pub(super) fn checked_out(&self) -> usize {
        self.taken.len()
    }

    /// How many connections should be removed from the pool.
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

    /// The pool should create more connections to satisfy the minimum
    /// connection requirement.
    #[inline]
    pub(super) fn should_create(&self) -> bool {
        self.total() + self.creating < self.min() + self.create
    }

    /// Check if the pool ban should be removed.
    #[inline]
    pub(super) fn check_ban(&mut self, now: Instant) -> bool {
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
        let max_age = self.config.max_age();
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
        let idle_timeout = self.config.idle_timeout();

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

    #[inline]
    /// Check a connection back into the pool if it's ok to do so.
    /// Otherwise, drop the connection and close it.
    ///
    /// Return: true if the pool should be banned, false otherwise.
    pub(super) fn maybe_check_in(&mut self, server: Server, now: Instant) -> bool {
        let id = *server.id();

        let index = self
            .taken
            .iter()
            .enumerate()
            .find(|(_i, p)| p.server == id)
            .map(|(i, _p)| i);

        if let Some(index) = index {
            self.taken.remove(index);
        }

        // Ban the pool from serving more clients.
        if server.error() {
            return self.maybe_ban(now, Error::ServerError);
        }

        // Pool is offline or paused, connection should be closed.
        if !self.online || self.paused {
            return false;
        }

        // Close connections exceeding max age.
        if server.age(now) >= self.config.max_age() {
            return false;
        }

        // Finally, if the server is ok,
        // place the connection back into the idle list.
        if server.done() {
            self.conns.push_back(server);
        }

        false
    }

    /// Ban the pool from serving traffic if that's allowed
    /// per configuration.
    #[inline]
    pub fn maybe_ban(&mut self, now: Instant, reason: Error) -> bool {
        if self.config.bannable || reason == Error::ManualBan {
            self.ban = Some(Ban {
                created_at: now,
                reason,
                ban_timeout: self.config.ban_timeout(),
            });

            true
        } else {
            false
        }
    }

    /// Remove the pool ban unless it' been manually banned.
    #[inline]
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

    /// Pool is banned from serving connections.
    #[inline]
    pub fn banned(&self) -> bool {
        self.ban.is_some()
    }

    /// Consume a create permit if there is one.
    #[inline]
    pub fn create_permit(&mut self) -> bool {
        if self.create > 0 {
            self.create -= 1;
            if self.can_create() {
                self.creating += 1;
                true
            } else {
                false
            } // Assert that a necessary connection
              // hasn't been created since the permit was issued.
        } else {
            false
        }
    }

    /// Create a create permit.
    #[inline]
    pub fn create(&mut self) {
        self.create += 1;
    }

    /// Created the connection.
    #[inline]
    pub fn created(&mut self) {
        self.creating += 1;
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use crate::net::messages::BackendKeyData;

    use super::*;

    #[test]
    fn test_invariants() {
        let mut inner = Inner::default();

        // Defaults.
        assert!(!inner.banned());
        assert!(inner.empty());
        assert_eq!(inner.idle(), 0);
        assert!(!inner.online);
        assert!(!inner.paused);

        // Create permits.
        inner.create();
        assert_eq!(inner.creating, 1);
        assert!(inner.create_permit());
        assert_eq!(inner.creating, 0);

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
        let banned = inner.maybe_check_in(Server::default(), Instant::now());
        assert!(!banned);
        assert_eq!(inner.idle(), 0); // pool offline

        inner.online = true;
        inner.paused = true;
        inner.maybe_check_in(Server::default(), Instant::now());
        assert_eq!(inner.total(), 0); // pool paused;
        inner.paused = false;
        assert!(!inner.maybe_check_in(Server::default(), Instant::now()));
        assert!(!inner.empty());
        assert_eq!(inner.idle(), 1);

        let server = Server::new_error();

        assert_eq!(inner.checked_out(), 0);
        inner.taken.push(Mapping {
            client: BackendKeyData::new(),
            server: *server.id(),
        });
        assert_eq!(inner.checked_out(), 1);

        let banned = inner.maybe_check_in(server, Instant::now());
        assert!(banned);
        assert_eq!(inner.ban.unwrap().reason, Error::ServerError);
        assert!(inner.taken.is_empty());

        inner.config.max = 5;
        assert!(inner.can_create());

        assert_eq!(inner.config.min, 1);
        assert_eq!(inner.idle(), 1);
        assert!(!inner.should_create());

        inner.config.min = 2;
        assert!(inner.should_create());

        inner.config.max = 1;
        assert!(!inner.can_create());

        inner.config.max = 3;
        inner.create();
        inner.create();
        assert!(!inner.should_create());
        // Consume permits but connections weren't created.
        assert!(inner.can_create());
        inner.create_permit();
        inner.create_permit();
        assert!(inner.should_create());
        // Consume permits and create connections successfully.
        inner.create();
        inner.create();
        inner.create_permit();
        inner.create_permit();
        inner.conns.push_back(Server::default());
        inner.conns.push_back(Server::default());
        assert!(!inner.should_create());
        assert!(!inner.can_create()); // pool is full of idle connections.

        // Close idle connections.
        inner.config.idle_timeout = 5_000; // 5 seconds.
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
        inner.config.max_age = 60_000;
        inner.close_old(Instant::now() + Duration::from_secs(59));
        assert_eq!(inner.idle(), 1);
        inner.close_old(Instant::now() + Duration::from_secs(61));
        assert_eq!(inner.idle(), 0); // This ignores the min setting!

        assert!(inner.should_create());
        assert!(inner.can_create());

        assert_eq!(inner.total(), 0);
        inner.taken.push(Mapping::default());
        assert_eq!(inner.total(), 1);
        inner.taken.clear();
        assert_eq!(inner.total(), 0);

        let server = Server::default();
        let banned = inner.maybe_check_in(server, Instant::now() + Duration::from_secs(61));

        assert!(!banned);
        // Not checked in because of max age.
        assert_eq!(inner.total(), 0);
    }
}
