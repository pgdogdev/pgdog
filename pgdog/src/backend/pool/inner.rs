//! Pool internals synchronized with a mutex.

use std::collections::VecDeque;
use std::{cmp::max, time::Instant};

use crate::backend::Server;

use super::{Ban, Config, Error, Mapping};

/// Pool internals protected by a mutex.
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
    pub(super) fn should_remove(&self) -> usize {
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
        self.total() + self.creating < self.min()
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
    pub(crate) fn close_old(&mut self, now: Instant) {
        let max_age = self.config.max_age();

        self.conns.retain(|c| {
            let age = c.age(now);
            age < max_age
        });
    }

    /// Close connections that have been idle for too long
    /// without affecting the minimum pool size requirement.
    #[inline]
    pub(crate) fn close_idle(&mut self, now: Instant) {
        let mut remove = self.should_remove();
        let idle_timeout = self.config.idle_timeout();

        self.conns.retain(|c| {
            let idle_for = c.idle_for(now);

            if remove > 0 && idle_for >= idle_timeout {
                remove -= 1;
                false
            } else {
                true
            }
        });
    }

    /// Pool configuration options.
    #[inline]
    pub(super) fn config(&self) -> &Config {
        &self.config
    }

    #[inline]
    /// Check a connection back into the pool if it's ok to do so.
    /// Otherwise, drop the connection and close it.
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
            }

            unbanned = true;
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
        if self.creating > 0 {
            self.creating -= 1;
            true
        } else {
            false
        }
    }

    /// Create a create permit.
    #[inline]
    pub fn create(&mut self) {
        self.creating += 1;
    }
}
