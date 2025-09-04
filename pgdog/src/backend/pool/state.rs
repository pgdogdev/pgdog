use std::time::Duration;

use crate::config::PoolerMode;
use tokio::time::Instant;

use super::{inner::ReplicaLag, Ban, Config, Pool, Stats};

/// Pool state.
#[derive(Debug)]
pub(crate) struct State {
    /// Number of connections checked out.
    pub(crate) checked_out: usize,
    /// Number of idle connections.
    pub(crate) idle: usize,
    /// Total number of connections managed by the pool.
    pub(crate) total: usize,
    /// Is the pool online?
    pub(crate) online: bool,
    /// Pool has no idle connections.
    pub(crate) empty: bool,
    /// Pool configuration.
    pub(crate) config: Config,
    /// The pool is paused.
    pub(crate) paused: bool,
    /// Number of clients waiting for a connection.
    pub(crate) waiting: usize,
    /// Pool ban.
    pub(crate) ban: Option<Ban>,
    /// Pool is banned.
    pub(crate) banned: bool,
    /// Errors.
    pub(crate) errors: usize,
    /// Out of sync
    pub(crate) out_of_sync: usize,
    /// Re-synced servers.
    pub(crate) re_synced: usize,
    /// Statistics
    pub(crate) stats: Stats,
    /// Max wait.
    pub(crate) maxwait: Duration,
    /// Pool mode
    pub(crate) pooler_mode: PoolerMode,
    /// Lag
    pub(crate) replica_lag: ReplicaLag,
}

impl State {
    pub(super) fn get(pool: &Pool) -> Self {
        let now = Instant::now();
        let guard = pool.lock();

        State {
            checked_out: guard.checked_out(),
            idle: guard.idle(),
            total: guard.total(),
            online: guard.online,
            empty: guard.idle() == 0,
            config: guard.config,
            paused: guard.paused,
            waiting: guard.waiting.len(),
            ban: guard.ban,
            banned: guard.ban.is_some(),
            errors: guard.errors,
            out_of_sync: guard.out_of_sync,
            re_synced: guard.re_synced,
            stats: guard.stats,
            maxwait: guard
                .waiting
                .iter()
                .next()
                .map(|req| now.duration_since(req.request.created_at))
                .unwrap_or(Duration::ZERO),
            pooler_mode: guard.config().pooler_mode,
            replica_lag: guard.replica_lag,
        }
    }
}
