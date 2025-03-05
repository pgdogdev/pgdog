use super::{Ban, Config, Pool, Stats};

/// Pool state.
pub struct State {
    /// Number of connections checked out.
    pub checked_out: usize,
    /// Number of idle connections.
    pub idle: usize,
    /// Total number of connections managed by the pool.
    pub total: usize,
    /// Is the pool online?
    pub online: bool,
    /// Pool has no idle connections.
    pub empty: bool,
    /// Pool configuration.
    pub config: Config,
    /// The pool is paused.
    pub paused: bool,
    /// Number of clients waiting for a connection.
    pub waiting: usize,
    /// Pool ban.
    pub ban: Option<Ban>,
    /// Pool is banned.
    pub banned: bool,
    /// Errors.
    pub errors: usize,
    /// Out of sync
    pub out_of_sync: usize,
    /// Statistics
    pub stats: Stats,
}

impl State {
    pub(super) fn get(pool: &Pool) -> Self {
        let guard = pool.lock();

        State {
            checked_out: guard.checked_out(),
            idle: guard.idle(),
            total: guard.total(),
            online: guard.online,
            empty: guard.idle() == 0,
            config: guard.config,
            paused: guard.paused,
            waiting: guard.waiting,
            ban: guard.ban,
            banned: guard.ban.is_some(),
            errors: guard.errors,
            out_of_sync: guard.out_of_sync,
            stats: guard.stats,
        }
    }
}
