use std::{
    ops::{Deref, DerefMut},
    time::Duration,
};

use tokio::time::Instant;

use super::Pool;

/// Pool state.
#[derive(Debug)]
pub struct State {
    inner: pgdog_stats::State,
}

impl Deref for State {
    type Target = pgdog_stats::State;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for State {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl State {
    pub(super) fn get(pool: &Pool) -> Self {
        let now = Instant::now();
        let lsn_stats = pool.lsn_stats();
        let guard = pool.lock();

        State {
            inner: pgdog_stats::State {
                checked_out: guard.checked_out(),
                idle: guard.idle(),
                total: guard.total(),
                online: guard.online,
                empty: guard.idle() == 0,
                config: *guard.config,
                paused: guard.paused,
                waiting: guard.waiting.len(),
                errors: guard.errors,
                out_of_sync: guard.out_of_sync,
                re_synced: guard.re_synced,
                stats: *guard.stats,
                maxwait: guard
                    .waiting
                    .iter()
                    .next()
                    .map(|req| now.duration_since(req.request.created_at))
                    .unwrap_or(Duration::ZERO),
                pooler_mode: guard.config().pooler_mode,
                replica_lag: guard.replica_lag,
                force_close: guard.force_close,
                lsn_stats: *lsn_stats,
            },
        }
    }
}
