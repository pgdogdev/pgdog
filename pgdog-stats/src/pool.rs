use std::{
    ops::{Add, Div, Sub},
    time::Duration,
};

use pgdog_config::{PoolerMode, pooling::ConnectionRecovery};
use serde::{Deserialize, Serialize};

use crate::LsnStats;

/// Pool statistics.
///
/// These are updated after each connection check-in.
///
#[derive(Debug, Clone, Default, Copy, Serialize, Deserialize)]
pub struct Counts {
    /// Number of committed transactions.
    pub xact_count: usize,
    /// Number of transactions committed with 2-phase commit.
    pub xact_2pc_count: usize,
    /// Number of executed queries.
    pub query_count: usize,
    /// How many times a server has been given to a client.
    /// In transaction mode, this equals to `xact_count`.
    pub server_assignment_count: usize,
    /// Number of bytes received by server connections.
    pub received: usize,
    /// Number of bytes sent to server connections.
    pub sent: usize,
    /// Total duration of all transactions.
    pub xact_time: Duration,
    /// Total time spent idling inside transactions.
    pub idle_xact_time: Duration,
    /// Total time spent executing queries.
    pub query_time: Duration,
    /// Total time clients spent waiting for a connection from the pool.
    pub wait_time: Duration,
    /// Total count of Parse messages sent to server connections.
    pub parse_count: usize,
    /// Total count of Bind messages sent to server connections.
    pub bind_count: usize,
    /// Number of times the pool had to rollback unfinished transactions.
    pub rollbacks: usize,
    /// Number of times the pool sent the health check query.
    pub healthchecks: usize,
    /// Total count of Close messages sent to server connections.
    pub close: usize,
    /// Total number of network-related errors detected on server connections.
    pub errors: usize,
    /// Total number of server connections that were cleaned after a dirty session.
    pub cleaned: usize,
    /// Total number of times servers had to synchronize prepared statements from Postgres'
    /// pg_prepared_statements view.
    pub prepared_sync: usize,
    /// Total time spent creating server connections.
    pub connect_time: Duration,
    /// Total number of times the pool attempted to create server connections.
    pub connect_count: usize,
    /// Number of read transactions.
    pub reads: usize,
    /// Number of write transactions.
    pub writes: usize,
}

impl Sub for Counts {
    type Output = Counts;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            xact_count: self.xact_count.saturating_sub(rhs.xact_count),
            xact_2pc_count: self.xact_2pc_count.saturating_sub(rhs.xact_2pc_count),
            query_count: self.query_count.saturating_sub(rhs.query_count),
            server_assignment_count: self
                .server_assignment_count
                .saturating_sub(rhs.server_assignment_count),
            received: self.received.saturating_sub(rhs.received),
            sent: self.sent.saturating_sub(rhs.sent),
            xact_time: self.xact_time.saturating_sub(rhs.xact_time),
            idle_xact_time: self.idle_xact_time.saturating_sub(rhs.idle_xact_time),
            query_time: self.query_time.saturating_sub(rhs.query_time),
            wait_time: self.wait_time.saturating_sub(rhs.wait_time),
            parse_count: self.parse_count.saturating_sub(rhs.parse_count),
            bind_count: self.bind_count.saturating_sub(rhs.bind_count),
            rollbacks: self.rollbacks.saturating_sub(rhs.rollbacks),
            healthchecks: self.healthchecks.saturating_sub(rhs.healthchecks),
            close: self.close.saturating_sub(rhs.close),
            errors: self.errors.saturating_sub(rhs.errors),
            cleaned: self.cleaned.saturating_sub(rhs.cleaned),
            prepared_sync: self.prepared_sync.saturating_sub(rhs.prepared_sync),
            connect_time: self.connect_time.saturating_sub(rhs.connect_time),
            connect_count: self.connect_count.saturating_sub(rhs.connect_count),
            reads: self.reads.saturating_sub(rhs.reads),
            writes: self.writes.saturating_sub(rhs.writes),
        }
    }
}

impl Add for Counts {
    type Output = Counts;

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            xact_count: self.xact_count.saturating_add(rhs.xact_count),
            xact_2pc_count: self.xact_2pc_count.saturating_add(rhs.xact_2pc_count),
            query_count: self.query_count.saturating_add(rhs.query_count),
            server_assignment_count: self
                .server_assignment_count
                .saturating_add(rhs.server_assignment_count),
            received: self.received.saturating_add(rhs.received),
            sent: self.sent.saturating_add(rhs.sent),
            xact_time: self.xact_time.saturating_add(rhs.xact_time),
            idle_xact_time: self.idle_xact_time.saturating_add(rhs.idle_xact_time),
            query_time: self.query_time.saturating_add(rhs.query_time),
            wait_time: self.wait_time.saturating_add(rhs.wait_time),
            parse_count: self.parse_count.saturating_add(rhs.parse_count),
            bind_count: self.bind_count.saturating_add(rhs.bind_count),
            rollbacks: self.rollbacks.saturating_add(rhs.rollbacks),
            healthchecks: self.healthchecks.saturating_add(rhs.healthchecks),
            close: self.close.saturating_add(rhs.close),
            errors: self.errors.saturating_add(rhs.errors),
            cleaned: self.cleaned.saturating_add(rhs.cleaned),
            prepared_sync: self.prepared_sync.saturating_add(rhs.prepared_sync),
            connect_count: self.connect_count.saturating_add(rhs.connect_count),
            connect_time: self.connect_time.saturating_add(rhs.connect_time),
            reads: self.reads.saturating_add(rhs.reads),
            writes: self.writes.saturating_add(rhs.writes),
        }
    }
}

impl Div<usize> for Counts {
    type Output = Counts;

    fn div(self, rhs: usize) -> Self::Output {
        Self {
            xact_count: self.xact_count.checked_div(rhs).unwrap_or(0),
            xact_2pc_count: self.xact_2pc_count.checked_div(rhs).unwrap_or(0),
            query_count: self.query_count.checked_div(rhs).unwrap_or(0),
            server_assignment_count: self.server_assignment_count.checked_div(rhs).unwrap_or(0),
            received: self.received.checked_div(rhs).unwrap_or(0),
            sent: self.sent.checked_div(rhs).unwrap_or(0),
            xact_time: self.xact_time.checked_div(rhs as u32).unwrap_or_default(),
            idle_xact_time: self
                .idle_xact_time
                .checked_div(rhs as u32)
                .unwrap_or_default(),
            query_time: self.query_time.checked_div(rhs as u32).unwrap_or_default(),
            wait_time: self.wait_time.checked_div(rhs as u32).unwrap_or_default(),
            parse_count: self.parse_count.checked_div(rhs).unwrap_or(0),
            bind_count: self.bind_count.checked_div(rhs).unwrap_or(0),
            rollbacks: self.rollbacks.checked_div(rhs).unwrap_or(0),
            healthchecks: self.healthchecks.checked_div(rhs).unwrap_or(0),
            close: self.close.checked_div(rhs).unwrap_or(0),
            errors: self.errors.checked_div(rhs).unwrap_or(0),
            cleaned: self.cleaned.checked_div(rhs).unwrap_or(0),
            prepared_sync: self.prepared_sync.checked_div(rhs).unwrap_or(0),
            connect_time: self
                .connect_time
                .checked_div(rhs as u32)
                .unwrap_or_default(),
            connect_count: self.connect_count.checked_div(rhs).unwrap_or(0),
            reads: self.reads.checked_div(rhs).unwrap_or(0),
            writes: self.writes.checked_div(rhs).unwrap_or(0),
        }
    }
}

#[derive(Debug, Clone, Default, Copy, Serialize, Deserialize)]
pub struct Stats {
    // Total counts.
    pub counts: Counts,
    /// Counts since last average calculation.
    #[serde(skip)]
    last_counts: Counts,
    // Average counts.
    pub averages: Counts,
}

impl Stats {
    /// Calculate averages.
    pub fn calc_averages(&mut self, time: Duration) {
        let secs = time.as_secs() as usize;
        if secs > 0 {
            let diff = self.counts - self.last_counts;
            self.averages = diff / secs;
            self.averages.query_time = diff.query_time
                / diff
                    .query_count
                    .try_into()
                    .unwrap_or(u32::MAX)
                    .clamp(1, u32::MAX);
            self.averages.xact_time = diff.xact_time
                / diff
                    .xact_count
                    .try_into()
                    .unwrap_or(u32::MAX)
                    .clamp(1, u32::MAX);
            self.averages.wait_time = diff.wait_time
                / diff
                    .server_assignment_count
                    .try_into()
                    .unwrap_or(u32::MAX)
                    .clamp(1, u32::MAX);
            self.averages.connect_time = diff.connect_time
                / diff
                    .connect_count
                    .try_into()
                    .unwrap_or(u32::MAX)
                    .clamp(1, u32::MAX);
            let queries_in_xact = diff
                .query_count
                .wrapping_sub(diff.xact_count)
                .clamp(1, u32::MAX as usize);
            self.averages.idle_xact_time =
                diff.idle_xact_time / queries_in_xact.try_into().unwrap_or(u32::MAX);
            self.averages
                .reads
                .checked_div(diff.xact_count)
                .unwrap_or_default();
            self.averages
                .writes
                .checked_div(diff.xact_count)
                .unwrap_or_default();

            self.last_counts = self.counts;
        }
    }
}

/// Real-time state of each connection pool.
/// Pool state.
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
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
    #[serde(skip)]
    pub config: Config,
    /// The pool is paused.
    pub paused: bool,
    /// Number of clients waiting for a connection.
    pub waiting: usize,
    /// Errors.
    pub errors: usize,
    /// Out of sync
    pub out_of_sync: usize,
    /// Re-synced servers.
    pub re_synced: usize,
    /// Statistics
    pub stats: Stats,
    /// Max wait.
    pub maxwait: Duration,
    /// Pool mode
    pub pooler_mode: PoolerMode,
    /// Lag
    pub replica_lag: Duration,
    /// Force closed.
    pub force_close: usize,
    // LSN stats.
    pub lsn_stats: LsnStats,
}

/// Pool configuration.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
pub struct Config {
    /// Minimum connections that should be in the pool.
    pub min: usize,
    /// Maximum connections allowed in the pool.
    pub max: usize,
    /// How long to wait for a connection before giving up.
    pub checkout_timeout: Duration, // ms
    /// Interval duration of DNS cache refresh.
    pub dns_ttl: Duration, // ms
    /// Close connections that have been idle for longer than this.
    pub idle_timeout: Duration, // ms
    /// How long to wait for connections to be created.
    pub connect_timeout: Duration, // ms
    /// How many times to attempt a connection before returning an error.
    pub connect_attempts: u64,
    /// How long to wait between connection attempts.
    pub connect_attempt_delay: Duration,
    /// How long a connection can be open.
    pub max_age: Duration,
    /// Can this pool be banned from serving traffic?
    pub bannable: bool,
    /// Healtheck timeout.
    pub healthcheck_timeout: Duration, // ms
    /// Healtcheck interval.
    pub healthcheck_interval: Duration, // ms
    /// Idle healthcheck interval.
    pub idle_healthcheck_interval: Duration, // ms
    /// Idle healthcheck delay.
    pub idle_healthcheck_delay: Duration, // ms
    /// Read timeout (dangerous).
    pub read_timeout: Duration, // ms
    /// Write timeout (dangerous).
    pub write_timeout: Duration, // ms
    /// Query timeout (dangerous).
    pub query_timeout: Duration, // ms
    /// Max ban duration.
    pub ban_timeout: Duration, // ms
    /// Rollback timeout for dirty connections.
    pub rollback_timeout: Duration,
    /// Statement timeout
    pub statement_timeout: Option<Duration>,
    /// Replication mode.
    pub replication_mode: bool,
    /// Pooler mode.
    pub pooler_mode: PoolerMode,
    /// Read only mode.
    pub read_only: bool,
    /// Maximum prepared statements per connection.
    pub prepared_statements_limit: usize,
    /// Stats averaging period.
    pub stats_period: Duration,
    /// Recovery algo.
    pub connection_recovery: ConnectionRecovery,
    /// LSN check interval.
    pub lsn_check_interval: Duration,
    /// LSN check timeout.
    pub lsn_check_timeout: Duration,
    /// LSN check delay.
    pub lsn_check_delay: Duration,
    /// Automatic role detection enabled.
    pub role_detection: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            min: 1,
            max: 10,
            checkout_timeout: Duration::from_millis(5_000),
            idle_timeout: Duration::from_millis(60_000),
            connect_timeout: Duration::from_millis(5_000),
            connect_attempts: 1,
            connect_attempt_delay: Duration::from_millis(10),
            max_age: Duration::from_millis(24 * 3600 * 1000),
            bannable: true,
            healthcheck_timeout: Duration::from_millis(5_000),
            healthcheck_interval: Duration::from_millis(30_000),
            idle_healthcheck_interval: Duration::from_millis(5_000),
            idle_healthcheck_delay: Duration::from_millis(5_000),
            read_timeout: Duration::MAX,
            write_timeout: Duration::MAX,
            query_timeout: Duration::MAX,
            ban_timeout: Duration::from_secs(300),
            rollback_timeout: Duration::from_secs(5),
            statement_timeout: None,
            replication_mode: false,
            pooler_mode: PoolerMode::default(),
            read_only: false,
            prepared_statements_limit: usize::MAX,
            stats_period: Duration::from_millis(15_000),
            dns_ttl: Duration::from_millis(60_000),
            connection_recovery: ConnectionRecovery::Recover,
            lsn_check_interval: Duration::from_millis(5_000),
            lsn_check_timeout: Duration::from_millis(5_000),
            lsn_check_delay: Duration::from_millis(5_000),
            role_detection: false,
        }
    }
}
