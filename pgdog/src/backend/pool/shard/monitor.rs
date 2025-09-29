use super::*;

/// Shard communication primitives.
#[derive(Debug)]
pub(super) struct ShardComms {
    pub shutdown: Notify,
}

impl Default for ShardComms {
    fn default() -> Self {
        Self {
            shutdown: Notify::new(),
        }
    }
}

/// Monitor shard connection pools for various stats.
///
/// Currently, only checking for replica lag, if any replicas are configured
/// and this is enabled.
pub(super) struct ShardMonitor {}

impl ShardMonitor {
    /// Run the shard monitor.
    pub fn run(shard: &Shard) {
        let shard = shard.clone();
        spawn(async move { Self::monitor_replicas(shard).await });
    }
}

impl ShardMonitor {
    async fn monitor_replicas(shard: Shard) {
        let cfg_handle = config();
        let Some(rl_config) = cfg_handle.config.replica_lag.as_ref() else {
            return;
        };

        let mut tick = interval(rl_config.check_interval);

        debug!("replica monitoring running");
        let comms = shard.comms();

        loop {
            select! {
                _ = tick.tick() => {
                    Self::process_replicas(&shard, rl_config.max_age).await;
                }
                _ = comms.shutdown.notified() => break,
            }
        }

        debug!("replica monitoring stopped");
    }

    async fn process_replicas(shard: &Shard, max_age: Duration) {
        let Some(primary) = shard.primary.as_ref() else {
            return;
        };

        primary.set_replica_lag(ReplicaLag::NonApplicable);

        let lsn_metrics = match collect_lsn_metrics(primary, max_age).await {
            Some(m) => m,
            None => {
                error!("failed to collect LSN metrics");
                return;
            }
        };

        for replica in shard.replicas.pools() {
            Self::process_single_replica(
                replica,
                lsn_metrics.max_lsn,
                lsn_metrics.average_bytes_per_sec,
                max_age,
            )
            .await;
        }
    }

    // TODO -> [process_single_replica]
    // - The current query logic prevents pools from executing any query once banned.
    // - This includes running their own LSN queries.
    // - For this reason, we cannot ban replicas for lagging just yet
    // - For now, we simply tracing::error!() it for now.
    // - It's sensible to ban replicas from making user queries when it's lagging too much, but...
    //   unexposed PgDog admin queries should be allowed on "banned" replicas.
    // - TLDR; We need a way to distinguish between user and admin queries, and let admin...
    //   queries run on "banned" replicas.

    async fn process_single_replica(
        replica: &Pool,
        primary_lsn: u64,
        lsn_throughput: f64,
        _max_age: Duration, // used to make banning decisions when it's supported later
    ) {
        if !replica.inner().health.healthy() {
            replica.set_replica_lag(ReplicaLag::Unknown);
            return;
        };

        let replay_lsn = match replica.wal_replay_lsn().await {
            Ok(lsn) => lsn,
            Err(e) => {
                error!("replica {} LSN query failed: {}", replica.id(), e);
                return;
            }
        };

        let bytes_behind = primary_lsn.saturating_sub(replay_lsn);

        let mut lag = ReplicaLag::Bytes(bytes_behind);
        if lsn_throughput > 0.0 {
            let duration = Duration::from_secs_f64(bytes_behind as f64 / lsn_throughput);
            lag = ReplicaLag::Duration(duration);
        }
        if bytes_behind == 0 {
            lag = ReplicaLag::Duration(Duration::ZERO);
        }

        replica.set_replica_lag(lag);
    }
}

// -------------------------------------------------------------------------------------------------
// ----- Utils :: Primary LSN Metrics --------------------------------------------------------------

struct LsnMetrics {
    pub average_bytes_per_sec: f64,
    pub max_lsn: u64,
}

/// Sample WAL LSN at 0, half, and full window; compute avg rate and max LSN.
async fn collect_lsn_metrics(primary: &Pool, window: Duration) -> Option<LsnMetrics> {
    if window.is_zero() {
        return None;
    }

    let half_window = window / 2;
    let half_window_in_seconds = half_window.as_secs_f64();

    // fire three futures at once
    let f0 = primary.wal_flush_lsn();
    let f1 = async {
        sleep(half_window).await;
        primary.wal_flush_lsn().await
    };
    let f2 = async {
        sleep(window).await;
        primary.wal_flush_lsn().await
    };

    // collect results concurrently
    let (r0, r1, r2) = join!(f0, f1, f2);

    let lsn_initial = r0.ok()?;
    let lsn_half = r1.ok()?;
    let lsn_full = r2.ok()?;

    let rate1 = (lsn_half.saturating_sub(lsn_initial)) as f64 / half_window_in_seconds;
    let rate2 = (lsn_full.saturating_sub(lsn_half)) as f64 / half_window_in_seconds;
    let average_rate = (rate1 + rate2) / 2.0;

    let max_lsn = lsn_initial.max(lsn_half).max(lsn_full);

    let metrics = LsnMetrics {
        average_bytes_per_sec: average_rate,
        max_lsn,
    };

    Some(metrics)
}
