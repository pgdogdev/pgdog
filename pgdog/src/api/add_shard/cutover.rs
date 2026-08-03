//! The cutover: park for the operator, pause omni writes fleet-wide,
//! drain replication to zero, swap the shard into the topology, and
//! finalize.

use std::time::{Duration, Instant};

use pgdog_config::CutoverTimeoutAction;
use tokio::select;
use tokio::time::interval;
use tracing::{info, warn};

use super::guards::Preflight;
use super::{AddShardStatus, AddShardTask, CutoverOutcome};
use crate::api::MigrationError;
use crate::api::async_task::AsyncTaskContext;
use crate::api::cutover_registry;
use crate::backend::databases::{activate_provisioning_shard, databases, persist_config};
use crate::backend::fleet::barrier;
use crate::backend::fleet::{self, Coordinator, Discovery};
use crate::backend::provisioning;
use crate::backend::replication::logical::Error;
use crate::backend::replication::logical::orchestrator::ReplicationWaiter;
use crate::backend::schema::sync::config::ShardConfig;
use crate::config::config;
use tokio_util::sync::CancellationToken;

/// How long `finalize` waits for the reloaded topology's pools.
const WAIT_READY_TIMEOUT: Duration = Duration::from_secs(60);

/// Releases the omni-write barrier on drop, so every error path
/// resumes writes.
struct BarrierGuard {
    database: String,
}

impl BarrierGuard {
    fn arm(database: &str) -> Self {
        barrier::start(database);
        Self {
            database: database.to_string(),
        }
    }
}

impl Drop for BarrierGuard {
    fn drop(&mut self) {
        barrier::stop(&self.database);
    }
}

/// Why the park ended.
pub(super) enum Parked {
    /// An operator `CUTOVER` targeted this task.
    CutoverRequested,
    /// The task was cancelled, or the stream ended on its own: the
    /// task returns.
    Ended,
}

/// Entry: caught up, replication streaming. Exit: an operator
/// `CUTOVER` arrived, or the task ended (cancelled, or the stream
/// finished by itself). Failure: a stream error while parked
/// propagates.
pub(super) async fn park(
    task: &AddShardTask,
    ctx: &AsyncTaskContext<AddShardTask>,
    token: &CancellationToken,
    waiter: &mut ReplicationWaiter,
) -> Result<Parked, MigrationError> {
    ctx.set_status(AddShardStatus::AwaitingCutover);
    let cutover =
        cutover_registry::register_cutover(ctx.root_id(), &task.database, Some(task.shard));

    select! {
        _ = token.cancelled() => {
            waiter.stop();
            waiter.wait().await?;
            Ok(Parked::Ended)
        }
        _ = cutover.requested() => Ok(Parked::CutoverRequested),
        res = waiter.wait() => {
            // The stream ended on its own: an error, or the slot
            // drained after a stop.
            res?;
            Ok(Parked::Ended)
        }
    }
}

/// Coordination failures fail the task in auto mode (no operator to
/// retry) and re-park it otherwise.
pub(super) fn repark_or_fail(
    task: &AddShardTask,
    why: Error,
) -> Result<CutoverOutcome, MigrationError> {
    if task.auto_cutover {
        Err(why.into())
    } else {
        warn!("[add shard] {}; re-parking", why);
        Ok(CutoverOutcome::Aborted)
    }
}

/// Entry: caught up (or an operator forced it). Exit `Done`: the shard
/// is in the topology and the fleet was finalized. Exit `Aborted`:
/// every barrier — local and peers' — was released and replication
/// continues. The swap is the point of no return: everything after it
/// is best-effort and the cutover reports success.
pub(super) async fn run(
    task: &AddShardTask,
    ctx: &AsyncTaskContext<AddShardTask>,
    waiter: &mut ReplicationWaiter,
    preflight: &Preflight,
) -> Result<CutoverOutcome, MigrationError> {
    // Other pgdog instances must pause their omni writes too. An
    // instance running an older config is in the fleet but not
    // registered on the new shard: refuse rather than diverge.
    let fleet = databases()
        .schema_owner(&task.database)
        .map_err(Error::from)?;
    let coordination =
        match Coordinator::discover(provisioning::TOPIC, &fleet, preflight.destination())
            .await
            .map_err(Error::from)?
        {
            Discovery::Solo => None,
            Discovery::Missing(missing) => {
                return repark_or_fail(task, Error::InstancesNotRegistered(missing));
            }
            Discovery::Ready(coordination) => Some(*coordination),
        };

    ctx.set_status(AddShardStatus::Draining);
    let barrier = BarrierGuard::arm(&task.database);

    // Arm every peer and wait for their acks before draining: until
    // they all park omni writes, the drain can't converge.
    if let Some(coordination) = &coordination {
        fleet::protocol::ensure_tables(coordination.medium())
            .await
            .map_err(Error::from)?;
        match coordination
            .broadcast_and_await(provisioning::STATE_ARMED, provisioning::ARM_ACK_TIMEOUT)
            .await
            .map_err(Error::from)?
        {
            None => {}
            Some(stragglers) => {
                coordination.publish(provisioning::STATE_RELEASED).await;
                drop(barrier);
                return repark_or_fail(task, Error::InstancesNotArmed(stragglers));
            }
        }
    }

    // The peers' abandoned-coordinator failsafe measures the armed
    // row's age: keep it fresh for as long as we hold their barriers,
    // however long the drain and stream shutdown take.
    let refresh = coordination
        .as_ref()
        .map(|coordination| coordination.keep_fresh(provisioning::STATE_ARMED));

    if !drain(waiter).await {
        drop(refresh);
        if let Some(coordination) = &coordination {
            coordination.publish(provisioning::STATE_RELEASED).await;
        }
        drop(barrier);
        return repark_or_fail(task, Error::AbortTimeout);
    }

    // Stop streaming and join the stream tasks: the real
    // drain-to-zero. Slots are dropped as the streams wind down.
    waiter.stop();
    let stopped = waiter.wait().await;
    drop(refresh);
    if let Err(err) = stopped {
        if let Some(coordination) = &coordination {
            coordination.publish(provisioning::STATE_RELEASED).await;
        }
        return Err(err.into());
    }

    // Point of no return: no cancellation from here on.
    ctx.set_status(AddShardStatus::SwappingTopology);
    let new_config = match activate_provisioning_shard(&task.database, task.shard).await {
        Ok(new_config) => new_config,
        Err(err) => {
            // The swap didn't happen; resume the fleet and fail.
            if let Some(coordination) = &coordination {
                coordination.publish(provisioning::STATE_RELEASED).await;
            }
            return Err(Error::from(err).into());
        }
    };

    // Local omni writes are safe the moment the swap lands: they
    // broadcast to all N+1 shards. The peers resume via finalize.
    drop(barrier);

    finalize(task, &new_config, coordination.as_ref()).await;

    Ok(CutoverOutcome::Done)
}

/// Drain replication to zero while omni writes are parked. `false`
/// means the drain timed out with the `abort` action.
async fn drain(waiter: &ReplicationWaiter) -> bool {
    let general = &config().config.general;
    let threshold = general.cutover_replication_lag_threshold;
    let last_transaction_delay = Duration::from_millis(general.cutover_last_transaction_delay);
    let timeout = Duration::from_millis(general.cutover_timeout);
    let timeout_action = general.cutover_timeout_action;

    // In-flight omni writes finish and replicate; new ones park at the
    // barrier. Sharded-table WAL doesn't hold the lag up — keepalives
    // advance past unpublished changes.
    let started = Instant::now();
    let mut check = interval(Duration::from_millis(50));
    loop {
        check.tick().await;

        let lag = waiter.lag().await;
        let last_transaction = waiter.last_transaction().await;
        let drained = lag.is_some_and(|lag| lag <= threshold)
            && last_transaction.is_none_or(|last| last > last_transaction_delay);

        if drained {
            return true;
        }

        if started.elapsed() > timeout {
            match timeout_action {
                CutoverTimeoutAction::Abort => {
                    warn!(
                        "[add shard] drain timed out after {:?}; resuming omni writes",
                        timeout
                    );
                    return false;
                }
                CutoverTimeoutAction::Cutover => return true,
            }
        }
    }
}

/// Everything after the swap: persist, refresh `pgdog.config` on every
/// shard, and activate the peers. The topology has already changed, so
/// every failure here is warned — with its recovery step — and never
/// propagated: the peers must still hear about the activation, and
/// stragglers converge on their own.
pub(super) async fn finalize(
    task: &AddShardTask,
    new_config: &pgdog_config::ConfigAndUsers,
    coordination: Option<&Coordinator>,
) {
    // Persist immediately to shrink the crash window between the
    // in-memory topology and the on-disk config.
    if new_config.config.general.cutover_save_config
        && let Err(err) = persist_config(new_config).await
    {
        warn!(
            "[add shard] failed to persist the config: {}; update the config source by hand",
            err
        );
    }

    // Refresh pgdog's own metadata on every shard: the shard total
    // changed cluster-wide, and the new shard's row is the marker
    // restarts converge from.
    match databases().schema_owner(&task.database) {
        Ok(cluster) => {
            if tokio::time::timeout(WAIT_READY_TIMEOUT, cluster.wait_ready())
                .await
                .is_err()
            {
                warn!(
                    "[add shard] the new topology wasn't ready after {:?}; continuing",
                    WAIT_READY_TIMEOUT
                );
            }
            if let Err(err) = ShardConfig::sync_all(&cluster).await {
                warn!(
                    "[add shard] failed to refresh pgdog.config: {}; run SETUP SCHEMA to write the convergence marker",
                    err
                );
            }
        }
        Err(err) => warn!(
            "[add shard] could not re-resolve \"{}\" after the swap: {}",
            task.database, err
        ),
    }

    info!(
        "[add shard] shard {} of \"{}\" is now active",
        task.shard, task.database
    );

    let Some(coordination) = coordination else {
        return;
    };
    match coordination
        .broadcast_and_await(
            provisioning::STATE_ACTIVATED,
            provisioning::ACTIVATE_ACK_TIMEOUT,
        )
        .await
    {
        Ok(None) => {
            // The state row is the stragglers' only signal: drop the
            // tables only once everyone is done.
            if let Err(err) = fleet::protocol::drop_tables(coordination.medium()).await {
                warn!("failed to drop coordination tables: {}", err);
            }
        }
        Ok(Some(stragglers)) => warn!(
            "[add shard] instance(s) [{}] haven't activated the new shard yet; they will converge on their own",
            stragglers
        ),
        Err(err) => warn!(
            "[add shard] failed to signal activation to the peers: {}; they will converge on reload or restart",
            err
        ),
    }
}
