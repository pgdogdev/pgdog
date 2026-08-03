//! The degenerate path: a database with no omnisharded tables has
//! nothing to copy or stream, and no writes to pause. Sync the schema
//! (pre and post phases), park for the operator unless auto, and
//! activate.

use tokio::select;

use super::guards::Preflight;
use super::{AddShardStatus, AddShardTask, CutoverOutcome, cutover, provision};
use crate::api::MigrationError;
use crate::api::async_task::AsyncTaskContext;
use crate::api::cutover_registry;
use crate::backend::databases::{activate_provisioning_shard, databases};
use crate::backend::fleet::{self, Coordinator, Discovery};
use crate::backend::provisioning;
use crate::backend::replication::logical::Error;
use crate::backend::replication::logical::orchestrator::Orchestrator;
use tokio_util::sync::CancellationToken;

/// Entry: guards passed, no omnisharded tables. Exit: DDL synced, the
/// shard activated fleet-wide. Failure before activation: propagates,
/// nothing to unwind beyond the guards. No publication, copy,
/// replication, or write pause is involved.
pub(super) async fn run(
    task: &AddShardTask,
    ctx: &AsyncTaskContext<AddShardTask>,
    token: &CancellationToken,
    preflight: &Preflight,
) -> Result<(), MigrationError> {
    let orchestrator =
        Orchestrator::for_provisioning(&task.database, preflight.destination().clone(), "", 0)?
            .schema_only();

    let orchestrator = provision::sync_schema_pre(task, ctx, orchestrator).await?;
    let _ = provision::sync_schema_post(task, ctx, orchestrator).await?;

    loop {
        if !task.auto_cutover {
            ctx.set_status(AddShardStatus::AwaitingCutover);
            let cutover =
                cutover_registry::register_cutover(ctx.root_id(), &task.database, Some(task.shard));
            select! {
                _ = token.cancelled() => return Ok(()),
                _ = cutover.requested() => {}
            }
        } else if token.is_cancelled() {
            return Ok(());
        }

        match activate(task, ctx, preflight).await? {
            CutoverOutcome::Done => return Ok(()),
            CutoverOutcome::Aborted => continue,
        }
    }
}

/// The schema-only cutover: check the fleet, swap, finalize. There is
/// no barrier to arm and nothing to drain; peers learn about the
/// activation through the state row.
async fn activate(
    task: &AddShardTask,
    ctx: &AsyncTaskContext<AddShardTask>,
    preflight: &Preflight,
) -> Result<CutoverOutcome, MigrationError> {
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
                return cutover::repark_or_fail(task, Error::InstancesNotRegistered(missing));
            }
            Discovery::Ready(coordination) => Some(*coordination),
        };

    if let Some(coordination) = &coordination {
        fleet::protocol::ensure_tables(coordination.medium())
            .await
            .map_err(Error::from)?;
    }

    ctx.set_status(AddShardStatus::SwappingTopology);
    let new_config = activate_provisioning_shard(&task.database, task.shard)
        .await
        .map_err(Error::from)?;

    cutover::finalize(task, &new_config, coordination.as_ref()).await;

    Ok(CutoverOutcome::Done)
}
