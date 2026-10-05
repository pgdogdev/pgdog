use std::collections::HashMap;
use std::time::Duration;

use crate::api::Task;
use crate::api::replication::ReplicationClusterTask;
use crate::api::task::TaskContext;
use crate::backend::replication::logical::Error;
use crate::backend::replication::logical::publisher::replication_progress::ReplicationProgress;
use crate::backend::replication::logical::resharding_state::ReshardingState;
use crate::util::safe_interval;
use pgdog_stats::{ReplicationDirection, SynchronizeTablesStatus, TaskDefinition};
use tokio::select;
use tracing::info;

const STATUS_REPORT_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug)]
pub(crate) struct SynchronizeTablesTask {
    pub(crate) state: ReshardingState,
}

impl Task for SynchronizeTablesTask {
    type Status = SynchronizeTablesStatus;
    type Output = ();
    type Error = Error;

    fn definition(&self) -> impl Into<TaskDefinition> {
        "synchronize tables"
    }

    async fn run(self, ctx: TaskContext<Self>) -> Result<(), Error> {
        ctx.set_status(SynchronizeTablesStatus::InitializingReplicationStreams);
        let mut state = self.state;
        state.reload()?;
        let mut tables = state.tables();

        // collect the max lsn for the tables grouping by shard
        let mut targets = tables
            .iter()
            .filter_map(|(shard, tables)| {
                tables
                    .iter()
                    .map(|table| table.lsn)
                    .max()
                    .map(|lsn| (*shard, lsn))
            })
            .collect::<HashMap<_, _>>();

        if targets.is_empty() {
            return Ok(());
        }

        let progress = ReplicationProgress::new(state.source.shards().len());

        // start the replication to make sure all the tables reach the same lsn on the shards
        let (task, stop) = ReplicationClusterTask::new(
            state.clone(),
            ReplicationDirection::Forward,
            progress.clone(),
        );
        let mut replication = Box::pin(ctx.run(task));
        let mut check = safe_interval(STATUS_REPORT_INTERVAL);

        loop {
            select! {
                result = &mut replication => {
                    result?;
                    return Err(Error::ReplicationStreamStopped);
                }
                _ = check.tick() => {
                    ctx.set_status(SynchronizeTablesStatus::SynchronizingTables {
                        progress: progress.snapshot(),
                    });

                    targets.retain(|shard, target| {
                        let reached = progress
                            .applied_lsn(*shard)
                            .is_some_and(|applied| applied >= *target);
                        if reached {
                            info!("source shard {shard} synchronized at {target}");
                        }
                        !reached
                    });

                    if targets.is_empty() {
                        stop.stop(None);
                        break;
                    }
                }
            }
        }

        replication.await?;

        // update the tables lsn - now they should be the same on the tables
        // on source shard.
        for (shard, tables) in &mut tables {
            if let Some(applied) = progress.applied_lsn(*shard) {
                for table in tables {
                    table.lsn = table.lsn.max(applied);
                }
            }
        }
        state.set_tables(tables);

        Ok(())
    }
}
