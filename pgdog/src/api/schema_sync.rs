//! Schema-sync background task (pre-data, post-data, or cutover).

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::OnceCell;

use futures::future::try_join_all;
use pgdog_stats::{
    Databases, SchemaShardDefinition, SchemaShardStatus, SchemaStatementFailure,
    SchemaSyncDefinition, SchemaSyncStatus, TaskDefinition,
};
use tracing::{info, warn};

use crate::api::Task;
use crate::api::task::TaskContext;
use crate::backend::replication::logical::schema_sync::{
    SchemaSync, ShardRestore, StatementOutcome,
};
use crate::backend::schema::sync::SchemaSyncError;
use crate::backend::schema::sync::pg_dump::{Collapsed, PgDumpOutput, Statement, SyncState};

/// The source dump, taken once and shared by every phase of one migration.
pub(crate) type SchemaDump = Arc<OnceCell<Arc<PgDumpOutput>>>;

#[derive(Clone, Copy, PartialEq, Eq, Debug, Display, FromStr)]
pub(crate) enum SchemaSyncPhase {
    #[display("pre")]
    Pre,
    #[display("post")]
    Post,
    #[display("cutover")]
    Cutover,
}

impl From<SchemaSyncPhase> for SyncState {
    fn from(phase: SchemaSyncPhase) -> Self {
        match phase {
            SchemaSyncPhase::Pre => SyncState::PreData,
            SchemaSyncPhase::Post => SyncState::PostData,
            SchemaSyncPhase::Cutover => SyncState::Cutover,
        }
    }
}

/// Sync one phase of a dump to the destination. Clone the builder before
/// setting the phase to give every phase the same dump.
#[derive(Debug, bon::Builder)]
#[builder(derive(Clone, Debug))]
pub(crate) struct SchemaSyncTask {
    #[builder(field)]
    dump: SchemaDump,
    databases: Databases,
    publication: String,
    phase: SchemaSyncPhase,
    #[builder(default)]
    ignore_errors: bool,
    #[builder(default)]
    dry_run: bool,
}

impl Task for SchemaSyncTask {
    type Status = SchemaSyncStatus;
    type Output = ();
    type Error = SchemaSyncError;

    fn cancel_timeout() -> Duration {
        Duration::from_secs(60)
    }

    fn definition(&self) -> impl Into<TaskDefinition> {
        SchemaSyncDefinition {
            databases: self.databases.clone(),
            sync_state: self.phase.into(),
            ignore_errors: self.ignore_errors,
            dry_run: self.dry_run,
        }
    }

    #[allow(clippy::print_stdout)]
    async fn run(self, ctx: TaskContext<Self>) -> Result<(), SchemaSyncError> {
        let cancel = ctx.cancellation_token();

        // Pools reload between phases, so resolve late.
        let mut schema_sync = SchemaSync::new(
            &self.databases.source,
            &self.databases.destination,
            &self.publication,
        )?;

        ctx.set_status(SchemaSyncStatus::LoadingSchema);
        let Some(dump) = cancel
            .run_until_cancelled(self.dump.get_or_try_init(|| schema_sync.dump()))
            .await
        else {
            return Err(SchemaSyncError::Aborted);
        };
        let dump = dump?.clone();
        let state = SyncState::from(self.phase);
        let statements = dump.statements(state)?;

        if self.dry_run {
            for statement in statements {
                println!("{}", statement.sql);
            }
            return Ok(());
        }

        let statements = Arc::new(statements);

        ctx.set_status(SchemaSyncStatus::ApplyingStatements {
            statements: statements.clone(),
        });

        let mut restores = Vec::with_capacity(schema_sync.shards());
        for shard in 0..schema_sync.shards() {
            if cancel.is_cancelled() {
                return Err(SchemaSyncError::Aborted);
            }
            restores.push(schema_sync.shard(shard).await?);
        }

        let shards = restores.into_iter().enumerate().map(|(shard, restore)| {
            ctx.run(SchemaShardTask {
                databases: self.databases.clone(),
                shard,
                state,
                statements: &statements,
                ignore_errors: self.ignore_errors,
                restore,
            })
        });

        try_join_all(shards).await?;

        if state == SyncState::PreData {
            if cancel.is_cancelled() {
                return Err(SchemaSyncError::Aborted);
            }
            schema_sync.reload_destination().await?;
        }

        Ok(())
    }
}

/// Applies one phase of a dump to one destination shard.
#[derive(Debug)]
struct SchemaShardTask<'a> {
    databases: Databases,
    shard: usize,
    state: SyncState,
    statements: &'a [Statement],
    ignore_errors: bool,
    restore: ShardRestore,
}

impl Task for SchemaShardTask<'_> {
    type Status = SchemaShardStatus;
    type Output = ();
    type Error = SchemaSyncError;

    fn definition(&self) -> impl Into<TaskDefinition> {
        SchemaShardDefinition {
            shard: self.shard as u64,
            databases: self.databases.clone(),
            sync_state: self.state,
        }
    }

    async fn run(self, ctx: TaskContext<Self>) -> Result<(), SchemaSyncError> {
        let cancel = ctx.cancellation_token();
        if cancel.is_cancelled() {
            return Err(SchemaSyncError::Aborted);
        }

        let total = self.statements.len();
        let mut status = SchemaShardStatus::new(self.shard as u64, total as u64);
        let mut restore = self.restore;

        ctx.set_status(status.clone());

        for (index, statement) in self.statements.iter().enumerate() {
            if cancel.is_cancelled() {
                warn!("stop requested, {} statements not applied", total - index);
                return Err(SchemaSyncError::Aborted);
            }

            info!(
                "[{}/{}] executing \"{}\"",
                index + 1,
                total,
                Collapsed(&statement.sql)
            );

            match restore.execute(statement, self.ignore_errors).await? {
                StatementOutcome::Applied => status.applied += 1,
                StatementOutcome::Skipped => status.skipped += 1,
                StatementOutcome::Failed(message) => status.failures.push(SchemaStatementFailure {
                    index: index as u64,
                    message,
                }),
            }

            ctx.set_status(status.clone());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgdog_stats::{SchemaStatementFailure, SchemaSyncStatement};

    #[test]
    fn schema_sync_status_renders_distinct_labels() {
        let labels = [
            SchemaSyncStatus::LoadingSchema,
            SchemaSyncStatus::ApplyingStatements {
                statements: Arc::default(),
            },
            SchemaSyncStatus::Other,
        ]
        .map(|status| status.to_string());

        assert!(labels.iter().all(|label| !label.is_empty()));
        let unique: std::collections::HashSet<_> = labels.iter().collect();
        assert_eq!(unique.len(), labels.len());
    }

    #[test]
    fn schema_sync_status_reports_the_plan() {
        let status = SchemaSyncStatus::ApplyingStatements {
            statements: Arc::new(vec![
                SchemaSyncStatement::new("CREATE TABLE a ()"),
                SchemaSyncStatement::new("CREATE TYPE t AS ENUM ('a')").set_skip_if_exists(),
            ]),
        };

        assert_eq!(status.to_string(), "applying 2 statements");
    }

    #[test]
    fn schema_shard_status_reports_its_cursor() {
        let failure = SchemaStatementFailure {
            index: 2,
            message: "relation \"users\" does not exist".into(),
        };
        let status = SchemaShardStatus {
            applied: 1,
            skipped: 1,
            failures: vec![failure.clone()],
            ..SchemaShardStatus::new(1, 6)
        };

        assert_eq!(status.done(), 3);
        assert_eq!(status.failures, vec![failure]);
        assert_eq!(
            status.to_string(),
            "shard 1: 3/6 statements, 1 skipped, 1 failed"
        );
    }

    #[test]
    fn schema_sync_phase_parses_and_displays() {
        for (text, phase) in [
            ("pre", SchemaSyncPhase::Pre),
            ("post", SchemaSyncPhase::Post),
            ("cutover", SchemaSyncPhase::Cutover),
        ] {
            assert_eq!(text.parse::<SchemaSyncPhase>().unwrap(), phase);
            assert_eq!(phase.to_string(), text);
        }
        // Parsing is case-insensitive; unknown phases are rejected.
        assert_eq!(
            "CUTOVER".parse::<SchemaSyncPhase>().unwrap(),
            SchemaSyncPhase::Cutover
        );
        assert!("bogus".parse::<SchemaSyncPhase>().is_err());
    }

    /// The CLI phase selects the stage the definition reports, and the row
    /// spells it the way `SHOW SCHEMA_SYNC` does. A swapped arm in
    /// `From<SchemaSyncPhase>` shows up here.
    #[test]
    fn schema_sync_definition_reports_its_phase() {
        for (phase, expected) in [
            (SchemaSyncPhase::Pre, "pre_data"),
            (SchemaSyncPhase::Post, "post_data"),
            (SchemaSyncPhase::Cutover, "cutover"),
        ] {
            let definition = TaskDefinition::from(SchemaSyncDefinition {
                databases: Databases {
                    source: "prod".into(),
                    destination: "prod_sharded".into(),
                },
                sync_state: phase.into(),
                ignore_errors: false,
                dry_run: false,
            });

            assert_eq!(definition.name, "schema_sync");
            assert_eq!(
                definition.to_string(),
                format!("schema_sync({expected}) prod -> prod_sharded")
            );
        }
    }

    /// Builder clones share one dump; separate builders do not. Replacing
    /// `#[builder(field)]` with `skip` or `default` breaks this.
    #[test]
    fn builder_clones_share_one_dump() {
        let builder = SchemaSyncTask::builder()
            .databases(Databases {
                source: "prod".into(),
                destination: "prod_sharded".into(),
            })
            .publication("pub".to_owned());

        let pre = builder.clone().phase(SchemaSyncPhase::Pre).build();
        let post = builder.phase(SchemaSyncPhase::Post).build();

        assert!(Arc::ptr_eq(&pre.dump, &post.dump));

        let other = SchemaSyncTask::builder()
            .databases(Databases {
                source: "prod".into(),
                destination: "prod_sharded".into(),
            })
            .publication("pub".to_owned())
            .phase(SchemaSyncPhase::Pre)
            .build();

        assert!(!Arc::ptr_eq(&pre.dump, &other.dump));
    }

    #[test]
    fn cutover_task_shares_the_dump_of_the_earlier_phases() {
        let builder = SchemaSyncTask::builder()
            .databases(Databases {
                source: "prod".into(),
                destination: "prod_sharded".into(),
            })
            .publication("pub".to_owned())
            .ignore_errors(true);

        let pre = builder.clone().phase(SchemaSyncPhase::Pre).build();
        let cutover = builder.phase(SchemaSyncPhase::Cutover).build();

        assert!(Arc::ptr_eq(&pre.dump, &cutover.dump));
        assert_eq!(cutover.phase, SchemaSyncPhase::Cutover);
        assert!(cutover.ignore_errors);
    }
}
