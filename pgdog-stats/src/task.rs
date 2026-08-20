//! Task identity, status and definition reports.

use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use indexmap::IndexMap;

use derive_more::{Display, Error, From, FromStr};
use pgdog_postgres_types::ToDataRowColumn;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_with::{TimestampMilliSeconds, serde_as, skip_serializing_none};

use crate::{Lsn, SyncState};

/// Identity of a task in the registry. Ids are unique per registry.
#[derive(
    Copy,
    Clone,
    Debug,
    Display,
    FromStr,
    Hash,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    JsonSchema,
)]
#[serde(transparent)]
pub struct TaskId(u64);

impl TaskId {
    pub const fn new(id: u64) -> Self {
        Self(id)
    }
}

impl ToDataRowColumn for TaskId {
    fn to_data_row_column(&self) -> pgdog_postgres_types::Data {
        self.0.to_data_row_column()
    }
}

/// The umbrella type for the well-known and generic types of statuses
#[derive(Debug, Clone, Default, PartialEq, Display, From, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TaskStatus {
    /// generic variants for any task
    RatioProgress(RatioProgress),
    /// specific tasks for known tasks used for enterprise
    Reshard(ReshardStatus),
    CopyData(CopyDataStatus),
    SchemaSync(SchemaSyncStatus),
    SchemaShard(SchemaShardStatus),
    TableCopy(TableCopyStatus),
    Replication(ReplicationStatus),
    ReplicationSlot(ReplicationSlotStatus),
    /// Any other task status that is either doesn't report any status
    /// or is not compatible with other versions of tasks.
    #[default]
    #[display("-")]
    #[serde(other)]
    Other,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Display, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum TaskProgress {
    #[display("started")]
    Started,
    #[display("running")]
    Running,
    #[display("finished")]
    Finished,
    /// Cancellation has been requested; the task is winding down
    /// cooperatively and has not yet reached a terminal state.
    #[display("cancelling")]
    Cancelling,
    #[display("cancelled")]
    Cancelled,
    #[display("failed: {message}")]
    Error { message: String },
    #[display("panicked: {message}")]
    Panic { message: String },
    /// Fallback value for the communication protocol,
    /// to cover versions mismatches
    #[default]
    #[display("unknown")]
    #[serde(other)]
    Unknown,
}

impl TaskProgress {
    pub fn error(message: impl Into<String>) -> Self {
        Self::Error {
            message: message.into(),
        }
    }

    pub fn panic(message: impl Into<String>) -> Self {
        Self::Panic {
            message: message.into(),
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Finished | Self::Cancelled | Self::Error { .. } | Self::Panic { .. }
        )
    }

    pub fn is_error(&self) -> bool {
        matches!(self, Self::Error { .. } | Self::Panic { .. })
    }
}

/// Tasks representation used by EE
#[serde_as]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct TaskEntry {
    pub id: TaskId,
    #[serde_as(as = "TimestampMilliSeconds<i64>")]
    pub started_at: SystemTime,
    #[serde_as(as = "TimestampMilliSeconds<i64>")]
    pub updated_at: SystemTime,
    pub progress: TaskProgress,
    pub status: TaskStatus,
    pub definition: Arc<TaskDefinition>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub subtasks: IndexMap<TaskId, TaskEntry>,
}

/// Updates for the tasks that could omit some fields from [`TaskEntry`]
/// and omit unchanged subtasks. A child list is partial: only changed
/// are present.
#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskUpdate {
    pub id: TaskId,
    #[serde_as(as = "TimestampMilliSeconds<i64>")]
    pub started_at: SystemTime,
    #[serde_as(as = "TimestampMilliSeconds<i64>")]
    pub updated_at: SystemTime,
    /// `None` when the task did not change since the receiver's cursor.
    pub progress: Option<TaskProgress>,
    /// `None` when the task did not change since the receiver's cursor.
    pub status: Option<TaskStatus>,
    /// `None` once the receiver holds it: it never changes.
    pub definition: Option<Arc<TaskDefinition>>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub subtasks: IndexMap<TaskId, TaskUpdate>,
}

/// Error for incomplete update - some of the required fields are missing
#[derive(Debug, Clone, PartialEq, Display, Error)]
#[display("update for task {id} omits its {field}")]
pub struct IncompleteUpdate {
    pub id: TaskId,
    pub field: &'static str,
}

/// Create the new [`TaskEntry`] from [`TaskUpdate`].
///
/// # Errors
///
/// If some required fields are missing the error is generated that should
/// requests the whole tree from the instance again.
impl TryFrom<TaskUpdate> for TaskEntry {
    type Error = IncompleteUpdate;

    fn try_from(update: TaskUpdate) -> Result<Self, IncompleteUpdate> {
        let TaskUpdate {
            id,
            started_at,
            updated_at,
            progress,
            status,
            definition,
            subtasks,
        } = update;

        let missing = |field| IncompleteUpdate { id, field };

        let subtasks = subtasks
            .into_values()
            .map(|child| Self::try_from(child).map(|entry| (entry.id, entry)))
            .collect::<Result<IndexMap<TaskId, Self>, IncompleteUpdate>>()?;

        Ok(Self {
            id,
            started_at,
            updated_at,
            progress: progress.ok_or_else(|| missing("progress"))?,
            status: status.ok_or_else(|| missing("status"))?,
            definition: definition.ok_or_else(|| missing("definition"))?,
            subtasks,
        })
    }
}

impl TaskEntry {
    /// Newest `updated_at` in this subtree, this task included.
    pub fn subtree_updated_at(&self) -> SystemTime {
        self.subtasks
            .values()
            .map(Self::subtree_updated_at)
            .fold(self.updated_at, SystemTime::max)
    }

    /// Whether this task reached a terminal state more than `ttl` ago. Only
    /// this task is read: a terminal task implies terminal children.
    pub fn expired(&self, now: SystemTime, ttl: Duration) -> bool {
        self.progress.is_terminal()
            && now
                .duration_since(self.updated_at)
                .is_ok_and(|age| age >= ttl)
    }

    /// Apply an update to current TaskEntry. The present fields will overwrite existing
    /// fields, otherwise the current value will be used.
    ///
    /// # Errors
    ///
    /// In case some required fields are missing (when the entry was not seen before) the
    /// error is raised.
    pub fn update(&mut self, update: TaskUpdate) -> Result<(), IncompleteUpdate> {
        if update.updated_at < self.updated_at {
            return Ok(());
        }

        self.updated_at = update.updated_at;
        if let Some(progress) = update.progress {
            self.progress = progress;
        }
        if let Some(status) = update.status {
            self.status = status;
        }
        if let Some(definition) = update.definition {
            self.definition = definition;
        }

        for (id, child) in update.subtasks {
            match self.subtasks.get_mut(&id) {
                Some(entry) => entry.update(child)?,
                None => {
                    self.subtasks.insert(id, Self::try_from(child)?);
                }
            }
        }

        Ok(())
    }
}

/// The definition of the task - initial options of the task
/// that helps to identify it
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct TaskDefinition {
    /// The group a payload-backed task belongs to — `reshard`, `table_copy`..
    pub name: Cow<'static, str>,
    #[serde(flatten)]
    pub kind: TaskDefinitionKind,
}

impl std::fmt::Display for TaskDefinition {
    /// The whole definition, detail and all, for the places that render a
    /// task as one line of text: the logs and `SHOW TASKS`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            TaskDefinitionKind::Other => write!(f, "{}", self.name),
            kind => write!(f, "{kind}"),
        }
    }
}

impl TaskDefinition {
    /// A name-only definition.
    pub fn named(name: impl Into<Cow<'static, str>>) -> Self {
        Self {
            name: name.into(),
            kind: TaskDefinitionKind::Other,
        }
    }
}

impl From<&'static str> for TaskDefinition {
    fn from(value: &'static str) -> Self {
        TaskDefinition::named(value)
    }
}

impl<T> From<T> for TaskDefinition
where
    T: Into<TaskDefinitionKind>,
{
    fn from(value: T) -> Self {
        let kind: TaskDefinitionKind = value.into();

        TaskDefinition {
            name: Cow::Borrowed(kind.kind()),
            kind,
        }
    }
}

/// The umbrella type for task definition kind
#[derive(Debug, Clone, Default, PartialEq, Display, From, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TaskDefinitionKind {
    Reshard(ReshardDefinition),
    CopyData(CopyDataDefinition),
    SchemaSync(SchemaSyncDefinition),
    Replication(ReplicationDefinition),
    TableCopy(TableCopyDefinition),
    ReplicationSlot(ReplicationSlotDefinition),
    SchemaShard(SchemaShardDefinition),
    /// No detail beyond the name, or a `kind` this build does not know.
    #[default]
    #[display("-")]
    #[serde(other)]
    Other,
}

impl TaskDefinitionKind {
    /// The wire `kind` tag.
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Reshard(_) => "reshard",
            Self::CopyData(_) => "copy_data",
            Self::SchemaSync(_) => "schema_sync",
            Self::Replication(_) => "replication",
            Self::TableCopy(_) => "table_copy",
            Self::ReplicationSlot(_) => "replication_slot",
            Self::SchemaShard(_) => "schema_shard",
            Self::Other => "other",
        }
    }
}

/// The two ends of a migration.
#[derive(Debug, Clone, PartialEq, Display, Serialize, Deserialize, JsonSchema)]
#[display("{source} -> {destination}")]
pub struct Databases {
    pub source: String,
    pub destination: String,
}

/// The full migration one reshard task runs, and which phases it was asked
/// to skip.
#[derive(Debug, Clone, PartialEq, Display, Serialize, Deserialize, JsonSchema)]
#[display("reshard {databases}")]
pub struct ReshardDefinition {
    pub databases: Databases,
    pub skip_schema_sync: bool,
    pub replicate_only: bool,
    pub sync_only: bool,
    pub auto_cutover: bool,
}

/// The bulk data copy one copy-data task runs.
#[derive(Debug, Clone, PartialEq, Display, Serialize, Deserialize, JsonSchema)]
#[display("copy_data {databases}")]
pub struct CopyDataDefinition {
    pub databases: Databases,
}

/// The schema sync one schema-sync task runs, and at which stage.
#[derive(Debug, Clone, PartialEq, Display, Serialize, Deserialize, JsonSchema)]
#[display("schema_sync({sync_state}) {databases}")]
pub struct SchemaSyncDefinition {
    pub databases: Databases,
    pub sync_state: SyncState,
    pub ignore_errors: bool,
    pub dry_run: bool,
}

/// The replication stream one replication task drives.
#[derive(Debug, Clone, PartialEq, Display, Serialize, Deserialize, JsonSchema)]
#[display("replication {databases}{}", if *reverse { " (reverse)" } else { "" })]
pub struct ReplicationDefinition {
    pub databases: Databases,
    /// The post-cutover stream that backs a rollback, rather than the
    /// initial migration.
    pub reverse: bool,
    pub auto_cutover: bool,
}

/// Generic "`done` of `total`" counter, reusable by any task that can count its
/// work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display, Serialize, Deserialize, JsonSchema)]
#[display("{done} of {total}")]
pub struct RatioProgress {
    pub done: u64,
    pub total: u64,
}

/// Stages of the migration, reported as the task's status. The fine-grained
/// schema-sync, copy, and replication stages live on the child tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ReshardStatus {
    /// Running the pre-data schema-sync child task.
    #[display("syncing schema")]
    SchemaSync,
    /// Running the data-copy child task.
    #[display("syncing data")]
    SyncingData,
    /// Running the post-data schema-sync child task (indexes, constraints).
    #[display("finalizing schema")]
    FinalizingSchema,
    /// Running the replication child task.
    #[display("replicating")]
    Replication,
    /// A stage this build does not know.
    #[display("-")]
    #[serde(other)]
    Other,
}

/// Stages of a bulk data copy, reported as the task's status. Per-table
/// progress lives on the [`TableCopyStatus`] child tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CopyDataStatus {
    /// Fetching table and column metadata from the source.
    #[display("loading table metadata")]
    LoadingTableMetadata,
    /// Checking that every table has a usable replica identity.
    #[display("validating tables")]
    ValidatingTables,
    /// Creating the replication slots the copy reads from.
    #[display("creating slots")]
    CreatingSlots,
    /// Copying table data to the destination shards.
    #[display("copying tables")]
    CopyingTables,
    /// A stage this build does not know.
    #[display("-")]
    #[serde(other)]
    Other,
}

/// One statement of a schema sync phase.
#[derive(Debug, Clone, PartialEq, Eq, Display, Serialize, Deserialize, JsonSchema)]
#[display("{sql}")]
pub struct SchemaSyncStatement {
    pub sql: String,
    /// The statement tolerates an "already exists" error from Postgres.
    pub skip_if_exists: bool,
}

impl SchemaSyncStatement {
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            skip_if_exists: false,
        }
    }

    pub fn set_skip_if_exists(mut self) -> Self {
        self.skip_if_exists = true;
        self
    }
}

/// Status of a schema sync. The phase it applies lives on
/// [`SchemaSyncDefinition`]. The plan is reported once, by the parent task.
/// Each shard subtask reports a cursor into it as a [`SchemaShardStatus`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Display, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum SchemaSyncStatus {
    /// Dumping the schema from the source.
    #[default]
    #[display("loading schema")]
    LoadingSchema,
    /// The dump is loaded and the phase's statements are known.
    #[display("applying {} statements", statements.len())]
    ApplyingStatements {
        #[serde(default)]
        statements: Arc<Vec<SchemaSyncStatement>>,
    },
    /// A status this build does not know.
    #[display("-")]
    #[serde(other)]
    Other,
}

/// A statement one shard could not apply. `index` points into the plan the
/// parent task reported, and `message` is the error Postgres returned. The
/// display is one-based, to match the statement counter in the logs.
#[derive(Debug, Clone, PartialEq, Eq, Display, Serialize, Deserialize, JsonSchema)]
#[display("statement {}: {message}", index + 1)]
pub struct SchemaStatementFailure {
    pub index: u64,
    pub message: String,
}

/// How far one destination shard got through the phase's statements. `applied`
/// counts the statements this shard ran, and `failures` records the ones it
/// could not.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct SchemaShardStatus {
    pub shard: u64,
    pub total: u64,
    pub applied: u64,
    pub skipped: u64,
    pub failures: Vec<SchemaStatementFailure>,
}

impl SchemaShardStatus {
    pub fn new(shard: u64, total: u64) -> Self {
        Self {
            shard,
            total,
            ..Default::default()
        }
    }

    pub fn done(&self) -> u64 {
        self.applied + self.skipped + self.failures.len() as u64
    }
}

impl fmt::Display for SchemaShardStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "shard {}: {}/{} statements",
            self.shard,
            self.done(),
            self.total
        )?;

        if self.skipped > 0 {
            write!(f, ", {} skipped", self.skipped)?;
        }
        if !self.failures.is_empty() {
            write!(f, ", {} failed", self.failures.len())?;
        }

        Ok(())
    }
}

/// Stages of logical replication, reported as the task's status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ReplicationStatus {
    /// Streaming changes to catch the destination up.
    #[display("replicating")]
    Replicating,
    /// Cutting traffic over to the destination.
    #[display("cutting over")]
    CuttingOver,
    /// Cutting traffic back to the original after a prior cutover (rollback).
    #[display("rolling back")]
    RollingBack,
    /// Winding down on a stop request.
    #[display("stopping")]
    Stopping,
    /// A stage this build does not know.
    #[display("-")]
    #[serde(other)]
    Other,
}

/// The slot one per-shard replication subtask streams from.
#[derive(Debug, Clone, PartialEq, Display, Serialize, Deserialize, JsonSchema)]
#[display("{slot} on {host}:{port}/{database_name}")]
pub struct ReplicationSlotDefinition {
    pub slot: String,
    pub host: String,
    pub port: u16,
    pub database_name: String,
    /// Temporary slot taken for an initial data copy, rather than a persistent
    /// streaming slot.
    pub copy_data: bool,
}

/// How far one replication slot has streamed.
#[derive(Debug, Clone, Copy, PartialEq, Display, Serialize, Deserialize, JsonSchema)]
#[display("lag {lag_bytes} bytes at {lsn}")]
pub struct ReplicationSlotStatus {
    pub lsn: Lsn,
    /// `pg_current_wal_lsn() - confirmed_flush_lsn`.
    pub lag_bytes: i64,
    /// Epoch millis of the last transaction applied through this slot.
    pub last_transaction: Option<i64>,
}

/// The table one copy subtask is copying.
#[derive(Debug, Clone, PartialEq, Display, Serialize, Deserialize, JsonSchema)]
#[display("{schema}.{table}")]
pub struct TableCopyDefinition {
    pub schema: String,
    pub table: String,
    pub sql: String,
}

/// How much of one table has been copied.
#[derive(Debug, Clone, Copy, PartialEq, Display, Serialize, Deserialize, JsonSchema)]
#[display("{rows} rows, {bytes} bytes, {bytes_per_sec} bytes/s")]
pub struct TableCopyStatus {
    pub rows: u64,
    pub bytes: u64,
    pub bytes_per_sec: u64,
}

/// The destination shard one schema-sync subtask restores into.
#[derive(Debug, Clone, PartialEq, Display, Serialize, Deserialize, JsonSchema)]
#[display("shard {shard} of {databases} ({sync_state})")]
pub struct SchemaShardDefinition {
    pub shard: u64,
    pub databases: Databases,
    pub sync_state: SyncState,
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};

    fn table_copy() -> TableCopyDefinition {
        TableCopyDefinition {
            schema: "public".into(),
            table: "users".into(),
            sql: "COPY ...".into(),
        }
    }

    fn databases() -> Databases {
        Databases {
            source: "prod".into(),
            destination: "prod_sharded".into(),
        }
    }

    /// One definition per kind. The exhaustive `match` in
    /// [`test_definition_round_trip`] forces a new kind into this list.
    fn definitions() -> [TaskDefinition; 8] {
        [
            "test task".into(),
            ReshardDefinition {
                databases: databases(),
                skip_schema_sync: false,
                replicate_only: false,
                sync_only: false,
                auto_cutover: true,
            }
            .into(),
            CopyDataDefinition {
                databases: databases(),
            }
            .into(),
            SchemaSyncDefinition {
                databases: databases(),
                sync_state: SyncState::PreData,
                ignore_errors: false,
                dry_run: false,
            }
            .into(),
            ReplicationDefinition {
                databases: databases(),
                reverse: true,
                auto_cutover: false,
            }
            .into(),
            table_copy().into(),
            ReplicationSlotDefinition {
                slot: "pgdog_0".into(),
                host: "127.0.0.1".into(),
                port: 5432,
                database_name: "prod".into(),
                copy_data: false,
            }
            .into(),
            SchemaShardDefinition {
                shard: 1,
                databases: Databases {
                    source: "old".into(),
                    destination: "new".into(),
                },
                sync_state: SyncState::PostData,
            }
            .into(),
        ]
    }

    #[test]
    fn test_definition_round_trip() {
        for definition in definitions() {
            let json = serde_json::to_string(&definition).unwrap();
            let back: TaskDefinition = serde_json::from_str(&json).unwrap();
            assert_eq!(definition, back, "{json}");
            assert!(!back.name.is_empty(), "{json}");
            assert_eq!(
                serde_json::to_value(&back).unwrap()["kind"],
                back.kind.kind(),
                "{json}"
            );

            match back.kind {
                TaskDefinitionKind::Reshard(_)
                | TaskDefinitionKind::CopyData(_)
                | TaskDefinitionKind::SchemaSync(_)
                | TaskDefinitionKind::Replication(_)
                | TaskDefinitionKind::TableCopy(_)
                | TaskDefinitionKind::ReplicationSlot(_)
                | TaskDefinitionKind::SchemaShard(_)
                | TaskDefinitionKind::Other => (),
            }
        }
    }

    /// `name` groups tasks; the rendered definition is the whole thing, which
    /// is what the logs and the `SHOW TASKS` row carry.
    #[test]
    fn test_definition_display() {
        assert_eq!(TaskDefinition::from("test task").to_string(), "test task");

        assert_eq!(
            TaskDefinition::from(ReshardDefinition {
                databases: databases(),
                skip_schema_sync: false,
                replicate_only: false,
                sync_only: false,
                auto_cutover: true,
            })
            .to_string(),
            "reshard prod -> prod_sharded"
        );

        assert_eq!(
            TaskDefinition::from(CopyDataDefinition {
                databases: databases()
            })
            .to_string(),
            "copy_data prod -> prod_sharded"
        );

        for (sync_state, expected) in [
            (
                SyncState::PreData,
                "schema_sync(pre_data) prod -> prod_sharded",
            ),
            (
                SyncState::PostData,
                "schema_sync(post_data) prod -> prod_sharded",
            ),
            (
                SyncState::Cutover,
                "schema_sync(cutover) prod -> prod_sharded",
            ),
        ] {
            assert_eq!(
                TaskDefinition::from(SchemaSyncDefinition {
                    databases: databases(),
                    sync_state,
                    ignore_errors: false,
                    dry_run: false,
                })
                .to_string(),
                expected
            );
        }

        // Only the reverse stream is marked; the forward one reads plainly.
        for (reverse, expected) in [
            (false, "replication prod -> prod_sharded"),
            (true, "replication prod -> prod_sharded (reverse)"),
        ] {
            assert_eq!(
                TaskDefinition::from(ReplicationDefinition {
                    databases: databases(),
                    reverse,
                    auto_cutover: false,
                })
                .to_string(),
                expected
            );
        }
    }

    /// `SHOW TASKS` renders the definition, so every kind has to produce
    /// something better than its bare group name.
    #[test]
    fn test_every_kind_renders_its_detail() {
        for definition in definitions() {
            let rendered = definition.to_string();
            assert!(!rendered.is_empty(), "{:?}", definition.kind);

            match definition.kind {
                // A bare name has no detail: it renders as itself.
                TaskDefinitionKind::Other => assert_eq!(rendered, definition.name),
                ref kind => {
                    assert_eq!(definition.name, kind.kind(), "name is the group");
                    assert_ne!(rendered, definition.name, "{rendered} lost its detail");
                }
            }
        }
    }

    /// A payload-derived name is the kind's wire tag, which is always a
    /// space-free identifier.
    #[test]
    fn test_names_are_identifiers() {
        for definition in definitions() {
            // A caller-supplied bare name is free-form.
            if matches!(definition.kind, TaskDefinitionKind::Other) {
                continue;
            }

            assert_eq!(definition.name, definition.kind.kind());
            assert!(
                !definition.name.contains(' '),
                "{} is not an identifier",
                definition.name
            );
        }
    }

    /// Variants are spelled out, not built through `From`, so a miswired `From`
    /// disagrees with its own tag here. The `match` forces a new one into the list.
    #[test]
    fn test_status_round_trip() {
        let statuses = [
            TaskStatus::RatioProgress(RatioProgress { done: 3, total: 12 }),
            TaskStatus::Reshard(ReshardStatus::SyncingData),
            TaskStatus::CopyData(CopyDataStatus::CopyingTables),
            TaskStatus::SchemaSync(SchemaSyncStatus::ApplyingStatements {
                statements: Arc::new(vec![
                    SchemaSyncStatement::new("CREATE INDEX ...").set_skip_if_exists(),
                ]),
            }),
            TaskStatus::SchemaShard(SchemaShardStatus {
                shard: 1,
                total: 4,
                applied: 2,
                skipped: 1,
                failures: vec![SchemaStatementFailure {
                    index: 3,
                    message: "relation \"users\" does not exist".into(),
                }],
            }),
            TaskStatus::TableCopy(TableCopyStatus {
                rows: 10,
                bytes: 2048,
                bytes_per_sec: 512,
            }),
            TaskStatus::Replication(ReplicationStatus::Replicating),
            TaskStatus::ReplicationSlot(ReplicationSlotStatus {
                lsn: Lsn {
                    high: 0,
                    low: 16,
                    lsn: 16,
                },
                lag_bytes: 4096,
                last_transaction: Some(1_700_000_000_000),
            }),
            TaskStatus::Other,
        ];

        for status in statuses {
            let json = serde_json::to_string(&status).unwrap();
            let back: TaskStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(status, back, "{json}");

            match back {
                TaskStatus::RatioProgress(_)
                | TaskStatus::Reshard(_)
                | TaskStatus::CopyData(_)
                | TaskStatus::SchemaSync(_)
                | TaskStatus::SchemaShard(_)
                | TaskStatus::TableCopy(_)
                | TaskStatus::Replication(_)
                | TaskStatus::ReplicationSlot(_)
                | TaskStatus::Other => (),
            }
        }
    }

    /// A literal name borrows, a decoded one is owned.
    #[test]
    fn test_literal_names_do_not_allocate() {
        let named = TaskDefinition::named("test");
        assert!(matches!(named.name, Cow::Borrowed(_)));
        assert!(matches!(
            TaskDefinition::from(table_copy()).name,
            Cow::Borrowed(_)
        ));

        let json = String::from(r#"{"name":"future task","kind":"other"}"#);
        let decoded: TaskDefinition = serde_json::from_str(&json).unwrap();
        assert!(matches!(decoded.name, Cow::Owned(_)));
    }

    /// A receiver older than its sender degrades to `Other` and keeps whatever it
    /// can still read. Never add `deny_unknown_fields`.
    #[test]
    fn test_unknown_kind_degrades_to_other() {
        assert_eq!(
            serde_json::from_str::<TaskStatus>(r#"{"kind":"quantum","qubits":4}"#).unwrap(),
            TaskStatus::Other
        );
        assert_eq!(
            serde_json::from_str::<TaskDefinition>(
                r#"{"name":"future task","kind":"quantum","qubits":4}"#
            )
            .unwrap(),
            TaskDefinition {
                name: "future task".into(),
                kind: TaskDefinitionKind::Other,
            }
        );

        assert_eq!(
            serde_json::from_str::<Vec<TaskStatus>>(
                r#"[{"kind":"quantum","qubits":4},{"kind":"ratio_progress","done":3,"total":12}]"#
            )
            .unwrap(),
            vec![
                TaskStatus::Other,
                TaskStatus::RatioProgress(RatioProgress { done: 3, total: 12 })
            ]
        );
    }

    #[test]
    fn test_unknown_inner_status_keeps_its_kind() {
        assert_eq!(
            serde_json::from_str::<TaskStatus>(r#"{"kind":"reshard","status":"new_stage"}"#)
                .unwrap(),
            TaskStatus::Reshard(ReshardStatus::Other)
        );
        assert_eq!(
            serde_json::from_str::<TaskStatus>(r#"{"kind":"copy_data","status":"new_stage"}"#)
                .unwrap(),
            TaskStatus::CopyData(CopyDataStatus::Other)
        );
        assert_eq!(
            serde_json::from_str::<TaskStatus>(r#"{"kind":"schema_sync","status":"new_status"}"#)
                .unwrap(),
            TaskStatus::SchemaSync(SchemaSyncStatus::Other)
        );
        assert_eq!(
            serde_json::from_str::<TaskStatus>(r#"{"kind":"schema_shard","total":471}"#).unwrap(),
            TaskStatus::SchemaShard(SchemaShardStatus::new(0, 471))
        );
        assert_eq!(
            serde_json::from_str::<TaskStatus>(r#"{"kind":"replication","status":"new_stage"}"#)
                .unwrap(),
            TaskStatus::Replication(ReplicationStatus::Other)
        );

        assert_eq!(
            serde_json::from_str::<Vec<TaskStatus>>(
                r#"[{"kind":"reshard","status":"new_stage"},{"kind":"reshard","status":"syncing_data"}]"#
            )
            .unwrap(),
            vec![
                TaskStatus::Reshard(ReshardStatus::Other),
                TaskStatus::Reshard(ReshardStatus::SyncingData)
            ]
        );
    }

    /// `Other` is a real tag on the wire, not an absence.
    #[test]
    fn test_other_round_trips() {
        assert_eq!(
            serde_json::to_string(&TaskStatus::Other).unwrap(),
            r#"{"kind":"other"}"#
        );
        assert_eq!(
            serde_json::from_str::<TaskStatus>(r#"{"kind":"other"}"#).unwrap(),
            TaskStatus::Other
        );
    }

    /// The wire carries a bare number, so the control plane can name `TaskId`
    /// where it used to carry a `u64` without changing a byte.
    #[test]
    fn test_task_id_is_transparent_on_the_wire() {
        assert_eq!(serde_json::to_string(&TaskId::new(7)).unwrap(), "7");
        assert_eq!(serde_json::from_str::<TaskId>("7").unwrap(), TaskId::new(7));
    }

    #[test]
    fn test_task_id_parses_renders_and_orders() {
        assert_eq!("7".parse::<TaskId>().unwrap(), TaskId::new(7));
        assert_eq!(TaskId::new(7).to_string(), "7");
        assert!("-1".parse::<TaskId>().is_err());
        assert!(TaskId::new(2) < TaskId::new(10));
    }

    /// A definition renders its name, its kind renders the detail.
    #[test]
    fn test_display() {
        assert_eq!(TaskStatus::Other.to_string(), "-");
        assert_eq!(TaskStatus::default(), TaskStatus::Other);
        assert_eq!(TaskDefinition::from("test task").to_string(), "test task");
        assert_eq!(TaskDefinitionKind::Other.to_string(), "-");
        assert_eq!(
            TaskDefinitionKind::from(table_copy()).to_string(),
            "public.users"
        );
        assert_eq!(
            TaskDefinitionKind::from(ReplicationSlotDefinition {
                slot: "pgdog_0".into(),
                host: "127.0.0.1".into(),
                port: 5432,
                database_name: "prod".into(),
                copy_data: false,
            })
            .to_string(),
            "pgdog_0 on 127.0.0.1:5432/prod"
        );
        assert_eq!(
            TaskStatus::Reshard(ReshardStatus::SyncingData).to_string(),
            "syncing data"
        );
        assert_eq!(
            TaskStatus::CopyData(CopyDataStatus::LoadingTableMetadata).to_string(),
            "loading table metadata"
        );
        assert_eq!(
            TaskStatus::RatioProgress(RatioProgress { done: 3, total: 12 }).to_string(),
            "3 of 12"
        );
        assert_eq!(
            TaskStatus::TableCopy(TableCopyStatus {
                rows: 10,
                bytes: 2048,
                bytes_per_sec: 512,
            })
            .to_string(),
            "10 rows, 2048 bytes, 512 bytes/s"
        );
    }

    fn at(ms: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_millis(ms)
    }

    fn create_task_update(id: u64, updated_at_ms: u64, subtasks: Vec<TaskUpdate>) -> TaskUpdate {
        TaskUpdate {
            id: TaskId::new(id),
            started_at: at(1),
            updated_at: at(updated_at_ms),
            progress: Some(TaskProgress::Running),
            status: Some(TaskStatus::Other),
            definition: Some(Arc::new(TaskDefinition::named("my-task"))),
            subtasks: subtasks.into_iter().map(|node| (node.id, node)).collect(),
        }
    }

    fn shallow_update(mut update: TaskUpdate) -> TaskUpdate {
        update.progress = None;
        update.status = None;
        update.definition = None;
        update
    }

    fn stored(update: TaskUpdate) -> TaskEntry {
        TaskEntry::try_from(update).unwrap()
    }

    #[test]
    fn test_update_merges_into_the_stored_tree() {
        let mut entry = stored(create_task_update(
            1,
            100,
            vec![
                create_task_update(2, 100, Vec::new()),
                create_task_update(3, 100, Vec::new()),
            ],
        ));

        entry
            .update(shallow_update(create_task_update(
                1,
                200,
                vec![shallow_update(create_task_update(3, 250, Vec::new()))],
            )))
            .unwrap();

        assert_eq!(entry.updated_at, at(200));
        assert_eq!(entry.progress, TaskProgress::Running);
        assert_eq!(entry.status, TaskStatus::Other);
        assert_eq!(entry.definition.name, "my-task");

        assert_eq!(entry.subtasks.len(), 2);
        assert_eq!(entry.subtasks[0].id, TaskId::new(2));
        assert_eq!(entry.subtasks[0].updated_at, at(100));
        assert_eq!(entry.subtasks[1].id, TaskId::new(3));
        assert_eq!(entry.subtasks[1].updated_at, at(250));
    }

    #[test]
    fn test_update_older_than_the_entry_is_ignored_whole() {
        let mut entry = stored(create_task_update(
            1,
            300,
            vec![create_task_update(2, 300, Vec::new())],
        ));

        entry
            .update(shallow_update(create_task_update(1, 100, Vec::new())))
            .unwrap();

        assert_eq!(entry.updated_at, at(300));
        assert_eq!(entry.progress, TaskProgress::Running);
        assert_eq!(entry.subtasks.len(), 1);
        assert_eq!(entry.subtasks[0].id, TaskId::new(2));
    }

    #[test]
    fn test_try_from_names_the_task_and_the_missing_field() {
        let err = TaskEntry::try_from(shallow_update(create_task_update(1, 100, Vec::new())))
            .unwrap_err();
        assert_eq!(err.id, TaskId::new(1));
        assert_eq!(err.field, "progress");

        let deep = create_task_update(
            1,
            100,
            vec![shallow_update(create_task_update(9, 100, Vec::new()))],
        );
        assert_eq!(TaskEntry::try_from(deep).unwrap_err().id, TaskId::new(9));
    }

    #[test]
    fn test_unknown_progress_decodes_without_failing_the_batch() {
        let tree: TaskUpdate = serde_json::from_str(
            r#"{"id":1,"started_at":1,"updated_at":1,
                "progress":{"state":"teleporting"},"status":{"kind":"other"},
                "subtasks":{
                    "2":{"id":2,"started_at":1,"updated_at":2,
                         "progress":{"state":"finished"},"status":{"kind":"other"}}
                }}"#,
        )
        .unwrap();

        assert_eq!(tree.progress, Some(TaskProgress::Unknown));
        assert_eq!(
            tree.subtasks[&TaskId::new(2)].progress,
            Some(TaskProgress::Finished)
        );
    }

    #[test]
    fn test_progress_error_carries_its_message() {
        let progress = TaskProgress::error("boom");
        let json = serde_json::to_string(&progress).unwrap();

        assert_eq!(json, r#"{"state":"error","message":"boom"}"#);
        assert_eq!(
            serde_json::from_str::<TaskProgress>(&json).unwrap(),
            progress
        );
        assert!(progress.is_terminal());
        assert!(progress.is_error());
    }
}
