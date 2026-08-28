//! Task identity, status and definition reports.

use std::borrow::Cow;

use derive_more::{Display, From, FromStr};
use pgdog_postgres_types::ToDataRowColumn;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{Lsn, StatementKind, SyncState, User};

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
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Display, From, Serialize, Deserialize, JsonSchema,
)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TaskStatus {
    /// generic variants for any task
    RatioProgress(RatioProgress),
    /// specific tasks for known tasks used for enterprise
    Reshard(ReshardStatus),
    CopyData(CopyDataStatus),
    SchemaSync(SchemaSyncStatus),
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
    SchemaStatement(SchemaStatementDefinition),
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
            Self::SchemaStatement(_) => "schema_statement",
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

/// Stages of a schema sync, reported as the task's status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum SchemaSyncStatus {
    /// Dumping the schema from the source.
    #[display("loading schema")]
    LoadingSchema,
    /// Restoring tables on the destination (pre-data).
    #[display("syncing tables")]
    SyncingTables,
    /// Creating indexes and constraints on the destination (post-data).
    #[display("creating indexes")]
    CreatingIndexes,
    /// Restoring cutover-time schema on the destination.
    #[display("syncing cutover schema")]
    Cutover,
    /// A stage this build does not know.
    #[display("-")]
    #[serde(other)]
    Other,
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

/// The DDL statement one schema-statement subtask runs, and where.
#[derive(Debug, Clone, PartialEq, Display, Serialize, Deserialize, JsonSchema)]
#[display("{statement_kind} on shard {shard} ({sync_state})")]
pub struct SchemaStatementDefinition {
    pub sql: String,
    pub shard: u64,
    pub user: User,
    pub statement_kind: StatementKind,
    pub sync_state: SyncState,
    pub table_schema: Option<String>,
    pub table_name: Option<String>,
}

#[cfg(test)]
mod test {
    use super::*;

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
            SchemaStatementDefinition {
                sql: "CREATE INDEX ...".into(),
                shard: 1,
                user: User {
                    user: "pgdog".into(),
                    database: "prod".into(),
                },
                statement_kind: StatementKind::Index,
                sync_state: SyncState::PostData,
                table_schema: Some("public".into()),
                table_name: Some("users".into()),
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
                | TaskDefinitionKind::SchemaStatement(_)
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
            (
                SyncState::PostCutover,
                "schema_sync(post_cutover) prod -> prod_sharded",
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
            TaskStatus::SchemaSync(SchemaSyncStatus::CreatingIndexes),
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
            serde_json::from_str::<TaskStatus>(r#"{"kind":"schema_sync","status":"new_stage"}"#)
                .unwrap(),
            TaskStatus::SchemaSync(SchemaSyncStatus::Other)
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
}
