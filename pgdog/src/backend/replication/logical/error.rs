use std::fmt;
use std::num::ParseIntError;

use derive_more::{Display, Error};

use crate::{
    backend::replication::publisher::PublicationTable,
    frontend::client::query_engine::two_pc::TwoPcTransaction,
    net::{CommandComplete, ErrorResponse},
};

/// The kind of validation failure, decoupled from which table it occurred on.
#[derive(Debug, Display)]
pub enum TableValidationErrorKind {
    #[display("has no replica identity columns")]
    NoIdentityColumns,
    #[display(
        "REPLICA IDENTITY NOTHING, UPDATE/DELETE carry no row identity and cannot be replicated; set it to DEFAULT, INDEX, or FULL"
    )]
    ReplicaIdentityNothing,
    #[display(
        "REPLICA IDENTITY FULL on a non-sharded table requires a unique index on the destination; add a unique index on the source or destination, use REPLICA IDENTITY USING INDEX on the source, or shard the table"
    )]
    FullIdentityOmniNoUniqueIndex,
}

/// A single table-level validation failure.
#[derive(Debug, Display, Error)]
#[display("table {table_name}: {kind}")]
pub struct TableValidationError {
    pub table_name: String,
    pub kind: TableValidationErrorKind,
}

/// Newtype that `Display`s a slice of `TableValidationError` as a human-readable list.
#[derive(Debug, Error)]
pub struct TableValidationErrors(#[error(ignore)] pub Vec<TableValidationError>);

impl fmt::Display for TableValidationErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Table validation failed:")?;
        for err in &self.0 {
            write!(f, "\n\t{err}")?;
        }
        Ok(())
    }
}

/// Sort `errors` by table display name and return `Err(TableValidation)` if non-empty,
/// otherwise continue. Mirrors `anyhow::ensure!` — uses `return` to exit the calling fn.
///
/// Only valid inside a function that returns `Result<_, Error>`.
macro_rules! ensure_validation {
    ($errors:expr_2021) => {{
        let mut __errors = $errors;
        if !__errors.is_empty() {
            __errors.sort_by_key(|e| e.table_name.clone());
            __errors.dedup_by(|a, b| a.table_name == b.table_name);
            return Err(
                $crate::backend::replication::logical::Error::TableValidation(
                    $crate::backend::replication::logical::TableValidationErrors(__errors),
                ),
            );
        }
    }};
}
// export macro
pub(crate) use ensure_validation;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("backend: {0}")]
    Backend(#[from] crate::backend::Error),

    #[error("pool: {0}")]
    Pool(#[from] crate::backend::pool::Error),

    #[error("router: {0}")]
    Router(#[from] crate::frontend::router::Error),

    #[error("net: {0}")]
    Net(#[from] crate::net::Error),

    #[error("type: {0}")]
    Type(#[from] pgdog_postgres_types::Error),

    #[error("transaction not started")]
    TransactionNotStarted,

    #[error("out of sync, got {0}")]
    OutOfSync(char),

    #[error("out of sync during commit, got {0}")]
    CommitOutOfSync(char),

    #[error("out of sync during relation prepare, got {0}")]
    RelationOutOfSync(char),

    #[error("out of sync during row write, got {0}")]
    SendOutOfSync(char),

    #[error("missing data")]
    MissingData,

    #[error("pg_query: {0}")]
    PgQuery(#[from] pg_query::Error),

    #[error("copy error")]
    Copy,

    #[error("pg_error: {0}")]
    PgError(Box<ErrorResponse>),

    #[error("table \"{0}\".\"{1}\" has no replica identity")]
    NoReplicaIdentity(String, String),

    #[error("lsn decode")]
    LsnDecode,

    #[error("replication slot \"{0}\" doesn't exist, but it should")]
    MissingReplicationSlot(String),

    #[error("parse int")]
    ParseInt(#[from] ParseIntError),

    #[error("shard has no primary")]
    NoPrimary,

    #[error("parser: {0}")]
    Parser(#[from] crate::frontend::router::parser::Error),

    #[error("not connected")]
    NotConnected,

    #[error("replication timeout")]
    ReplicationTimeout,

    #[error("shard {0} has no replication tables")]
    NoReplicationTables(usize),

    #[error("shard {0} has no replication slot")]
    NoReplicationSlot(usize),

    #[error("parallel connection error")]
    ParallelConnection,

    #[error("no replicas available for table sync")]
    NoReplicas,

    #[error("{0}")]
    TableValidation(TableValidationErrors),

    #[error("router returned incorrect command")]
    IncorrectCommand,

    #[error("schema: {0}")]
    SchemaSync(Box<crate::backend::schema::sync::error::Error>),

    #[error("schema isn't loaded")]
    NoSchema,

    #[error("config wasn't updated with new cluster")]
    NoNewCluster,

    #[error("tokio: {0}")]
    JoinError(#[from] tokio::task::JoinError),

    #[error("copy for table {0} been aborted")]
    CopyAborted(PublicationTable),

    #[error("data sync has been aborted")]
    DataSyncAborted,

    #[error("replication has been aborted")]
    ReplicationAborted,

    #[error("waiter has no publisher")]
    NoPublisher,

    #[error("cutover abort timeout")]
    AbortTimeout,

    #[error("task not found")]
    TaskNotFound,

    #[error("task is not a replication task")]
    NotReplication,

    #[error("binary format mismatch (likely int -> bigint), use text copy instead: {0}")]
    BinaryFormatMismatch(Box<ErrorResponse>),

    #[error("frontend: {0}")]
    Frontend(#[from] crate::frontend::Error),

    #[error("command complete has no rows: {0}")]
    CommandCompleteNoRows(CommandComplete),

    #[error("missing key in replication stream, out of sync")]
    MissingKey,

    #[error("toasted identity column in UPDATE: {table} (oid {oid})")]
    ToastedIdentityColumn {
        table: String,
        oid: pgdog_postgres_types::Oid,
    },

    #[error("toasted column in PK-change UPDATE: {table} (oid {oid})")]
    ToastedRowMigration {
        table: String,
        oid: pgdog_postgres_types::Oid,
    },

    /// Source replica identity changed mid-stream: an UPDATE or DELETE arrived without an OLD pre-image
    /// while the destination expected one. Re-sync the table to recover.
    #[error(
        "FULL identity {op} on {table} (oid {oid}): missing OLD pre-image; source replica identity changed mid-stream"
    )]
    FullIdentityMissingOld {
        table: String,
        oid: pgdog_postgres_types::Oid,
        op: &'static str,
    },

    #[error("2pc commit failed for {transaction}: {source}")]
    TwoPcCleanupPending {
        transaction: TwoPcTransaction,
        #[source]
        source: Box<Error>,
    },
}

impl From<ErrorResponse> for Error {
    fn from(value: ErrorResponse) -> Self {
        Self::PgError(Box::new(value))
    }
}

impl From<crate::backend::schema::sync::error::Error> for Error {
    fn from(value: crate::backend::schema::sync::error::Error) -> Self {
        Self::SchemaSync(Box::new(value))
    }
}

impl From<TableValidationError> for Error {
    fn from(e: TableValidationError) -> Self {
        Self::TableValidation(TableValidationErrors(vec![e]))
    }
}

impl Error {
    /// Two-phase commit transaction that still needs manager cleanup, if any.
    pub fn two_pc_cleanup_transaction(&self) -> Option<TwoPcTransaction> {
        match self {
            Self::TwoPcCleanupPending { transaction, .. } => Some(*transaction),
            _ => None,
        }
    }

    /// Whether the table copy should be retried after this error.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::TwoPcCleanupPending { source, .. } => source.is_retryable(),
            Self::Net(inner) => inner.is_retryable(),
            Self::Pool(inner) => inner.is_retryable(),
            Self::Backend(inner) => inner.is_retryable(),
            // No connection yet, or primary is down.
            Self::NotConnected | Self::NoPrimary => true,
            // Replication stalled; temporary slot is gone, next attempt starts fresh.
            Self::ReplicationTimeout => true,
            // Postgres sent a transient error (e.g. admin_shutdown, cannot_connect_now).
            Self::PgError(inner) => inner.is_retryable(),
            // TODO: escape-hatch when using ParallelConnection wrapper
            // the underlying error could be anything and to handler it properly
            // either the ParallelConnection wrapper should be removed or
            // the proper error should be propagated
            Self::ParallelConnection => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::pool::Error as PE;
    use crate::backend::replication::publisher::PublicationTable;
    use crate::net::Error as NE;

    #[test]
    fn retryable() {
        let io = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset");
        assert!(Error::Net(NE::Io(io)).is_retryable());
        assert!(Error::Net(NE::UnexpectedEof).is_retryable());
        assert!(Error::Pool(PE::NoPrimary).is_retryable());
        assert!(Error::Pool(PE::CheckoutTimeout).is_retryable());
        assert!(Error::NotConnected.is_retryable());
        assert!(Error::NoPrimary.is_retryable());
        assert!(Error::ReplicationTimeout.is_retryable());
    }

    /// Regression: replication apply receives Postgres errors wrapped as
    /// `Backend(ExecutionError)` / `Backend(ConnectionError)`, not `PgError`.
    /// Before the fix, the Backend arm bypassed the SQLSTATE list and the publisher
    /// retry loop never fired on shard kill (57P01) -- see `copy_data/retry_test`.
    /// All three wrappers must route through `ErrorResponse::is_retryable`.
    #[test]
    fn sqlstate_classification_routes_through_all_wrappers() {
        use crate::backend::Error as BE;
        use crate::net::messages::ErrorResponse;

        fn resp(code: &str) -> ErrorResponse {
            ErrorResponse {
                code: code.into(),
                ..Default::default()
            }
        }

        type WrapperFn = fn(ErrorResponse) -> Error;
        type WrapperCase = (&'static str, WrapperFn);

        let wrappers: [WrapperCase; 3] = [
            ("PgError", |r| Error::PgError(Box::new(r))),
            ("Backend(ExecutionError)", |r| {
                Error::Backend(BE::ExecutionError(Box::new(r)))
            }),
            ("Backend(ConnectionError)", |r| {
                Error::Backend(BE::ConnectionError(Box::new(r)))
            }),
        ];

        for (label, wrap) in wrappers {
            // 57P01 admin_shutdown is the canonical retryable case (shard restart).
            assert!(
                wrap(resp("57P01")).is_retryable(),
                "{label}(57P01) must be retryable"
            );
            // 23505 unique_violation is a deterministic data error; retrying repeats it.
            assert!(
                !wrap(resp("23505")).is_retryable(),
                "{label}(23505) must NOT be retryable"
            );
        }
    }

    #[test]
    fn retryable_via_backend_wrapper() {
        use crate::backend::Error as BE;

        // IO reset wrapped as Backend — the common path for network drops during COPY.
        let io = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset");
        assert!(Error::Backend(BE::Io(io)).is_retryable());

        // Read timeout mid-stream.
        assert!(Error::Backend(BE::ReadTimeout).is_retryable());

        // Pool couldn't hand out a connection.
        assert!(Error::Backend(BE::Pool(PE::CheckoutTimeout)).is_retryable());
        assert!(Error::Backend(BE::Pool(PE::NoPrimary)).is_retryable());
        assert!(Error::Backend(BE::Pool(PE::AllReplicasDown)).is_retryable());

        // Connection variants.
        assert!(Error::Backend(BE::NotConnected).is_retryable());
        assert!(Error::Backend(BE::ClusterNotConnected).is_retryable());
    }

    #[test]
    fn not_retryable_via_backend_wrapper() {
        use crate::backend::Error as BE;

        // Protocol violations are not transient.
        assert!(!Error::Backend(BE::ProtocolOutOfSync).is_retryable());
    }

    #[test]
    fn not_retryable() {
        assert!(!Error::CopyAborted(PublicationTable::default()).is_retryable());
        assert!(!Error::DataSyncAborted.is_retryable());
        assert!(
            !Error::from(TableValidationError {
                table_name: String::new(),
                kind: TableValidationErrorKind::NoIdentityColumns,
            })
            .is_retryable()
        );
        assert!(
            !Error::FullIdentityMissingOld {
                table: "test_table".to_string(),
                oid: pgdog_postgres_types::Oid::from(1234u32),
                op: "UPDATE",
            }
            .is_retryable()
        );
        assert!(!Error::NoReplicaIdentity("s".into(), "t".into()).is_retryable());
    }

    #[test]
    fn table_validation_error_display() {
        // Single error: header + one indented entry.
        let single = Error::from(TableValidationError {
            table_name: "\"public\".\"orders\"".into(),
            kind: TableValidationErrorKind::NoIdentityColumns,
        });
        assert_eq!(
            single.to_string(),
            "Table validation failed:\n\ttable \"public\".\"orders\": has no replica identity columns",
        );

        // Multiple errors: header + one indented line per entry.
        let multi = Error::TableValidation(TableValidationErrors(vec![
            TableValidationError {
                table_name: "\"public\".\"orders\"".into(),
                kind: TableValidationErrorKind::NoIdentityColumns,
            },
            TableValidationError {
                table_name: "\"public\".\"items\"".into(),
                kind: TableValidationErrorKind::ReplicaIdentityNothing,
            },
        ]));
        assert_eq!(
            multi.to_string(),
            "Table validation failed:\n\ttable \"public\".\"orders\": has no replica identity columns\n\ttable \"public\".\"items\": REPLICA IDENTITY NOTHING, UPDATE/DELETE carry no row identity and cannot be replicated; set it to DEFAULT, INDEX, or FULL",
        );
    }
}
