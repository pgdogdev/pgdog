//! # Logical Transaction Management in PgDog
//!
//! Exposes a unified logical transaction interface while coordinating and validating transactions
//! across shards, preventing illegal behavior.
//!
//! PgDog presents a single-node PostgreSQL interface to clients, concealing the underlying shard topology.
//! `LogicalTransaction` tracks transaction state to ensure consistent transactional behavior across horizontally
//! sharded backend Postgres servers.
//!
//! Sharding hints and "dirty" shard tracking enforce single-shard constraints within transactions.
//!
//! Example (valid on single-node Postgres):
//! -- BEGIN;
//! -- INSERT INTO users (id) VALUES (123);
//! -- INSERT INTO users (id) VALUES (345);
//! -- COMMIT;
//!
//! Counterexample (invalid cross-shard sequence without 2PC):
//! -- BEGIN;
//! -- SET pgdog_shard = 0;
//! -- INSERT INTO users (id) VALUES (123);
//! -- SET pgdog_shard = 8;
//! -- INSERT INTO users (id) VALUES (345);
//! -- COMMIT;
//!
//! Future: `allow_cross_shard_transaction = true` may enable PgDog to manage 2PCs automatically with a performance hit.
//!

use std::error::Error;
use std::fmt;

use super::router::parser::Shard;

// -----------------------------------------------------------------------------
// ----- LogicalTransaction ----------------------------------------------------

#[derive(Debug)]
pub struct LogicalTransaction {
    pub status: TransactionStatus,
    manual_shard: Option<Shard>,
    dirty_shard: Option<Shard>,
}

impl LogicalTransaction {
    pub fn new() -> Self {
        Self {
            status: TransactionStatus::Idle,
            manual_shard: None,
            dirty_shard: None,
        }
    }
}

// -----------------------------------------------------------------------------
// ----- LogicalTransaction: Public methods ------------------------------------

impl LogicalTransaction {
    /// Return the shard to apply statements to.
    /// If a manual shard is set, returns it. Otherwise returns the touched shard.
    /// In practice, either only one value is set, or both values are the same.
    pub fn active_shard(&self) -> Option<Shard> {
        self.dirty_shard
            .clone()
            .or_else(|| self.manual_shard.clone())
    }

    /// Return whether a transaction is currently open or pending.
    /// This is because we don't actually trigger the begin until the first statement is executed.
    pub fn in_transaction(&self) -> bool {
        matches!(
            self.status,
            TransactionStatus::BeginPending | TransactionStatus::InProgress
        )
    }

    /// Mark that a `BEGIN` is pending.
    ///
    /// Transitions `Idle -> BeginPending`.
    ///
    /// # Errors
    /// - `AlreadyInTransaction` if already `BeginPending` or `InProgress`.
    /// - `AlreadyFinalized` if `Committed` or `RolledBack`.tx or finalized.
    pub fn soft_begin(&mut self) -> Result<(), TransactionError> {
        match self.status {
            TransactionStatus::Idle => {
                self.status = TransactionStatus::BeginPending;
                Ok(())
            }
            TransactionStatus::BeginPending => Err(TransactionError::ExpectedIdle),
            TransactionStatus::InProgress => Err(TransactionError::ExpectedIdle),
        }
    }

    /// Execute a query against `shard`, updating transactional state.
    ///
    /// - Touches the shard (enforcing the shard conflict rules).
    /// - Transitions `BeginPending -> InProgress` on first statement.
    /// - No-op state change when already `InProgress`.
    ///
    /// # Errors
    /// - `NoPendingBegins` if status is `Idle`.
    /// - `AlreadyFinalized` if `Committed` or `RolledBack`.
    /// - `InvalidManualShardType` if `shard` is not `Shard::Direct(_)`.
    /// - `ShardConflict` if `active_shard` is set to a different shard.
    pub fn execute_query(&mut self, shard: Shard) -> Result<(), TransactionError> {
        self.touch_shard(shard)?;

        match self.status {
            TransactionStatus::Idle => Err(TransactionError::ExpectedPendingOrActive),
            TransactionStatus::BeginPending => {
                self.status = TransactionStatus::InProgress;
                Ok(())
            }
            TransactionStatus::InProgress => Ok(()),
        }
    }

    /// Commit the transaction.
    ///
    /// Transitions `InProgress -> Committed`.
    ///
    /// # Errors
    /// - `NoPendingBegins` if `Idle`.
    /// - `NoActiveTransaction` if `BeginPending` (nothing ran).
    /// - `AlreadyFinalized` if already `Committed` or `RolledBack`.
    pub fn commit(&mut self) -> Result<(), TransactionError> {
        match self.status {
            TransactionStatus::Idle => Err(TransactionError::ExpectedActive),
            TransactionStatus::BeginPending => Err(TransactionError::ExpectedActive),
            TransactionStatus::InProgress => {
                self.reset();
                Ok(())
            }
        }
    }

    /// Roll back the transaction.
    ///
    /// Transitions `InProgress -> RolledBack`.
    ///
    /// # Errors
    /// - `NoPendingBegins` if `Idle`.
    /// - `NoActiveTransaction` if `BeginPending` (nothing ran).
    /// - `AlreadyFinalized` if already `Committed` or `RolledBack`.
    pub fn rollback(&mut self) -> Result<(), TransactionError> {
        match self.status {
            TransactionStatus::Idle => Err(TransactionError::ExpectedActive),
            TransactionStatus::BeginPending => Err(TransactionError::ExpectedActive),
            TransactionStatus::InProgress => {
                self.reset();
                Ok(())
            }
        }
    }

    /// Reset all transactional/session state.
    ///
    /// Sets status to `Idle`, clears manual and dirty shard
    /// Safe to call in any state.
    pub fn reset(&mut self) {
        self.status = TransactionStatus::Idle;
        self.manual_shard = None;
        self.dirty_shard = None;
    }

    /// Pin the transaction to a specific shard.
    ///
    /// Accepts only `Shard::Direct(_)`.
    /// No-op if setting the same shard again.
    /// If a different shard was already touched, fails.
    ///
    /// # Errors
    /// - `InvalidManualShardType` unless `Shard::Direct(_)`.
    /// - `ShardConflict` if `dirty_shard` is set to a different shard.
    pub fn set_manual_shard(&mut self, shard: Shard) -> Result<(), TransactionError> {
        // only Shard::Direct(n) is valid in a transaction
        if !matches!(shard, Shard::Direct(_)) {
            return Err(TransactionError::InvalidShardType);
        }

        // no-op if unchanged
        if self.manual_shard.as_ref().map_or(false, |h| h == &shard) {
            return Ok(());
        }

        // if we already touched a different shard, error
        if let Some(d) = &self.dirty_shard {
            if *d != shard {
                return Err(TransactionError::ShardConflict);
            }
        }

        self.manual_shard = Some(shard);
        Ok(())
    }
}

// -----------------------------------------------------------------------------
// ----- LogicalTransaction: Private methods -----------------------------------

impl LogicalTransaction {
    /// Record that this transaction touched `shard`.
    /// Enforces single-shard discipline.
    fn touch_shard(&mut self, shard: Shard) -> Result<(), TransactionError> {
        // Shard must be of type Shard::Direct(n).
        if !matches!(shard, Shard::Direct(_)) {
            return Err(TransactionError::InvalidShardType);
        }

        // Already pinned to a manual shard → forbid drift.
        if let Some(hint) = &self.manual_shard {
            if *hint != shard {
                return Err(TransactionError::ShardConflict);
            }
        }

        // Already dirtied another shard → forbid drift.
        if let Some(dirty) = &self.dirty_shard {
            if *dirty != shard {
                return Err(TransactionError::ShardConflict);
            }
        }

        // Nothing in conflict; mark the shard.
        self.dirty_shard = Some(shard);
        Ok(())
    }
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum TransactionError {
    // Transaction lifecycle
    ExpectedIdle,
    ExpectedPending,
    ExpectedPendingOrActive,
    ExpectedActive,

    // Sharding policy
    InvalidShardType,
    ShardConflict,
}

impl fmt::Display for TransactionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use TransactionError::*;
        match self {
            ExpectedIdle => write!(f, "transaction already started"),
            ExpectedPending => write!(f, "transaction not pending"),
            ExpectedActive => write!(f, "no active transaction"),
            ExpectedPendingOrActive => write!(f, "no active/pending transaction"),
            InvalidShardType => write!(f, "sharding hints must be ::Direct(n)"),
            ShardConflict => {
                write!(f, "can't run a transaction on multiple shards")
            }
        }
    }
}

impl Error for TransactionError {}

// -----------------------------------------------------------------------------
// ----- SubStruct: TransactionStatus ------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionStatus {
    /// No transaction started.
    Idle,
    /// BEGIN issued by client; waiting to relay it until first in-transaction query.
    BeginPending,
    /// Transaction active.
    InProgress,
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_transaction_is_idle() {
        let tx = LogicalTransaction::new();
        assert_eq!(tx.status, TransactionStatus::Idle);
        assert_eq!(tx.manual_shard, None);
        assert_eq!(tx.dirty_shard, None);
    }

    #[test]
    fn test_soft_begin_from_idle() {
        let mut tx = LogicalTransaction::new();
        tx.soft_begin().unwrap();
        assert_eq!(tx.status, TransactionStatus::BeginPending);
    }

    #[test]
    fn test_soft_begin_already_pending_errors() {
        let mut tx = LogicalTransaction::new();
        tx.soft_begin().unwrap();
        let err = tx.soft_begin().unwrap_err();
        assert!(matches!(err, TransactionError::ExpectedIdle));
    }

    #[test]
    fn test_soft_begin_in_progress_errors() {
        let mut tx = LogicalTransaction::new();

        tx.soft_begin().unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();

        let err = tx.soft_begin().unwrap_err();
        assert!(matches!(err, TransactionError::ExpectedIdle));
    }

    #[test]
    fn test_soft_begin_after_commit_errors() {
        let mut tx = LogicalTransaction::new();

        tx.soft_begin().unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();
        tx.commit().unwrap();

        tx.soft_begin().unwrap(); // no panic
    }

    #[test]
    fn test_soft_begin_after_rollback_errors() {
        let mut tx = LogicalTransaction::new();

        tx.soft_begin().unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();
        tx.rollback().unwrap();

        tx.soft_begin().unwrap(); // no panic
    }

    #[test]
    fn test_execute_query_from_begin_pending() {
        let mut tx = LogicalTransaction::new();

        tx.soft_begin().unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();

        assert_eq!(tx.status, TransactionStatus::InProgress);
        assert_eq!(tx.dirty_shard, Some(Shard::Direct(0)));
    }

    #[test]
    fn test_execute_query_from_idle_errors() {
        let mut tx = LogicalTransaction::new();
        let err = tx.execute_query(Shard::Direct(0)).unwrap_err();
        assert!(matches!(err, TransactionError::ExpectedPendingOrActive));
    }

    #[test]
    fn test_execute_query_after_commit_errors() {
        let mut ltx = LogicalTransaction::new();

        ltx.soft_begin().unwrap();
        ltx.execute_query(Shard::Direct(0)).unwrap();
        ltx.execute_query(Shard::Direct(0)).unwrap();
        ltx.execute_query(Shard::Direct(0)).unwrap();
        ltx.commit().unwrap();

        let err = ltx.execute_query(Shard::Direct(0)).unwrap_err();
        assert!(matches!(err, TransactionError::ExpectedPendingOrActive));
    }

    #[test]
    fn test_execute_query_multiple_on_same_shard() {
        let mut tx = LogicalTransaction::new();

        tx.soft_begin().unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();

        assert_eq!(tx.dirty_shard, Some(Shard::Direct(0)));
        assert_eq!(tx.status, TransactionStatus::InProgress);
    }

    #[test]
    fn test_execute_query_cross_shard_errors() {
        let mut tx = LogicalTransaction::new();

        tx.soft_begin().unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();

        let err = tx.execute_query(Shard::Direct(1)).unwrap_err();
        assert!(matches!(err, TransactionError::ShardConflict));
    }

    #[test]
    fn test_execute_query_invalid_shard_type_errors() {
        let mut tx = LogicalTransaction::new();

        tx.soft_begin().unwrap();

        let err = tx.execute_query(Shard::All).unwrap_err();
        assert!(matches!(err, TransactionError::InvalidShardType));
    }

    #[test]
    fn test_commit_from_in_progress() {
        let mut tx = LogicalTransaction::new();

        tx.soft_begin().unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();
        tx.commit().unwrap();

        assert_eq!(tx.status, TransactionStatus::Idle);
    }

    #[test]
    fn test_commit_from_idle_errors() {
        let mut tx = LogicalTransaction::new();
        let err = tx.commit().unwrap_err();
        assert!(matches!(err, TransactionError::ExpectedActive));
    }

    #[test]
    fn test_commit_from_begin_pending_errors() {
        let mut tx = LogicalTransaction::new();

        tx.soft_begin().unwrap();

        let err = tx.commit().unwrap_err();
        assert!(matches!(err, TransactionError::ExpectedActive));
    }

    #[test]
    fn test_commit_already_committed_errors() {
        let mut tx = LogicalTransaction::new();

        tx.soft_begin().unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();
        tx.commit().unwrap();

        let err = tx.commit().unwrap_err();
        assert!(matches!(err, TransactionError::ExpectedActive));
    }

    #[test]
    fn test_rollback_from_in_progress() {
        let mut tx = LogicalTransaction::new();

        tx.soft_begin().unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();
        tx.rollback().unwrap();

        assert_eq!(tx.status, TransactionStatus::Idle);
    }

    #[test]
    fn test_rollback_from_begin_pending_errors() {
        let mut tx = LogicalTransaction::new();

        tx.soft_begin().unwrap();

        let err = tx.rollback().unwrap_err();
        assert!(matches!(err, TransactionError::ExpectedActive));
    }

    #[test]
    fn test_reset_clears_state() {
        let mut tx = LogicalTransaction::new();

        tx.soft_begin().unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();
        tx.set_manual_shard(Shard::Direct(0)).unwrap();
        tx.reset();

        assert_eq!(tx.status, TransactionStatus::Idle);
        assert_eq!(tx.manual_shard, None);
        assert_eq!(tx.dirty_shard, None);
    }

    #[test]
    fn test_set_matching_manual_shard_before_touch() {
        let mut tx = LogicalTransaction::new();

        tx.soft_begin().unwrap();
        tx.set_manual_shard(Shard::Direct(0)).unwrap();

        assert_eq!(tx.manual_shard, Some(Shard::Direct(0)));

        tx.execute_query(Shard::Direct(0)).unwrap(); // should succeed
        tx.execute_query(Shard::Direct(0)).unwrap(); // should succeed
        tx.execute_query(Shard::Direct(0)).unwrap(); // should succeed
    }

    #[test]
    fn test_set_manual_shard_after_touch_same_ok() {
        let mut tx = LogicalTransaction::new();

        tx.soft_begin().unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();
        tx.set_manual_shard(Shard::Direct(0)).unwrap();

        assert_eq!(tx.manual_shard, Some(Shard::Direct(0)));
    }

    #[test]
    fn test_set_manual_shard_after_touch_different_errors() {
        let mut tx = LogicalTransaction::new();

        // touch shard 0
        tx.soft_begin().unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();

        // manually set shard 1
        let err = tx.set_manual_shard(Shard::Direct(1)).unwrap_err();
        assert!(matches!(err, TransactionError::ShardConflict));
    }

    #[test]
    fn test_manual_then_dirty_conflict() {
        let mut tx = LogicalTransaction::new();
        tx.soft_begin().unwrap();

        // pin to shard 0
        tx.set_manual_shard(Shard::Direct(0)).unwrap();

        // touching another shard must fail
        let err = tx.execute_query(Shard::Direct(1)).unwrap_err();
        assert!(matches!(err, TransactionError::ShardConflict));
    }

    #[test]
    fn test_set_manual_shard_invalid_type_errors() {
        let mut tx = LogicalTransaction::new();
        let err = tx.set_manual_shard(Shard::All).unwrap_err();
        assert!(matches!(err, TransactionError::InvalidShardType));
    }

    #[test]
    fn test_active_shard_dirty() {
        let mut tx = LogicalTransaction::new();

        tx.soft_begin().unwrap();
        tx.execute_query(Shard::Direct(69)).unwrap();
        tx.execute_query(Shard::Direct(69)).unwrap();
        tx.execute_query(Shard::Direct(69)).unwrap();

        assert_eq!(tx.active_shard(), Some(Shard::Direct(69)));
    }

    #[test]
    fn test_active_shard_manual() {
        let mut tx = LogicalTransaction::new();
        tx.set_manual_shard(Shard::Direct(1)).unwrap();
        assert_eq!(tx.active_shard(), Some(Shard::Direct(1)));
    }

    #[test]
    fn test_rollback_from_idle_errors() {
        let mut tx = LogicalTransaction::new();
        let err = tx.rollback().unwrap_err();
        assert!(matches!(err, TransactionError::ExpectedActive));
    }

    #[test]
    fn test_commit_after_rollback_errors() {
        let mut tx = LogicalTransaction::new();

        tx.soft_begin().unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();
        tx.rollback().unwrap();

        let err = tx.commit().unwrap_err();
        assert!(matches!(err, TransactionError::ExpectedActive));
    }

    #[test]
    fn test_rollback_after_commit_errors() {
        let mut tx = LogicalTransaction::new();

        tx.soft_begin().unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();
        tx.commit().unwrap();

        let err = tx.rollback().unwrap_err();
        assert!(matches!(err, TransactionError::ExpectedActive));
    }

    #[test]
    fn test_rollback_already_rolledback_errors() {
        let mut tx = LogicalTransaction::new();

        tx.soft_begin().unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();
        tx.rollback().unwrap();

        let err = tx.rollback().unwrap_err();
        println!("Error: {:?}", err);
        assert!(matches!(err, TransactionError::ExpectedActive));
    }

    #[test]
    fn test_execute_query_after_rollback_errors() {
        let mut tx = LogicalTransaction::new();

        tx.soft_begin().unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();
        tx.rollback().unwrap();

        let err = tx.execute_query(Shard::Direct(0)).unwrap_err();
        assert!(matches!(err, TransactionError::ExpectedPendingOrActive));
    }

    #[test]
    fn test_set_manual_shard_multiple_changes_before_execute() {
        let mut tx = LogicalTransaction::new();

        tx.soft_begin().unwrap();
        tx.set_manual_shard(Shard::Direct(1)).unwrap();
        tx.set_manual_shard(Shard::Direct(2)).unwrap(); // change, no error.

        assert_eq!(tx.manual_shard, Some(Shard::Direct(2)));

        tx.execute_query(Shard::Direct(2)).unwrap();
        tx.execute_query(Shard::Direct(2)).unwrap();
        tx.execute_query(Shard::Direct(2)).unwrap();
        tx.execute_query(Shard::Direct(2)).unwrap();

        let err = tx.execute_query(Shard::Direct(1)).unwrap_err();
        assert!(matches!(err, TransactionError::ShardConflict));
    }

    #[test]
    fn test_set_manual_shard_after_commit_same_ok() {
        let mut tx = LogicalTransaction::new();

        tx.soft_begin().unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();
        tx.commit().unwrap();

        tx.set_manual_shard(Shard::Direct(0)).unwrap();
        assert_eq!(tx.manual_shard, Some(Shard::Direct(0)));
    }

    #[test]
    fn test_set_manual_shard_after_commit_different_errors() {
        let mut tx = LogicalTransaction::new();

        tx.soft_begin().unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();
        tx.commit().unwrap();

        tx.set_manual_shard(Shard::Direct(1)).unwrap(); // should not panic
    }

    #[test]
    fn test_set_manual_shard_after_rollback_same_ok() {
        let mut tx = LogicalTransaction::new();

        tx.soft_begin().unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();
        tx.rollback().unwrap();

        tx.set_manual_shard(Shard::Direct(88)).unwrap();
        assert_eq!(tx.manual_shard, Some(Shard::Direct(88))); // no panic
    }

    #[test]
    fn test_set_manual_shard_after_rollback_different_errors() {
        let mut tx = LogicalTransaction::new();

        tx.soft_begin().unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();
        tx.rollback().unwrap();

        tx.set_manual_shard(Shard::Direct(1)).unwrap(); // should not panic
    }

    #[test]
    fn test_active_shard_none() {
        let tx = LogicalTransaction::new();
        assert_eq!(tx.active_shard(), None);
    }

    #[test]
    fn test_set_manual_shard_in_idle() {
        let mut tx = LogicalTransaction::new();
        tx.set_manual_shard(Shard::Direct(0)).unwrap();
        assert_eq!(tx.manual_shard, Some(Shard::Direct(0)));
    }

    #[test]
    fn test_soft_begin_after_reset_from_finalized() {
        let mut tx = LogicalTransaction::new();

        tx.soft_begin().unwrap();
        tx.execute_query(Shard::Direct(0)).unwrap();
        tx.commit().unwrap();

        tx.reset();

        tx.soft_begin().unwrap();
        assert_eq!(tx.status, TransactionStatus::BeginPending);
    }

    #[test]
    fn test_active_shard_both_same() {
        let mut tx = LogicalTransaction::new();

        tx.set_manual_shard(Shard::Direct(3)).unwrap();
        tx.soft_begin().unwrap();
        tx.execute_query(Shard::Direct(3)).unwrap();

        assert_eq!(tx.active_shard(), Some(Shard::Direct(3)));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
