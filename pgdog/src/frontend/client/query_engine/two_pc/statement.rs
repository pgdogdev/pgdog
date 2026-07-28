//! Per-shard two-phase commit transaction names and control statements.

use std::{fmt::Display, str::FromStr};

use tracing::warn;

use crate::frontend::client::query_engine::two_pc::TwoPcTransaction;
use crate::util::is_safe_identifier;

use super::TwoPcPhase;

/// 2pc transaction executed on a shard. We
/// make them unique per shard in case two or more
/// shards are located on the same postgres server.
pub(crate) struct TwoPcTransactionOnShard {
    transaction: TwoPcTransaction,
    shard: usize,
}

impl TwoPcTransactionOnShard {
    /// Create new 2pc transaction on shard x.
    pub(crate) fn new(transaction: TwoPcTransaction, shard: usize) -> Self {
        Self { transaction, shard }
    }

    /// Get the coordinator transaction.
    pub(crate) fn transaction(&self) -> TwoPcTransaction {
        self.transaction
    }

    /// The GID this transaction is prepared under on this shard when
    /// rendered with `prefix`, the coordinator GID prefix recorded at
    /// transaction creation.
    pub(crate) fn gid(&self, prefix: &str) -> String {
        format!("{}{}_{}", prefix, self.transaction.number(), self.shard)
    }

    /// Whether `gid`, as listed in `pg_prepared_xacts`, refers to this
    /// transaction on this shard. GID prefixes embed the identity of the
    /// PgDog process that created the transaction, which can change
    /// across restarts, so matching uses the durable numeric transaction
    /// ID and the shard index. This is the fallback for transactions
    /// restored from WAL records that did not store the coordinator GID
    /// prefix, and for recorded prefixes that fail the identifier
    /// alphabet check; with a recorded prefix, cleanup matches the exact
    /// GID from [`Self::gid`] instead.
    ///
    /// A matched GID is guaranteed to contain only [`is_safe_identifier`]
    /// characters, so it can be embedded verbatim in a quoted SQL literal.
    /// PgDog only generates such GIDs (`NODE_ID` and `DEPLOYMENT_ID` are
    /// validated at startup); a GID that matches the numeric ID but not
    /// the alphabet was not created by PgDog and is refused with a
    /// warning.
    pub(crate) fn matches_gid(&self, gid: &str) -> bool {
        let matches = match gid.parse::<Self>() {
            Ok(parsed) => parsed.transaction == self.transaction && parsed.shard == self.shard,
            Err(()) => false,
        };

        if matches && !is_safe_identifier(gid) {
            warn!(
                "[2pc] prepared transaction {:?} matches transaction {} on shard {} \
                 but contains characters PgDog never generates; refusing to resolve it",
                gid, self.transaction, self.shard
            );
            return false;
        }

        matches
    }
}

impl Display for TwoPcTransactionOnShard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_{}", self.transaction, self.shard)
    }
}

impl FromStr for TwoPcTransactionOnShard {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (transaction, shard) = s.rsplit_once('_').ok_or(())?;

        Ok(Self {
            transaction: transaction.parse()?,
            shard: shard.parse().map_err(|_| ())?,
        })
    }
}

/// Build `PREPARE TRANSACTION`, `COMMIT PREPARED`, or `ROLLBACK PREPARED`
/// for a shard participant.
pub(crate) fn phase_control(
    transaction: TwoPcTransaction,
    shard: usize,
    phase: TwoPcPhase,
) -> String {
    let txn = TwoPcTransactionOnShard::new(transaction, shard);

    match phase {
        TwoPcPhase::Phase1 => format!("PREPARE TRANSACTION '{txn}'"),
        TwoPcPhase::Phase2 => format!("COMMIT PREPARED '{txn}'"),
        TwoPcPhase::Rollback => format!("ROLLBACK PREPARED '{txn}'"),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn transaction_on_shard_appends_index() {
        let transaction = TwoPcTransaction::new();

        assert_eq!(
            TwoPcTransactionOnShard::new(transaction, 0).to_string(),
            format!("{transaction}_0")
        );
        assert_eq!(
            TwoPcTransactionOnShard::new(transaction, 3).to_string(),
            format!("{transaction}_3")
        );
    }

    #[test]
    fn parse_transaction_on_shard() {
        let transaction = TwoPcTransaction::new();
        let parsed: TwoPcTransactionOnShard = format!("{transaction}_3")
            .parse()
            .expect("valid transaction on shard");

        assert_eq!(parsed.transaction, transaction);
        assert_eq!(parsed.shard, 3);
    }

    #[test]
    fn reject_invalid_transaction_on_shard() {
        assert!("invalid".parse::<TwoPcTransactionOnShard>().is_err());
        assert!("invalid_0".parse::<TwoPcTransactionOnShard>().is_err());
        assert!(
            "__pgdog_2pc_1_invalid"
                .parse::<TwoPcTransactionOnShard>()
                .is_err()
        );
    }

    #[test]
    fn matches_gid_ignores_prefix() {
        let transaction: TwoPcTransaction = "__pgdog_2pc_123".parse().unwrap();
        let target = TwoPcTransactionOnShard::new(transaction, 1);

        // Rendered by this process.
        assert!(target.matches_gid(&target.to_string()));
        // Rendered by a process with a different instance ID.
        assert!(target.matches_gid("__pgdog_2pc_deadbeef_123_1"));
        // Rendered by a process with a deployment ID.
        assert!(target.matches_gid("__pgdog_2pc_prod_deadbeef_123_1"));

        // Wrong shard.
        assert!(!target.matches_gid("__pgdog_2pc_deadbeef_123_0"));
        // Wrong transaction ID.
        assert!(!target.matches_gid("__pgdog_2pc_deadbeef_9123_1"));
        // Prepared transactions from other applications.
        assert!(!target.matches_gid("app_txn_123_1"));
        assert!(!target.matches_gid("123_1"));
        assert!(!target.matches_gid(""));
        // Matching ID but characters PgDog never generates: such GIDs
        // are refused so they can never reach a quoted SQL literal.
        assert!(!target.matches_gid("__pgdog_2pc_it's_123_1"));
        assert!(!target.matches_gid("__pgdog_2pc_a\\'b_123_1"));
        assert!(!target.matches_gid("__pgdog_2pc_a b_123_1"));
    }

    #[test]
    fn gid_renders_recorded_prefix() {
        let transaction: TwoPcTransaction = "__pgdog_2pc_123".parse().unwrap();
        let target = TwoPcTransactionOnShard::new(transaction, 1);

        assert_eq!(
            target.gid("__pgdog_2pc_prod_deadbeef_"),
            "__pgdog_2pc_prod_deadbeef_123_1"
        );
        // The recorded prefix names one exact GID; the same number
        // under a different prefix is a different transaction.
        assert_ne!(target.gid("__pgdog_2pc_a_"), target.gid("__pgdog_2pc_b_"));
    }

    #[test]
    fn phase_control_statements() {
        let transaction = TwoPcTransaction::new();

        assert_eq!(
            phase_control(transaction, 1, TwoPcPhase::Phase1),
            format!("PREPARE TRANSACTION '{transaction}_1'")
        );
        assert_eq!(
            phase_control(transaction, 1, TwoPcPhase::Phase2),
            format!("COMMIT PREPARED '{transaction}_1'")
        );
        assert_eq!(
            phase_control(transaction, 1, TwoPcPhase::Rollback),
            format!("ROLLBACK PREPARED '{transaction}_1'")
        );
    }
}
