//! Per-shard two-phase commit transaction names and control statements.

use std::{fmt::Display, str::FromStr};

use crate::frontend::client::query_engine::two_pc::TwoPcTransaction;

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
