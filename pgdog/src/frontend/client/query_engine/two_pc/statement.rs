//! Per-shard two-phase commit transaction names and control statements.

use crate::frontend::client::query_engine::two_pc::TwoPcTransaction;

use super::TwoPcPhase;

/// Prepared transaction name for a coordinator transaction on one shard.
pub fn shard_name(transaction: TwoPcTransaction, shard: usize) -> String {
    format!("{transaction}_{shard}")
}

/// Build `PREPARE TRANSACTION`, `COMMIT PREPARED`, or `ROLLBACK PREPARED`
/// for a shard participant.
pub fn phase_control(transaction: TwoPcTransaction, shard: usize, phase: TwoPcPhase) -> String {
    let name = shard_name(transaction, shard);

    match phase {
        TwoPcPhase::Phase1 => format!("PREPARE TRANSACTION '{name}'"),
        TwoPcPhase::Phase2 => format!("COMMIT PREPARED '{name}'"),
        TwoPcPhase::Rollback => format!("ROLLBACK PREPARED '{name}'"),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn shard_name_appends_index() {
        let transaction = TwoPcTransaction::new();

        assert_eq!(shard_name(transaction, 0), format!("{transaction}_0"));
        assert_eq!(shard_name(transaction, 3), format!("{transaction}_3"));
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
