//! Per-shard two-phase commit transaction names and control statements.

use super::TwoPcPhase;

/// Prepared transaction name for a coordinator transaction on one shard.
pub fn shard_name(transaction: &str, shard: usize) -> String {
    format!("{transaction}_{shard}")
}

/// Build `PREPARE TRANSACTION`, `COMMIT PREPARED`, or `ROLLBACK PREPARED`
/// for a shard participant.
pub fn phase_control(transaction: &str, shard: usize, phase: TwoPcPhase) -> String {
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
        assert_eq!(shard_name("__pgdog_2pc_42", 0), "__pgdog_2pc_42_0");
        assert_eq!(shard_name("test_txn", 3), "test_txn_3");
    }

    #[test]
    fn phase_control_statements() {
        assert_eq!(
            phase_control("test", 1, TwoPcPhase::Phase1),
            "PREPARE TRANSACTION 'test_1'"
        );
        assert_eq!(
            phase_control("test", 1, TwoPcPhase::Phase2),
            "COMMIT PREPARED 'test_1'"
        );
        assert_eq!(
            phase_control("test", 1, TwoPcPhase::Rollback),
            "ROLLBACK PREPARED 'test_1'"
        );
    }
}
