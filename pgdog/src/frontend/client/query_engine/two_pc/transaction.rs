use rand::{Rng, rng};
use serde::{Deserialize, Serialize};
use std::{fmt::Display, str::FromStr};

use crate::util::{deployment_id, instance_id};

#[derive(Debug, Clone, Copy, PartialEq, Hash, Eq, Serialize, Deserialize)]
pub struct TwoPcTransaction(usize);

static PREFIX: &str = "__pgdog_2pc_";

impl TwoPcTransaction {
    pub(crate) fn new() -> Self {
        // Transactions have random identifiers,
        // so multiple instances of PgDog don't create an identical transaction.
        Self(rng().random_range(0..usize::MAX))
    }

    /// This transaction was created by this process.
    pub(crate) fn is_mine(&self) -> bool {
        self.to_string().starts_with(&Self::global_prefix())
    }

    /// A prefix to identify two-phase commit transactions generated
    /// by this PgDog process.
    pub(crate) fn global_prefix() -> String {
        format!(
            "{PREFIX}{}{}_",
            if let Some(cluster_id) = deployment_id() {
                format!("{}_", cluster_id)
            } else {
                "".into()
            },
            instance_id(),
        )
    }
}

impl Display for TwoPcTransaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}", Self::global_prefix(), self.0)
    }
}

impl FromStr for TwoPcTransaction {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let id = s.rsplit("_").next().map(|id| id.parse());

        if let Some(Ok(id)) = id {
            Ok(Self(id))
        } else {
            Err(())
        }
    }
}

#[cfg(test)]
mod test {
    use crate::test_utils::set_env_var;

    use super::*;

    #[test]
    fn test_2pc_transaction_id() {
        let transaction = TwoPcTransaction::new();
        assert!(transaction.to_string().contains("__pgdog_2pc_"));
        let reverse = TwoPcTransaction::from_str(transaction.to_string().as_str()).unwrap();
        assert_eq!(reverse.0, transaction.0);
    }

    #[test]
    fn test_instance_id() {
        for id in [1024, 11111111, usize::MAX, usize::MIN] {
            let transaction = TwoPcTransaction(id);
            let instance_id = instance_id(); // Generate it, it's a singleton.
            assert_eq!(
                format!("__pgdog_2pc_{instance_id}_{id}"),
                transaction.to_string()
            );
        }
    }

    #[test]
    fn test_deployment_id() {
        let _guard = set_env_var("DEPLOYMENT_ID", "1");
        let txn = TwoPcTransaction(1678);
        let instance_id = instance_id(); // Generate it, it's a singleton.
        assert_eq!(format!("__pgdog_2pc_1_{instance_id}_1678"), txn.to_string());
    }
}
