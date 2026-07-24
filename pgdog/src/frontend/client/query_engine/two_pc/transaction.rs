use rand::{Rng, rng};
use std::sync::Arc;
use std::{
    fmt::Display,
    hash::{Hash, Hasher},
    str::FromStr,
};

use crate::util::{deployment_id, instance_id};

/// Coordinator identifier for a two-phase commit transaction.
///
/// A live transaction is just a random `id`; its gid string is rendered on
/// demand from this process's `instance_id`/`deployment_id`. A restarted
/// PgDog generates a fresh `instance_id`, so a transaction rebuilt during WAL
/// recovery carries the original gid verbatim in `gid` instead of
/// re-rendering it (which would no longer match the name Postgres holds in
/// `pg_prepared_xacts`).
///
/// Identity (`Hash`/`Eq`) is the `id` only: the gid embeds it as its trailing
/// component, so a recovered transaction and its live counterpart compare
/// equal and collate in the same map slot.
#[derive(Debug, Clone)]
pub struct TwoPcTransaction {
    id: usize,
    /// Full coordinator gid, set only when it must be preserved verbatim
    /// (a transaction recovered from the WAL). `None` for transactions
    /// created in this process, where `Display` renders the gid live.
    gid: Option<Arc<str>>,
}

static PREFIX: &str = "__pgdog_2pc_";

impl TwoPcTransaction {
    pub(crate) fn new() -> Self {
        // Transactions have random identifiers,
        // so multiple instances of PgDog don't create an identical transaction.
        Self {
            id: rng().random_range(0..usize::MAX),
            gid: None,
        }
    }

    /// Rebuild a transaction from the raw id stored in the WAL. The gid is
    /// reattached separately via [`Self::with_gid`] when known.
    pub(crate) fn from_id(id: usize) -> Self {
        Self { id, gid: None }
    }

    /// Raw id, as persisted in the WAL record.
    pub(crate) fn id(&self) -> usize {
        self.id
    }

    /// Attach the exact gid this transaction was prepared with, so `Display`
    /// reproduces it verbatim regardless of the current process's
    /// `instance_id`. Used by WAL recovery.
    pub(crate) fn with_gid(mut self, gid: impl Into<Arc<str>>) -> Self {
        self.gid = Some(gid.into());
        self
    }

    /// A prefix to identify two-phase commit transactions generated
    /// by this PgDog process.
    fn global_prefix() -> String {
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
        match &self.gid {
            Some(gid) => f.write_str(gid),
            None => write!(f, "{}{}", Self::global_prefix(), self.id),
        }
    }
}

impl PartialEq for TwoPcTransaction {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for TwoPcTransaction {}

impl Hash for TwoPcTransaction {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl FromStr for TwoPcTransaction {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let id = s.rsplit("_").next().map(|id| id.parse());

        if let Some(Ok(id)) = id {
            Ok(Self {
                id,
                // Preserve the parsed name verbatim: it may carry another
                // process's instance_id that this one cannot reproduce.
                gid: Some(Arc::from(s)),
            })
        } else {
            Err(())
        }
    }
}

#[cfg(test)]
mod test {
    use crate::test_utils::set_env_var;

    use super::*;

    fn with_id(id: usize) -> TwoPcTransaction {
        TwoPcTransaction { id, gid: None }
    }

    #[test]
    fn test_2pc_transaction_id() {
        let transaction = TwoPcTransaction::new();
        assert!(transaction.to_string().contains("__pgdog_2pc_"));
        let reverse = TwoPcTransaction::from_str(transaction.to_string().as_str()).unwrap();
        assert_eq!(reverse.id, transaction.id);
    }

    #[test]
    fn recovered_gid_is_rendered_verbatim() {
        // A gid from another process (different instance_id) must round-trip
        // through Display unchanged, not be re-rendered with this process's
        // prefix.
        let stored = "__pgdog_2pc_oldnode_42";
        let txn = stored.parse::<TwoPcTransaction>().unwrap();
        assert_eq!(txn.to_string(), stored);
        assert_eq!(txn.id, 42);
    }

    #[test]
    fn test_instance_id() {
        for id in [1024, 11111111, usize::MAX, usize::MIN] {
            let transaction = with_id(id);
            let instance_id = instance_id(); // It's a singleton.
            assert_eq!(
                format!("__pgdog_2pc_{instance_id}_{id}"),
                transaction.to_string()
            );
        }
    }

    #[test]
    fn test_deployment_id() {
        let _guard = set_env_var("DEPLOYMENT_ID", "1");
        let txn = with_id(1678);
        let instance_id = instance_id(); // It's a singleton.
        assert_eq!(format!("__pgdog_2pc_1_{instance_id}_1678"), txn.to_string());
    }
}
