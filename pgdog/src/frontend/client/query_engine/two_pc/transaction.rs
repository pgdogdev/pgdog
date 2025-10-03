use rand::{thread_rng, Rng};
use std::fmt::Display;

#[derive(Debug, Clone, Copy, PartialEq, Hash, Eq)]
pub struct TwoPcTransaction(usize);

impl TwoPcTransaction {
    pub(crate) fn new() -> Self {
        // Transactions have random identifiers,
        // so multiple instances of PgDog don't create an identical transaction.
        Self(thread_rng().gen())
    }
}

impl Display for TwoPcTransaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "__pgdog_2pc_{}", self.0)
    }
}
