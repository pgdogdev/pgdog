use std::ops::Deref;
#[cfg(test)]
use std::vec::IntoIter;

use super::*;
use crate::net::Query;

/// Parameter synchronization state.
///
/// Keeps things in check while we perform the actual
/// synchronization on the server.
#[derive(Debug, Clone)]
pub(crate) struct ParametersSync<'a> {
    pub(super) reset: Vec<String>,
    pub(super) queries: Vec<Query>,
    pub(super) source_store: &'a ParameterStore,
}

impl ParametersSync<'_> {
    /// Get queries that should sync the in-transaction
    /// parameter state.
    pub(crate) fn transaction_sync(&self) -> Vec<Query> {
        self.source_store.transaction_sync()
    }
}

impl Deref for ParametersSync<'_> {
    type Target = Vec<Query>;

    fn deref(&self) -> &Self::Target {
        &self.queries
    }
}

#[cfg(test)]
impl IntoIterator for ParametersSync<'_> {
    type Item = Query;
    type IntoIter = IntoIter<Query>;

    fn into_iter(self) -> Self::IntoIter {
        self.queries.into_iter()
    }
}

#[cfg(test)]
impl PartialEq<Vec<Query>> for ParametersSync<'_> {
    fn eq(&self, other: &Vec<Query>) -> bool {
        self.queries.eq(other)
    }
}
