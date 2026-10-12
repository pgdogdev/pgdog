use crate::{net::Query, util::escape_identifier};

use super::*;
use std::mem::take;

/// Handle the complexity of managing parameters
/// on the client and server.
pub(crate) struct ParameterStore {
    // Parameters this client or server had when it was first created.
    startup: Parameters,
    // Parameters currently stored on the client or server.
    params: Parameters,
    // Parameters thar are scoped to the current transaction. If the transaction
    // is committed, these will be copied into `parameters`. Otherwise, they
    // are cleared.
    transaction_params: Parameters,
    // Parameters set via `SET LOCAL` inside a transaction. These are saved
    // because we need to handle them before connecting to a server.
    transaction_local_params: Parameters,
    // Parameters that are being reset to their original values
    // inside a transaction. They are stored to support rolling back
    // this action inside a transaction.
    reset_params: Parameters,
    // Current transaction state.
    in_transaction: bool,
}

impl ParameterStore {
    /// Commit parameters to store after a transaction is complete.
    pub(crate) fn commit(&mut self) {
        self.params.extend(take(&mut self.transaction_params));
        self.transaction_end();
    }

    /// Rollback changes to the store after a transaction is aborted.
    pub(crate) fn rollback(&mut self) {
        self.params.extend(take(&mut self.reset_params));
        self.transaction_end();
    }

    /// Indicate a transaction has been started and all subsequent
    /// operations on the store are transaction-scoped.
    pub(crate) fn begin(&mut self) {
        self.in_transaction = true;
    }

    /// Handle a parameter reset, while respecting transactional
    /// semantics.
    pub(crate) fn reset(&mut self, name: impl AsRef<str>) {
        let name = name.as_ref();

        if let Some(exists) = self.params.remove(name) {
            // If the client had this value in its params,
            // store it so we can rollback this change.
            if self.in_transaction {
                self.reset_params.insert(name, exists);
            }
        }

        // Reset parameter to its startup value, if provided.
        if let Some(startup) = self.startup.get(name) {
            self.params.insert(name, startup.clone());
        }

        // TODO(lev): Handle rollback ^.

        self.transaction_local_params.remove(name);
        self.transaction_params.remove(name);
    }

    /// Handle resetting all parameters to their initial values,
    /// accounting for transactional semantics.
    pub(crate) fn reset_all(&mut self) {
        let names: Vec<_> = self
            .params
            .keys()
            .filter(|name| !UNTRACKED_PARAMS.contains(&name.as_str()))
            .cloned()
            .collect();
        for name in names {
            self.reset(name);
        }

        self.transaction_local_params.clear();
        self.transaction_params.clear();
    }

    /// Get a parameter value from the store, respecting transactional semantics.
    pub(crate) fn get(&self, name: &str) -> Option<&ParameterValue> {
        self.transaction_local_params
            .get(name)
            .or(self.transaction_params.get(name).or(self.params.get(name)))
    }

    /// Insert a parameter into the store, respecting transactional semantics.
    pub(crate) fn set(
        &mut self,
        name: impl AsRef<str>,
        value: impl Into<ParameterValue>,
        local: bool,
    ) -> Option<ParameterValue> {
        let name = name.as_ref();

        if self.in_transaction {
            // This replicates Postgres behavior of the
            // latest SET command overriding previous SET.
            if local {
                self.transaction_params.remove(name);
                self.transaction_local_params.insert(name, value)
            } else {
                self.transaction_local_params.remove(name);
                self.transaction_params.insert(name, value)
            }
        } else {
            self.params.insert(name, value)
        }
    }

    /// Sync parameters in this store with parameters in the
    /// other. This happens when a client store is linked with the
    /// server store.
    ///
    /// Returns a list of queries that need to be executed on the
    /// server to make this sync actually stick.
    pub(crate) fn sync(&mut self, other: &Self) -> Vec<Query> {
        // This is the fast and most common path.
        if self.params.identical(&other.params) {
            return vec![];
        }

        // Get all parameters that we have, but other does not.
        // We don't want our value to persist, so we are resetting
        // them to their default server values.
        let reset: Vec<_> = self
            .params
            .tracked()
            .into_iter()
            .filter(|(name, _)| !other.params.contains_key(name.as_str()))
            .map(|(name, _)| reset_to_sql(name))
            .collect();

        // Get all parameters other wants to set on the server.
        let set: Vec<_> = other
            .params
            .tracked()
            .into_iter()
            .filter(|(name, value)| self.get(name) != Some(*value)) // Only set parameters that are different.
            .map(|(name, value)| param_to_sql(name, value, false))
            .collect();

        self.params.extend(other.params.clone());
        self.transaction_local_params
            .extend(other.transaction_local_params.clone());
        self.transaction_params
            .extend(other.transaction_params.clone());

        reset.into_iter().chain(set).collect()
    }

    /// Generate the queries we need to sync the server transaction
    /// to what the client expects.
    pub(crate) fn transaction_sync(&self) -> Vec<Query> {
        self.transaction_params
            .iter()
            .map(|(name, value)| param_to_sql(name, value, false))
            .chain(
                self.transaction_local_params
                    .iter()
                    .map(|(name, value)| param_to_sql(name, value, true)),
            )
            .collect()
    }

    fn transaction_end(&mut self) {
        self.transaction_params.clear();
        self.transaction_local_params.clear();
        self.reset_params.clear();
        self.in_transaction = false;
    }
}
