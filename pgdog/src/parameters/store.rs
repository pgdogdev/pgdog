use crate::{net::Query, stats::memory::MemoryUsage};

use super::*;
use std::{mem::take, ops::Deref};

/// Handle the complexity of managing parameters
/// on the client and server.
#[derive(Default, Debug, Clone)]
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
    /// Create new parameter store from startup parameters.
    pub(crate) fn new(startup: Parameters) -> Self {
        Self {
            params: startup.clone(),
            startup,
            ..Default::default()
        }
    }

    /// Restore the startup state, discarding all session and transaction changes.
    pub(crate) fn restore_startup(&mut self) {
        self.params.restore_from(&self.startup);
        self.transaction_end();
    }

    /// Forget cached state after the server executes RESET or DISCARD.
    pub(crate) fn server_reset(&mut self) {
        self.params.clear();
    }

    pub(crate) fn get_default<'a>(&'a self, name: &str, default: &'a str) -> &'a str {
        self.get(name)
            .and_then(ParameterValue::as_str)
            .unwrap_or(default)
    }

    /// The value restored by RESET, before any session or transaction changes.
    pub(crate) fn get_startup(&self, name: &str) -> Option<&ParameterValue> {
        self.startup.get(name)
    }

    pub(crate) fn search_path(&self) -> Option<&ParameterValue> {
        self.get("search_path")
    }

    /// Commit parameters to store after a transaction is complete.
    pub(crate) fn commit(&mut self) -> bool {
        let changed = !self.transaction_params.is_empty() || !self.reset_params.is_empty();

        self.params.extend(take(&mut self.transaction_params));
        self.transaction_end();
        changed
    }

    /// Rollback changes to the store after a transaction is aborted.
    pub(crate) fn rollback(&mut self) -> bool {
        let changed = !self.reset_params.is_empty();
        self.params.extend(take(&mut self.reset_params));
        self.transaction_end();
        changed
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
            if self.in_transaction && !self.reset_params.contains_key(name) {
                self.reset_params.insert(name, exists);
            }
        }

        // Reset parameter to its startup value, if provided.
        if let Some(startup) = self.startup.get(name) {
            self.params.insert(name, startup.clone());
        }

        self.transaction_local_params.remove(name);
        self.transaction_params.remove(name);
    }

    /// Handle resetting all parameters to their initial values,
    /// accounting for transactional semantics.
    pub(crate) fn reset_all(&mut self) {
        let names: Vec<_> = self
            .params
            .keys()
            .chain(self.startup.keys())
            .filter(|name| !is_untracked(name.as_str()))
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
    ) -> Option<ParameterValue> {
        self.set_internal(name, value, false)
    }

    /// Insert a transaction-local parameter into the store, respecting transactional semantics.
    pub(crate) fn set_local(
        &mut self,
        name: impl AsRef<str>,
        value: impl Into<ParameterValue>,
    ) -> Option<ParameterValue> {
        self.set_internal(name, value, true)
    }

    fn set_internal(
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
    pub(crate) fn sync<'a>(&self, other: &'a Self) -> ParametersSync<'a> {
        let (queries, reset) = if !self.params.identical(&other.params) {
            // Get all parameters that we have, but other does not.
            // We don't want our value to persist, so we are resetting
            // them to their default server values.
            let reset_names: Vec<_> = self
                .params
                .tracked()
                .into_iter()
                .filter(|(name, _)| !other.params.contains_key(name.as_str()))
                .map(|(name, _)| name.to_string())
                .collect();

            let reset: Vec<_> = reset_names.iter().map(|name| reset_to_sql(name)).collect();

            // Get all parameters other wants to set on the server.
            let set: Vec<_> = other
                .params
                .tracked()
                .into_iter()
                .filter(|(name, value)| self.get(name.as_str()) != Some(*value)) // Only set parameters that are different.
                .map(|(name, value)| param_to_sql(name.as_str(), value, false))
                .collect();

            (reset.into_iter().chain(set).collect(), reset_names)
        } else {
            (vec![], vec![])
        };

        ParametersSync {
            reset,
            queries,
            source_store: other,
        }
    }

    /// Complete parameter synchronization.
    pub(crate) fn sync_complete(&mut self, sync: ParametersSync) {
        self.params.extend(sync.source_store.params.clone());
        // Remove parameters that were reset.
        self.params
            .remove_many(sync.reset.iter().map(|name| name.as_str()));
        self.transaction_local_params
            .restore_from(&sync.source_store.transaction_local_params);
        self.transaction_params
            .restore_from(&sync.source_store.transaction_params);
        self.in_transaction = sync.source_store.in_transaction;
    }

    /// Generate the queries we need to sync the server transaction
    /// to what the client expects.
    pub(crate) fn transaction_sync(&self) -> Vec<Query> {
        self.transaction_params
            .iter()
            .map(|(name, value)| param_to_sql(name.as_str(), value, false))
            .chain(
                self.transaction_local_params
                    .iter()
                    .map(|(name, value)| param_to_sql(name.as_str(), value, true)),
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

// TODO(lev): Remove this Deref
// and be more explicit about the state of parameters.
//
// Callers are expected to know when it's okay to read parameters, i.e.,
// bypassing `Self::get`, before a transaction started or after it finished,
// and the state of params is settled.
//
// This is obviously super bug-prone.
impl Deref for ParameterStore {
    type Target = Parameters;

    fn deref(&self) -> &Self::Target {
        &self.params
    }
}

impl MemoryUsage for ParameterStore {
    fn memory_usage(&self) -> usize {
        self.startup.memory_usage()
            + self.params.memory_usage()
            + self.transaction_params.memory_usage()
            + self.transaction_local_params.memory_usage()
            + self.reset_params.memory_usage()
            + self.in_transaction.memory_usage()
    }
}
