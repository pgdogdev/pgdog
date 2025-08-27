//! # Logical Session Management in PgDog
//!
//! This module provides a unified logical session interface to coordinate and
//! validate session variables across shards in PgDog.
//!
//! PgDog emulates a single-node PostgreSQL interface for clients, hiding the
//! underlying sharded topology. The `LogicalSession` struct maintains session
//! state to guarantee consistent behavior across horizontally sharded backend
//! PostgreSQL servers.
//!
//! Session variables configured via `SET` commands are logically tracked and
//! propagated (fanned out) to relevant shards during multi-shard query execution.
//! This avoids inconsistencies in query behavior caused by differing variable
//! settings across shards.
//!
//! Example (valid on single-node Postgres, fanned out by PgDog):
//! -- SET search_path TO public;
//! -- SELECT * FROM users; -- PgDog fans out the SET to all relevant shards before querying.
//!
//! Counterexample (invalid if not fanned out):
//! -- SET TimeZone = 'UTC';
//! -- SELECT NOW(); -- Without fanout, shards might use different timezones.
//!
//! ## Future Improvements
//! - Optimize synchronization by tracking "synced" shards on a per-variable
//!   basis to minimize redundant `SET` commands.

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;

use super::router::parser::Shard;

// -----------------------------------------------------------------------------
// ----- LogicalSession --------------------------------------------------------

#[derive(Debug)]
pub struct LogicalSession<'a> {
    configuration_parameters: HashMap<ConfigParameter<'a>, ConfigValue>,
    synced_shards: HashSet<&'a Shard>,
    swapped_values: HashMap<(&'a Shard, ConfigParameter<'a>), ConfigValue>,
}

impl<'a> Default for LogicalSession<'a> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> LogicalSession<'a> {
    pub fn new() -> Self {
        Self {
            configuration_parameters: HashMap::new(),
            synced_shards: HashSet::new(),
            swapped_values: HashMap::new(),
        }
    }
}

// -----------------------------------------------------------------------------
// ----- Named Struct: ConfigParameter(&str) -----------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConfigParameter<'a>(&'a str);

impl<'a> From<&'a str> for ConfigParameter<'a> {
    fn from(s: &'a str) -> Self {
        ConfigParameter(s)
    }
}

impl<'a> fmt::Display for ConfigParameter<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

// -----------------------------------------------------------------------------
// ----- Named Struct: ConfigValue(Strig) --------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigValue(String);

impl From<String> for ConfigValue {
    fn from(s: String) -> Self {
        ConfigValue(s)
    }
}

impl From<&str> for ConfigValue {
    fn from(s: &str) -> Self {
        ConfigValue(s.to_owned())
    }
}

impl fmt::Display for ConfigValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

// -----------------------------------------------------------------------------
// ----- LogicalSession: Public methods ----------------------------------------

impl<'a> LogicalSession<'a> {
    /// Set a logical configuration parameter to a new value, clearing shard sync state.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the configuration parameter (e.g., "TimeZone").
    /// * `value` - The desired value (e.g., "UTC").
    pub fn set_variable<P, V>(&mut self, name: P, value: V) -> Result<(), SessionError>
    where
        P: Into<ConfigParameter<'a>>,
        V: Into<ConfigValue>,
    {
        let key = name.into();
        let val = value.into();
        Self::verify_can_set(key, &val)?;
        self.configuration_parameters.insert(key, val);
        self.synced_shards.clear();
        Ok(())
    }

    /// Retrieve the current value of a configuration parameter, if set.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the configuration parameter to lookup.
    pub fn get_variable<P>(&self, name: P) -> Option<ConfigValue>
    where
        P: Into<ConfigParameter<'a>>,
    {
        let key = name.into();
        self.configuration_parameters.get(&key).cloned()
    }

    /// Mark a shard as having been synced with the latest parameter state.
    ///
    /// # Arguments
    ///
    /// * `shard` - Reference to the shard that has been updated.
    pub fn sync_shard(&mut self, shard: &'a Shard) {
        self.synced_shards.insert(shard);
    }

    /// Check if a given shard is already synced with current parameters.
    ///
    /// # Arguments
    ///
    /// * `shard` - The shard to check sync status for.
    pub fn is_shard_synced(&self, shard: &Shard) -> bool {
        self.synced_shards.contains(shard)
    }

    /// Store the previous value pulled from a shard before overwriting it.
    ///
    /// # Arguments
    ///
    /// * `shard` - The shard where the swap occurred.
    /// * `name`  - The configuration parameter name.
    /// * `value` - The old value returned by the shard.
    ///
    /// # Example
    /// ```sql
    /// -- 1. Retrieve current TimeZone from shard 0:
    /// SHOW TimeZone;  -- returns 'UTC'
    ///
    /// -- 2. Internally store the old value before changing:
    /// -- LogicalSession::new().store_swapped_value(Shard::Direct(0), 'TimeZone', 'UTC');
    ///
    /// -- 3. Apply new setting:
    /// SET TimeZone = 'America/New_York';
    pub fn store_swapped_value<P, V>(&mut self, shard: &'a Shard, name: P, value: V)
    where
        P: Into<ConfigParameter<'a>>,
        V: Into<ConfigValue>,
    {
        let key = (shard, name.into());
        self.swapped_values.insert(key, value.into());
    }

    /// Remove and return the stored swapped value for a shard+parameter, if any.
    ///
    /// # Arguments
    ///
    /// * `shard` - The shard to retrieve the swapped value from.
    /// * `name`  - The configuration parameter name.
    pub fn take_swapped_value<P>(&mut self, shard: &'a Shard, name: P) -> Option<ConfigValue>
    where
        P: Into<ConfigParameter<'a>>,
    {
        let key = (shard, name.into());
        self.swapped_values.remove(&key)
    }

    /// Reset the session state (e.g., on connection close or explicit RESET).
    pub fn reset(&mut self) {
        self.configuration_parameters.clear();
        self.synced_shards.clear();
        self.swapped_values.clear();
    }

    /// Reset the session state, returning all stored swapped values before clearing.
    ///
    /// # Returns
    /// A map of `(Shard, ConfigParameter) -> ConfigValue` containing all swapped values.
    pub fn reset_after_take(&mut self) -> HashMap<(&'a Shard, ConfigParameter<'a>), ConfigValue> {
        // take swapped_values out and leave an empty map
        let prev = std::mem::take(&mut self.swapped_values);

        // clear other session state
        self.configuration_parameters.clear();
        self.synced_shards.clear();

        prev
    }
}

// -----------------------------------------------------------------------------
// ----- LogicalSession: Private methods ---------------------------------------

impl<'a> LogicalSession<'a> {
    /// Ensures the configuration parameters key and values are allowed.
    /// Currently whitelists everything.
    fn verify_can_set(
        _name: ConfigParameter<'a>,
        _value: &ConfigValue,
    ) -> Result<(), SessionError> {
        Ok(())
    }
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum SessionError {
    InvalidVariableName(String),
}

impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SessionError::InvalidVariableName(name) => {
                write!(
                    f,
                    "invalid or disallowed session configuration parameter: {}",
                    name
                )
            }
        }
    }
}

impl Error for SessionError {}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let session = LogicalSession::new();
        assert!(session.configuration_parameters.is_empty());
        assert!(session.synced_shards.is_empty());
        assert!(session.swapped_values.is_empty());
    }

    #[test]
    fn test_set_and_get_variable() {
        let mut session = LogicalSession::new();
        session.set_variable("TimeZone", "UTC").unwrap();
        let gotten = session.get_variable("TimeZone");

        assert_eq!(gotten, Some(ConfigValue("UTC".to_owned())));
        assert_eq!(session.get_variable("NonExistent"), None);
    }

    #[test]
    fn test_set_clears_synced_shards() {
        let mut session = LogicalSession::new();
        let shard1 = Shard::Direct(1);
        session.sync_shard(&shard1);
        assert!(session.is_shard_synced(&shard1));

        session.set_variable("search_path", "public").unwrap();
        assert!(!session.is_shard_synced(&shard1));
    }

    #[test]
    fn test_sync_and_check_shard() {
        let mut session = LogicalSession::new();
        let shard1 = Shard::Direct(1);
        let shard2 = Shard::Direct(2);

        session.sync_shard(&shard1);
        assert!(session.is_shard_synced(&shard1));
        assert!(!session.is_shard_synced(&shard2));

        session.sync_shard(&shard2);
        assert!(session.is_shard_synced(&shard2));
    }

    #[test]
    fn test_store_and_take_swapped_value() {
        let shard1 = Shard::Direct(1);
        let shard2 = Shard::Direct(2);

        let mut session = LogicalSession::new();
        session.store_swapped_value(&shard1, "TimeZone", "UTC");

        // Value can be taken once
        let taken = session.take_swapped_value(&shard1, "TimeZone");
        assert_eq!(taken, Some(ConfigValue("UTC".to_owned())));

        // Value that has been taken is not there
        let taken = session.take_swapped_value(&shard1, "TimeZone");
        assert_eq!(taken, None);

        // Value that has never been set is not there
        let taken = session.take_swapped_value(&shard2, "TimeZone");
        assert_eq!(taken, None);
    }

    #[test]
    fn test_reset() {
        let mut session = LogicalSession::new();
        let shard1 = Shard::Direct(1);

        session.set_variable("TimeZone", "UTC").unwrap();
        session.sync_shard(&shard1);
        session.store_swapped_value(&shard1, "TimeZone", "OldUTC");

        session.reset();
        assert!(session.configuration_parameters.is_empty());
        assert!(session.synced_shards.is_empty());
        assert!(session.swapped_values.is_empty());
    }

    #[test]
    fn test_multiple_operations() {
        let mut session = LogicalSession::new();
        let shard1 = Shard::Direct(1);
        let shard2 = Shard::Direct(2);

        session.set_variable("TimeZone", "UTC").unwrap();
        session.set_variable("search_path", "public").unwrap();
        assert_eq!(
            session.get_variable("TimeZone"),
            Some(ConfigValue("UTC".to_owned()))
        );

        session.sync_shard(&shard1);
        session.sync_shard(&shard2);
        assert!(session.is_shard_synced(&shard1));

        session.store_swapped_value(&shard1, "TimeZone", "America/New_York");
        assert_eq!(
            session.take_swapped_value(&shard1, "TimeZone"),
            Some(ConfigValue("America/New_York".to_owned()))
        );

        session.set_variable("TimeZone", "PST").unwrap(); // Should clear synced_shards
        assert!(!session.is_shard_synced(&shard1));
        assert!(!session.is_shard_synced(&shard2));
    }

    #[test]
    fn reset_after_take_returns_swapped_and_clears() {
        let mut sess = LogicalSession::new();
        let shard1 = &Shard::Direct(1);
        let shard2 = &Shard::Direct(2);
        sess.set_variable("a", "1").unwrap();

        // Caller has mapped over every shard, pulled their existing value and set "a" to "1".
        sess.store_swapped_value(shard1, "a", "2");
        sess.store_swapped_value(shard2, "a", "2");
        sess.sync_shard(shard1);

        let swapped = sess.reset_after_take();
        assert_eq!(swapped.len(), 2);
        assert_eq!(swapped.get(&(shard1, ConfigParameter("a"))).unwrap().0, "2");
        assert_eq!(swapped.get(&(shard2, ConfigParameter("a"))).unwrap().0, "2");

        assert!(sess.configuration_parameters.is_empty());
        assert!(sess.synced_shards.is_empty());
        assert!(sess.swapped_values.is_empty());
        assert!(sess.get_variable("a").is_none());
        assert!(!sess.is_shard_synced(shard1));
        assert!(sess.take_swapped_value(shard1, "b").is_none());
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
