//! Startup parameter.
use bytes::{BufMut, Bytes, BytesMut};
use tracing::debug;

use std::{
    collections::{btree_map, BTreeMap},
    fmt::Display,
    hash::{DefaultHasher, Hash, Hasher},
    ops::{Deref, DerefMut},
};

use once_cell::sync::Lazy;

use crate::{net::ToBytes, stats::memory::MemoryUsage};

use super::{messages::Query, Error};

static IMMUTABLE_PARAMS: Lazy<Vec<String>> = Lazy::new(|| {
    Vec::from([
        String::from("database"),
        String::from("user"),
        String::from("client_encoding"),
        String::from("replication"),
        String::from("pgdog.role"),
    ])
});

// static IMMUTABLE_PARAMS: &[&str] = &["database", "user", "client_encoding"];

/// Startup parameter.
#[derive(Debug, Clone, PartialEq)]
pub struct Parameter {
    /// Parameter name.
    pub name: String,
    /// Parameter value.
    pub value: ParameterValue,
}

impl<T: ToString> From<(T, T)> for Parameter {
    fn from(value: (T, T)) -> Self {
        Self {
            name: value.0.to_string(),
            value: ParameterValue::String(value.1.to_string()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MergeResult {
    pub queries: Vec<Query>,
    pub changed_params: usize,
}

#[derive(Debug, Clone, Hash, PartialEq)]
pub enum ParameterValue {
    String(String),
    Tuple(Vec<String>),
}

impl ToBytes for ParameterValue {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let mut bytes = BytesMut::new();
        match self {
            Self::String(string) => bytes.put_slice(string.as_bytes()),
            Self::Tuple(ref values) => {
                for value in values {
                    bytes.put_slice(value.as_bytes());
                }
            }
        }
        bytes.put_u8(0);

        Ok(bytes.freeze())
    }
}

impl MemoryUsage for ParameterValue {
    #[inline]
    fn memory_usage(&self) -> usize {
        match self {
            Self::String(v) => v.memory_usage(),
            Self::Tuple(vals) => vals.memory_usage(),
        }
    }
}

impl Display for ParameterValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn quote(value: &str) -> String {
            let value = if value.starts_with("\"") && value.ends_with("\"") {
                let mut value = value.to_string();
                value.remove(0);
                value.pop();
                value.replace("\"", "\"\"") // Escape any double quotes.
            } else {
                value.to_string()
            };

            format!(r#""{}""#, value)
        }
        match self {
            Self::String(s) => write!(f, "{}", quote(s)),
            Self::Tuple(t) => write!(
                f,
                "{}",
                t.iter()
                    .map(|s| format!("{}", quote(s)))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }
    }
}

impl From<&str> for ParameterValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

impl From<String> for ParameterValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl ParameterValue {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(s) => Some(s.as_str()),
            _ => None,
        }
    }
}

/// List of parameters.
#[derive(Default, Debug, Clone, PartialEq)]
pub struct Parameters {
    /// Save parameters set at connection startup & set with `SET` command
    /// outside a transaction.
    params: BTreeMap<String, ParameterValue>,
    /// Save parameters set with `SET` inside a transaction. These will
    /// need to be rolled back or saved depending on if the transaction is
    /// rolled back or not.
    transaction_params: BTreeMap<String, ParameterValue>,
    /// Parameters set with `SET LOCAL`. These need to be thrown away no matter
    /// what but we need to intercept them for databases that have cross shard
    /// queries disabled.
    transaction_local_params: BTreeMap<String, ParameterValue>,
    /// Hash of `params` to avoid syncing params between clients and servers
    /// when they are the same.
    hash: u64,
}

impl MemoryUsage for Parameters {
    #[inline]
    fn memory_usage(&self) -> usize {
        self.params.memory_usage() + self.hash.memory_usage()
    }
}

impl From<BTreeMap<String, ParameterValue>> for Parameters {
    fn from(value: BTreeMap<String, ParameterValue>) -> Self {
        let hash = Self::compute_hash(&value);
        Self {
            params: value,
            hash,
            transaction_params: BTreeMap::new(),
            transaction_local_params: BTreeMap::new(),
        }
    }
}

impl Parameters {
    /// Lowercase all param names.
    pub fn insert(
        &mut self,
        name: impl ToString,
        value: impl Into<ParameterValue>,
    ) -> Option<ParameterValue> {
        let name = name.to_string().to_lowercase();
        let result = self.params.insert(name, value.into());

        self.hash = Self::compute_hash(&self.params);

        result
    }

    /// Get parameter.
    pub fn get(&self, name: &str) -> Option<&ParameterValue> {
        if let Some(param) = self.transaction_local_params.get(name) {
            Some(param)
        } else if let Some(param) = self.transaction_params.get(name) {
            Some(param)
        } else {
            self.params.get(name)
        }
    }

    /// Get an iterator over in-transaction params.
    pub fn in_transaction_iter(&self) -> btree_map::Iter<'_, String, ParameterValue> {
        self.transaction_params.iter()
    }

    /// Insert a parameter, but only for the duration of the transaction.
    pub fn insert_transaction(
        &mut self,
        name: impl ToString,
        value: impl Into<ParameterValue>,
        local: bool,
    ) -> Option<ParameterValue> {
        let name = name.to_string().to_lowercase();
        if local {
            self.transaction_local_params.insert(name, value.into())
        } else {
            self.transaction_params.insert(name, value.into())
        }
    }

    /// Commit params we saved during the transaction.
    pub fn commit(&mut self) {
        debug!(
            "saved {} in-transaction params",
            self.transaction_params.len()
        );
        self.params
            .extend(std::mem::take(&mut self.transaction_params));
        self.transaction_local_params.clear();
        self.hash = Self::compute_hash(&self.params);
    }

    /// Remove any params we saved during the transaction.
    pub fn rollback(&mut self) {
        self.transaction_params.clear();
        self.transaction_local_params.clear();
    }

    fn compute_hash(params: &BTreeMap<String, ParameterValue>) -> u64 {
        let mut hasher = DefaultHasher::new();
        let mut entries = 0;

        for (k, v) in params {
            if IMMUTABLE_PARAMS.contains(k) {
                continue;
            }
            entries += 1;

            k.hash(&mut hasher);
            v.hash(&mut hasher);
        }

        if entries > 0 {
            hasher.finish()
        } else {
            0
        }
    }

    pub fn tracked(&self) -> Parameters {
        self.params
            .iter()
            .filter(|(k, _)| !IMMUTABLE_PARAMS.contains(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<BTreeMap<_, _>>()
            .into()
    }

    /// Merge params from self into other, generating the queries
    /// needed to sync that state on the server.
    pub fn identical(&self, other: &Self) -> bool {
        self.hash == other.hash
    }

    /// Generate SET queries to change server state.
    ///
    /// # Arguments
    ///
    /// * `transaction`: Generate `SET` statements from in-transaction params only.
    ///
    pub fn set_queries(&self, transaction_only: bool) -> Vec<Query> {
        fn query(name: &str, value: &ParameterValue, local: bool) -> Query {
            let set = if local { "SET LOCAL" } else { "SET" };
            Query::new(format!(r#"{} "{}" TO {}"#, set, name, value))
        }

        if transaction_only {
            let mut sets = self
                .transaction_params
                .iter()
                .map(|(key, value)| query(key, value, false))
                .collect::<Vec<_>>();

            sets.extend(
                self.transaction_local_params
                    .iter()
                    .map(|(key, value)| query(key, value, true)),
            );

            sets
        } else {
            self.params
                .iter()
                .map(|(key, value)| query(key, value, false))
                .collect()
        }
    }

    pub fn reset_queries(&self) -> Vec<Query> {
        self.params
            .keys()
            .map(|name| Query::new(format!(r#"RESET "{}""#, name)))
            .collect()
    }

    /// Get self-declared shard number.
    pub fn shard(&self) -> Option<usize> {
        if let Some(ParameterValue::String(application_name)) = self.get("application_name") {
            if application_name.starts_with("pgdog_shard_") {
                application_name
                    .replace("pgdog_shard_", "")
                    .parse::<usize>()
                    .ok()
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Get parameter value or returned an error.
    pub fn get_required(&self, name: &str) -> Result<&str, Error> {
        self.get(name)
            .and_then(|s| s.as_str())
            .ok_or(Error::MissingParameter(name.into()))
    }

    /// Get parameter value or returned a default value if it doesn't exist.
    pub fn get_default<'a>(&'a self, name: &str, default_value: &'a str) -> &'a str {
        self.get(name)
            .map_or(default_value, |p| p.as_str().unwrap_or(default_value))
    }

    /// Merge other into self.
    pub fn merge(&mut self, other: Self) {
        self.params.extend(other.params);
        self.transaction_params.extend(other.transaction_params);
        self.transaction_local_params
            .extend(other.transaction_local_params);
        Self::compute_hash(&self.params);
    }

    /// Copy params set inside the transaction.
    pub fn copy_in_transaction(&mut self, other: &Self) {
        self.transaction_params.extend(
            other
                .transaction_params
                .iter()
                .map(|(key, value)| (key.clone(), value.clone())),
        );
        self.transaction_local_params.extend(
            other
                .transaction_local_params
                .iter()
                .map(|(key, value)| (key.clone(), value.clone())),
        );
    }
}

impl Deref for Parameters {
    type Target = BTreeMap<String, ParameterValue>;

    fn deref(&self) -> &Self::Target {
        &self.params
    }
}

impl DerefMut for Parameters {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.params
    }
}

impl From<Vec<Parameter>> for Parameters {
    fn from(value: Vec<Parameter>) -> Self {
        let params = value
            .into_iter()
            .map(|p| (p.name, p.value))
            .collect::<BTreeMap<_, _>>();
        let hash = Self::compute_hash(&params);
        Self {
            params,
            hash,
            transaction_params: BTreeMap::new(),
            transaction_local_params: BTreeMap::new(),
        }
    }
}

impl From<&Parameters> for Vec<Parameter> {
    fn from(val: &Parameters) -> Self {
        let mut result = vec![];
        for (key, value) in &val.params {
            result.push(Parameter {
                name: key.to_string(),
                value: value.clone(),
            });
        }

        result
    }
}

#[cfg(test)]
mod test {
    use crate::net::parameter::ParameterValue;

    use super::Parameters;

    #[test]
    fn test_identical() {
        let mut me = Parameters::default();
        me.insert("application_name", "something");
        me.insert("TimeZone", "UTC");
        me.insert(
            "search_path",
            ParameterValue::Tuple(vec!["$user".into(), "public".into()]),
        );

        let mut other = Parameters::default();
        other.insert("TimeZone", "UTC");

        let same = me.identical(&other);
        assert!(!same);

        assert!(Parameters::default().identical(&Parameters::default()));
    }
}
