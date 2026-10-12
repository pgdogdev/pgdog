use std::{collections::BTreeMap, fmt::Display, ops::Deref};

use crate::{
    net::{Error, Parameter},
    stats::memory::MemoryUsage,
};

use super::{ParameterValue, UNTRACKED_PARAMS, hash_parameters};

#[derive(Default, Debug, Clone, PartialEq)]
pub(crate) struct Parameters {
    parameters: BTreeMap<String, ParameterValue>,
    hash: u64,
}

impl Deref for Parameters {
    type Target = BTreeMap<String, ParameterValue>;

    fn deref(&self) -> &Self::Target {
        &self.parameters
    }
}

impl Parameters {
    /// Get a string parameter, or report that it is missing.
    pub(crate) fn get_required(&self, name: &str) -> Result<&str, Error> {
        self.get(name)
            .and_then(ParameterValue::as_str)
            .ok_or_else(|| Error::MissingParameter(name.into()))
    }

    /// Get a string parameter, falling back to the supplied default.
    pub(crate) fn get_default<'a>(&'a self, name: &str, default: &'a str) -> &'a str {
        self.get(name)
            .and_then(ParameterValue::as_str)
            .unwrap_or(default)
    }

    pub(crate) fn search_path(&self) -> Option<&ParameterValue> {
        self.get("search_path")
    }

    /// Create new parameters store from values.
    pub(crate) fn new(values: impl IntoIterator<Item = (String, ParameterValue)>) -> Self {
        let parameters: BTreeMap<_, _> = values
            .into_iter()
            .map(|(name, value)| (name.to_lowercase(), value))
            .collect();
        let hash = hash_parameters(parameters.iter());

        Self { parameters, hash }
    }

    /// Insert a parameter into the store, returning previous value if
    /// the parameter exists.
    pub(crate) fn insert(
        &mut self,
        name: impl AsRef<str>,
        value: impl Into<ParameterValue>,
    ) -> Option<ParameterValue> {
        let name = name.as_ref().to_lowercase();
        let existing = self.parameters.insert(name, value.into());
        self.hash = hash_parameters(self.parameters.iter());
        existing
    }

    /// Remove a parameter from the store, recomputing the hash if necessary.
    pub(crate) fn remove(&mut self, name: impl AsRef<str>) -> Option<ParameterValue> {
        let name = name.as_ref().to_lowercase();
        if let Some(exists) = self.parameters.remove(&name) {
            self.hash = hash_parameters(self.parameters.iter());
            Some(exists)
        } else {
            None
        }
    }

    /// Remove multiple parameters from the store.
    pub(crate) fn remove_many<'a>(&'a mut self, names: impl IntoIterator<Item = &'a str>) {
        for name in names.into_iter() {
            self.parameters.remove(name);
        }
        self.hash = hash_parameters(self.parameters.iter());
    }

    /// Compare hashes to quickly check if the two parameter stores
    /// are identical.
    ///
    /// This data structure makes modifications expensive, in order
    /// to make comparisons like this one very very cheap.
    pub(crate) fn identical(&self, other: &Self) -> bool {
        self.hash == other.hash
    }

    /// Reset parameters.
    pub(crate) fn clear(&mut self) {
        self.parameters.clear();
        self.hash = 0;
    }

    /// Copy parameters from other into self, recomputing the hash.
    pub(super) fn extend(&mut self, other: Self) {
        if !other.parameters.is_empty() {
            self.parameters.extend(other.parameters);
            self.hash = hash_parameters(self.parameters.iter());
        }
    }

    /// Get parameters we should be updating on the server/client
    /// during synchronization.
    pub(super) fn tracked(&self) -> impl IntoIterator<Item = (&String, &ParameterValue)> {
        self.parameters
            .iter()
            .filter(|(name, _)| !UNTRACKED_PARAMS.contains(&name.as_str()))
    }

    /// Delete all parameters and copy parameters from other,
    /// recomputing the hash.
    pub(super) fn restore_from(&mut self, other: &Self) {
        self.parameters.clone_from(&other.parameters);
        self.hash = hash_parameters(self.parameters.iter());
    }
}

impl<'a> From<&'a [(&'a str, &'a str)]> for Parameters {
    fn from(params: &[(&'a str, &'a str)]) -> Self {
        let mut parameters = Self::default();
        for (name, value) in params {
            parameters.insert(name, *value);
        }

        parameters
    }
}

impl Display for Parameters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let output = self
            .iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "{output}")
    }
}

impl MemoryUsage for Parameters {
    fn memory_usage(&self) -> usize {
        self.parameters.memory_usage() + self.hash.memory_usage()
    }
}

impl From<Vec<Parameter>> for Parameters {
    fn from(params: Vec<Parameter>) -> Self {
        Self::new(params.into_iter().map(|param| (param.name, param.value)))
    }
}

impl From<&Parameters> for Vec<Parameter> {
    fn from(params: &Parameters) -> Self {
        params
            .iter()
            .map(|(name, value)| Parameter {
                name: name.clone(),
                value: value.clone(),
            })
            .collect()
    }
}
