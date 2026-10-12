use std::{collections::BTreeMap, ops::Deref};

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
    pub(super) fn tracked<'a>(
        &'a self,
    ) -> impl IntoIterator<Item = (&'a String, &'a ParameterValue)> {
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
