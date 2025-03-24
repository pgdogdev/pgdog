//! Startup parameter.

use std::ops::{Deref, DerefMut};

use super::{messages::Query, Error};

static CHANGEABLE_PARAMS: &[&str] = &["application_name", "statement_timeout", "lock_timeout"];

/// Startup parameter.
#[derive(Debug, Clone, PartialEq)]
pub struct Parameter {
    /// Parameter name.
    pub name: String,
    /// Parameter value.
    pub value: String,
}

/// List of parameters.
#[derive(Default, Debug)]
pub struct Parameters {
    params: Vec<Parameter>,
}

impl Parameters {
    /// Find a parameter by name.
    pub fn get(&self, name: &str) -> Option<&str> {
        self.params
            .iter()
            .find(|p| p.name == name)
            .map(|p| p.value.as_str())
    }

    /// Set parameter to a value.
    ///
    /// We don't use a HashMap because clients/servers have very few params
    /// and its faster to iterate through a list than to use a hash (in theory).
    pub fn set(&mut self, name: &str, value: &str) -> bool {
        if !CHANGEABLE_PARAMS.contains(&name) {
            return false;
        }

        for param in self.params.iter_mut() {
            if param.name == name {
                if param.value != value {
                    param.value = value.to_string();
                    return true;
                } else {
                    return false;
                }
            }
        }

        self.params.push(Parameter {
            name: name.to_owned(),
            value: value.to_string(),
        });

        true
    }

    /// Merge params from self into other, generating the queries
    /// needed to sync that state on the server.
    pub fn merge(&self, other: &mut Self) -> Vec<Query> {
        let mut queries = vec![];
        for param in &self.params {
            let changed = other.set(&param.name, &param.value);
            if changed {
                queries.push(Query::new(format!(
                    "SET \"{}\" TO '{}'",
                    param.name, param.value
                )));
            }
        }

        queries
    }

    /// Get self-declared shard number.
    pub fn shard(&self) -> Option<usize> {
        self.params
            .iter()
            .find(|p| p.name == "application_name" && p.value.starts_with("pgdog_shard_"))
            .and_then(|param| {
                param
                    .value
                    .replace("pgdog_shard_", "")
                    .parse::<usize>()
                    .ok()
            })
    }

    /// Get parameter value or returned an error.
    pub fn get_required(&self, name: &str) -> Result<&str, Error> {
        self.get(name).ok_or(Error::MissingParameter(name.into()))
    }

    /// Get parameter value or returned a default value if it doesn't exist.
    pub fn get_default<'a>(&'a self, name: &str, default_value: &'a str) -> &'a str {
        self.get(name).map_or(default_value, |p| p)
    }
}

impl Deref for Parameters {
    type Target = Vec<Parameter>;

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
        Self { params: value }
    }
}
