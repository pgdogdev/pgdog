//! Startup parameter.

use std::ops::{Deref, DerefMut};

use super::Error;

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
    /// Find a paramaeter by name.
    pub fn get(&self, name: &str) -> Option<&str> {
        self.params
            .iter().find(|p| p.name == name)
            .map(|p| p.value.as_str())
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
