//! Startup parameter.

use std::{
    collections::BTreeMap,
    fmt::Display,
    ops::{Deref, DerefMut},
};

use once_cell::sync::Lazy;

use super::{messages::Query, Error};

static IMMUTABLE_PARAMS: Lazy<Vec<String>> = Lazy::new(|| {
    Vec::from([
        String::from("database"),
        String::from("user"),
        String::from("client_encoding"),
    ])
});

// static IMMUTABLE_PARAMS: &[&str] = &["database", "user", "client_encoding"];

/// Startup parameter.
#[derive(Debug, Clone, PartialEq)]
pub struct Parameter {
    /// Parameter name.
    pub name: String,
    /// Parameter value.
    pub value: String,
}

impl<T: ToString> From<(T, T)> for Parameter {
    fn from(value: (T, T)) -> Self {
        Self {
            name: value.0.to_string(),
            value: value.1.to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MergeResult {
    pub queries: Vec<Query>,
    pub changed_params: usize,
}

#[derive(Debug, Clone)]
pub enum ParameterValue {
    String(String),
    Tuple(Vec<String>),
}

impl PartialEq for ParameterValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::String(a), Self::String(b)) => a.eq(b),
            (Self::Tuple(a), Self::Tuple(b)) => a.eq(b),
            _ => false,
        }
    }
}

impl PartialEq<String> for ParameterValue {
    fn eq(&self, other: &String) -> bool {
        if let Self::String(s) = self {
            s.eq(other)
        } else {
            false
        }
    }
}

impl Display for ParameterValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::String(s) => write!(f, "'{}'", s),
            Self::Tuple(t) => write!(
                f,
                "{}",
                t.iter()
                    .map(|s| format!("'{}'", s))
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
    params: BTreeMap<String, ParameterValue>,
}

impl Parameters {
    /// Lowercase all param names.
    pub fn insert(
        &mut self,
        name: impl ToString,
        value: impl Into<ParameterValue>,
    ) -> Option<ParameterValue> {
        let name = name.to_string().to_lowercase();
        self.params.insert(name, value.into())
    }

    /// Merge params from self into other, generating the queries
    /// needed to sync that state on the server.
    pub fn merge(&self, other: &mut Self) -> MergeResult {
        let mut different = vec![];
        for (k, v) in &self.params {
            if IMMUTABLE_PARAMS.contains(k) {
                continue;
            }
            if let Some(other) = other.get(k) {
                if v != other {
                    different.push((k, v));
                }
            } else {
                different.push((k, v));
            }
        }

        for (k, v) in &different {
            other.insert(k.to_string(), (*v).clone());
        }

        let queries = if different.is_empty() {
            vec![]
        } else {
            let mut queries = vec![];

            for (k, v) in different {
                queries.push(Query::new(format!(r#"SET "{}" TO {}"#, k, v)));
            }

            queries
        };

        MergeResult {
            changed_params: if queries.is_empty() { 0 } else { queries.len() },
            queries,
        }
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
        Self {
            params: value
                .into_iter()
                .map(|p| (p.name, ParameterValue::String(p.value)))
                .collect(),
        }
    }
}

impl From<&Parameters> for Vec<Parameter> {
    fn from(val: &Parameters) -> Self {
        let mut result = vec![];
        for (key, value) in &val.params {
            result.push(Parameter {
                name: key.to_string(),
                value: value.to_string(),
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
    fn test_merge() {
        let mut me = Parameters::default();
        me.insert("application_name", "something");
        me.insert("TimeZone", "UTC");
        me.insert(
            "search_path",
            ParameterValue::Tuple(vec!["$user".into(), "public".into()]),
        );

        let mut other = Parameters::default();
        other.insert("TimeZone", "UTC");

        let diff = me.merge(&mut other);
        assert_eq!(diff.changed_params, 2);
        assert_eq!(diff.queries.len(), 2);
        assert_eq!(
            diff.queries[0].query(),
            r#"SET "application_name" TO 'something'"#
        );
        assert_eq!(
            diff.queries[1].query(),
            r#"SET "search_path" TO '$user', 'public'"#,
        );
    }
}
