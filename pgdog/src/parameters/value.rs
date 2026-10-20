use std::fmt::Display;

use bytes::{BufMut, Bytes, BytesMut};
use pgdog_postgres_types::{Data, ToDataRowColumn};

use crate::{net::ToBytes, stats::memory::MemoryUsage};

#[derive(Debug, Clone, Hash, PartialEq)]
pub(crate) enum ParameterValue {
    String(String),
    Tuple(Vec<String>),
}

impl ToBytes for ParameterValue {
    fn to_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::new();
        match self {
            Self::String(string) => bytes.put_slice(string.as_bytes()),
            Self::Tuple(values) => {
                let values = values
                    .iter()
                    .map(|value| value.as_bytes().to_vec())
                    .collect::<Vec<_>>()
                    .join(", ".as_bytes());
                bytes.put(Bytes::from(values));
            }
        }
        bytes.put_u8(0);

        bytes.freeze()
    }
}

impl ToDataRowColumn for ParameterValue {
    fn to_data_row_column(&self) -> Data {
        match self {
            Self::String(s) => s.to_data_row_column(),
            Self::Tuple(_) => self.to_bytes().to_data_row_column(),
        }
    }
}

impl ToDataRowColumn for &'_ ParameterValue {
    fn to_data_row_column(&self) -> Data {
        (*self).to_data_row_column()
    }
}

impl MemoryUsage for ParameterValue {
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
                value
            } else {
                value.to_string()
            };

            if value.is_empty() || value.contains("\"") {
                format!("'{}'", value.replace("'", "''"))
            } else {
                format!(r#""{}""#, value.replace("\"", "\"\""))
            }
        }
        match self {
            Self::String(s) => write!(f, "{}", quote(s)),
            Self::Tuple(t) => write!(
                f,
                "{}",
                t.iter()
                    .map(|s| quote(s).to_string())
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
    /// Get parameter value as a string. If the value is a tuple,
    /// returns nothing.
    pub(crate) fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(s) => Some(s.as_str()),
            _ => None,
        }
    }
}
