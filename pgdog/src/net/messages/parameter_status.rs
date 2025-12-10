//! ParameterStatus (B) message.

use crate::net::{
    c_string_buf,
    messages::{code, prelude::*},
    parameter::ParameterValue,
    Parameter,
};

/// ParameterStatus (B) message.
#[derive(Debug)]
pub struct ParameterStatus {
    /// Parameter name, e.g. `client_encoding`.
    pub name: String,
    /// Parameter value, e.g. `UTF8`.
    pub value: ParameterValue,
}

impl From<Parameter> for ParameterStatus {
    fn from(value: Parameter) -> Self {
        ParameterStatus {
            name: value.name,
            value: value.value,
        }
    }
}

impl<T: ToString> From<(T, T)> for ParameterStatus {
    fn from(value: (T, T)) -> Self {
        Self {
            name: value.0.to_string(),
            value: ParameterValue::String(value.1.to_string()),
        }
    }
}

impl From<ParameterStatus> for Parameter {
    fn from(value: ParameterStatus) -> Self {
        Parameter {
            name: value.name,
            value: value.value,
        }
    }
}

impl ParameterStatus {
    /// Fake parameter status messages we can return
    /// to a client to make this seem like a legitimate PostgreSQL connection.
    pub fn fake() -> Vec<ParameterStatus> {
        vec![
            ParameterStatus {
                name: "server_name".into(),
                value: "PgDog".into(),
            },
            ParameterStatus {
                name: "server_encoding".into(),
                value: "UTF8".into(),
            },
            ParameterStatus {
                name: "client_encoding".into(),
                value: "UTF8".into(),
            },
            ParameterStatus {
                name: "server_version".into(),
                value: (env!("CARGO_PKG_VERSION").to_string() + " (PgDog)").into(),
            },
            ParameterStatus {
                name: "standard_conforming_strings".into(),
                value: "on".into(),
            },
        ]
    }
}

impl ToBytes for ParameterStatus {
    fn to_bytes(&self) -> Result<bytes::Bytes, crate::net::Error> {
        let mut payload = Payload::named(self.code());

        payload.put_string(&self.name);
        payload.put(self.value.to_bytes()?);

        Ok(payload.freeze())
    }
}

impl FromBytes for ParameterStatus {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'S');

        let _len = bytes.get_i32();

        let name = c_string_buf(&mut bytes);
        let mut value = c_string_buf(&mut bytes)
            .split(",")
            .map(|s| s.trim().to_string())
            .collect::<Vec<_>>();
        let value = if value.len() == 1 {
            ParameterValue::String(value.pop().unwrap())
        } else {
            ParameterValue::Tuple(value)
        };

        Ok(Self { name, value })
    }
}

impl Protocol for ParameterStatus {
    fn code(&self) -> char {
        'S'
    }
}
