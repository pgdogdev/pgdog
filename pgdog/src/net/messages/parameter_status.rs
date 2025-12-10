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

#[cfg(test)]
mod test {
    use super::*;
    use bytes::BytesMut;

    fn make_parameter_status_bytes(name: &str, value: &str) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u8(b'S');
        // Length: 4 bytes for len + name + null + value + null
        let len = 4 + name.len() + 1 + value.len() + 1;
        buf.put_i32(len as i32);
        buf.put_slice(name.as_bytes());
        buf.put_u8(0);
        buf.put_slice(value.as_bytes());
        buf.put_u8(0);
        buf.freeze()
    }

    #[test]
    fn test_from_bytes_single_value() {
        let bytes = make_parameter_status_bytes("client_encoding", "UTF8");
        let status = ParameterStatus::from_bytes(bytes).unwrap();

        assert_eq!(status.name, "client_encoding");
        assert_eq!(status.value, ParameterValue::String("UTF8".into()));
    }

    #[test]
    fn test_from_bytes_comma_separated_tuple() {
        let bytes = make_parameter_status_bytes("search_path", "$user, public");
        let status = ParameterStatus::from_bytes(bytes).unwrap();

        assert_eq!(status.name, "search_path");
        assert_eq!(
            status.value,
            ParameterValue::Tuple(vec!["$user".into(), "public".into()])
        );
    }

    #[test]
    fn test_from_bytes_comma_separated_with_extra_spaces() {
        let bytes = make_parameter_status_bytes("search_path", "  $user  ,  public  ");
        let status = ParameterStatus::from_bytes(bytes).unwrap();

        assert_eq!(status.name, "search_path");
        assert_eq!(
            status.value,
            ParameterValue::Tuple(vec!["$user".into(), "public".into()])
        );
    }

    #[test]
    fn test_to_bytes_roundtrip_string() {
        let original = ParameterStatus {
            name: "client_encoding".into(),
            value: ParameterValue::String("UTF8".into()),
        };

        let bytes = original.to_bytes().unwrap();
        let parsed = ParameterStatus::from_bytes(bytes).unwrap();

        assert_eq!(parsed.name, original.name);
        assert_eq!(parsed.value, original.value);
    }

    #[test]
    fn test_from_tuple() {
        let status: ParameterStatus = ("test_name", "test_value").into();

        assert_eq!(status.name, "test_name");
        assert_eq!(status.value, ParameterValue::String("test_value".into()));
    }

    #[test]
    fn test_from_parameter() {
        let param = Parameter {
            name: "search_path".into(),
            value: ParameterValue::Tuple(vec!["$user".into(), "public".into()]),
        };

        let status: ParameterStatus = param.into();

        assert_eq!(status.name, "search_path");
        assert_eq!(
            status.value,
            ParameterValue::Tuple(vec!["$user".into(), "public".into()])
        );
    }

    #[test]
    fn test_to_parameter() {
        let status = ParameterStatus {
            name: "timezone".into(),
            value: ParameterValue::String("UTC".into()),
        };

        let param: Parameter = status.into();

        assert_eq!(param.name, "timezone");
        assert_eq!(param.value, ParameterValue::String("UTC".into()));
    }
}
