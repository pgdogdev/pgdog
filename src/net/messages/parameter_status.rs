//! ParameterStatus (B) message.

use super::{Payload, Protocol, ToBytes};

/// ParameterStatus (B) message.
pub struct ParameterStatus {
    name: String,
    value: String,
}

impl ParameterStatus {
    ///
    pub fn fake() -> Vec<ParameterStatus> {
        vec![
            ParameterStatus {
                name: "server_name".into(),
                value: "pgDog".into(),
            },
            ParameterStatus {
                name: "server_encoding".into(),
                value: "UTF8".into(),
            },
            ParameterStatus {
                name: "client_encoding".into(),
                value: "UTF8".into(),
            },
        ]
    }
}

impl ToBytes for ParameterStatus {
    fn to_bytes(&self) -> Result<bytes::Bytes, crate::net::Error> {
        let mut payload = Payload::named(self.code());

        payload.put_string(&self.name);
        payload.put_string(&self.value);

        Ok(payload.freeze())
    }
}

impl Protocol for ParameterStatus {
    fn code(&self) -> char {
        'S'
    }
}
