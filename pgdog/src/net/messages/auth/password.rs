//! Password messages.

use crate::net::c_string_buf;

use super::super::code;
use super::super::prelude::*;

/// Password message.
#[derive(Debug)]
pub enum Password {
    /// SASLInitialResponse (F)
    SASLInitialResponse { name: String, response: String },
    /// PasswordMessage (F) or SASLResponse (F)
    /// TODO: This requires a NULL byte at end. Need to rewrite this struct.
    PasswordMessage { response: String },
}

impl Password {
    /// Create new SASL initial response.
    pub fn sasl_initial(response: &str) -> Self {
        Self::SASLInitialResponse {
            name: "SCRAM-SHA-256".to_string(),
            response: response.to_owned(),
        }
    }

    pub fn new_password(response: impl ToString) -> Self {
        Self::PasswordMessage {
            response: response.to_string() + "\0",
        }
    }

    pub fn password(&self) -> Option<&str> {
        match self {
            Password::SASLInitialResponse { .. } => None,
            Password::PasswordMessage { response } => Some(response),
        }
    }
}

impl FromBytes for Password {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'p');
        let _len = bytes.get_i32();
        let content = c_string_buf(&mut bytes);

        if bytes.has_remaining() {
            let len = bytes.get_i32();
            let response = if len >= 0 {
                c_string_buf(&mut bytes)
            } else {
                String::new()
            };

            Ok(Self::SASLInitialResponse {
                name: content,
                response,
            })
        } else {
            Ok(Password::PasswordMessage { response: content })
        }
    }
}

impl ToBytes for Password {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let mut payload = Payload::named(self.code());
        match self {
            Password::SASLInitialResponse { name, response } => {
                payload.put_string(name);
                payload.put_i32(response.len() as i32);
                payload.put(Bytes::copy_from_slice(response.as_bytes()));
            }

            Password::PasswordMessage { response } => {
                payload.put(Bytes::copy_from_slice(response.as_bytes()));
            }
        }

        Ok(payload.freeze())
    }
}

impl Protocol for Password {
    fn code(&self) -> char {
        'p'
    }
}
