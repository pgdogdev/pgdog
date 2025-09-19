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
    /// GSSResponse (F) - Also uses code 'p'
    GssapiResponse { data: Vec<u8> },
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

    pub fn gssapi_response(data: Vec<u8>) -> Self {
        Self::GssapiResponse { data }
    }

    pub fn password(&self) -> Option<&str> {
        match self {
            Password::SASLInitialResponse { .. } => None,
            Password::PasswordMessage { response } => Some(response),
            Password::GssapiResponse { .. } => None,
        }
    }
}

impl FromBytes for Password {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'p');
        let _len = bytes.get_i32();

        // Check if this looks like a GSSAPI token
        // GSSAPI tokens typically start with ASN.1 tags like 0x60 (APPLICATION)
        // or other ASN.1 constructed tags
        if !bytes.is_empty() {
            let first_byte = bytes[0];
            if first_byte == 0x60 || first_byte == 0xa0 || first_byte == 0x6f {
                // This appears to be a GSSAPI token, read it as binary data
                let data = bytes.to_vec();
                return Ok(Self::GssapiResponse { data });
            }
        }

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

            Password::GssapiResponse { data } => {
                payload.put(Bytes::from(data.clone()));
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_gssapi_response() {
        let data = vec![10, 20, 30, 40, 50];
        let password = Password::gssapi_response(data.clone());
        let bytes = password.to_bytes().unwrap();

        // Verify the message starts with 'p' and contains our data
        assert_eq!(bytes[0], b'p');

        // The actual data should be at the end of the message
        let _payload_len = bytes.len() - 5; // Skip 'p' and 4-byte length
        let payload_start = 5;
        let payload = &bytes[payload_start..];
        assert_eq!(payload, &data[..]);
    }

    #[test]
    fn test_gssapi_response_password_method() {
        let data = vec![1, 2, 3];
        let password = Password::gssapi_response(data);
        // GssapiResponse should return None for password()
        assert_eq!(password.password(), None);
    }
}
