//! Authentication messages.

use crate::net::c_string_buf;

use super::{code, prelude::*};

use super::FromBytes;

pub mod password;
pub use password::Password;

/// Authentication messages.
#[derive(Debug)]
pub enum Authentication {
    /// AuthenticationOk (F)
    Ok,
    /// AuthenticationSASL (B)
    Sasl(String),
    /// AuthenticationSASLContinue (B)
    SaslContinue(String),
    /// AuthenticationSASLFinal (B)
    SaslFinal(String),
    /// Md5 authentication challenge (B).
    Md5(Bytes),
    /// AuthenticationCleartextPassword (B).
    ClearTextPassword,
}

impl Authentication {
    /// Request SCRAM-SHA-256 auth.
    pub fn scram() -> Authentication {
        Authentication::Sasl("SCRAM-SHA-256".to_string())
    }
}

impl FromBytes for Authentication {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'R');

        let _len = bytes.get_i32();

        let status = bytes.get_i32();

        match status {
            0 => Ok(Authentication::Ok),
            3 => Ok(Authentication::ClearTextPassword),
            5 => {
                let mut salt = vec![0u8; 4];
                bytes.copy_to_slice(&mut salt);
                Ok(Authentication::Md5(Bytes::from(salt)))
            }
            10 => {
                let mechanism = c_string_buf(&mut bytes);
                Ok(Authentication::Sasl(mechanism))
            }
            11 => {
                let data = c_string_buf(&mut bytes);
                Ok(Authentication::SaslContinue(data))
            }
            12 => {
                let data = c_string_buf(&mut bytes);
                Ok(Authentication::SaslFinal(data))
            }
            status => Err(Error::UnsupportedAuthentication(status)),
        }
    }
}

impl Protocol for Authentication {
    fn code(&self) -> char {
        'R'
    }
}

impl ToBytes for Authentication {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let mut payload = Payload::named(self.code());

        match self {
            Authentication::Ok => {
                payload.put_i32(0);

                Ok(payload.freeze())
            }

            Authentication::ClearTextPassword => {
                payload.put_i32(3);
                Ok(payload.freeze())
            }

            Authentication::Md5(salt) => {
                payload.put_i32(5);
                payload.put(salt.clone());

                Ok(payload.freeze())
            }

            Authentication::Sasl(mechanism) => {
                payload.put_i32(10);
                payload.put_string(mechanism);
                payload.put_u8(0);

                Ok(payload.freeze())
            }

            Authentication::SaslContinue(data) => {
                payload.put_i32(11);
                payload.put(Bytes::copy_from_slice(data.as_bytes()));

                Ok(payload.freeze())
            }

            Authentication::SaslFinal(data) => {
                payload.put_i32(12);
                payload.put(Bytes::copy_from_slice(data.as_bytes()));

                Ok(payload.freeze())
            }
        }
    }
}
