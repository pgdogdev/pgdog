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
    AuthenticationSASL(String),
    /// AuthenticationSASLContinue (B)
    AuthenticationSASLContinue(String),
    /// AuthenticationSASLFinal (B)
    AuthenticationSASLFinal(String),
}

impl FromBytes for Authentication {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'R');

        let _len = bytes.get_i32();

        let status = bytes.get_i32();

        match status {
            0 => Ok(Authentication::Ok),
            10 => {
                let mechanism = c_string_buf(&mut bytes);
                Ok(Authentication::AuthenticationSASL(mechanism))
            }
            11 => {
                let data = c_string_buf(&mut bytes);
                Ok(Authentication::AuthenticationSASLContinue(data))
            }
            12 => {
                let data = c_string_buf(&mut bytes);
                Ok(Authentication::AuthenticationSASLFinal(data))
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

            Authentication::AuthenticationSASL(mechanism) => {
                payload.put_i32(10);
                payload.put_string(&mechanism);

                Ok(payload.freeze())
            }

            Authentication::AuthenticationSASLContinue(data) => {
                payload.put_i32(11);
                payload.put_string(&data);

                Ok(payload.freeze())
            }

            Authentication::AuthenticationSASLFinal(data) => {
                payload.put_i32(12);
                payload.put_string(&data);

                Ok(payload.freeze())
            }
        }
    }
}
