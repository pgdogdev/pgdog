//! Authentication messages.

use super::{code, prelude::*};

use super::FromBytes;

/// Authentication messages.
#[derive(Debug)]
pub enum Authentication {
    /// AuthenticationOk (F)
    Ok,
}

impl FromBytes for Authentication {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes.get_u8() as char, 'R');

        let _len = bytes.get_i32();

        let status = bytes.get_i32();

        match status {
            0 => Ok(Authentication::Ok),
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
        }
    }
}
