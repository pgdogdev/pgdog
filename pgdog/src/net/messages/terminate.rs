//! Terminate (B & F) message.
use super::code;
use super::prelude::*;

/// Terminate the connection.
#[derive(Debug)]
pub struct Terminate;

impl FromBytes for Terminate {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'X');
        let _len = bytes.get_i32();

        Ok(Terminate)
    }
}

impl ToBytes for Terminate {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let payload = Payload::named(self.code());
        Ok(payload.freeze())
    }
}

impl Protocol for Terminate {
    fn code(&self) -> char {
        'X'
    }
}
