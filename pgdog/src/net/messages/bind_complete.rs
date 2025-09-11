//! BindComplete (B) message.
use super::code;
use super::prelude::*;

#[derive(Debug, Clone)]
pub struct BindComplete;

impl FromBytes for BindComplete {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, '2');
        let _len = bytes.get_i32();
        Ok(Self)
    }
}

impl ToBytes for BindComplete {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let payload = Payload::named(self.code());
        Ok(payload.freeze())
    }
}

impl Protocol for BindComplete {
    fn code(&self) -> char {
        '2'
    }
}
