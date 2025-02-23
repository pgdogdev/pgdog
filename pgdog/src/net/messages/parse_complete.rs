//! ParseComplete (B) message.
use super::code;
use super::prelude::*;

#[derive(Debug, Clone)]
pub struct ParseComplete;

impl FromBytes for ParseComplete {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, '1');
        let _len = bytes.get_i32();
        Ok(Self)
    }
}

impl ToBytes for ParseComplete {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let payload = Payload::named(self.code());
        Ok(payload.freeze())
    }
}

impl Protocol for ParseComplete {
    fn code(&self) -> char {
        '1'
    }
}
