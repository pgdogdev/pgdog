use super::code;
use super::prelude::*;

#[derive(Debug, Clone)]
pub struct Sync;

impl FromBytes for Sync {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'S');
        let _len = bytes.get_i32();

        Ok(Sync)
    }
}

impl Protocol for Sync {
    fn code(&self) -> char {
        'S'
    }
}

impl ToBytes for Sync {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let payload = Payload::named(self.code());
        Ok(payload.freeze())
    }
}
