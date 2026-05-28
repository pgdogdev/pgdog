//! Flush (F) message.

use super::code;
use super::prelude::*;

/// Flush (F) message.
#[derive(Debug)]
pub struct Flush;

impl FromBytes for Flush {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'H');
        let _len = bytes.get_i32();

        Ok(Flush)
    }
}

impl ToBytes for Flush {
    fn to_bytes(&self) -> Bytes {
        let payload = Payload::named(self.code());
        payload.freeze()
    }
}

impl Protocol for Flush {
    fn code(&self) -> char {
        'H'
    }
}
