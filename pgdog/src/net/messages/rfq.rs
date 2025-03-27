//! ReadyForQuery (B) message.

use crate::net::messages::{code, prelude::*};

// ReadyForQuery (F).
#[derive(Debug)]
pub struct ReadyForQuery {
    pub status: char,
}

impl ReadyForQuery {
    /// New idle message.
    pub fn idle() -> Self {
        ReadyForQuery { status: 'I' }
    }

    /// In transaction message.
    pub fn in_transaction(in_transaction: bool) -> Self {
        if in_transaction {
            ReadyForQuery { status: 'T' }
        } else {
            Self::idle()
        }
    }
}

impl ToBytes for ReadyForQuery {
    fn to_bytes(&self) -> Result<bytes::Bytes, Error> {
        let mut payload = Payload::named(self.code());
        payload.put_u8(self.status as u8);

        Ok(payload.freeze())
    }
}

impl FromBytes for ReadyForQuery {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'Z');

        let _len = bytes.get_i32();
        let status = bytes.get_u8() as char;

        Ok(Self { status })
    }
}

impl Protocol for ReadyForQuery {
    fn code(&self) -> char {
        'Z'
    }
}
