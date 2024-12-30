//! ReadyForQuery message, indicating that the backend server
//! is ready to receive the next query.

use crate::net::messages::{code, prelude::*};

// ReadyForQuery (F).
#[derive(Debug)]
pub struct ReadyForQuery {
    status: char,
}

impl ReadyForQuery {
    /// New idle message.
    pub fn idle() -> Self {
        ReadyForQuery { status: 'I' }
    }

    /// In transaction message.
    pub fn in_transaction() -> Self {
        ReadyForQuery { status: 'T' }
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
        code!(bytes.get_u8() as char, 'R');

        let _len = bytes.get_i32();
        let status = bytes.get_u8() as char;

        Ok(Self { status })
    }
}

#[async_trait::async_trait]
impl Protocol for ReadyForQuery {
    fn code(&self) -> char {
        'Z'
    }
}
