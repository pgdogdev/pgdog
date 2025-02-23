//! Describe (F) message.
use crate::net::c_string_buf;

use super::code;
use super::prelude::*;

/// Describe (F) message.
#[derive(Debug, Clone)]
pub struct Describe {
    pub kind: char,
    pub statement: String,
}

impl FromBytes for Describe {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'D');
        let _len = bytes.get_i32();
        let kind = bytes.get_u8() as char;
        let statement = c_string_buf(&mut bytes);

        Ok(Self { kind, statement })
    }
}

impl ToBytes for Describe {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let mut payload = Payload::named(self.code());
        payload.put_u8(self.kind as u8);
        payload.put_string(&self.statement);

        Ok(payload.freeze())
    }
}

impl Protocol for Describe {
    fn code(&self) -> char {
        'D'
    }
}

impl Describe {
    pub fn anonymous(&self) -> bool {
        self.kind != 'S' || self.statement.is_empty()
    }

    pub fn rename(mut self, name: impl ToString) -> Self {
        self.statement = name.to_string();
        self
    }
}
