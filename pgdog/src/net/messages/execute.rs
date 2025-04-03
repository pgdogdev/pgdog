use crate::net::c_string_buf;

use super::code;
use super::prelude::*;

#[derive(Debug, Clone)]
pub struct Execute {
    portal: String,
    rows: usize,
}

impl Default for Execute {
    fn default() -> Self {
        Self::new()
    }
}

impl Execute {
    pub fn new() -> Self {
        Self {
            portal: "".into(),
            rows: 0,
        }
    }
}

impl Protocol for Execute {
    fn code(&self) -> char {
        'E'
    }
}

impl FromBytes for Execute {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'E');
        let _len = bytes.get_i32();
        let portal = c_string_buf(&mut bytes);
        let rows = bytes.get_i32() as usize;

        Ok(Self { portal, rows })
    }
}

impl ToBytes for Execute {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let mut payload = Payload::named(self.code());
        payload.put_string(&self.portal);
        payload.put_i32(self.rows as i32);
        Ok(payload.freeze())
    }
}
