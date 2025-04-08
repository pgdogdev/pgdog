//! Close (F) message.
use crate::net::c_string_buf;

use super::code;
use super::prelude::*;

#[derive(Debug, Clone)]
pub struct Close {
    pub kind: char,
    pub name: String,
}

impl Close {
    pub fn named(name: &str) -> Self {
        Self {
            kind: 'S',
            name: name.to_owned(),
        }
    }

    pub fn portal(name: &str) -> Self {
        Self {
            kind: 'P',
            name: name.to_owned(),
        }
    }

    pub fn anonymous(&self) -> bool {
        self.name.is_empty() || self.kind != 'S'
    }

    pub fn len(&self) -> usize {
        self.name.len() + 1 // NULL
        + 4 // len
        + 1 // code
        + 1 // kind
    }
}

impl FromBytes for Close {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'C');
        let _len = bytes.get_i32();
        let kind = bytes.get_u8() as char;
        let name = c_string_buf(&mut bytes);

        Ok(Self { kind, name })
    }
}

impl ToBytes for Close {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let mut payload = Payload::named(self.code());
        payload.put_u8(self.kind as u8);
        payload.put_string(&self.name);

        Ok(payload.freeze())
    }
}

impl Protocol for Close {
    fn code(&self) -> char {
        'C'
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_close() {
        let close = Close::named("test");
        assert_eq!(close.len(), close.to_bytes().unwrap().len());
    }
}
