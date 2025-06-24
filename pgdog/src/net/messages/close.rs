//! Close (F) message.
use std::fmt::Debug;
use std::str::from_utf8;
use std::str::from_utf8_unchecked;

use super::code;
use super::prelude::*;

#[derive(Clone)]
pub struct Close {
    payload: Bytes,
}

impl Debug for Close {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Close")
            .field("kind", &self.kind())
            .field("name", &self.name())
            .finish()
    }
}

impl Close {
    pub fn named(name: &str) -> Self {
        let mut payload = Payload::named('C');
        payload.put_u8(b'S');
        payload.put_string(name);
        Self {
            payload: payload.freeze(),
        }
    }

    pub fn portal(name: &str) -> Self {
        let mut payload = Payload::named('C');
        payload.put_u8(b'P');
        payload.put_string(name);
        Self {
            payload: payload.freeze(),
        }
    }

    pub fn anonymous(&self) -> bool {
        self.name().is_empty() || self.kind() != 'S'
    }

    pub fn is_statement(&self) -> bool {
        self.kind() == 'S'
    }

    pub fn name(&self) -> &str {
        // SAFETY: Name is checked for utf-8 in Bytes::from_bytes
        unsafe { from_utf8_unchecked(&self.payload[6..self.payload.len() - 1]) }
    }

    pub fn kind(&self) -> char {
        self.payload[5] as char
    }

    pub fn len(&self) -> usize {
        self.payload.len()
    }
}

impl FromBytes for Close {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        let original = bytes.clone();
        code!(bytes, 'C');
        from_utf8(&original[6..original.len() - 1])?;

        Ok(Self { payload: original })
    }
}

impl ToBytes for Close {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        Ok(self.payload.clone())
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
