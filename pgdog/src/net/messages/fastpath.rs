//! Fastpath (F) message.

use super::code;
use super::prelude::*;

/// Fastpath function call message (frontend only, code `F`).
#[derive(Clone, PartialEq, Debug)]
pub struct Fastpath {
    body: Bytes,
}

impl Fastpath {
    pub fn len(&self) -> usize {
        self.body.len()
    }
}

impl FromBytes for Fastpath {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'F');
        let _len = bytes.get_i32();

        Ok(Self { body: bytes })
    }
}

impl ToBytes for Fastpath {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let mut payload = Payload::named(self.code());
        payload.put(self.body.clone());

        Ok(payload.freeze())
    }
}

impl Protocol for Fastpath {
    fn code(&self) -> char {
        'F'
    }
}
#[cfg(test)]
mod test {
    use super::*;
    use bytes::{BufMut, BytesMut};

    #[test]
    fn test_fastpath_roundtrip() {
        // Build a minimal Fastpath wire frame: code(1) + length(4) + body.
        let body = b"hello fastpath";
        let mut buf = BytesMut::new();
        buf.put_u8(b'F');
        buf.put_i32(4 + body.len() as i32);
        buf.put_slice(body);
        let original = buf.freeze();

        let fp = Fastpath::from_bytes(original.clone()).unwrap();
        assert_eq!(fp.len(), body.len());
        assert_eq!(fp.code(), 'F');

        // to_bytes must reproduce the exact wire frame.
        let serialized = fp.to_bytes().unwrap();
        assert_eq!(serialized, original);
    }
}
