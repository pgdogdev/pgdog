//! CopyData (F & B) message.
use super::code;
use super::prelude::*;

/// CopyData (F & B) message.
#[derive(Debug, Clone)]
pub struct CopyData {
    data: Bytes,
}

impl CopyData {
    /// New copy data row.
    pub fn new(data: &[u8]) -> Self {
        Self {
            data: Bytes::copy_from_slice(data),
        }
    }

    /// Get copy data.
    pub fn data(&self) -> &[u8] {
        &self.data[..]
    }
}

impl FromBytes for CopyData {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'd');
        let _len = bytes.get_i32();

        Ok(Self { data: bytes })
    }
}

impl ToBytes for CopyData {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let mut payload = Payload::named(self.code());
        payload.put(self.data.clone());

        Ok(payload.freeze())
    }
}

impl Protocol for CopyData {
    fn code(&self) -> char {
        'd'
    }
}
