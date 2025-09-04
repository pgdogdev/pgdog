use super::{code, prelude::*};

#[derive(Debug, Clone)]
pub(crate) struct CopyDone;

impl FromBytes for CopyDone {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'c');
        let _len = bytes.get_i32();

        Ok(Self)
    }
}

impl ToBytes for CopyDone {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        Ok(Payload::named(self.code()).freeze())
    }
}

impl Protocol for CopyDone {
    fn code(&self) -> char {
        'c'
    }
}

impl CopyDone {
    pub(crate) fn len(&self) -> usize {
        4
    }
}
