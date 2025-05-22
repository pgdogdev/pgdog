use super::{code, prelude::*};

#[derive(Debug, Copy, Clone, Default)]
pub struct EmptyQueryResponse;

impl FromBytes for EmptyQueryResponse {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'I');
        let _len = bytes.get_i32();

        Ok(Self)
    }
}

impl ToBytes for EmptyQueryResponse {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        Ok(Payload::named(self.code()).freeze())
    }
}

impl Protocol for EmptyQueryResponse {
    fn code(&self) -> char {
        'I'
    }
}
