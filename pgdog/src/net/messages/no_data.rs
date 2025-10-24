use super::{code, prelude::*};

#[derive(Debug, Clone, Default)]
pub struct NoData;

impl FromBytes for NoData {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'n');
        Ok(Self)
    }
}

impl ToBytes for NoData {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let payload = Payload::named(self.code());
        Ok(payload.freeze())
    }
}

impl Protocol for NoData {
    fn code(&self) -> char {
        'n'
    }
}
