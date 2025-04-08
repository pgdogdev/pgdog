use super::code;
use super::prelude::*;

#[derive(Debug, Clone)]
pub struct CloseComplete;

impl Protocol for CloseComplete {
    fn code(&self) -> char {
        '3'
    }
}

impl FromBytes for CloseComplete {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, '3');
        Ok(Self)
    }
}

impl ToBytes for CloseComplete {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        Ok(Payload::named('3').freeze())
    }
}
