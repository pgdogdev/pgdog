use super::code;
use super::prelude::*;

#[derive(Debug, Clone)]
pub struct ParameterDescription {
    params: Vec<i32>,
}

impl FromBytes for ParameterDescription {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 't');
        let _len = bytes.get_i32();
        let num_params = bytes.get_i16();
        let mut params = vec![];
        for _ in 0..num_params {
            params.push(bytes.get_i32());
        }
        Ok(Self { params })
    }
}

impl ToBytes for ParameterDescription {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let mut payload = Payload::named(self.code());
        payload.put_i16(self.params.len() as i16);
        for param in &self.params {
            payload.put_i32(*param);
        }

        Ok(payload.freeze())
    }
}

impl Protocol for ParameterDescription {
    fn code(&self) -> char {
        't'
    }
}
