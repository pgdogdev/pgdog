use super::*;
use bytes::{Buf, Bytes};

impl DataType for i64 {
    fn decode(mut bytes: &[u8], encoding: Format) -> Result<Self, Error> {
        match encoding {
            Format::Binary => Ok(bytes.get_i64()),

            Format::Text => {
                let s = String::decode(bytes, Format::Text)?;
                Ok(s.parse()?)
            }
        }
    }

    fn encode(&self, encoding: Format) -> Result<Bytes, Error> {
        match encoding {
            Format::Text => Ok(Bytes::copy_from_slice(self.to_string().as_bytes())),
            Format::Binary => Ok(Bytes::copy_from_slice(&self.to_be_bytes())),
        }
    }
}
