use super::*;
use bytes::{Buf, Bytes};

impl FromDataType for i32 {
    fn decode(bytes: &[u8], encoding: Format) -> Result<Self, Error> {
        match encoding {
            Format::Binary => {
                let bytes: [u8; 4] = bytes.try_into()?;
                Ok(bytes.as_slice().get_i32())
            }

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
