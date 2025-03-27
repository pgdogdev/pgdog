use super::*;
use crate::net::messages::DataRow;

use bytes::{Buf, Bytes};

impl FromDataType for i64 {
    fn decode(bytes: &[u8], encoding: Format) -> Result<Self, Error> {
        match encoding {
            Format::Binary => {
                let val = match bytes.len() {
                    2 => {
                        let bytes: [u8; 2] = bytes.try_into()?;
                        bytes.as_slice().get_i16().into()
                    }

                    4 => {
                        let bytes: [u8; 4] = bytes.try_into()?;
                        bytes.as_slice().get_i32().into()
                    }

                    8 => {
                        let bytes: [u8; 8] = bytes.try_into()?;
                        bytes.as_slice().get_i64()
                    }

                    _ => return Err(Error::WrongSizeBinary(bytes.len())),
                };
                Ok(val)
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

impl From<DataRow> for i64 {
    fn from(value: DataRow) -> Self {
        value.get_int(0, true).unwrap_or(0)
    }
}
