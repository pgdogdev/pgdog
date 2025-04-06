use bytes::Bytes;

use super::Error;
use super::Format;
use super::FromDataType;
use crate::net::DataRow;

impl FromDataType for bool {
    fn decode(bytes: &[u8], encoding: Format) -> Result<Self, Error> {
        match encoding {
            Format::Text => {
                let s = String::decode(bytes, encoding)?;
                match s.as_str() {
                    "t" => Ok(true),
                    "f" => Ok(false),
                    _ => Err(Error::UnexpectedPayload),
                }
            }

            Format::Binary => Err(Error::NotTextEncoding),
        }
    }

    fn encode(&self, encoding: Format) -> Result<Bytes, Error> {
        match encoding {
            Format::Text => Ok(Bytes::copy_from_slice(if *self {
                "t".as_bytes()
            } else {
                "f".as_bytes()
            })),
            Format::Binary => Err(Error::NotTextEncoding),
        }
    }
}

impl From<DataRow> for Option<bool> {
    fn from(value: DataRow) -> Self {
        value.get(0, Format::Text)
    }
}
