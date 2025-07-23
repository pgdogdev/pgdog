use super::*;
use bytes::{Buf, Bytes};

impl FromDataType for bool {
    fn decode(mut bytes: &[u8], encoding: Format) -> Result<Self, Error> {
        if bytes.is_empty() {
            return Err(Error::NotBoolean);
        }
        match encoding {
            Format::Text => {
                let s = String::decode(bytes, encoding)?;
                match s.as_str() {
                    "t" => Ok(true),
                    "f" => Ok(false),
                    _ => return Err(Error::NotBoolean),
                }
            }

            Format::Binary => match bytes.get_u8() {
                1 => Ok(true),
                0 => Ok(false),
                _ => return Err(Error::NotBoolean),
            },
        }
    }

    fn encode(&self, encoding: Format) -> Result<Bytes, Error> {
        match encoding {
            Format::Binary => match *self {
                true => Ok(Bytes::from(vec![1_u8])),
                false => Ok(Bytes::from(vec![0_u8])),
            },

            Format::Text => match *self {
                true => Ok(Bytes::from(b"t".to_vec())),
                false => Ok(Bytes::from(b"f".to_vec())),
            },
        }
    }
}
