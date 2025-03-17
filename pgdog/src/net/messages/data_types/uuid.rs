use std::str::FromStr;

use super::*;
use ::uuid::Uuid;

impl FromDataType for Uuid {
    fn decode(bytes: &[u8], encoding: Format) -> Result<Self, Error> {
        match encoding {
            Format::Text => {
                let s = String::decode(bytes, encoding)?;
                Ok(Uuid::from_str(&s)?)
            }

            Format::Binary => Ok(bytes.try_into().map(Uuid::from_bytes)?),
        }
    }

    fn encode(&self, encoding: Format) -> Result<Bytes, Error> {
        match encoding {
            Format::Text => Ok(Bytes::copy_from_slice(self.to_string().as_bytes())),
            Format::Binary => Ok(Bytes::copy_from_slice(self.as_bytes())),
        }
    }
}

impl ToDataRowColumn for Uuid {
    fn to_data_row_column(&self) -> Data {
        self.encode(Format::Text).unwrap().into()
    }
}
