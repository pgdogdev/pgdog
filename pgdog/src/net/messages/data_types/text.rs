use std::str::from_utf8;

use super::*;
use crate::net::{messages::DataRow, Error};

use bytes::Bytes;

impl FromDataType for String {
    fn decode(bytes: &[u8], _: Format) -> Result<Self, Error> {
        Ok(from_utf8(bytes)?.to_owned())
    }

    fn encode(&self, _: Format) -> Result<Bytes, Error> {
        Ok(Bytes::copy_from_slice(self.as_bytes()))
    }
}

impl From<DataRow> for String {
    fn from(value: DataRow) -> Self {
        value.get_text(0).unwrap_or_default()
    }
}
