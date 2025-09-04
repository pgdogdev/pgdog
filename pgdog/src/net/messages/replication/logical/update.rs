use super::super::super::code;
use super::super::super::prelude::*;
use super::tuple_data::{Column, TupleData};

#[derive(Debug, Clone)]
pub(crate) struct Update {
    pub(crate) oid: i32,
    pub(crate) key: Option<TupleData>,
    pub(crate) old: Option<TupleData>,
    pub(crate) new: TupleData,
}

impl Update {
    /// Get column at index.
    pub(crate) fn column(&self, index: usize) -> Option<&Column> {
        self.new.columns.get(index)
    }
}

impl FromBytes for Update {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'U');
        let oid = bytes.get_i32();
        let identifier = bytes.get_u8() as char;

        let key = if identifier == 'K' {
            let key = TupleData::from_buffer(&mut bytes)?;
            Some(key)
        } else {
            None
        };

        let old = if identifier == 'O' {
            let old = TupleData::from_buffer(&mut bytes)?;
            Some(old)
        } else {
            None
        };

        let new = if identifier == 'N' {
            TupleData::from_bytes(bytes)?
        } else {
            code!(bytes, 'N');
            TupleData::from_bytes(bytes)?
        };

        Ok(Self { oid, key, old, new })
    }
}
