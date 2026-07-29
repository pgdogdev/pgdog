//! WAL record.
//!
//! See [`Record`] for documentation on the format.
//!
use bytes::{Buf, BufMut, Bytes};

use crate::net::{Payload, ToBytes};

pub(crate) mod add;
pub(crate) mod records;
pub(crate) mod remove;
pub(crate) use add::*;
pub(crate) use records::*;
pub(crate) use remove::*;

/// Record format matches the [`crate::net::CopyData`] format.
///
/// # Format
///
/// | Column | Data type | Length   |
/// |--------|-----------|----------|
/// | code   | char      | 1        |
/// | length | i32       | 4        |
/// | data   | bytes     | variable |
///
///
/// Each record type is responsible for its own (de)serialization
/// and is identified by the `code` column.
///
#[derive(Debug, Clone)]
pub(crate) struct Record {
    pub(super) code: char, // Type of record it is.
    pub(super) data: Bytes,
}

impl Record {
    /// Size of this record in bytes.
    pub(crate) fn len(&self) -> usize {
        self.data.len() + 1 + 4
    }
}

impl ToBytes for Record {
    fn to_bytes(&self) -> Bytes {
        let mut payload = Payload::named(self.code);
        payload.put(self.data.clone());

        payload.freeze()
    }
}
