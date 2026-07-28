//! WAL record.
//!
//! See [`Record`] for documentation on the format.
//!
use bytes::{Buf, BufMut, Bytes};

use crate::net::{Error, FromBytes, Payload, ToBytes};

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

impl FromBytes for Record {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        if bytes.len() < 5 {
            return Err(Error::UnexpectedEof);
        }

        let code = bytes.get_u8() as char;
        let len = bytes.get_i32();
        if len < 4 {
            return Err(Error::UnexpectedEof);
        }

        let data_len = len as usize - 4;
        if bytes.remaining() < data_len {
            return Err(Error::UnexpectedEof);
        }

        let data = bytes.split_to(data_len);

        Ok(Self { code, data })
    }
}

impl ToBytes for Record {
    fn to_bytes(&self) -> Bytes {
        let mut payload = Payload::named(self.code);
        payload.put(self.data.clone());

        payload.freeze()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_invalid_length() {
        let bytes = Bytes::from_static(&[b'1', 0, 0, 0, 3]);

        assert!(matches!(
            Record::from_bytes(bytes),
            Err(Error::UnexpectedEof)
        ));
    }

    #[test]
    fn test_truncated_record() {
        let bytes = Bytes::from_static(&[b'1', 0, 0, 0, 5]);

        assert!(matches!(
            Record::from_bytes(bytes),
            Err(Error::UnexpectedEof)
        ));
    }
}
