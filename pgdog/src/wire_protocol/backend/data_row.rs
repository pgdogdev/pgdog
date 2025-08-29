//! Module: wire_protocol::backend::data_row
//!
//! Provides parsing and serialization for the DataRow message ('D') in the protocol.
//!
//! - `DataRowFrame`: represents the DataRow message with column values.
//! - `DataRowError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `DataRowFrame`.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::{error::Error as StdError, fmt};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataRowFrame<'a> {
    pub columns: Vec<ColumnValue<'a>>,
}

// -----------------------------------------------------------------------------
// ----- Subproperties ---------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnValue<'a> {
    Null,
    Value(&'a [u8]),
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum DataRowError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
    UnexpectedEof,
    InvalidColumnLength(i32),
}

impl fmt::Display for DataRowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataRowError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
            DataRowError::UnexpectedLength(len) => write!(f, "unexpected length: {len}"),
            DataRowError::UnexpectedEof => write!(f, "unexpected EOF"),
            DataRowError::InvalidColumnLength(len) => write!(f, "invalid column length: {len}"),
        }
    }
}

impl StdError for DataRowError {}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for DataRowFrame<'a> {
    type Error = DataRowError;

    fn from_bytes(mut bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.remaining() < 7 {
            return Err(DataRowError::UnexpectedEof);
        }

        let tag = bytes.get_u8();
        if tag != b'D' {
            return Err(DataRowError::UnexpectedTag(tag));
        }

        let len = bytes.get_u32();
        if len < 6 {
            return Err(DataRowError::UnexpectedLength(len));
        }

        if bytes.remaining() != (len - 4) as usize {
            return Err(DataRowError::UnexpectedLength(len));
        }

        let num_columns = bytes.get_i16();
        if num_columns < 0 {
            return Err(DataRowError::UnexpectedLength(len));
        }

        let num = num_columns as usize;
        let mut columns = Vec::with_capacity(num);

        for _ in 0..num {
            if bytes.remaining() < 4 {
                return Err(DataRowError::UnexpectedEof);
            }

            let col_len = bytes.get_i32();
            let col_val = if col_len == -1 {
                ColumnValue::Null
            } else if col_len < 0 {
                return Err(DataRowError::InvalidColumnLength(col_len));
            } else {
                let col_len_usize = col_len as usize;
                if bytes.remaining() < col_len_usize {
                    return Err(DataRowError::UnexpectedEof);
                }

                let value = &bytes[0..col_len_usize];
                bytes = &bytes[col_len_usize..];
                ColumnValue::Value(value)
            };

            columns.push(col_val);
        }

        if bytes.has_remaining() {
            return Err(DataRowError::UnexpectedLength(len));
        }

        Ok(DataRowFrame { columns })
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        let mut body = BytesMut::with_capacity(self.body_size());
        body.put_i16(self.columns.len() as i16);
        for col in &self.columns {
            match col {
                ColumnValue::Null => body.put_i32(-1),
                ColumnValue::Value(val) => {
                    body.put_i32(val.len() as i32);
                    body.extend_from_slice(val);
                }
            }
        }

        let mut frame = BytesMut::with_capacity(body.len() + 5);
        frame.put_u8(b'D');
        frame.put_u32((body.len() + 4) as u32);
        frame.extend_from_slice(&body);

        Ok(frame.freeze())
    }

    fn body_size(&self) -> usize {
        2 + self
            .columns
            .iter()
            .map(|col| {
                4 + match col {
                    ColumnValue::Null => 0,
                    ColumnValue::Value(val) => val.len(),
                }
            })
            .sum::<usize>()
    }
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_frame<'a>() -> DataRowFrame<'a> {
        DataRowFrame {
            columns: vec![
                ColumnValue::Null,
                ColumnValue::Value(b"col2_value"),
                ColumnValue::Value(&[]), // empty
            ],
        }
    }

    #[test]
    fn serialize_data_row() {
        let frame = make_frame();
        let bytes = frame.to_bytes().unwrap();
        // 'D' + len(4 + 2 + 4*3 + 10 + 0) = len=4+2+12+10=28, u32=28
        // i16=3, -1, 10 + "col2_value", 0
        let expected =
            b"D\x00\x00\x00\x1C\x00\x03\xff\xff\xff\xff\x00\x00\x00\x0Acol2_value\x00\x00\x00\x00";
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn deserialize_data_row() {
        let data =
            b"D\x00\x00\x00\x1C\x00\x03\xff\xff\xff\xff\x00\x00\x00\x0Acol2_value\x00\x00\x00\x00";
        let frame = DataRowFrame::from_bytes(data).unwrap();
        assert_eq!(frame.columns.len(), 3);
        matches!(frame.columns[0], ColumnValue::Null);
        if let ColumnValue::Value(val) = frame.columns[1] {
            assert_eq!(val, b"col2_value");
        } else {
            panic!("expected Value");
        }
        if let ColumnValue::Value(val) = frame.columns[2] {
            assert_eq!(val, b"");
        } else {
            panic!("expected Value");
        }
    }

    #[test]
    fn roundtrip_data_row() {
        let original = make_frame();
        let bytes = original.to_bytes().unwrap();
        let decoded = DataRowFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn invalid_tag() {
        let data =
            b"E\x00\x00\x00\x1C\x00\x03\xff\xff\xff\xff\x00\x00\x00\x0Acol2_value\x00\x00\x00\x00";
        let err = DataRowFrame::from_bytes(data).unwrap_err();
        matches!(err, DataRowError::UnexpectedTag(_));
    }

    #[test]
    fn invalid_length_short() {
        let data = b"D\x00\x00\x00\x05";
        let err = DataRowFrame::from_bytes(data).unwrap_err();
        matches!(err, DataRowError::UnexpectedLength(_));
    }

    #[test]
    fn unexpected_eof() {
        let data = b"D\x00\x00\x00\x1C\x00\x03\xff\xff\xff\xff\x00\x00\x00\x0Acol2_valu"; // short by 1
        let err = DataRowFrame::from_bytes(data).unwrap_err();
        matches!(err, DataRowError::UnexpectedEof);
    }

    #[test]
    fn extra_data_after() {
        let data = b"D\x00\x00\x00\x1C\x00\x03\xff\xff\xff\xff\x00\x00\x00\x0Acol2_value\x00\x00\x00\x00\x00";
        let err = DataRowFrame::from_bytes(data).unwrap_err();
        matches!(err, DataRowError::UnexpectedLength(_));
    }

    #[test]
    fn invalid_column_length() {
        let data = b"D\x00\x00\x00\x0E\x00\x01\xff\xff\xff\xfe"; // len=-2
        let err = DataRowFrame::from_bytes(data).unwrap_err();
        matches!(err, DataRowError::InvalidColumnLength(-2));
    }

    #[test]
    fn empty_row() {
        let frame = DataRowFrame { columns: vec![] };
        let bytes = frame.to_bytes().unwrap();
        let expected = b"D\x00\x00\x00\x06\x00\x00";
        assert_eq!(bytes.as_ref(), expected);
        let decoded = DataRowFrame::from_bytes(expected).unwrap();
        assert_eq!(decoded.columns.len(), 0);
    }

    #[test]
    fn single_empty_value() {
        let frame = DataRowFrame {
            columns: vec![ColumnValue::Value(&[])],
        };

        let bytes = frame.to_bytes().unwrap();
        let expected = b"D\x00\x00\x00\x0A\x00\x01\x00\x00\x00\x00";
        assert_eq!(bytes.as_ref(), expected);

        let decoded = DataRowFrame::from_bytes(expected).unwrap();

        if let [ColumnValue::Value(val)] = &decoded.columns[..] {
            assert_eq!(*val, &[] as &[u8]);
        } else {
            panic!("expected empty Value");
        }
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
