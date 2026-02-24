use std::ops::Deref;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::net::messages::ToBytes;

use super::header::Header;

#[derive(Debug, Clone)]
pub enum Data {
    Null,
    Column(Bytes),
}

impl Data {
    pub fn len(&self) -> usize {
        match self {
            Self::Null => 0,
            Self::Column(bytes) => bytes.len(),
        }
    }

    pub fn encoded_len(&self) -> i32 {
        match self {
            Self::Null => -1,
            Self::Column(bytes) => bytes.len() as i32,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Debug, Clone)]
pub struct Tuple {
    row: Vec<Data>,
    oid: Option<i32>,
    end: bool,
}

impl Tuple {
    pub fn new(row: &[Data]) -> Self {
        Self {
            row: row.to_vec(),
            oid: None,
            end: false,
        }
    }

    pub fn new_end() -> Self {
        Self {
            row: vec![],
            oid: None,
            end: true,
        }
    }

    /// Calculate the total bytes needed to read a complete tuple.
    /// Returns None if there isn't enough data to determine the size.
    fn calculate_needed_bytes(header: &Header, data: &[u8]) -> Option<usize> {
        let mut offset = 0;

        // Need at least 2 bytes for num_cols
        if data.len() < 2 {
            return None;
        }
        let num_cols = i16::from_be_bytes([data[0], data[1]]);
        offset += 2;

        // Terminator (-1) only needs the 2 bytes we already checked
        if num_cols == -1 {
            return Some(2);
        }

        // OID if header has it
        if header.has_oid {
            offset += 4;
            if data.len() < offset {
                return None;
            }
        }

        // Each column has a 4-byte length, plus the data
        for _ in 0..num_cols {
            if data.len() < offset + 4 {
                return None;
            }
            let len = i32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]);
            offset += 4;

            if len >= 0 {
                offset += len as usize;
                if data.len() < offset {
                    return None;
                }
            }
        }

        Some(offset)
    }

    pub(super) fn read(header: &Header, buf: &mut impl Buf) -> Option<Self> {
        // Get a view of the buffer data to calculate needed bytes
        let data = buf.chunk();

        // Check if we have enough data for a complete tuple
        let needed = Self::calculate_needed_bytes(header, data)?;

        // We have enough data - now actually parse it
        let num_cols = buf.get_i16();
        if num_cols == -1 {
            return Some(Tuple {
                row: vec![],
                oid: None,
                end: true,
            });
        }

        let oid = if header.has_oid {
            Some(buf.get_i32())
        } else {
            None
        };

        let mut row = Vec::with_capacity(num_cols as usize);
        for _ in 0..num_cols {
            let len = buf.get_i32();
            if len == -1 {
                row.push(Data::Null);
            } else {
                let bytes = buf.copy_to_bytes(len as usize);
                row.push(Data::Column(bytes));
            }
        }

        debug_assert_eq!(
            needed,
            2 + row.len() * 4
                + row.iter().map(|r| r.len()).sum::<usize>()
                + if header.has_oid { 4 } else { 0 }
        );

        Some(Self {
            row,
            oid,
            end: false,
        })
    }

    pub(super) fn bytes_read(&self, header: &Header) -> usize {
        std::mem::size_of::<i16>()
            + self.row.len() * std::mem::size_of::<i32>()
            + (self.row.iter().map(|r| r.len()).sum::<usize>())
            + if header.has_oid {
                std::mem::size_of::<i32>()
            } else {
                0
            }
    }

    pub fn end(&self) -> bool {
        self.end
    }
}

impl ToBytes for Tuple {
    fn to_bytes(&self) -> Result<Bytes, crate::net::Error> {
        let mut result = BytesMut::new();
        if self.end {
            result.put_i16(-1);
        } else {
            result.put_i16(self.row.len() as i16);
            if let Some(oid) = self.oid {
                result.put_i32(oid);
            }
            for col in &self.row {
                result.put_i32(col.encoded_len());
                if let Data::Column(col) = col {
                    result.extend(col);
                }
            }
        }

        Ok(result.freeze())
    }
}

impl Deref for Tuple {
    type Target = Vec<Data>;

    fn deref(&self) -> &Self::Target {
        &self.row
    }
}
