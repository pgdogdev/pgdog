use std::io::Read;
use std::ops::Deref;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::net::messages::ToBytes;

use super::super::Error;
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
    pub(super) fn read(header: &Header, buf: &mut impl Buf) -> Result<Option<Self>, Error> {
        if !buf.has_remaining() {
            return Ok(None);
        }
        let num_cols = buf.get_i16();
        if num_cols == -1 {
            return Ok(Some(Tuple {
                row: vec![],
                oid: None,
                end: true,
            }));
        }
        let oid = if header.has_oid {
            Some(buf.get_i32())
        } else {
            None
        };

        let mut row = vec![];
        for _ in 0..num_cols {
            let len = buf.get_i32();
            if len == -1 {
                row.push(Data::Null);
            } else {
                let mut bytes = BytesMut::zeroed(len as usize);
                buf.reader().read_exact(&mut bytes[..])?;
                row.push(Data::Column(bytes.freeze()));
            }
        }

        Ok(Some(Self {
            row,
            oid,
            end: false,
        }))
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

        Ok(result.freeze())
    }
}

impl Deref for Tuple {
    type Target = Vec<Data>;

    fn deref(&self) -> &Self::Target {
        &self.row
    }
}
