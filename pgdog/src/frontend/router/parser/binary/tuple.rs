use std::io::Read;
use std::ops::Deref;

use bytes::{Buf, Bytes, BytesMut};

use super::super::Error;
use super::header::Header;

#[derive(Debug, Clone)]
pub struct Tuple {
    row: Vec<Bytes>,
}

impl Tuple {
    pub(super) fn read(header: &Header, buf: &mut impl Buf) -> Result<Option<Self>, Error> {
        let num_cols = buf.get_i16();
        if num_cols == -1 {
            return Ok(None);
        }
        if header.has_oid {
            let _oid = buf.get_i32();
        }

        let mut row = vec![];
        for _ in 0..num_cols {
            let len = buf.get_i32();
            let mut bytes = BytesMut::zeroed(len as usize);
            buf.reader().read_exact(&mut bytes[..])?;
            row.push(bytes.freeze());
        }

        Ok(Some(Self { row }))
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
}

impl Deref for Tuple {
    type Target = Vec<Bytes>;

    fn deref(&self) -> &Self::Target {
        &self.row
    }
}
