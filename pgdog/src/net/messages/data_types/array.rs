use bytes::{Buf, Bytes};

use super::{Error, Format, FromDataType};

#[derive(Debug, Clone, Ord, PartialOrd, PartialEq, Eq)]
pub struct Array {
    payload: Vec<Bytes>,
    oid: i32,
    flags: i32,
    dim: Dimension,
}

#[derive(Debug, Clone, Ord, PartialOrd, PartialEq, Eq, Default)]
struct Dimension {
    size: i32,
    lower_bound: i32,
}

impl FromDataType for Array {
    fn decode(bytes: &[u8], encoding: Format) -> Result<Self, Error> {
        match encoding {
            Format::Text => todo!(),
            Format::Binary => {
                let mut bytes = Bytes::copy_from_slice(bytes);
                let dims = bytes.get_i32() as usize;
                if dims > 1 {
                    return Err(Error::ArrayDimensions(dims));
                }
                let flags = bytes.get_i32();
                let oid = bytes.get_i32();

                let dim = Dimension {
                    size: bytes.get_i32(),
                    lower_bound: bytes.get_i32(),
                };

                let mut payload = vec![];

                while bytes.has_remaining() {
                    let len = bytes.get_i32();
                    if len < 0 {
                        payload.push(Bytes::new())
                    } else {
                        let element = bytes.split_to(len as usize);
                        payload.push(element);
                    }
                }

                Ok(Self {
                    payload,
                    oid,
                    flags,
                    dim,
                })
            }
        }
    }

    fn encode(&self, _encoding: Format) -> Result<Bytes, Error> {
        todo!()
    }
}
