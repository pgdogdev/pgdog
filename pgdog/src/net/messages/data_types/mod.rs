use super::{bind::Format, Error};
use bytes::Bytes;

pub mod bigint;
pub mod integer;
pub mod interval;
pub mod text;
pub mod timestamp;
pub mod timestamptz;

pub trait DataType: Sized + PartialOrd + Ord + PartialEq {
    fn decode(bytes: Bytes, encoding: Format) -> Result<Self, Error>;
    fn encode(&self, encoding: Format) -> Result<Bytes, Error>;
}
