use super::{bind::Format, Error};
use ::uuid::Uuid;
use bytes::Bytes;
use interval::Interval;

pub mod bigint;
pub mod integer;
pub mod interval;
pub mod text;
pub mod timestamp;
pub mod timestamptz;
pub mod uuid;

pub trait DataType: Sized + PartialOrd + Ord + PartialEq {
    fn decode(bytes: &[u8], encoding: Format) -> Result<Self, Error>;
    fn encode(&self, encoding: Format) -> Result<Bytes, Error>;
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord)]
pub enum Datum {
    Bigint(i64),
    Integer(i32),
    Interval(Interval),
    Text(String),
    Timestamp,
    TeimstampTz,
    Uuid(Uuid),
}
