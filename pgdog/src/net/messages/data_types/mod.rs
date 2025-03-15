use std::ops::Add;

use super::{bind::Format, Error, ToDataRowColumn};
use ::uuid::Uuid;
use bytes::Bytes;

pub mod bigint;
pub mod integer;
pub mod interval;
pub mod numeric;
pub mod text;
pub mod timestamp;
pub mod timestamptz;
pub mod uuid;

pub use interval::Interval;
pub use numeric::Numeric;
pub use timestamp::Timestamp;
pub use timestamptz::TimestampTz;

pub trait FromDataType: Sized + PartialOrd + Ord + PartialEq {
    fn decode(bytes: &[u8], encoding: Format) -> Result<Self, Error>;
    fn encode(&self, encoding: Format) -> Result<Bytes, Error>;
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord)]
pub enum Datum {
    /// BIGINT.
    Bigint(i64),
    /// INTEGER.
    Integer(i32),
    /// SMALLINT.
    SmallInt(i16),
    /// INTERVAL.
    Interval(Interval),
    /// TEXT/VARCHAR.
    Text(String),
    /// TIMESTAMP.
    Timestamp(Timestamp),
    /// TIMESTAMPTZ.
    TimestampTz(TimestampTz),
    /// UUID.
    Uuid(Uuid),
    /// NUMERIC, REAL, DOUBLE PRECISION.
    Numeric(Numeric),
    /// NULL.
    Null,
}

impl ToDataRowColumn for Datum {
    fn to_data_row_column(&self) -> Bytes {
        use Datum::*;

        match self {
            Bigint(val) => val.to_data_row_column(),
            Integer(val) => (*val as i64).to_data_row_column(),
            SmallInt(val) => (*val as i64).to_data_row_column(),
            Interval(interval) => interval.to_data_row_column(),
            Text(text) => text.to_data_row_column(),
            Timestamp(t) => t.to_data_row_column(),
            TimestampTz(tz) => tz.to_data_row_column(),
            Uuid(uuid) => uuid.to_data_row_column(),
            Numeric(num) => num.to_data_row_column(),
            Null => Bytes::new(),
        }
    }
}

impl Add for Datum {
    type Output = Datum;

    fn add(self, rhs: Self) -> Self::Output {
        use Datum::*;

        match (self, rhs) {
            (Bigint(a), Bigint(b)) => Bigint(a + b),
            (Integer(a), Integer(b)) => Integer(a + b),
            (SmallInt(a), SmallInt(b)) => SmallInt(a + b),
            (Interval(a), Interval(b)) => Interval(a + b),
            (Numeric(a), Numeric(b)) => Numeric(a + b),
            _ => Datum::Null, // Might be good to raise an error.
        }
    }
}

impl Datum {
    pub fn new(bytes: &[u8], data_type: DataType, encoding: Format) -> Result<Self, Error> {
        if bytes.is_empty() {
            return Ok(Datum::Null);
        }

        match data_type {
            DataType::Bigint => Ok(Datum::Bigint(i64::decode(bytes, encoding)?)),
            DataType::Integer => Ok(Datum::Integer(i32::decode(bytes, encoding)?)),
            DataType::Text => Ok(Datum::Text(String::decode(bytes, encoding)?)),
            DataType::Interval => Ok(Datum::Interval(Interval::decode(bytes, encoding)?)),
            DataType::Numeric | DataType::DoublePrecision | DataType::Real => {
                Ok(Datum::Numeric(Numeric::decode(bytes, encoding)?))
            }
            DataType::Uuid => Ok(Datum::Uuid(Uuid::decode(bytes, encoding)?)),
            DataType::Timestamp => Ok(Datum::Timestamp(Timestamp::decode(bytes, encoding)?)),
            DataType::TimestampTz => Ok(Datum::TimestampTz(TimestampTz::decode(bytes, encoding)?)),
            _ => Ok(Datum::Null),
        }
    }
}

/// PostgreSQL data types.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum DataType {
    Bigint,
    Integer,
    Text,
    Interval,
    Timestamp,
    TimestampTz,
    Real,
    DoublePrecision,
    Bool,
    SmallInt,
    TinyInt,
    Numeric,
    Other(i32),
    Uuid,
}
