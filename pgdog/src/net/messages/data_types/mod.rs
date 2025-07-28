use std::cmp::Ordering;
use std::ops::Add;

use super::{bind::Format, data_row::Data, Error, ToDataRowColumn};
use ::uuid::Uuid;
use bytes::Bytes;

pub mod array;
pub mod bigint;
pub mod boolean;
pub mod integer;
pub mod interval;
pub mod numeric;
pub mod text;
pub mod timestamp;
pub mod timestamptz;
pub mod uuid;
pub mod vector;

pub use interval::Interval;
pub use numeric::Numeric;
pub use timestamp::Timestamp;
pub use timestamptz::TimestampTz;
pub use vector::Vector;

pub trait FromDataType: Sized + PartialOrd + Ord + PartialEq {
    fn decode(bytes: &[u8], encoding: Format) -> Result<Self, Error>;
    fn encode(&self, encoding: Format) -> Result<Bytes, Error>;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
    /// Vector
    Vector(Vector),
    /// We don't know.
    Unknown(Bytes),
    /// NULL.
    Null,
    /// Boolean
    Boolean(bool),
}

impl ToDataRowColumn for Datum {
    fn to_data_row_column(&self) -> Data {
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
            Vector(vector) => vector.to_data_row_column(),
            Unknown(bytes) => bytes.clone().into(),
            Null => Data::null(),
            Boolean(val) => val.to_data_row_column(),
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
            (Datum::Null, b) => b,
            (a, Datum::Null) => a,
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
            DataType::Vector => Ok(Datum::Vector(Vector::decode(bytes, encoding)?)),
            DataType::Bool => Ok(Datum::Boolean(bool::decode(bytes, encoding)?)),
            _ => Ok(Datum::Unknown(Bytes::copy_from_slice(bytes))),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Datum::Null)
    }

    pub fn encode(&self, format: Format) -> Result<Bytes, Error> {
        match self {
            Datum::Bigint(i) => i.encode(format),
            Datum::Integer(i) => i.encode(format),
            Datum::Uuid(uuid) => uuid.encode(format),
            Datum::Text(s) => s.encode(format),
            Datum::Boolean(b) => b.encode(format),
            _ => Err(Error::UnexpectedPayload),
        }
    }
}

impl PartialOrd for Datum {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use Datum::*;

        match (self, other) {
            // Null handling
            (Null, Null) => Some(Ordering::Equal),
            (Null, _) => Some(Ordering::Less),
            (_, Null) => Some(Ordering::Greater),

            // Same type comparisons
            (Bigint(a), Bigint(b)) => a.partial_cmp(b),
            (Integer(a), Integer(b)) => a.partial_cmp(b),
            (SmallInt(a), SmallInt(b)) => a.partial_cmp(b),
            (Text(a), Text(b)) => a.partial_cmp(b),
            (Boolean(a), Boolean(b)) => a.partial_cmp(b),
            (Timestamp(a), Timestamp(b)) => a.partial_cmp(b),
            (TimestampTz(a), TimestampTz(b)) => a.partial_cmp(b),
            (Numeric(a), Numeric(b)) => a.partial_cmp(b),
            (Interval(a), Interval(b)) => a.partial_cmp(b),
            (Uuid(a), Uuid(b)) => a.partial_cmp(b),
            (Vector(a), Vector(b)) => a.partial_cmp(b),
            (Unknown(a), Unknown(b)) => a.partial_cmp(b),

            // Cross-type numeric comparisons (optional, for compatibility)
            (Integer(a), Bigint(b)) => (*a as i64).partial_cmp(b),
            (Bigint(a), Integer(b)) => a.partial_cmp(&(*b as i64)),
            (SmallInt(a), Integer(b)) => (*a as i32).partial_cmp(b),
            (Integer(a), SmallInt(b)) => a.partial_cmp(&(*b as i32)),
            (SmallInt(a), Bigint(b)) => (*a as i64).partial_cmp(b),
            (Bigint(a), SmallInt(b)) => a.partial_cmp(&(*b as i64)),

            // Different types - no natural ordering
            _ => None,
        }
    }
}

impl Ord for Datum {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
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
    Vector,
}
