use std::ops::Add;

use bytes::Bytes;
use pgdog_vector::{Float, Vector};
use uuid::Uuid;

use crate::{
    Array, Data, Double, Error, Format, FromDataType, Interval, Numeric, Oid, Timestamp,
    TimestampTz, ToDataRowColumn,
};

#[derive(Debug, Clone, PartialEq)]
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
    /// NUMERIC.
    Numeric(Numeric),
    /// REAL (float4).
    Float(Float),
    /// DOUBLE PRECISION (float8).
    Double(Double),
    /// Vector
    Vector(Vector),
    /// OID.
    Oid(Oid),
    /// Array.
    Array(Array),
    /// We don't know.
    Unknown(Bytes),
    /// NULL.
    Null,
    /// Boolean
    Boolean(bool),
}

impl Eq for Datum {}

impl std::hash::Hash for Datum {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        use Datum::*;
        std::mem::discriminant(self).hash(state);
        match self {
            Bigint(v) => v.hash(state),
            Integer(v) => v.hash(state),
            SmallInt(v) => v.hash(state),
            Interval(v) => v.hash(state),
            Text(v) => v.hash(state),
            Timestamp(v) => v.hash(state),
            TimestampTz(v) => v.hash(state),
            Uuid(v) => v.hash(state),
            Numeric(v) => v.hash(state),
            Float(v) => {
                if v.0.is_nan() {
                    0u32.hash(state);
                } else {
                    v.0.to_bits().hash(state);
                }
            }
            Double(v) => {
                if v.0.is_nan() {
                    0u64.hash(state);
                } else {
                    v.0.to_bits().hash(state);
                }
            }
            Vector(v) => v.hash(state),
            Oid(v) => v.hash(state),
            Array(v) => v.hash(state),
            Unknown(v) => v.hash(state),
            Null => {}
            Boolean(v) => v.hash(state),
        }
    }
}

impl PartialOrd for Datum {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Datum {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use Datum::*;
        match (self, other) {
            (Bigint(a), Bigint(b)) => a.cmp(b),
            (Integer(a), Integer(b)) => a.cmp(b),
            (SmallInt(a), SmallInt(b)) => a.cmp(b),
            (Interval(a), Interval(b)) => a.cmp(b),
            (Text(a), Text(b)) => a.cmp(b),
            (Timestamp(a), Timestamp(b)) => a.cmp(b),
            (TimestampTz(a), TimestampTz(b)) => a.cmp(b),
            (Uuid(a), Uuid(b)) => a.cmp(b),
            (Numeric(a), Numeric(b)) => a.cmp(b),
            (Float(a), Float(b)) => a.cmp(b),
            (Double(a), Double(b)) => a.cmp(b),
            (Vector(a), Vector(b)) => a.cmp(b),
            (Oid(a), Oid(b)) => a.cmp(b),
            (Array(a), Array(b)) => a.cmp(b),
            (Unknown(a), Unknown(b)) => a.cmp(b),
            (Boolean(a), Boolean(b)) => a.cmp(b),
            (Null, Null) => std::cmp::Ordering::Equal,
            // For different variants, compare by their variant index
            _ => {
                fn variant_index(datum: &Datum) -> u8 {
                    match datum {
                        Datum::Bigint(_) => 0,
                        Datum::Integer(_) => 1,
                        Datum::SmallInt(_) => 2,
                        Datum::Interval(_) => 3,
                        Datum::Text(_) => 4,
                        Datum::Timestamp(_) => 5,
                        Datum::TimestampTz(_) => 6,
                        Datum::Uuid(_) => 7,
                        Datum::Numeric(_) => 8,
                        Datum::Float(_) => 9,
                        Datum::Double(_) => 10,
                        Datum::Vector(_) => 11,
                        Datum::Oid(_) => 12,
                        Datum::Array(_) => 13,
                        Datum::Unknown(_) => 14,
                        Datum::Null => 15,
                        Datum::Boolean(_) => 16,
                    }
                }
                variant_index(self).cmp(&variant_index(other))
            }
        }
    }
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
            Float(val) => val.to_data_row_column(),
            Double(val) => val.to_data_row_column(),
            Vector(vector) => vector.to_data_row_column(),
            Oid(oid) => oid.to_data_row_column(),
            Array(array) => array
                .encode(Format::Text)
                .expect("array text encode should succeed for any decoded array")
                .into(),
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
            (Float(a), Float(b)) => Float(crate::Float(a.0 + b.0)),
            (Double(a), Double(b)) => Double(crate::Double(a.0 + b.0)),
            (Datum::Null, b) => b,
            (a, Datum::Null) => a,
            _ => Datum::Null, // Might be good to raise an error.
        }
    }
}

impl Datum {
    pub fn new(
        bytes: &[u8],
        data_type: DataType,
        encoding: Format,
        null: bool,
    ) -> Result<Self, crate::Error> {
        if null {
            return Ok(Datum::Null);
        }

        match data_type {
            DataType::Bigint => Ok(Datum::Bigint(i64::decode(bytes, encoding)?)),
            DataType::Integer => Ok(Datum::Integer(i32::decode(bytes, encoding)?)),
            DataType::Text => Ok(Datum::Text(String::decode(bytes, encoding)?)),
            DataType::Interval => Ok(Datum::Interval(Interval::decode(bytes, encoding)?)),
            DataType::Numeric => Ok(Datum::Numeric(Numeric::decode(bytes, encoding)?)),
            DataType::Real => Ok(Datum::Float(Float::decode(bytes, encoding)?)),
            DataType::DoublePrecision => Ok(Datum::Double(Double::decode(bytes, encoding)?)),
            DataType::Uuid => Ok(Datum::Uuid(Uuid::decode(bytes, encoding)?)),
            DataType::Timestamp => Ok(Datum::Timestamp(Timestamp::decode(bytes, encoding)?)),
            DataType::TimestampTz => Ok(Datum::TimestampTz(TimestampTz::decode(bytes, encoding)?)),
            DataType::Vector => Ok(Datum::Vector(Vector::decode(bytes, encoding)?)),
            DataType::SmallInt => Ok(Datum::SmallInt(i16::decode(bytes, encoding)?)),
            DataType::Oid => Ok(Datum::Oid(Oid::decode(bytes, encoding)?)),
            DataType::Bool => Ok(Datum::Boolean(bool::decode(bytes, encoding)?)),
            DataType::Array(element_oid) => match Array::decode_typed(bytes, encoding, element_oid)
            {
                Ok(array) => Ok(Datum::Array(array)),
                Err(Error::ArrayDimensions(_)) => Ok(Datum::Unknown(Bytes::copy_from_slice(bytes))),
                Err(err) => Err(err),
            },
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
            Datum::Float(f) => f.encode(format),
            Datum::Double(d) => d.encode(format),
            Datum::Numeric(n) => n.encode(format),
            Datum::Timestamp(t) => t.encode(format),
            Datum::TimestampTz(tz) => tz.encode(format),
            Datum::SmallInt(i) => i.encode(format),
            Datum::Interval(i) => i.encode(format),
            Datum::Vector(v) => v.encode(format),
            Datum::Oid(o) => o.encode(format),
            Datum::Array(a) => a.encode(format),
            Datum::Null => Ok(Bytes::new()),
            Datum::Unknown(bytes) => Ok(bytes.clone()),
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
    Oid,
    Vector,
    /// Array type, carrying the element type OID.
    Array(i32),
}

impl DataType {
    /// Map a PostgreSQL type OID to a DataType.
    pub fn from_oid(oid: i32) -> Self {
        match oid {
            16 => DataType::Bool,
            20 => DataType::Bigint,
            21 => DataType::SmallInt,
            23 => DataType::Integer,
            25 => DataType::Text,
            26 => DataType::Oid,
            700 => DataType::Real,
            701 => DataType::DoublePrecision,
            1043 => DataType::Text, // varchar
            1114 => DataType::Timestamp,
            1184 => DataType::TimestampTz,
            1186 => DataType::Interval,
            1700 => DataType::Numeric,
            2950 => DataType::Uuid,
            // Array OIDs → Array(element_oid)
            1000 => DataType::Array(16),   // bool[]
            1005 => DataType::Array(21),   // int2[]
            1007 => DataType::Array(23),   // int4[]
            1009 => DataType::Array(25),   // text[]
            1015 => DataType::Array(1043), // varchar[]
            1016 => DataType::Array(20),   // int8[]
            1021 => DataType::Array(700),  // float4[]
            1022 => DataType::Array(701),  // float8[]
            1028 => DataType::Array(26),   // oid[]
            1115 => DataType::Array(1114), // timestamp[]
            1185 => DataType::Array(1184), // timestamptz[]
            1187 => DataType::Array(1186), // interval[]
            1231 => DataType::Array(1700), // numeric[]
            2951 => DataType::Array(2950), // uuid[]
            _ => DataType::Other(oid),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};

    #[test]
    fn test_multidimensional_text_array_falls_back_to_unknown() {
        let input = b"{{1,2},{3,4}}";
        let datum = Datum::new(input, DataType::Array(23), Format::Text, false).unwrap();

        assert!(matches!(datum, Datum::Unknown(_)));
        assert_eq!(
            datum.encode(Format::Text).unwrap(),
            Bytes::from_static(input)
        );
    }

    #[test]
    fn test_multidimensional_binary_array_falls_back_to_unknown() {
        let mut buf = BytesMut::new();
        buf.put_i32(2);
        buf.put_i32(0);
        buf.put_i32(23);
        buf.put_i32(2);
        buf.put_i32(1);
        buf.put_i32(2);
        buf.put_i32(1);

        for value in [1_i32, 2, 3, 4] {
            buf.put_i32(4);
            buf.put_i32(value);
        }

        let input = buf.freeze();
        let datum = Datum::new(&input, DataType::Array(23), Format::Binary, false).unwrap();

        assert!(matches!(datum, Datum::Unknown(_)));
        assert_eq!(datum.encode(Format::Binary).unwrap(), input);
    }
}
