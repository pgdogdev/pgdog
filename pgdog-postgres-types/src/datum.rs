use std::cmp::{Ordering, PartialOrd};
use std::fmt;

use bytes::Bytes;
use pgdog_vector::{Float, Vector};
use uuid::Uuid;

use crate::{
    Array, Data, Double, Error, Format, FromDataType, Interval, Numeric, Oid, Timestamp,
    TimestampTz, ToDataRowColumn,
};

/// Represents a single piece of data in expression position. Trait
/// implementations for Rust operators match the semantics of that
/// operator/opclass in expression position in PG
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

impl PartialOrd for Datum {
    fn partial_cmp(&self, other: &Datum) -> Option<Ordering> {
        use Datum::*;

        match (self, other) {
            (Bigint(a), Bigint(b)) => a.partial_cmp(b),
            (Bigint(_), _) | (_, Bigint(_)) => None,
            (Integer(a), Integer(b)) => a.partial_cmp(b),
            (Integer(_), _) | (_, Integer(_)) => None,
            (SmallInt(a), SmallInt(b)) => a.partial_cmp(b),
            (SmallInt(_), _) | (_, SmallInt(_)) => None,
            (Interval(a), Interval(b)) => a.partial_cmp(b),
            (Interval(_), _) | (_, Interval(_)) => None,
            (Text(a), Text(b)) => a.partial_cmp(b),
            (Text(_), _) | (_, Text(_)) => None,
            (Timestamp(a), Timestamp(b)) => a.partial_cmp(b),
            (Timestamp(_), _) | (_, Timestamp(_)) => None,
            (TimestampTz(a), TimestampTz(b)) => a.partial_cmp(b),
            (TimestampTz(_), _) | (_, TimestampTz(_)) => None,
            (Uuid(a), Uuid(b)) => a.partial_cmp(b),
            (Uuid(_), _) | (_, Uuid(_)) => None,
            (Numeric(a), Numeric(b)) => a.partial_cmp(b),
            (Numeric(_), _) | (_, Numeric(_)) => None,
            (Float(a), Float(b)) => a.partial_cmp(b),
            (Float(_), _) | (_, Float(_)) => None,
            (Double(a), Double(b)) => a.partial_cmp(b),
            (Double(_), _) | (_, Double(_)) => None,
            (Vector(a), Vector(b)) => a.partial_cmp(b),
            (Vector(_), _) | (_, Vector(_)) => None,
            (Oid(a), Oid(b)) => a.partial_cmp(b),
            (Oid(_), _) | (_, Oid(_)) => None,
            (Array(a), Array(b)) => a.partial_cmp(b),
            (Array(_), _) | (_, Array(_)) => None,
            (Unknown(a), Unknown(b)) => a.partial_cmp(b),
            (Unknown(_), _) | (_, Unknown(_)) => None,
            (Boolean(a), Boolean(b)) => a.partial_cmp(b),
            (Boolean(_), _) | (_, Boolean(_)) => None,
            (Null, _) => None,
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

    pub fn data_type(&self) -> DataType {
        match self {
            Datum::Bigint(..) => DataType::Bigint,
            Datum::Integer(..) => DataType::Integer,
            Datum::Uuid(..) => DataType::Uuid,
            Datum::Text(..) => DataType::Text,
            Datum::Boolean(..) => DataType::Bool,
            Datum::Float(..) => DataType::Real,
            Datum::Double(..) => DataType::DoublePrecision,
            Datum::Numeric(..) => DataType::Numeric,
            Datum::Timestamp(..) => DataType::Timestamp,
            Datum::TimestampTz(..) => DataType::TimestampTz,
            Datum::SmallInt(..) => DataType::SmallInt,
            Datum::Interval(..) => DataType::Interval,
            Datum::Vector(..) => DataType::Vector,
            Datum::Oid(..) => DataType::Oid,
            Datum::Array(a) => DataType::Array(a.element_oid),
            Datum::Null => DataType::Other(0),
            Datum::Unknown(..) => DataType::Other(0),
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

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        use DataType::*;
        match self {
            Bigint => write!(f, "bigint"),
            Integer => write!(f, "integer"),
            Text => write!(f, "text"),
            Interval => write!(f, "interval"),
            Timestamp => write!(f, "timestamp"),
            TimestampTz => write!(f, "timestamptz"),
            Real => write!(f, "real"),
            DoublePrecision => write!(f, "double precision"),
            Bool => write!(f, "boolean"),
            SmallInt => write!(f, "smallint"),
            TinyInt => write!(f, "tinyint"),
            Numeric => write!(f, "numeric"),
            Other(i) => write!(f, "unknown type {i}"),
            Uuid => write!(f, "uuid"),
            Oid => write!(f, "oid"),
            Vector => write!(f, "vector"),
            Array(i) => write!(f, "{}[]", Self::from_oid(*i)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};
    use std::assert_matches;

    #[test]
    fn test_multidimensional_text_array_falls_back_to_unknown() {
        let input = b"{{1,2},{3,4}}";
        let datum = Datum::new(input, DataType::Array(23), Format::Text, false).unwrap();

        assert_matches!(datum, Datum::Unknown(_));
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

        assert_matches!(datum, Datum::Unknown(_));
        assert_eq!(datum.encode(Format::Binary).unwrap(), input);
    }
}
