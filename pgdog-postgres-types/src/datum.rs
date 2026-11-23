use std::cmp::{Ordering, PartialOrd};
use std::fmt;

use bytes::Bytes;
use pgdog_vector::{Float, Vector};
use rust_decimal::Decimal;
use uuid::Uuid;

use crate::{
    Array, Data, Double, Error, Format, FromDataType, Inet, Interval, Money, Numeric, Oid,
    Timestamp, TimestampTz, ToDataRowColumn,
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
    /// MONEY.
    Money(Money),
    /// INET or CIDR.
    Inet(Inet),
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
            (Money(a), Money(b)) => a.partial_cmp(b),
            (Money(_), _) | (_, Money(_)) => None,
            (Inet(a), Inet(b)) => a.partial_cmp(b),
            (Inet(_), _) | (_, Inet(_)) => None,
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
            Money(money) => money.to_data_row_column(),
            Inet(inet) => inet.to_data_row_column(),
            Unknown(bytes) => bytes.clone().into(),
            Null => Data::null(),
            Boolean(val) => val.to_data_row_column(),
        }
    }
}

impl<T> From<Option<T>> for Datum
where
    Datum: From<T>,
{
    fn from(val: Option<T>) -> Self {
        match val {
            Some(v) => v.into(),
            None => Datum::Null,
        }
    }
}

impl From<i64> for Datum {
    fn from(i: i64) -> Self {
        Datum::Bigint(i)
    }
}

impl From<f64> for Datum {
    fn from(f: f64) -> Self {
        Datum::Double(f.into())
    }
}

impl From<Decimal> for Datum {
    fn from(d: Decimal) -> Self {
        Datum::Numeric(d.into())
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
            DataType::Money => Ok(Datum::Money(Money::decode(bytes, encoding)?)),
            DataType::Inet => Ok(Datum::Inet(Inet::decode(bytes, encoding)?)),
            DataType::Cidr => Ok(Datum::Inet(Inet::decode(bytes, encoding)?.into_cidr())),
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
            Datum::Money(m) => m.encode(format),
            Datum::Inet(i) => i.encode(format),
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
            Datum::Money(..) => DataType::Money,
            Datum::Inet(inet) => inet.data_type(),
            Datum::Null => DataType::Other(0),
            Datum::Unknown(..) => DataType::Other(0),
        }
    }

    pub fn as_i64(&self) -> Result<i64, Error> {
        match *self {
            Datum::Bigint(i) => Ok(i),
            Datum::Integer(i) => Ok(i.into()),
            Datum::SmallInt(i) => Ok(i.into()),
            Datum::Float(f) => Ok(f.0 as i64),
            Datum::Double(f) => Ok(f.0 as i64),
            _ => Err(Error::InvalidCast {
                from: self.data_type(),
                to: DataType::Bigint,
            }),
        }
    }

    /// Orders numeric values of different widths the way PostgreSQL does.
    /// Returns `None` if either side isn't numeric.
    pub fn compare_numeric(&self, other: &Datum) -> Option<Ordering> {
        Some(self.as_i128()?.cmp(&other.as_i128()?))
    }

    fn as_i128(&self) -> Option<i128> {
        match self {
            Datum::Bigint(value) => Some((*value).into()),
            Datum::Integer(value) => Some((*value).into()),
            Datum::SmallInt(value) => Some((*value).into()),
            _ => None,
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
    Numeric,
    Other(i32),
    Uuid,
    Oid,
    Vector,
    /// Array type, carrying the element type OID.
    Array(i32),
    Money,
    Inet,
    Cidr,
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
            650 => DataType::Cidr,
            700 => DataType::Real,
            701 => DataType::DoublePrecision,
            790 => DataType::Money,
            869 => DataType::Inet,
            1043 => DataType::Text, // varchar
            1114 => DataType::Timestamp,
            1184 => DataType::TimestampTz,
            1186 => DataType::Interval,
            1700 => DataType::Numeric,
            2950 => DataType::Uuid,
            // Array OIDs → Array(element_oid)
            651 => DataType::Array(650),   // cidr[]
            791 => DataType::Array(790),   // money[]
            1000 => DataType::Array(16),   // bool[]
            1005 => DataType::Array(21),   // int2[]
            1007 => DataType::Array(23),   // int4[]
            1009 => DataType::Array(25),   // text[]
            1015 => DataType::Array(1043), // varchar[]
            1016 => DataType::Array(20),   // int8[]
            1021 => DataType::Array(700),  // float4[]
            1022 => DataType::Array(701),  // float8[]
            1028 => DataType::Array(26),   // oid[]
            1041 => DataType::Array(869),  // inet[]
            1115 => DataType::Array(1114), // timestamp[]
            1185 => DataType::Array(1184), // timestamptz[]
            1187 => DataType::Array(1186), // interval[]
            1231 => DataType::Array(1700), // numeric[]
            2951 => DataType::Array(2950), // uuid[]
            _ => DataType::Other(oid),
        }
    }

    pub fn to_oid(&self) -> Option<i32> {
        match self {
            DataType::Bool => Some(16),
            DataType::Bigint => Some(20),
            DataType::SmallInt => Some(21),
            DataType::Integer => Some(23),
            DataType::Text => Some(25), //or 1043 (varchar)
            DataType::Oid => Some(26),
            DataType::Real => Some(700),
            DataType::DoublePrecision => Some(701),
            DataType::Timestamp => Some(1114),
            DataType::TimestampTz => Some(1184),
            DataType::Interval => Some(1186),
            DataType::Numeric => Some(1700),
            DataType::Uuid => Some(2950),
            DataType::Array(oid) => Some(*oid),
            DataType::Other(oid) => Some(*oid),
            DataType::Vector => None,
            DataType::Money => todo!(),
            DataType::Inet => todo!(),
            DataType::Cidr => todo!(),
        }
    }

    /// Returns the OID of the array type of the built-in type `type_oid`, e.g., 3807 (jsonb[]) for jsonb (3802).
    ///
    /// Returns None for
    /// - array types (e.g., bigint[]), since Postgres has no array type for bigint[] (in pg_type, typarray is 0 for bigint[]).
    /// - types that aren't built into Postgres (e.g., enums or vector from pgvector), which aren't in `BUILTIN_ARRAY_TYPES`.
    pub fn array_type_oid(type_oid: i32) -> Option<i32> {
        for (element_oid, array_oid, _) in BUILTIN_ARRAY_TYPES {
            if element_oid == type_oid {
                return Some(array_oid);
            }
        }

        None
    }

    /// Returns the name of the built-in array type `array_oid` in pg_catalog, e.g., "_jsonb" for jsonb[] (3807).
    pub fn array_type_name(array_oid: i32) -> Option<&'static str> {
        for (_, oid, name) in BUILTIN_ARRAY_TYPES {
            if oid == array_oid {
                return Some(name);
            }
        }

        None
    }

    /// Returns the OID of the elements of the built-in array type `array_oid`, e.g., 20 (bigint) for bigint[] (1016).
    /// Returns None for types that aren't arrays of built-in types (e.g., bigint or arrays of enums).
    pub fn array_element_oid(array_oid: i32) -> Option<i32> {
        for (element_oid, oid, _) in BUILTIN_ARRAY_TYPES {
            if oid == array_oid {
                return Some(element_oid);
            }
        }

        None
    }

    pub fn pg_catalog_type_to_datatype(pg_catalog_type: &str) -> Option<Self> {
        Some(match pg_catalog_type {
            "int8" => DataType::Bigint,
            "int4" => DataType::Integer,
            "text" => DataType::Text,
            "interval" => DataType::Interval,
            "timestamp" => DataType::Timestamp,
            "timestamptz" => DataType::TimestampTz,
            "float4" => DataType::Real,
            "float8" => DataType::DoublePrecision,
            "bool" => DataType::Bool,
            "int2" => DataType::SmallInt,
            "numeric" => DataType::Numeric,
            "uuid" => DataType::Uuid,
            "oid" => DataType::Oid,
            "_bool" => DataType::Array(16),
            "_int2" => DataType::Array(21),
            "_int4" => DataType::Array(23),
            "_text" => DataType::Array(25),
            "_varchar" => DataType::Array(1043),
            "_int8" => DataType::Array(20),
            "_float4" => DataType::Array(700),
            "_float8" => DataType::Array(701),
            "_oid" => DataType::Array(26),
            "_timestamp" => DataType::Array(1114),
            "_timestamptz" => DataType::Array(1184),
            "_interval" => DataType::Array(1186),
            "_numeric" => DataType::Array(1700),
            "_uuid" => DataType::Array(2950),
            _ => return None,
        })
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
            Numeric => write!(f, "numeric"),
            Other(i) => write!(f, "unknown type {i}"),
            Uuid => write!(f, "uuid"),
            Oid => write!(f, "oid"),
            Vector => write!(f, "vector"),
            Array(i) => write!(f, "{}[]", Self::from_oid(*i)),
            Money => write!(f, "money"),
            Inet => write!(f, "inet"),
            Cidr => write!(f, "cidr"),
        }
    }
}

/// Types that are built into Postgres, as well as the array type of each type, e.g., (3802, 3807, "_jsonb") for jsonb and jsonb[]
/// Each entry tuple represents (the OID of the type, the OID of the array type, the name of the array type)
///
/// Generated on Postgres 18 with the query below. The query lists the types in pg_catalog that
/// - are built in (`oid < 10000`). Postgres assigns these OIDs in its source code
/// - have a binary format (`typsend <> 0 and typreceive <> 0`), to ensure that we can copy values of these types into arrays in binary format without decoding the values
/// - are base types (e.g., int8), range types (e.g., int4range) or multirange types (e.g., int4multirange) (`typtype in ('b','r','m')`)
///
/// `select oid, typarray, (select typname from pg_type a where a.oid = t.typarray) from pg_type t where typnamespace = 'pg_catalog'::regnamespace and typarray <> 0 and oid < 10000 and typtype in ('b','r','m') and typsend <> 0 and typreceive <> 0 order by oid`
///
/// <https://www.postgresql.org/docs/current/datatype.html>
const BUILTIN_ARRAY_TYPES: [(i32, i32, &str); 74] = [
    (16, 1000, "_bool"),
    (17, 1001, "_bytea"),
    (18, 1002, "_char"),
    (19, 1003, "_name"),
    (20, 1016, "_int8"),
    (21, 1005, "_int2"),
    (22, 1006, "_int2vector"),
    (23, 1007, "_int4"),
    (24, 1008, "_regproc"),
    (25, 1009, "_text"),
    (26, 1028, "_oid"),
    (27, 1010, "_tid"),
    (28, 1011, "_xid"),
    (29, 1012, "_cid"),
    (30, 1013, "_oidvector"),
    (114, 199, "_json"),
    (142, 143, "_xml"),
    (600, 1017, "_point"),
    (601, 1018, "_lseg"),
    (602, 1019, "_path"),
    (603, 1020, "_box"),
    (604, 1027, "_polygon"),
    (628, 629, "_line"),
    (650, 651, "_cidr"),
    (700, 1021, "_float4"),
    (701, 1022, "_float8"),
    (718, 719, "_circle"),
    (774, 775, "_macaddr8"),
    (790, 791, "_money"),
    (829, 1040, "_macaddr"),
    (869, 1041, "_inet"),
    (1042, 1014, "_bpchar"),
    (1043, 1015, "_varchar"),
    (1082, 1182, "_date"),
    (1083, 1183, "_time"),
    (1114, 1115, "_timestamp"),
    (1184, 1185, "_timestamptz"),
    (1186, 1187, "_interval"),
    (1266, 1270, "_timetz"),
    (1560, 1561, "_bit"),
    (1562, 1563, "_varbit"),
    (1700, 1231, "_numeric"),
    (1790, 2201, "_refcursor"),
    (2202, 2207, "_regprocedure"),
    (2203, 2208, "_regoper"),
    (2204, 2209, "_regoperator"),
    (2205, 2210, "_regclass"),
    (2206, 2211, "_regtype"),
    (2950, 2951, "_uuid"),
    (2970, 2949, "_txid_snapshot"),
    (3220, 3221, "_pg_lsn"),
    (3614, 3643, "_tsvector"),
    (3615, 3645, "_tsquery"),
    (3734, 3735, "_regconfig"),
    (3769, 3770, "_regdictionary"),
    (3802, 3807, "_jsonb"),
    (3904, 3905, "_int4range"),
    (3906, 3907, "_numrange"),
    (3908, 3909, "_tsrange"),
    (3910, 3911, "_tstzrange"),
    (3912, 3913, "_daterange"),
    (3926, 3927, "_int8range"),
    (4072, 4073, "_jsonpath"),
    (4089, 4090, "_regnamespace"),
    (4096, 4097, "_regrole"),
    (4191, 4192, "_regcollation"),
    (4451, 6150, "_int4multirange"),
    (4532, 6151, "_nummultirange"),
    (4533, 6152, "_tsmultirange"),
    (4534, 6153, "_tstzmultirange"),
    (4535, 6155, "_datemultirange"),
    (4536, 6157, "_int8multirange"),
    (5038, 5039, "_pg_snapshot"),
    (5069, 271, "_xid8"),
];

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};
    use std::assert_matches;

    #[test]
    fn test_compare_numeric_across_integer_widths() {
        for (left, right, expected) in [
            (Datum::Bigint(2), Datum::Integer(2), Ordering::Equal),
            (Datum::Integer(2), Datum::SmallInt(3), Ordering::Less),
            (Datum::SmallInt(3), Datum::Bigint(2), Ordering::Greater),
            (
                Datum::Bigint(i64::MAX),
                Datum::Integer(i32::MAX),
                Ordering::Greater,
            ),
        ] {
            assert_eq!(left.compare_numeric(&right), Some(expected));
        }

        assert_eq!(
            Datum::Text("2".into()).compare_numeric(&Datum::Integer(2)),
            None
        );
        assert_eq!(Datum::Null.compare_numeric(&Datum::Integer(2)), None);
    }

    fn text_datum(oid: i32, value: &str) -> Datum {
        Datum::new(
            value.as_bytes(),
            DataType::from_oid(oid),
            Format::Text,
            false,
        )
        .expect(value)
    }

    #[test]
    fn test_money_inet_oids_decode_to_typed_datums() {
        for (oid, value, data_type) in [
            (790, "$5.00", DataType::Money),
            (869, "10.0.0.10", DataType::Inet),
            (650, "10.0.0.0/8", DataType::Cidr),
            (791, "{$5.00}", DataType::Array(790)),
            (1041, "{10.0.0.10}", DataType::Array(869)),
            (651, "{10.0.0.0/8}", DataType::Array(650)),
        ] {
            assert_eq!(text_datum(oid, value).data_type(), data_type, "{value}");
        }
    }

    #[test]
    fn test_money_inet_data_row_column_is_original_text() {
        for (oid, value) in [(790, "₹1,00,000.00"), (869, "10.0.0.1/32")] {
            let column = text_datum(oid, value).to_data_row_column();
            assert_eq!(column.data, Bytes::from(value));
        }
    }

    #[test]
    fn test_money_inet_do_not_compare_with_other_types() {
        let money = text_datum(790, "$5.00");
        let inet = text_datum(869, "10.0.0.1");
        assert_eq!(money.partial_cmp(&Datum::Bigint(500)), None);
        assert_eq!(inet.partial_cmp(&money), None);

        for (data_type, name) in [
            (DataType::Money, "money"),
            (DataType::Inet, "inet"),
            (DataType::Cidr, "cidr"),
        ] {
            assert_eq!(data_type.to_string(), name);
        }
    }

    #[test]
    fn test_type_arrays_compare_elements_by_value() {
        let larger = text_datum(791, r#"{"₹1,000.00"}"#);
        let smaller = text_datum(791, "{₹5.00}");
        assert_eq!(larger.partial_cmp(&smaller), Some(Ordering::Greater));

        let larger = text_datum(1041, "{10.0.0.10}");
        let smaller = text_datum(1041, "{9.0.0.1}");
        assert_eq!(larger.partial_cmp(&smaller), Some(Ordering::Greater));
        assert_eq!(
            larger.encode(Format::Text).unwrap(),
            Bytes::from_static(b"{10.0.0.10}")
        );
    }

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
