use std::{cmp::Ordering, fmt::Display, hash::Hash, ops::Add, str::FromStr};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use postgres_types::{FromSql, ToSql, Type};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde::{
    Serialize,
    de::{self, Visitor},
};

use crate::Data;

use super::*;

/// Internal sign representation of NaN
const POSTGRES_NAN: u16 = 0xC000;

/// Enum to represent different numeric values including NaN.
#[derive(Copy, Clone, Debug)]
enum NumericValue {
    Number(Decimal),
    NaN,
}

/// PostgreSQL NUMERIC type representation using exact decimal arithmetic.
///
/// Note: rust_decimal has a maximum of 28 decimal digits of precision.
/// Values exceeding this will return an error.
/// Supports special NaN (Not-a-Number) value following PostgreSQL semantics.
#[derive(Copy, Clone, Debug)]
#[repr(C)]
pub struct Numeric {
    value: NumericValue,
}

impl Display for Numeric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.value {
            NumericValue::Number(n) => write!(f, "{}", n),
            NumericValue::NaN => write!(f, "NaN"),
        }
    }
}

impl Hash for Numeric {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self.value {
            NumericValue::Number(n) => {
                0u8.hash(state); // Discriminant for Number
                n.hash(state);
            }
            NumericValue::NaN => {
                1u8.hash(state); // Discriminant for NaN
            }
        }
    }
}

impl PartialEq for Numeric {
    fn eq(&self, other: &Self) -> bool {
        match (&self.value, &other.value) {
            (NumericValue::Number(a), NumericValue::Number(b)) => a == b,
            // PostgreSQL treats NaN as equal to NaN for indexing purposes
            (NumericValue::NaN, NumericValue::NaN) => true,
            _ => false,
        }
    }
}

impl Eq for Numeric {}

impl PartialOrd for Numeric {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Numeric {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (&self.value, &other.value) {
            (NumericValue::Number(a), NumericValue::Number(b)) => a.cmp(b),
            // PostgreSQL: NaN is greater than all non-NaN values
            (NumericValue::NaN, NumericValue::NaN) => Ordering::Equal,
            (NumericValue::NaN, _) => Ordering::Greater,
            (_, NumericValue::NaN) => Ordering::Less,
        }
    }
}

impl Add for Numeric {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        match (self.value, rhs.value) {
            (NumericValue::Number(a), NumericValue::Number(b)) => Self::new(a + b),
            // Any operation with NaN yields NaN
            _ => Self::nan(),
        }
    }
}

impl FromDataType for Numeric {
    fn decode(bytes: &[u8], encoding: Format) -> Result<Self, Error> {
        match encoding {
            Format::Text => {
                let s = String::decode(bytes, encoding)?;
                match Decimal::from_str(&s) {
                    Ok(decimal) => Ok(Self {
                        value: NumericValue::Number(decimal),
                    }),
                    Err(e) => {
                        // Check for special PostgreSQL values
                        match s.to_uppercase().as_str() {
                            "NAN" => Ok(Self {
                                value: NumericValue::NaN,
                            }),
                            "INFINITY" | "+INFINITY" | "-INFINITY" => Err(Error::UnexpectedPayload),
                            _ => Err(Error::NotFloat(e.to_string().parse::<f64>().unwrap_err())),
                        }
                    }
                }
            }

            Format::Binary => {
                // PostgreSQL NUMERIC binary format
                if bytes.len() < 8 {
                    return Err(Error::WrongSizeBinary(bytes.len()));
                }

                let mut buf = bytes;

                let ndigits = buf.get_i16();
                if ndigits < 0 {
                    return Err(Error::UnexpectedPayload);
                }
                let weight = buf.get_i16();
                let sign = buf.get_u16();
                let dscale = buf.get_i16();

                if dscale < 0 {
                    return Err(Error::UnexpectedPayload);
                }

                // Verify that weight is within a reasonable range to prevent
                // decoding from hitting an overflow
                if weight < -8192 || weight == i16::MAX {
                    return Err(Error::UnexpectedPayload);
                }

                if sign == POSTGRES_NAN {
                    if ndigits == 0 {
                        Ok(Self::nan())
                    } else {
                        Err(Error::UnexpectedPayload)
                    }
                } else {
                    Decimal::from_sql(&Type::NUMERIC, bytes)
                        .map(Self::new)
                        .map_err(|_| Error::UnexpectedPayload)
                }
            }
        }
    }

    fn encode(&self, encoding: Format) -> Result<Bytes, Error> {
        match encoding {
            Format::Text => match self.value {
                NumericValue::Number(n) => Ok(Bytes::copy_from_slice(n.to_string().as_bytes())),
                NumericValue::NaN => Ok(Bytes::copy_from_slice(b"NaN")),
            },
            Format::Binary => match self.value {
                NumericValue::NaN => {
                    // NaN encoding: ndigits=0, weight=0, sign=0xC000, dscale=0
                    let mut buf = BytesMut::new();
                    buf.put_i16(0); // ndigits
                    buf.put_i16(0); // weight
                    buf.put_u16(POSTGRES_NAN); // NaN sign
                    buf.put_i16(0); // dscale
                    Ok(buf.freeze())
                }
                NumericValue::Number(decimal) => {
                    let mut buf = BytesMut::new();
                    decimal
                        .to_sql(&Type::NUMERIC, &mut buf)
                        .map(|_| buf.freeze())
                        .map_err(|_| Error::UnexpectedPayload)
                }
            },
        }
    }
}

impl ToDataRowColumn for Numeric {
    fn to_data_row_column(&self) -> Data {
        self.encode(Format::Text).unwrap().into()
    }
}

impl From<i32> for Numeric {
    fn from(value: i32) -> Self {
        Self {
            value: NumericValue::Number(Decimal::from(value)),
        }
    }
}

impl From<i64> for Numeric {
    fn from(value: i64) -> Self {
        Self {
            value: NumericValue::Number(Decimal::from(value)),
        }
    }
}

impl From<f32> for Numeric {
    fn from(value: f32) -> Self {
        if value.is_nan() {
            Self {
                value: NumericValue::NaN,
            }
        } else {
            Self {
                // Note: This may lose precision
                value: NumericValue::Number(
                    Decimal::from_f32_retain(value).unwrap_or(Decimal::ZERO),
                ),
            }
        }
    }
}

impl From<f64> for Numeric {
    fn from(value: f64) -> Self {
        if value.is_nan() {
            Self {
                value: NumericValue::NaN,
            }
        } else {
            Self {
                // Note: This may lose precision
                value: NumericValue::Number(
                    Decimal::from_f64_retain(value).unwrap_or(Decimal::ZERO),
                ),
            }
        }
    }
}

impl From<Decimal> for Numeric {
    fn from(value: Decimal) -> Self {
        Self::new(value)
    }
}

// Helper methods for Numeric
impl Numeric {
    /// Create a new Numeric value
    pub fn new(value: Decimal) -> Self {
        Self {
            value: NumericValue::Number(value),
        }
    }

    /// Create a NaN Numeric value
    pub fn nan() -> Self {
        Self {
            value: NumericValue::NaN,
        }
    }

    /// Check if this is a NaN value
    pub fn is_nan(&self) -> bool {
        matches!(self.value, NumericValue::NaN)
    }

    /// Get the underlying Decimal value if not NaN
    pub fn as_decimal(&self) -> Option<&Decimal> {
        match &self.value {
            NumericValue::Number(n) => Some(n),
            NumericValue::NaN => None,
        }
    }

    /// Convert to f64
    pub fn to_f64(&self) -> Option<f64> {
        match &self.value {
            NumericValue::Number(n) => {
                // Use rust_decimal's to_f64 method
                use rust_decimal::prelude::ToPrimitive;
                n.to_f64()
            }
            NumericValue::NaN => Some(f64::NAN),
        }
    }
}

struct NumericVisitor;

impl<'de> Visitor<'de> for NumericVisitor {
    type Value = Numeric;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a numeric value (integer, float, or decimal string)")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if v.eq_ignore_ascii_case("nan") {
            Ok(Numeric::nan())
        } else {
            match Decimal::from_str(v) {
                Ok(decimal) => Ok(Numeric {
                    value: NumericValue::Number(decimal),
                }),
                Err(_) => Err(de::Error::custom("failed to parse decimal")),
            }
        }
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if v.is_nan() {
            Ok(Numeric::nan())
        } else {
            match Decimal::from_f64_retain(v) {
                Some(decimal) => Ok(Numeric {
                    value: NumericValue::Number(decimal),
                }),
                None => Err(de::Error::custom("failed to convert f64 to decimal")),
            }
        }
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Numeric {
            value: NumericValue::Number(Decimal::from(v)),
        })
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Numeric {
            value: NumericValue::Number(Decimal::from(v)),
        })
    }
}

impl<'de> Deserialize<'de> for Numeric {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_any(NumericVisitor)
    }
}

impl Serialize for Numeric {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Serialize as string to preserve precision
        match self.value {
            NumericValue::Number(n) => serializer.serialize_str(&n.to_string()),
            NumericValue::NaN => serializer.serialize_str("NaN"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_numeric_text_parsing() {
        let test_cases = vec![
            ("123.456", "123.456"),
            ("0", "0"),
            ("-123.456", "-123.456"),
            ("999999999999999999", "999999999999999999"),
        ];

        for (input, expected) in test_cases {
            let numeric = Numeric::decode(input.as_bytes(), Format::Text).unwrap();
            assert_eq!(numeric.to_string(), expected);
        }
    }

    #[test]
    fn test_numeric_precision() {
        let a = Numeric::from(Decimal::from_str("0.1").unwrap());
        let b = Numeric::from(Decimal::from_str("0.2").unwrap());
        let c = a + b;

        assert_eq!(c.to_string(), "0.3");
    }

    #[test]
    fn test_numeric_comparison() {
        let a = Numeric::from(Decimal::from_str("100.5").unwrap());
        let b = Numeric::from(Decimal::from_str("100.50").unwrap());
        let c = Numeric::from(Decimal::from_str("100.51").unwrap());

        assert_eq!(a, b);
        assert!(a < c);
        assert!(c > a);
    }

    #[test]
    fn test_binary_format_structure() {
        // Test exact binary format structure for known values
        struct TestCase {
            value: &'static str,
            expected_ndigits: i16,
            expected_weight: i16,
            expected_sign: u16,
            expected_dscale: i16,
            expected_digits: Vec<i16>,
        }

        let test_cases = vec![
            TestCase {
                value: "12.34",
                expected_ndigits: 2, // PostgreSQL uses 2 digits
                expected_weight: 0,
                expected_sign: 0x0000,
                expected_dscale: 2,
                expected_digits: vec![12, 3400], // PostgreSQL format: [12, 3400]
            },
            TestCase {
                value: "0.01",
                expected_ndigits: 1,
                expected_weight: -1,
                expected_sign: 0x0000,
                expected_dscale: 2,
                expected_digits: vec![100],
            },
            TestCase {
                value: "-999.99",
                expected_ndigits: 2,
                expected_weight: 0,
                expected_sign: 0x4000,
                expected_dscale: 2,
                expected_digits: vec![999, 9900], // PostgreSQL format: [999, 9900]
            },
            TestCase {
                value: "10000",
                expected_ndigits: 2, // Decimal crate adds an extra zero
                expected_weight: 1,
                expected_sign: 0x0000,
                expected_dscale: 0,
                expected_digits: vec![1, 0], // [1] and [1, 0] are equivalent
            },
            TestCase {
                value: "0.0001",
                expected_ndigits: 1,
                expected_weight: -1,
                expected_sign: 0x0000,
                expected_dscale: 4,
                expected_digits: vec![1],
            },
            TestCase {
                value: "0",
                expected_ndigits: 0, // Zero has no digits
                expected_weight: 0,
                expected_sign: 0x0000,
                expected_dscale: 0,
                expected_digits: vec![], // No digits for zero
            },
            TestCase {
                value: "100000000000000000000", // 10^20
                expected_ndigits: 6,
                expected_weight: 5,
                expected_sign: 0x0000,
                expected_dscale: 0,
                expected_digits: vec![1, 0, 0, 0, 0, 0], // Decimal crate adds a lot of extra zeroes but it's fine
            },
        ];

        for test_case in test_cases {
            let decimal = Decimal::from_str(test_case.value).unwrap();
            let numeric = Numeric::from(decimal);
            let encoded = numeric.encode(Format::Binary).unwrap();

            // Parse the binary format
            let mut reader = &encoded[..];
            let ndigits = reader.get_i16();
            let weight = reader.get_i16();
            let sign = reader.get_u16();
            let dscale = reader.get_i16();

            // Check header
            assert_eq!(
                ndigits, test_case.expected_ndigits,
                "ndigits mismatch for {}",
                test_case.value
            );
            assert_eq!(
                weight, test_case.expected_weight,
                "weight mismatch for {}",
                test_case.value
            );
            assert_eq!(
                sign, test_case.expected_sign,
                "sign mismatch for {}",
                test_case.value
            );
            assert_eq!(
                dscale, test_case.expected_dscale,
                "dscale mismatch for {}",
                test_case.value
            );

            // Check digits
            let mut actual_digits = Vec::new();
            for _ in 0..ndigits {
                actual_digits.push(reader.get_i16());
            }
            assert_eq!(
                actual_digits, test_case.expected_digits,
                "digits mismatch for {}",
                test_case.value
            );
        }
    }

    #[test]
    fn test_invalid_binary_format() {
        // Test that we properly reject invalid binary formats

        // Test 1: Too short header (less than 8 bytes)
        let too_short = vec![0, 1, 0, 2]; // Only 4 bytes
        let result = Numeric::decode(&too_short, Format::Binary);
        assert!(result.is_err(), "Should reject too short header");

        // Test 2: NaN value (sign = 0xC000)
        let mut nan_bytes = Vec::new();
        nan_bytes.extend_from_slice(&1i16.to_be_bytes()); // ndigits
        nan_bytes.extend_from_slice(&0i16.to_be_bytes()); // weight
        nan_bytes.extend_from_slice(&0xC000u16.to_be_bytes()); // NaN sign
        nan_bytes.extend_from_slice(&0i16.to_be_bytes()); // dscale
        nan_bytes.extend_from_slice(&1234i16.to_be_bytes()); // digit
        let result = Numeric::decode(&nan_bytes, Format::Binary);
        assert!(result.is_err(), "Should reject NaN values");

        // Test 3: Not enough digit data for claimed ndigits
        let mut truncated = Vec::new();
        truncated.extend_from_slice(&3i16.to_be_bytes()); // ndigits = 3
        truncated.extend_from_slice(&0i16.to_be_bytes()); // weight
        truncated.extend_from_slice(&0u16.to_be_bytes()); // sign
        truncated.extend_from_slice(&0i16.to_be_bytes()); // dscale
        truncated.extend_from_slice(&1234i16.to_be_bytes()); // Only 1 digit, but claimed 3
        let result = Numeric::decode(&truncated, Format::Binary);
        assert!(result.is_err(), "Should reject truncated digit data");

        // Test 4: Invalid sign value (not 0x0000, 0x4000, or 0xC000)
        let mut bad_sign = Vec::new();
        bad_sign.extend_from_slice(&1i16.to_be_bytes()); // ndigits
        bad_sign.extend_from_slice(&0i16.to_be_bytes()); // weight
        bad_sign.extend_from_slice(&0x8000u16.to_be_bytes()); // Invalid sign
        bad_sign.extend_from_slice(&0i16.to_be_bytes()); // dscale
        bad_sign.extend_from_slice(&1234i16.to_be_bytes()); // digit
        let _result = Numeric::decode(&bad_sign, Format::Binary);
        // This might actually succeed as we only check for 0x0000 and 0x4000
        // Let's see what happens

        // Test 5: Extreme weight that would cause overflow
        let mut extreme_weight = Vec::new();
        extreme_weight.extend_from_slice(&1i16.to_be_bytes()); // ndigits
        extreme_weight.extend_from_slice(&i16::MAX.to_be_bytes()); // Extreme weight
        extreme_weight.extend_from_slice(&0u16.to_be_bytes()); // sign
        extreme_weight.extend_from_slice(&0i16.to_be_bytes()); // dscale
        extreme_weight.extend_from_slice(&1i16.to_be_bytes()); // digit
        let _result = Numeric::decode(&extreme_weight, Format::Binary);
        // This will likely fail when trying to construct the string
    }

    #[test]
    fn test_high_dscale_pure_fractional() {
        // Test case that reproduces the panic: weight=-1, small fractional_str, large dscale
        // This tests the fix for the bug where we tried to slice beyond fractional_str bounds

        // Create a binary representation with:
        // - ndigits = 1
        // - weight = -1 (pure fractional, first digit at 10^-4 position)
        // - sign = 0x0000 (positive)
        // - dscale = 15 (want 15 decimal places)
        // - digit = 10 (represents 0.0010)
        let mut binary_data = Vec::new();
        binary_data.extend_from_slice(&1i16.to_be_bytes()); // ndigits = 1
        binary_data.extend_from_slice(&(-1i16).to_be_bytes()); // weight = -1
        binary_data.extend_from_slice(&0x0000u16.to_be_bytes()); // sign = positive
        binary_data.extend_from_slice(&15i16.to_be_bytes()); // dscale = 15
        binary_data.extend_from_slice(&10i16.to_be_bytes()); // digit = 10

        // This should decode to "0.001000000000000" (15 decimal places)
        let decoded = Numeric::decode(&binary_data, Format::Binary)
            .expect("Should decode high dscale pure fractional");

        // The number should be 0.001 with trailing zeros to make 15 decimal places
        let expected = Decimal::from_str("0.001000000000000").unwrap();
        assert_eq!(
            decoded.as_decimal(),
            Some(&expected),
            "High dscale pure fractional mismatch"
        );

        // Also test with even higher dscale
        let mut binary_data2 = Vec::new();
        binary_data2.extend_from_slice(&1i16.to_be_bytes()); // ndigits = 1
        binary_data2.extend_from_slice(&(-2i16).to_be_bytes()); // weight = -2 (10^-8 position)
        binary_data2.extend_from_slice(&0x0000u16.to_be_bytes()); // sign = positive
        binary_data2.extend_from_slice(&20i16.to_be_bytes()); // dscale = 20
        binary_data2.extend_from_slice(&1234i16.to_be_bytes()); // digit = 1234

        // weight=-2 means 4 leading zeros, then 1234 -> "0.00001234" padded to 20 places
        let decoded2 = Numeric::decode(&binary_data2, Format::Binary)
            .expect("Should decode very high dscale pure fractional");

        let expected2 = Decimal::from_str("0.00001234000000000000").unwrap();
        assert_eq!(
            decoded2.as_decimal(),
            Some(&expected2),
            "Very high dscale pure fractional mismatch"
        );
    }

    #[test]
    fn test_nan_support() {
        // Test NaN text parsing
        let nan_text = Numeric::decode(b"NaN", Format::Text).unwrap();
        assert!(nan_text.is_nan());
        assert_eq!(nan_text.to_string(), "NaN");

        // Test case-insensitive NaN parsing
        let nan_lower = Numeric::decode(b"nan", Format::Text).unwrap();
        assert!(nan_lower.is_nan());
        let nan_mixed = Numeric::decode(b"NaN", Format::Text).unwrap();
        assert!(nan_mixed.is_nan());

        // Test NaN binary encoding/decoding
        let nan = Numeric::nan();
        let encoded = nan.encode(Format::Binary).unwrap();

        // Verify binary format: ndigits=0, weight=0, sign=0xC000, dscale=0
        let mut reader = &encoded[..];
        use bytes::Buf;
        assert_eq!(reader.get_i16(), 0); // ndigits
        assert_eq!(reader.get_i16(), 0); // weight
        assert_eq!(reader.get_u16(), 0xC000); // NaN sign
        assert_eq!(reader.get_i16(), 0); // dscale

        // Test binary roundtrip
        let decoded = Numeric::decode(&encoded, Format::Binary).unwrap();
        assert!(decoded.is_nan());

        // Test NaN text encoding
        let nan_text_encoded = nan.encode(Format::Text).unwrap();
        assert_eq!(&nan_text_encoded[..], b"NaN");
    }

    #[test]
    fn test_nan_comparison() {
        let nan1 = Numeric::nan();
        let nan2 = Numeric::nan();
        let num = Numeric::from(42);

        // NaN equals NaN (for indexing)
        assert_eq!(nan1, nan2);
        assert_eq!(nan1, nan1);

        // NaN not equal to number
        assert_ne!(nan1, num);

        // NaN is greater than all numbers (for sorting)
        assert!(nan1 > num);
        assert!(nan2 > num);
        assert!(nan1 >= num);

        // Number is less than NaN
        assert!(num < nan1);
        assert!(num <= nan1);

        // Two NaNs are equal in ordering
        assert_eq!(nan1.cmp(&nan2), Ordering::Equal);
    }

    #[test]
    fn test_nan_arithmetic() {
        let nan = Numeric::nan();
        let num = Numeric::from(42);

        // Any operation with NaN yields NaN
        let result = nan + num;
        assert!(result.is_nan());

        let result = num + nan;
        assert!(result.is_nan());

        let result = nan + nan;
        assert!(result.is_nan());
    }

    #[test]
    fn test_nan_sorting() {
        let mut values = [
            Numeric::from(10),
            Numeric::nan(),
            Numeric::from(5),
            Numeric::from(20),
            Numeric::nan(),
            Numeric::from(1),
        ];

        values.sort();

        // Numbers should be sorted first, NaNs should be last
        assert_eq!(values[0], Numeric::from(1));
        assert_eq!(values[1], Numeric::from(5));
        assert_eq!(values[2], Numeric::from(10));
        assert_eq!(values[3], Numeric::from(20));
        assert!(values[4].is_nan());
        assert!(values[5].is_nan());
    }

    #[test]
    fn test_nan_from_float() {
        let nan_f32 = Numeric::from(f32::NAN);
        assert!(nan_f32.is_nan());

        let nan_f64 = Numeric::from(f64::NAN);
        assert!(nan_f64.is_nan());

        // Regular floats still work
        let num_f32 = Numeric::from(std::f32::consts::PI);
        assert!(!num_f32.is_nan());

        let num_f64 = Numeric::from(std::f64::consts::E);
        assert!(!num_f64.is_nan());
    }

    #[test]
    fn test_numeric_binary_roundtrip() {
        // Data-driven test for binary format roundtrip
        let test_cases = [
            "0",                              // Zero
            "1",                              // Simple positive
            "9999",                           // Max single base-10000 digit
            "10000",                          // Requires multiple digits
            "123456",                         // Multiple digits
            "123456789012345678901234567",    // Very large (near rust_decimal limit)
            "-1",                             // Negative simple
            "-123",                           // Negative multiple digits
            "-123456789012345678901234567",   // Very large negative
            "12.34",                          // Simple decimal
            "-12.34",                         // Negative decimal
            "0.1",                            // Small decimal
            "0.01",                           // Smaller decimal
            "999.99",                         // Decimal near boundary
            "1000.01",                        // Decimal over boundary
            "0.0001",                         // Very small decimal
            "100000000000000000000.00001",    // 10^20 + 10^-5 (outside f64 precision)
            "100000000000000000000.0000001",  // 10^20 + 10^-7
            "0.0000000000000000000000001",    // 10^-25
            "9999999999999999999999999999",   // 28 nines (max rust_decimal)
            "0.0000000000000000000000000001", // 28 decimal places
            "12345678901234567890.12345678",  // Mixed precision (20 + 8 = 28 total)
            // Classic floating-point problems
            "0.3",                            // 0.1 + 0.2 result
            "100000000000000000001.0000001",  // 10^20 + 1 + 10^-7 (middle value lost in f64)
            "1000000000000000.1",             // 10^15 + 0.1 (decimal precision boundary)
            "1.0000000000000001",             // Catastrophic cancellation example
            "0.3333333333333333333333333333", // 1/3 to 28 digits
            "0.1428571428571428571428571429", // 1/7 to 28 digits
            "9007199254740991",               // 2^53 - 1 (largest exact integer in f64)
            "9007199254740993",               // 2^53 + 1 (can't be represented in f64)
            "0.735",                          // 0.70 * 1.05 (financial calculation)
            "2.9985",                         // 19.99 * 0.15 (discount calculation)
        ];

        for test_value in test_cases {
            let original_decimal = Decimal::from_str(test_value).unwrap();
            let original_numeric = Numeric::from(original_decimal);

            // Encode to binary
            let encoded = original_numeric
                .encode(Format::Binary)
                .unwrap_or_else(|_| panic!("Failed to encode {}", test_value));

            // Decode back
            let decoded_numeric = Numeric::decode(&encoded, Format::Binary)
                .unwrap_or_else(|_| panic!("Failed to decode {}", test_value));

            // Verify roundtrip
            assert_eq!(
                original_numeric, decoded_numeric,
                "Roundtrip failed for {}: original={}, decoded={}",
                test_value, original_numeric, decoded_numeric
            );
        }
    }

    #[test]
    fn test_negative_dscale_binary_format_rejected() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1i16.to_be_bytes()); // ndigits
        bytes.extend_from_slice(&0i16.to_be_bytes()); // weight
        bytes.extend_from_slice(&0u16.to_be_bytes()); // sign
        bytes.extend_from_slice(&(-1i16).to_be_bytes()); // invalid dscale
        bytes.extend_from_slice(&1i16.to_be_bytes()); // digit

        let result = Numeric::decode(&bytes, Format::Binary);
        assert!(
            result.is_err(),
            "negative dscale is malformed and should be rejected"
        );
    }

    #[test]
    fn test_minimum_weight_binary_format_rejected() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1i16.to_be_bytes()); // ndigits
        bytes.extend_from_slice(&i16::MIN.to_be_bytes()); // invalid weight for decoder math
        bytes.extend_from_slice(&0u16.to_be_bytes()); // sign
        bytes.extend_from_slice(&1i16.to_be_bytes()); // dscale
        bytes.extend_from_slice(&1i16.to_be_bytes()); // digit

        let result = Numeric::decode(&bytes, Format::Binary);
        assert!(
            result.is_err(),
            "minimum i16 weight should be rejected instead of panicking"
        );
    }
}
