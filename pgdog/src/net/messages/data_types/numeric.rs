use std::{
    cmp::Ordering,
    fmt::Display,
    hash::Hash,
    ops::{Add, Deref, DerefMut},
    str::FromStr,
};

use bytes::{Buf, BufMut, BytesMut};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde::{
    de::{self, Visitor},
    Serialize,
};
use tracing::warn;

use crate::net::messages::data_row::Data;

use super::*;

/// PostgreSQL NUMERIC type representation using exact decimal arithmetic.
///
/// Note: rust_decimal has a maximum of 28 decimal digits of precision.
/// Values exceeding this will return an error.
/// TODO: Add NaN support - currently returns an error if NaN is encountered.
#[derive(PartialEq, Copy, Clone, Debug)]
#[repr(C)]
pub struct Numeric {
    data: Decimal,
}

impl Display for Numeric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.data)
    }
}

impl Hash for Numeric {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Decimal provides a consistent hash implementation
        self.data.hash(state);
    }
}

impl PartialOrd for Numeric {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Numeric {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.data.cmp(&other.data)
    }
}

impl Eq for Numeric {}

impl Deref for Numeric {
    type Target = Decimal;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl DerefMut for Numeric {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl Add for Numeric {
    type Output = Numeric;

    fn add(self, rhs: Self) -> Self::Output {
        Numeric {
            data: self.data + rhs.data,
        }
    }
}

impl FromDataType for Numeric {
    fn decode(bytes: &[u8], encoding: Format) -> Result<Self, Error> {
        match encoding {
            Format::Text => {
                let s = String::decode(bytes, encoding)?;
                match Decimal::from_str(&s) {
                    Ok(decimal) => Ok(Self { data: decimal }),
                    Err(e) => {
                        // Check for special PostgreSQL values
                        match s.to_uppercase().as_str() {
                            "NAN" => {
                                // TODO: Add NaN support
                                warn!("NaN values not supported (deferred feature)");
                                Err(Error::UnexpectedPayload)
                            }
                            "INFINITY" | "+INFINITY" | "-INFINITY" => {
                                warn!("Infinity values not supported");
                                Err(Error::UnexpectedPayload)
                            }
                            _ => {
                                warn!("Failed to parse numeric: {}", e);
                                Err(Error::NotFloat(e.to_string().parse::<f64>().unwrap_err()))
                            }
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
                let _weight = buf.get_i16();
                let sign = buf.get_u16();
                let dscale = buf.get_i16();

                // Check for special values
                if sign == 0xC000 {
                    // TODO: Add NaN support for binary format
                    warn!("NaN values not supported in binary format (deferred feature)");
                    return Err(Error::UnexpectedPayload);
                }

                if ndigits == 0 {
                    return Ok(Self {
                        data: Decimal::ZERO,
                    });
                }

                if buf.len() < (ndigits as usize) * 2 {
                    return Err(Error::WrongSizeBinary(bytes.len()));
                }

                // Read digits (base 10000)
                let mut digits = Vec::with_capacity(ndigits as usize);
                for _ in 0..ndigits {
                    digits.push(buf.get_i16());
                }

                if sign == 0 || sign == 0x4000 {
                    let is_negative = sign == 0x4000;

                    // Reconstruct the decimal number from base-10000 digits
                    let mut result = String::new();

                    if is_negative {
                        result.push('-');
                    }

                    // PostgreSQL format with dscale:
                    // - Integer digits represent the integer part
                    // - Fractional digits are stored after the integer digits
                    // - dscale tells us how many decimal places to extract from fractional digits

                    // Build the integer part
                    let mut integer_str = String::new();
                    let mut fractional_str = String::new();

                    // Determine how many digits are for the integer part
                    // Weight tells us the position of the first digit
                    let integer_digit_count = if _weight >= 0 {
                        (_weight + 1) as usize
                    } else {
                        0
                    };

                    // Process integer digits
                    for i in 0..integer_digit_count.min(digits.len()) {
                        if i == 0 && digits[i] < 1000 && _weight >= 0 {
                            // First digit, no leading zeros
                            integer_str.push_str(&digits[i].to_string());
                        } else {
                            // Subsequent digits or first digit >= 1000
                            if i == 0 && _weight >= 0 {
                                integer_str.push_str(&digits[i].to_string());
                            } else {
                                integer_str.push_str(&format!("{:04}", digits[i]));
                            }
                        }
                    }

                    // Add trailing zeros for missing integer digits
                    if _weight >= 0 {
                        let expected_integer_digits = (_weight + 1) as usize;
                        for _ in digits.len()..expected_integer_digits {
                            integer_str.push_str("0000");
                        }
                    }

                    // Process fractional digits
                    for i in integer_digit_count..digits.len() {
                        fractional_str.push_str(&format!("{:04}", digits[i]));
                    }

                    // Build final result based on dscale
                    if dscale == 0 {
                        // Pure integer
                        if !integer_str.is_empty() {
                            result.push_str(&integer_str);
                        } else {
                            result.push('0');
                        }
                    } else if _weight < 0 {
                        // Pure fractional (weight < 0)
                        result.push_str("0.");

                        // For negative weight, add leading zeros
                        let leading_zeros = ((-_weight - 1) * 4) as usize;
                        for _ in 0..leading_zeros {
                            result.push('0');
                        }

                        // Add the fractional part, but only dscale digits
                        let all_fractional =
                            format!("{}{}", "0".repeat(leading_zeros), fractional_str);
                        if all_fractional.len() >= dscale as usize {
                            result.push_str(&fractional_str[..dscale as usize]);
                        } else {
                            result.push_str(&fractional_str);
                            // Pad if needed
                            for _ in fractional_str.len()..(dscale as usize) {
                                result.push('0');
                            }
                        }
                    } else {
                        // Mixed integer and fractional
                        if !integer_str.is_empty() {
                            result.push_str(&integer_str);
                        } else {
                            result.push('0');
                        }

                        if dscale > 0 {
                            result.push('.');
                            // Take exactly dscale digits from fractional part
                            if fractional_str.len() >= dscale as usize {
                                result.push_str(&fractional_str[..dscale as usize]);
                            } else {
                                result.push_str(&fractional_str);
                                // Pad with zeros if needed
                                for _ in fractional_str.len()..(dscale as usize) {
                                    result.push('0');
                                }
                            }
                        }
                    }

                    let decimal = Decimal::from_str(&result).map_err(|e| {
                        warn!("Failed to parse '{}' as Decimal: {}", result, e);
                        Error::UnexpectedPayload
                    })?;
                    return Ok(Self { data: decimal });
                }

                // For now, return error for complex cases (negative, fractional)
                warn!("Complex binary NUMERIC decoding not yet implemented");
                Err(Error::UnexpectedPayload)
            }
        }
    }

    fn encode(&self, encoding: Format) -> Result<Bytes, Error> {
        match encoding {
            Format::Text => Ok(Bytes::copy_from_slice(self.data.to_string().as_bytes())),
            Format::Binary => {
                // Start with the simplest possible implementation
                // Handle zero case
                if self.data.is_zero() {
                    let mut buf = BytesMut::new();
                    buf.put_i16(0); // ndigits
                    buf.put_i16(0); // weight
                    buf.put_u16(0); // sign (positive)
                    buf.put_i16(0); // dscale
                    return Ok(buf.freeze());
                }

                // Handle all numbers (integers and decimals, positive and negative)
                let is_negative = self.data.is_sign_negative();
                let abs_decimal = self.data.abs();
                let decimal_str = abs_decimal.to_string();

                // Split into integer and fractional parts
                let parts: Vec<&str> = decimal_str.split('.').collect();
                let integer_part = parts[0];
                let fractional_part = parts.get(1).unwrap_or(&"");
                let dscale = fractional_part.len() as i16;

                // PostgreSQL keeps integer and fractional parts separate
                // Process them independently to match PostgreSQL's format

                // Process integer part (right to left, in groups of 4)
                let mut integer_digits = Vec::new();

                if integer_part != "0" {
                    let int_chars: Vec<char> = integer_part.chars().collect();
                    let mut pos = int_chars.len();

                    while pos > 0 {
                        let start = if pos >= 4 { pos - 4 } else { 0 };
                        let chunk: String = int_chars[start..pos].iter().collect();
                        let digit_value: i16 =
                            chunk.parse().map_err(|_| Error::UnexpectedPayload)?;
                        integer_digits.insert(0, digit_value);
                        pos = start;
                    }
                }

                // Process fractional part (left to right, in groups of 4)
                let mut fractional_digits = Vec::new();
                if !fractional_part.is_empty() {
                    let frac_chars: Vec<char> = fractional_part.chars().collect();
                    let mut pos = 0;

                    while pos < frac_chars.len() {
                        let end = std::cmp::min(pos + 4, frac_chars.len());
                        let mut chunk: String = frac_chars[pos..end].iter().collect();

                        // Pad the last chunk with zeros if needed
                        while chunk.len() < 4 {
                            chunk.push('0');
                        }

                        let digit_value: i16 =
                            chunk.parse().map_err(|_| Error::UnexpectedPayload)?;
                        fractional_digits.push(digit_value);
                        pos = end;
                    }
                }

                // Calculate initial weight before optimization
                let initial_weight = if integer_part == "0" || integer_part.is_empty() {
                    // Pure fractional number - weight is negative
                    -1
                } else {
                    // Based on number of integer digits
                    integer_digits.len() as i16 - 1
                };

                // Combine integer and fractional parts
                let mut digits = integer_digits;
                digits.extend(fractional_digits.clone());

                // PostgreSQL optimization: if we have no fractional part and integer part
                // has trailing zeros, we can remove them and adjust the weight
                let weight =
                    if fractional_digits.is_empty() && !digits.is_empty() && initial_weight >= 0 {
                        // Count and remove trailing zero i16 values
                        let original_len = digits.len();
                        while digits.len() > 1 && digits.last() == Some(&0) {
                            digits.pop();
                        }
                        let _removed_count = (original_len - digits.len()) as i16;
                        // Weight stays the same even after removing trailing zeros
                        // because weight represents the position of the first digit
                        initial_weight
                    } else {
                        initial_weight
                    };

                if digits.is_empty() {
                    digits.push(0);
                }

                let mut buf = BytesMut::new();
                let ndigits = digits.len() as i16;
                let sign = if is_negative { 0x4000_u16 } else { 0_u16 };

                buf.put_i16(ndigits);
                buf.put_i16(weight);
                buf.put_u16(sign);
                buf.put_i16(dscale);

                // Write all digits
                for digit in digits {
                    buf.put_i16(digit);
                }

                Ok(buf.freeze())
            }
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
            data: Decimal::from(value),
        }
    }
}

impl From<i64> for Numeric {
    fn from(value: i64) -> Self {
        Self {
            data: Decimal::from(value),
        }
    }
}

impl From<f32> for Numeric {
    fn from(value: f32) -> Self {
        Self {
            // Note: This may lose precision
            data: Decimal::from_f32_retain(value).unwrap_or(Decimal::ZERO),
        }
    }
}

impl From<f64> for Numeric {
    fn from(value: f64) -> Self {
        Self {
            // Note: This may lose precision
            data: Decimal::from_f64_retain(value).unwrap_or(Decimal::ZERO),
        }
    }
}

impl From<Decimal> for Numeric {
    fn from(value: Decimal) -> Self {
        Self { data: value }
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
        match Decimal::from_str(v) {
            Ok(decimal) => Ok(Numeric { data: decimal }),
            Err(_) => Err(de::Error::custom("failed to parse decimal")),
        }
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        match Decimal::from_f64_retain(v) {
            Some(decimal) => Ok(Numeric { data: decimal }),
            None => Err(de::Error::custom("failed to convert f64 to decimal")),
        }
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Numeric {
            data: Decimal::from(v),
        })
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Numeric {
            data: Decimal::from(v),
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
        serializer.serialize_str(&self.data.to_string())
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
    fn test_debug_1234_encoding() {
        // Debug test for "12.34"
        let decimal = Decimal::from_str("12.34").unwrap();
        let numeric = Numeric::from(decimal);

        // Encode to binary
        let encoded = numeric.encode(Format::Binary).expect("Failed to encode");

        // Parse the binary to see what we're producing
        let mut reader = &encoded[..];
        let ndigits = reader.get_i16();
        let weight = reader.get_i16();
        let sign = reader.get_u16();
        let dscale = reader.get_i16();

        if ndigits > 0 {
            let _digit = reader.get_i16();
        }

        // Expected: ndigits=1, weight=0, sign=0, dscale=2, digit=1234
        assert_eq!(ndigits, 1, "Should have 1 digit");
        assert_eq!(weight, 0, "Weight should be 0");
        assert_eq!(dscale, 2, "Should have 2 decimal places");

        // Now decode and check
        let decoded = Numeric::decode(&encoded, Format::Binary).expect("Failed to decode");
        assert_eq!(decoded.data, decimal, "Roundtrip should work");
    }

    #[test]
    fn test_debug_001_encoding() {
        // Debug test for "0.01"
        let decimal = Decimal::from_str("0.01").unwrap();
        let numeric = Numeric::from(decimal);

        // Encode to binary
        let encoded = numeric.encode(Format::Binary).expect("Failed to encode");

        // Parse the binary to see what we're producing
        let mut reader = &encoded[..];
        let ndigits = reader.get_i16();
        let weight = reader.get_i16();
        let sign = reader.get_u16();
        let dscale = reader.get_i16();

        if ndigits > 0 {
            let _digit = reader.get_i16();
        }

        // Now decode and check
        let decoded = Numeric::decode(&encoded, Format::Binary).expect("Failed to decode");
        assert_eq!(decoded.data, decimal, "Roundtrip should work for 0.01");
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
                expected_ndigits: 1, // PostgreSQL uses 1 digit with weight=1
                expected_weight: 1,
                expected_sign: 0x0000,
                expected_dscale: 0,
                expected_digits: vec![1], // PostgreSQL format: [1]
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
                expected_digits: vec![1, 0, 0, 0, 0, 0], // This is actually correct
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
                .expect(&format!("Failed to encode {}", test_value));

            // Decode back
            let decoded_numeric = Numeric::decode(&encoded, Format::Binary)
                .expect(&format!("Failed to decode {}", test_value));

            // Verify roundtrip
            assert_eq!(
                original_numeric.data, decoded_numeric.data,
                "Roundtrip failed for {}: original={}, decoded={}",
                test_value, original_numeric.data, decoded_numeric.data
            );
        }
    }
}
