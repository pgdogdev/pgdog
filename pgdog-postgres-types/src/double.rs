use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::Display;
use std::hash::{Hash, Hasher};

use crate::Data;

use super::*;

/// Wrapper type for f64 that implements Ord for PostgreSQL compatibility
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Double(pub f64);

impl PartialOrd for Double {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Double {
    fn cmp(&self, other: &Self) -> Ordering {
        // PostgreSQL ordering: NaN is greater than all other values
        match (self.0.is_nan(), other.0.is_nan()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => self.0.partial_cmp(&other.0).unwrap_or(Ordering::Equal),
        }
    }
}

impl PartialEq for Double {
    fn eq(&self, other: &Self) -> bool {
        // PostgreSQL treats NaN as equal to NaN for indexing purposes
        if self.0.is_nan() && other.0.is_nan() {
            true
        } else {
            self.0 == other.0
        }
    }
}

impl Eq for Double {}

impl FromDataType for Double {
    fn decode(bytes: &[u8], encoding: Format) -> Result<Self, Error> {
        match encoding {
            Format::Text => {
                let s = String::decode(bytes, encoding)?;
                match s.to_uppercase().as_str() {
                    "NAN" => Ok(Double(f64::NAN)),
                    "INFINITY" => Ok(Double(f64::INFINITY)),
                    "-INFINITY" => Ok(Double(f64::NEG_INFINITY)),
                    _ => s.parse::<f64>().map(Double).map_err(Error::NotFloat),
                }
            }
            Format::Binary => {
                // PostgreSQL float8 is 8 bytes in network byte order (big-endian)
                if bytes.len() != 8 {
                    return Err(Error::WrongSizeBinary(bytes.len()));
                }

                let mut buf = bytes;
                let bits = buf.get_u64();
                Ok(Double(f64::from_bits(bits)))
            }
        }
    }

    fn encode(&self, encoding: Format) -> Result<Bytes, Error> {
        match encoding {
            Format::Text => {
                let s = if self.0.is_nan() {
                    "NaN".to_string()
                } else if self.0.is_infinite() {
                    if self.0.is_sign_positive() {
                        "Infinity".to_string()
                    } else {
                        "-Infinity".to_string()
                    }
                } else {
                    self.0.to_string()
                };
                Ok(Bytes::copy_from_slice(s.as_bytes()))
            }
            Format::Binary => {
                let mut buf = BytesMut::new();
                // Write as 8-byte float in network byte order (big-endian)
                buf.put_u64(self.0.to_bits());
                Ok(buf.freeze())
            }
        }
    }
}

impl ToDataRowColumn for Double {
    fn to_data_row_column(&self) -> Data {
        Data::from(self.encode(Format::Text).unwrap_or_else(|_| Bytes::new()))
    }
}

impl From<f64> for Double {
    fn from(value: f64) -> Self {
        Double(value)
    }
}

impl From<Double> for f64 {
    fn from(value: Double) -> Self {
        value.0
    }
}

impl Display for Double {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.is_nan() {
            write!(f, "NaN")
        } else if self.0.is_infinite() {
            if self.0.is_sign_positive() {
                write!(f, "Infinity")
            } else {
                write!(f, "-Infinity")
            }
        } else {
            write!(f, "{}", self.0)
        }
    }
}

impl Hash for Double {
    fn hash<H: Hasher>(&self, state: &mut H) {
        if self.0.is_nan() {
            // All NaN values hash to the same value
            0u8.hash(state);
        } else {
            // Use bit representation for consistent hashing
            self.0.to_bits().hash(state);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_double_nan_handling() {
        // Test NaN text parsing
        let nan_text = Double::decode(b"NaN", Format::Text).unwrap();
        assert!(nan_text.0.is_nan());
        assert_eq!(nan_text.to_string(), "NaN");

        // Test case-insensitive NaN parsing
        let nan_lower = Double::decode(b"nan", Format::Text).unwrap();
        assert!(nan_lower.0.is_nan());

        // Test NaN binary encoding/decoding
        let nan = Double(f64::NAN);
        let encoded = nan.encode(Format::Binary).unwrap();
        assert_eq!(encoded.len(), 8);

        let decoded = Double::decode(&encoded, Format::Binary).unwrap();
        assert!(decoded.0.is_nan());

        // Test NaN text encoding
        let nan_text_encoded = nan.encode(Format::Text).unwrap();
        assert_eq!(&nan_text_encoded[..], b"NaN");
    }

    #[test]
    fn test_double_infinity_handling() {
        // Test positive infinity
        let pos_inf_text = Double::decode(b"Infinity", Format::Text).unwrap();
        assert!(pos_inf_text.0.is_infinite() && pos_inf_text.0.is_sign_positive());
        assert_eq!(pos_inf_text.to_string(), "Infinity");

        // Test negative infinity
        let neg_inf_text = Double::decode(b"-Infinity", Format::Text).unwrap();
        assert!(neg_inf_text.0.is_infinite() && neg_inf_text.0.is_sign_negative());
        assert_eq!(neg_inf_text.to_string(), "-Infinity");

        // Test binary encoding/decoding of infinity
        let pos_inf = Double(f64::INFINITY);
        let encoded = pos_inf.encode(Format::Binary).unwrap();
        let decoded = Double::decode(&encoded, Format::Binary).unwrap();
        assert_eq!(decoded.0, f64::INFINITY);

        let neg_inf = Double(f64::NEG_INFINITY);
        let encoded = neg_inf.encode(Format::Binary).unwrap();
        let decoded = Double::decode(&encoded, Format::Binary).unwrap();
        assert_eq!(decoded.0, f64::NEG_INFINITY);
    }

    #[test]
    fn test_double_ordering() {
        let nan = Double(f64::NAN);
        let pos_inf = Double(f64::INFINITY);
        let neg_inf = Double(f64::NEG_INFINITY);
        let zero = Double(0.0);
        let one = Double(1.0);
        let neg_one = Double(-1.0);

        // NaN is greater than all other values
        assert!(nan > pos_inf);
        assert!(nan > neg_inf);
        assert!(nan > zero);
        assert!(nan > one);
        assert!(nan > neg_one);

        // Regular ordering for non-NaN values
        assert!(pos_inf > one);
        assert!(one > zero);
        assert!(zero > neg_one);
        assert!(neg_one > neg_inf);

        // NaN equals NaN
        assert_eq!(nan, Double(f64::NAN));
    }

    #[test]
    fn test_double_binary_roundtrip() {
        let test_values = vec![
            0.0,
            -0.0,
            1.0,
            -1.0,
            f64::MIN,
            f64::MAX,
            f64::EPSILON,
            f64::MIN_POSITIVE,
            std::f64::consts::PI,
            std::f64::consts::E,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ];

        for val in test_values {
            let original = Double(val);
            let encoded = original.encode(Format::Binary).unwrap();
            let decoded = Double::decode(&encoded, Format::Binary).unwrap();

            if val.is_nan() {
                assert!(decoded.0.is_nan());
            } else {
                assert_eq!(original.0.to_bits(), decoded.0.to_bits());
            }
        }
    }

    #[test]
    fn test_double_hash_consistency() {
        use std::collections::hash_map::DefaultHasher;

        let nan1 = Double(f64::NAN);
        let nan2 = Double(f64::NAN);
        let zero = Double(0.0);
        let neg_zero = Double(-0.0);

        // NaN values should hash to the same value
        let mut hasher1 = DefaultHasher::new();
        nan1.hash(&mut hasher1);
        let hash1 = hasher1.finish();

        let mut hasher2 = DefaultHasher::new();
        nan2.hash(&mut hasher2);
        let hash2 = hasher2.finish();

        assert_eq!(hash1, hash2);

        // Different values should (likely) have different hashes
        let mut hasher3 = DefaultHasher::new();
        zero.hash(&mut hasher3);
        let hash3 = hasher3.finish();

        let mut hasher4 = DefaultHasher::new();
        neg_zero.hash(&mut hasher4);
        let hash4 = hasher4.finish();

        // Note: 0.0 and -0.0 have different bit patterns
        assert_ne!(hash3, hash4);
    }

    #[test]
    fn test_double_sorting() {
        let mut values = vec![
            Double(10.0),
            Double(f64::NAN),
            Double(5.0),
            Double(f64::INFINITY),
            Double(20.0),
            Double(f64::NEG_INFINITY),
            Double(f64::NAN),
            Double(1.0),
        ];

        values.sort();

        // Expected order: -inf, 1, 5, 10, 20, +inf, NaN, NaN
        assert_eq!(values[0].0, f64::NEG_INFINITY);
        assert_eq!(values[1].0, 1.0);
        assert_eq!(values[2].0, 5.0);
        assert_eq!(values[3].0, 10.0);
        assert_eq!(values[4].0, 20.0);
        assert_eq!(values[5].0, f64::INFINITY);
        assert!(values[6].0.is_nan());
        assert!(values[7].0.is_nan());
    }
}
