use crate::Data;
use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::*;

pub use pgdog_vector::Float;

impl FromDataType for Float {
    fn decode(bytes: &[u8], encoding: Format) -> Result<Self, Error> {
        match encoding {
            Format::Text => {
                let s = String::decode(bytes, encoding)?;
                match s.to_uppercase().as_str() {
                    "NAN" => Ok(Float(f32::NAN)),
                    "INFINITY" => Ok(Float(f32::INFINITY)),
                    "-INFINITY" => Ok(Float(f32::NEG_INFINITY)),
                    _ => s.parse::<f32>().map(Float).map_err(Error::NotFloat),
                }
            }
            Format::Binary => {
                // PostgreSQL float4 is 4 bytes in network byte order (big-endian)
                if bytes.len() != 4 {
                    return Err(Error::WrongSizeBinary(bytes.len()));
                }

                let mut buf = bytes;
                let bits = buf.get_u32();
                Ok(Float(f32::from_bits(bits)))
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
                // Write as 4-byte float in network byte order (big-endian)
                buf.put_u32(self.0.to_bits());
                Ok(buf.freeze())
            }
        }
    }
}

impl ToDataRowColumn for Float {
    fn to_data_row_column(&self) -> Data {
        Data::from(self.encode(Format::Text).unwrap_or_else(|_| Bytes::new()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::hash::{Hash, Hasher};

    #[test]
    fn test_float_nan_handling() {
        // Test NaN text parsing
        let nan_text = Float::decode(b"NaN", Format::Text).unwrap();
        assert!(nan_text.0.is_nan());
        assert_eq!(nan_text.to_string(), "NaN");

        // Test case-insensitive NaN parsing
        let nan_lower = Float::decode(b"nan", Format::Text).unwrap();
        assert!(nan_lower.0.is_nan());

        // Test NaN binary encoding/decoding
        let nan = Float(f32::NAN);
        let encoded = nan.encode(Format::Binary).unwrap();
        assert_eq!(encoded.len(), 4);

        let decoded = Float::decode(&encoded, Format::Binary).unwrap();
        assert!(decoded.0.is_nan());

        // Test NaN text encoding
        let nan_text_encoded = nan.encode(Format::Text).unwrap();
        assert_eq!(&nan_text_encoded[..], b"NaN");
    }

    #[test]
    fn test_float_infinity_handling() {
        // Test positive infinity
        let pos_inf_text = Float::decode(b"Infinity", Format::Text).unwrap();
        assert!(pos_inf_text.0.is_infinite() && pos_inf_text.0.is_sign_positive());
        assert_eq!(pos_inf_text.to_string(), "Infinity");

        // Test negative infinity
        let neg_inf_text = Float::decode(b"-Infinity", Format::Text).unwrap();
        assert!(neg_inf_text.0.is_infinite() && neg_inf_text.0.is_sign_negative());
        assert_eq!(neg_inf_text.to_string(), "-Infinity");

        // Test binary encoding/decoding of infinity
        let pos_inf = Float(f32::INFINITY);
        let encoded = pos_inf.encode(Format::Binary).unwrap();
        let decoded = Float::decode(&encoded, Format::Binary).unwrap();
        assert_eq!(decoded.0, f32::INFINITY);

        let neg_inf = Float(f32::NEG_INFINITY);
        let encoded = neg_inf.encode(Format::Binary).unwrap();
        let decoded = Float::decode(&encoded, Format::Binary).unwrap();
        assert_eq!(decoded.0, f32::NEG_INFINITY);
    }

    #[test]
    fn test_float_ordering() {
        let nan = Float(f32::NAN);
        let pos_inf = Float(f32::INFINITY);
        let neg_inf = Float(f32::NEG_INFINITY);
        let zero = Float(0.0);
        let one = Float(1.0);
        let neg_one = Float(-1.0);

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
        assert_eq!(nan, Float(f32::NAN));
    }

    #[test]
    fn test_float_binary_roundtrip() {
        let test_values = vec![
            0.0,
            -0.0,
            1.0,
            -1.0,
            f32::MIN,
            f32::MAX,
            f32::EPSILON,
            f32::MIN_POSITIVE,
            std::f32::consts::PI,
            std::f32::consts::E,
            f32::NAN,
            f32::INFINITY,
            f32::NEG_INFINITY,
        ];

        for val in test_values {
            let original = Float(val);
            let encoded = original.encode(Format::Binary).unwrap();
            let decoded = Float::decode(&encoded, Format::Binary).unwrap();

            if val.is_nan() {
                assert!(decoded.0.is_nan());
            } else {
                assert_eq!(original.0.to_bits(), decoded.0.to_bits());
            }
        }
    }

    #[test]
    fn test_float_hash_consistency() {
        use std::collections::hash_map::DefaultHasher;

        let nan1 = Float(f32::NAN);
        let nan2 = Float(f32::NAN);
        let zero = Float(0.0);
        let neg_zero = Float(-0.0);

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
    fn test_float_sorting() {
        let mut values = vec![
            Float(10.0),
            Float(f32::NAN),
            Float(5.0),
            Float(f32::INFINITY),
            Float(20.0),
            Float(f32::NEG_INFINITY),
            Float(f32::NAN),
            Float(1.0),
        ];

        values.sort();

        // Expected order: -inf, 1, 5, 10, 20, +inf, NaN, NaN
        assert_eq!(values[0].0, f32::NEG_INFINITY);
        assert_eq!(values[1].0, 1.0);
        assert_eq!(values[2].0, 5.0);
        assert_eq!(values[3].0, 10.0);
        assert_eq!(values[4].0, 20.0);
        assert_eq!(values[5].0, f32::INFINITY);
        assert!(values[6].0.is_nan());
        assert!(values[7].0.is_nan());
    }
}
