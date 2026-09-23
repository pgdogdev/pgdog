use std::collections::HashSet;

use super::*;

fn numeric(text: &str) -> Numeric {
    Numeric::decode(text.as_bytes(), Format::Text).expect("valid PostgreSQL numeric")
}

#[test]
fn infinity_wire_roundtrip() {
    // PostgreSQL numeric_send() emits an eight-byte header with no digits.
    for (text, sign) in [("Infinity", 0xd0), ("-Infinity", 0xf0)] {
        let binary = [0, 0, 0, 0, sign, 0, 0, 0];
        let from_text = numeric(text);
        let from_binary =
            Numeric::decode(&binary, Format::Binary).expect("numeric infinity header");
        assert_eq!(from_text, from_binary);
        for value in [from_text, from_binary] {
            assert_eq!(value.encode(Format::Text).expect("text encoding"), text);
            assert_eq!(
                value.encode(Format::Binary).expect("binary encoding"),
                &binary[..]
            );
            assert_eq!(value.to_string(), text);
            assert!(value.as_decimal().is_none());
            assert!(!value.is_nan());
        }
    }
}

#[test]
fn infinity_text_aliases() {
    for text in ["Infinity", "+Infinity", "infinity", "INF", "+inf"] {
        assert_eq!(numeric(text), numeric("Infinity"), "{text}");
    }
    for text in ["-Infinity", "-infinity", "-INF"] {
        assert_eq!(numeric(text), numeric("-Infinity"), "{text}");
    }
}

#[test]
fn infinity_ordering_and_hashing() {
    let ordered = ["-Infinity", "-1", "0", "1", "Infinity", "NaN"].map(numeric);
    for (left_index, left) in ordered.iter().enumerate() {
        for (right_index, right) in ordered.iter().enumerate() {
            assert_eq!(left.cmp(right), left_index.cmp(&right_index));
            assert_eq!(left == right, left_index == right_index);
        }
    }
    let mut groups: HashSet<_> = ordered.into_iter().collect();
    for text in ["Infinity", "-Infinity", "NaN", "1.0"] {
        assert!(
            !groups.insert(numeric(text)),
            "{text} must join its existing group"
        );
    }
    assert_eq!(groups.len(), 6);
}

#[test]
fn infinity_addition() {
    for (left, right, expected) in [
        ("Infinity", "2.5", "Infinity"),
        ("Infinity", "Infinity", "Infinity"),
        ("-Infinity", "-2.5", "-Infinity"),
        ("-Infinity", "-Infinity", "-Infinity"),
        ("Infinity", "-Infinity", "NaN"),
        ("Infinity", "NaN", "NaN"),
        ("-Infinity", "NaN", "NaN"),
    ] {
        assert_eq!(numeric(left) + numeric(right), numeric(expected));
        assert_eq!(numeric(right) + numeric(left), numeric(expected));
        let mut sum = numeric(left);
        sum += numeric(right);
        assert_eq!(sum, numeric(expected));
    }
}

#[test]
fn infinity_multiplication() {
    for (input, multiplier, expected) in [
        ("Infinity", "0.25", "Infinity"),
        ("Infinity", "-0.25", "-Infinity"),
        ("-Infinity", "0.25", "-Infinity"),
        ("-Infinity", "-0.25", "Infinity"),
        ("Infinity", "0", "NaN"),
        ("-Infinity", "0", "NaN"),
    ] {
        let multiplier = Decimal::from_str(multiplier).expect("finite decimal multiplier");
        assert_eq!(numeric(input) * multiplier, numeric(expected));
    }
}

#[test]
fn infinity_float_conversions() {
    for (value, text) in [
        (f64::INFINITY, "Infinity"),
        (f64::NEG_INFINITY, "-Infinity"),
    ] {
        assert_eq!(Numeric::from(value), numeric(text));
        assert_eq!(Numeric::from(value as f32), numeric(text));
        assert_eq!(numeric(text).to_f64(), Some(value));
    }
}

#[test]
fn infinity_with_digits_is_rejected() {
    for sign in [0xd0, 0xf0] {
        let binary = [0, 1, 0, 0, sign, 0, 0, 0, 0, 1];
        assert!(matches!(
            Numeric::decode(&binary, Format::Binary),
            Err(Error::UnexpectedPayload)
        ));
    }
}
