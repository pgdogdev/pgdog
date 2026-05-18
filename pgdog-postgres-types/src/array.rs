use std::cmp::Ordering;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::{Error, Format};
use crate::{DataType, Datum};

#[derive(Debug, Clone)]
pub struct Array {
    elements: Vec<Option<Datum>>,
    element_oid: i32,
    dim: Dimension,
}

impl PartialEq for Array {
    fn eq(&self, other: &Self) -> bool {
        self.elements == other.elements
            && self.element_oid == other.element_oid
            && self.dim == other.dim
    }
}

impl Eq for Array {}

impl PartialOrd for Array {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Array {
    fn cmp(&self, other: &Self) -> Ordering {
        self.elements
            .cmp(&other.elements)
            .then(self.element_oid.cmp(&other.element_oid))
            .then(self.dim.cmp(&other.dim))
    }
}

impl std::hash::Hash for Array {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.elements.hash(state);
        self.element_oid.hash(state);
        self.dim.hash(state);
    }
}

#[derive(Debug, Clone, Hash, Ord, PartialOrd, PartialEq, Eq)]
struct Dimension {
    size: i32,
    lower_bound: i32,
}

impl Default for Dimension {
    fn default() -> Self {
        Self {
            size: 0,
            lower_bound: 1,
        }
    }
}

impl Array {
    /// Decode an array with a known element type OID.
    ///
    /// For binary format, the element OID is also present in the wire header
    /// and is validated against the provided `element_oid`.
    /// For text format, the element OID must be supplied by the caller
    /// (typically from the column's RowDescription).
    pub fn decode_typed(bytes: &[u8], format: Format, element_oid: i32) -> Result<Self, Error> {
        match format {
            Format::Text => {
                let input = std::str::from_utf8(bytes)?;
                decode_text(input, element_oid)
            }
            Format::Binary => decode_binary(bytes, element_oid),
        }
    }

    /// Borrow the decoded element list. `None` entries are SQL NULLs.
    pub fn elements(&self) -> &[Option<Datum>] {
        &self.elements
    }

    /// OID of the array's element type.
    pub fn element_oid(&self) -> i32 {
        self.element_oid
    }

    /// Encode the array to the specified wire format.
    ///
    /// Cross-format encoding works because elements are stored as typed
    /// Datum values that can be encoded to any format.
    pub fn encode(&self, format: Format) -> Result<Bytes, Error> {
        match format {
            Format::Text => self.encode_text(),
            Format::Binary => self.encode_binary(),
        }
    }

    fn encode_text(&self) -> Result<Bytes, Error> {
        let mut result = String::new();
        if self.dim.lower_bound != 1 {
            use std::fmt::Write;
            let ub = if self.dim.size == 0 {
                self.dim
                    .lower_bound
                    .checked_sub(1)
                    .ok_or(Error::UnexpectedPayload)?
            } else {
                self.dim
                    .lower_bound
                    .checked_add(self.dim.size - 1)
                    .ok_or(Error::UnexpectedPayload)?
            };
            write!(result, "[{}:{}]=", self.dim.lower_bound, ub).unwrap();
        }
        result.push('{');
        for (i, element) in self.elements.iter().enumerate() {
            if i > 0 {
                result.push(',');
            }
            match element {
                None => result.push_str("NULL"),
                Some(datum) => {
                    let encoded = datum.encode(Format::Text)?;
                    let text = std::str::from_utf8(&encoded)?;
                    if needs_quoting(text) {
                        result.push('"');
                        for ch in text.chars() {
                            match ch {
                                '"' => result.push_str("\\\""),
                                '\\' => result.push_str("\\\\"),
                                _ => result.push(ch),
                            }
                        }
                        result.push('"');
                    } else {
                        result.push_str(text);
                    }
                }
            }
        }
        result.push('}');
        Ok(Bytes::from(result))
    }

    fn encode_binary(&self) -> Result<Bytes, Error> {
        let mut buf = BytesMut::with_capacity(12 + 8 + self.elements.len() * 8);
        // Use ndims=0 only for truly empty arrays with default bounds.
        // Non-default bounds need ndims=1 + size=0 to preserve the bounds.
        let has_dim = !self.elements.is_empty() || self.dim.lower_bound != 1;
        let dims = if has_dim { 1i32 } else { 0 };
        let has_nulls = self.elements.iter().any(|e| e.is_none());
        buf.put_i32(dims);
        buf.put_i32(if has_nulls { 1 } else { 0 });
        buf.put_i32(self.element_oid);
        if dims > 0 {
            buf.put_i32(self.dim.size);
            buf.put_i32(self.dim.lower_bound);
        }
        for element in &self.elements {
            match element {
                None => buf.put_i32(-1),
                Some(datum) => {
                    let encoded = datum.encode(Format::Binary)?;
                    buf.put_i32(encoded.len() as i32);
                    buf.extend_from_slice(&encoded);
                }
            }
        }
        Ok(buf.freeze())
    }
}

fn needs_quoting(s: &str) -> bool {
    s.is_empty()
        || s.eq_ignore_ascii_case("null")
        || s.bytes()
            .any(|b| matches!(b, b',' | b'{' | b'}' | b'"' | b'\\') || b.is_ascii_whitespace())
}

fn decode_text(input: &str, element_oid: i32) -> Result<Array, Error> {
    let input = input.trim();
    let element_type = DataType::from_oid(element_oid);
    let mut chars = input.chars().peekable();

    // Parse optional bounds prefix: [lower:upper]=
    let (lower_bound, expected_count) = if chars.peek() == Some(&'[') {
        chars.next();
        let mut lb_str = String::new();
        while let Some(&ch) = chars.peek() {
            if ch == ':' {
                break;
            }
            lb_str.push(ch);
            chars.next();
        }
        chars.next(); // skip ':'
        let mut ub_str = String::new();
        while let Some(&ch) = chars.peek() {
            if ch == ']' {
                break;
            }
            ub_str.push(ch);
            chars.next();
        }
        chars.next(); // skip ']'
        chars.next(); // skip '='
        let lb = lb_str
            .parse::<i32>()
            .map_err(|_| Error::UnexpectedPayload)?;
        let ub = ub_str
            .parse::<i32>()
            .map_err(|_| Error::UnexpectedPayload)?;
        let expected = ub
            .checked_sub(lb)
            .and_then(|d| d.checked_add(1))
            .ok_or(Error::UnexpectedPayload)?;
        (lb, Some(expected))
    } else {
        (1, None)
    };

    if chars.next() != Some('{') {
        return Err(Error::UnexpectedPayload);
    }

    while chars.peek().is_some_and(|c| c.is_ascii_whitespace()) {
        chars.next();
    }

    // Empty array
    if chars.peek() == Some(&'}') {
        chars.next();
        if chars.next().is_some() {
            return Err(Error::UnexpectedPayload);
        }
        if let Some(expected) = expected_count
            && expected != 0
        {
            return Err(Error::UnexpectedPayload);
        }
        return Ok(Array {
            elements: vec![],
            element_oid,
            dim: Dimension {
                size: 0,
                lower_bound,
            },
        });
    }

    let mut elements = Vec::new();

    loop {
        while chars.peek().is_some_and(|c| c.is_ascii_whitespace()) {
            chars.next();
        }

        match chars.peek() {
            Some(&'"') => {
                chars.next();
                let mut elem = String::new();
                loop {
                    match chars.next() {
                        Some('\\') => {
                            if let Some(escaped) = chars.next() {
                                elem.push(escaped);
                            }
                        }
                        Some('"') => break,
                        Some(ch) => elem.push(ch),
                        None => return Err(Error::UnexpectedPayload),
                    }
                }
                let datum = Datum::new(elem.as_bytes(), element_type, Format::Text, false)?;
                elements.push(Some(datum));
            }
            Some(_) => {
                let mut elem = String::new();
                let mut has_escapes = false;
                while let Some(&ch) = chars.peek() {
                    if ch == ',' || ch == '}' {
                        break;
                    }
                    if ch == '{' {
                        return Err(Error::ArrayDimensions(2));
                    }
                    if ch == '\\' {
                        chars.next();
                        match chars.next() {
                            Some(escaped) => elem.push(escaped),
                            None => return Err(Error::UnexpectedPayload),
                        }
                        has_escapes = true;
                    } else {
                        elem.push(ch);
                        chars.next();
                    }
                }

                if has_escapes {
                    if elem.is_empty() {
                        return Err(Error::UnexpectedPayload);
                    }
                    let datum = Datum::new(elem.as_bytes(), element_type, Format::Text, false)?;
                    elements.push(Some(datum));
                } else {
                    let trimmed = elem.trim_end_matches(|c: char| c.is_ascii_whitespace());
                    if trimmed.is_empty() {
                        return Err(Error::UnexpectedPayload);
                    }
                    if trimmed.eq_ignore_ascii_case("NULL") {
                        elements.push(None);
                    } else {
                        let datum =
                            Datum::new(trimmed.as_bytes(), element_type, Format::Text, false)?;
                        elements.push(Some(datum));
                    }
                }
            }
            None => return Err(Error::UnexpectedPayload),
        }

        while chars.peek().is_some_and(|c| c.is_ascii_whitespace()) {
            chars.next();
        }

        match chars.peek() {
            Some(&',') => {
                chars.next();
            }
            Some(&'}') => break,
            _ => return Err(Error::UnexpectedPayload),
        }
    }

    chars.next(); // consume '}'
    if chars.next().is_some() {
        return Err(Error::UnexpectedPayload);
    }

    let size = elements.len() as i32;

    if let Some(expected) = expected_count
        && size != expected
    {
        return Err(Error::UnexpectedPayload);
    }

    Ok(Array {
        elements,
        element_oid,
        dim: Dimension { size, lower_bound },
    })
}

fn decode_binary(bytes: &[u8], expected_element_oid: i32) -> Result<Array, Error> {
    let mut bytes = Bytes::copy_from_slice(bytes);

    if bytes.remaining() < 12 {
        return Err(Error::UnexpectedPayload);
    }
    let dims = bytes.get_i32() as usize;
    if dims > 1 {
        return Err(Error::ArrayDimensions(dims));
    }
    let _flags = bytes.get_i32();
    let wire_oid = bytes.get_i32();

    // Validate wire OID matches the caller-supplied OID
    let element_oid = if expected_element_oid != 0 {
        if wire_oid != expected_element_oid {
            return Err(Error::UnexpectedPayload);
        }
        expected_element_oid
    } else {
        wire_oid
    };
    let element_type = DataType::from_oid(element_oid);

    let dim = if dims == 0 {
        if bytes.has_remaining() {
            return Err(Error::UnexpectedPayload);
        }
        Dimension::default()
    } else {
        if bytes.remaining() < 8 {
            return Err(Error::UnexpectedPayload);
        }
        let size = bytes.get_i32();
        if size < 0 {
            return Err(Error::UnexpectedPayload);
        }
        let lower_bound = bytes.get_i32();
        if size > 0 {
            lower_bound
                .checked_add(size - 1)
                .ok_or(Error::UnexpectedPayload)?;
        }
        Dimension { size, lower_bound }
    };

    let mut elements = vec![];

    while bytes.has_remaining() {
        if bytes.remaining() < 4 {
            return Err(Error::UnexpectedPayload);
        }
        let len = bytes.get_i32();
        if len == -1 {
            elements.push(None);
        } else if len < 0 {
            return Err(Error::UnexpectedPayload);
        } else {
            if bytes.remaining() < len as usize {
                return Err(Error::UnexpectedPayload);
            }
            let element_bytes = bytes.split_to(len as usize);
            let datum = Datum::new(&element_bytes, element_type, Format::Binary, false)?;
            elements.push(Some(datum));
        }
    }

    if elements.len() != dim.size as usize {
        return Err(Error::UnexpectedPayload);
    }

    Ok(Array {
        elements,
        element_oid,
        dim,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ToDataRowColumn;

    // ── Text decode ──────────────────────────────────────────────────

    #[test]
    fn test_text_decode_table() {
        let cases: Vec<(&str, &str, i32, Vec<Option<&str>>, i32)> = vec![
            ("empty", "{}", 23, vec![], 1),
            ("single int", "{1}", 23, vec![Some("1")], 1),
            (
                "three ints",
                "{1,2,3}",
                23,
                vec![Some("1"), Some("2"), Some("3")],
                1,
            ),
            ("negative int", "{-1}", 23, vec![Some("-1")], 1),
            ("single NULL", "{NULL}", 23, vec![None], 1),
            ("NULL mixed case", "{null}", 23, vec![None], 1),
            (
                "NULL start",
                "{NULL,1,2}",
                23,
                vec![None, Some("1"), Some("2")],
                1,
            ),
            ("all NULLs", "{NULL,NULL}", 23, vec![None, None], 1),
            (
                "text elements",
                "{hello,world}",
                25,
                vec![Some("hello"), Some("world")],
                1,
            ),
            ("quoted comma", r#"{"a,b"}"#, 25, vec![Some("a,b")], 1),
            ("quoted quote", r#"{"a\"b"}"#, 25, vec![Some("a\"b")], 1),
            ("quoted backslash", r#"{"a\\b"}"#, 25, vec![Some("a\\b")], 1),
            ("empty string", r#"{""}"#, 25, vec![Some("")], 1),
            (
                "quoted NULL literal",
                r#"{"NULL"}"#,
                25,
                vec![Some("NULL")],
                1,
            ),
            (
                "whitespace around",
                "{ 1 , 2 }",
                23,
                vec![Some("1"), Some("2")],
                1,
            ),
            ("escaped comma", r#"{a\,b}"#, 25, vec![Some("a,b")], 1),
            (
                "escaped trailing space",
                r#"{a\ }"#,
                25,
                vec![Some("a ")],
                1,
            ),
            (
                "escaped null literal",
                r#"{\N\U\L\L}"#,
                25,
                vec![Some("NULL")],
                1,
            ),
            (
                "custom lower bound",
                "[0:2]={1,2,3}",
                23,
                vec![Some("1"), Some("2"), Some("3")],
                0,
            ),
        ];

        for (name, input, oid, expected_elements, expected_lb) in cases {
            let array = Array::decode_typed(input.as_bytes(), Format::Text, oid).expect(name);
            assert_eq!(array.dim.lower_bound, expected_lb, "lower_bound: {name}");
            assert_eq!(
                array.elements.len(),
                expected_elements.len(),
                "count: {name}"
            );
            for (i, (got, want)) in array
                .elements
                .iter()
                .zip(expected_elements.iter())
                .enumerate()
            {
                match (got, want) {
                    (None, None) => {}
                    (Some(datum), Some(expected)) => {
                        let encoded = datum.encode(Format::Text).expect(name);
                        assert_eq!(
                            std::str::from_utf8(&encoded).unwrap(),
                            *expected,
                            "element {i}: {name}"
                        );
                    }
                    _ => panic!("element {i} mismatch: {name}: got {got:?} want {want:?}"),
                }
            }
        }
    }

    // ── Text roundtrip ───────────────────────────────────────────────

    #[test]
    fn test_text_roundtrip() {
        let cases: Vec<(&str, i32)> = vec![
            ("{}", 23),
            ("{1,2,3}", 23),
            ("{NULL,1,2}", 23),
            ("{hello,world}", 25),
            (r#"{"a,b"}"#, 25),
            (r#"{"a\"b"}"#, 25),
            (r#"{"a\\b"}"#, 25),
            (r#"{""}"#, 25),
            (r#"{"",NULL}"#, 25),
            (r#"{"NULL"}"#, 25),
            ("{t,f}", 16),
            ("[0:2]={1,2,3}", 23),
        ];

        for (input, oid) in cases {
            let decoded = Array::decode_typed(input.as_bytes(), Format::Text, oid).expect(input);
            let encoded = decoded.encode(Format::Text).expect(input);
            assert_eq!(
                std::str::from_utf8(&encoded).unwrap(),
                input,
                "roundtrip: {input}"
            );
        }
    }

    // ── Cross-format encode ──────────────────────────────────────────

    #[test]
    fn test_text_to_binary_roundtrip() {
        // Decode from text, encode to binary, decode back from binary, encode to text
        let input = "{1,2,3}";
        let text_decoded = Array::decode_typed(input.as_bytes(), Format::Text, 23).unwrap();

        let binary = text_decoded.encode(Format::Binary).unwrap();
        let binary_decoded = Array::decode_typed(&binary, Format::Binary, 23).unwrap();

        let text_again = binary_decoded.encode(Format::Text).unwrap();
        assert_eq!(std::str::from_utf8(&text_again).unwrap(), input);
    }

    #[test]
    fn test_cross_format_with_nulls() {
        let input = "{NULL,42,NULL}";
        let decoded = Array::decode_typed(input.as_bytes(), Format::Text, 23).unwrap();

        let binary = decoded.encode(Format::Binary).unwrap();
        let from_binary = Array::decode_typed(&binary, Format::Binary, 23).unwrap();

        let text = from_binary.encode(Format::Text).unwrap();
        assert_eq!(std::str::from_utf8(&text).unwrap(), input);
    }

    #[test]
    fn test_binary_to_text() {
        // Build a proper binary int4[] and verify it text-encodes correctly
        let decoded = Array::decode_typed(b"{10,20,30}", Format::Text, 23).unwrap();
        let binary = decoded.encode(Format::Binary).unwrap();
        let from_binary = Array::decode_typed(&binary, Format::Binary, 23).unwrap();
        let text = from_binary.encode(Format::Text).unwrap();
        assert_eq!(std::str::from_utf8(&text).unwrap(), "{10,20,30}");
    }

    // ── Binary roundtrip ─────────────────────────────────────────────

    #[test]
    fn test_binary_encode_roundtrip() {
        let cases: Vec<(&str, &str, i32)> = vec![
            ("empty", "{}", 23),
            ("single int", "{1}", 23),
            ("three ints", "{1,2,3}", 23),
            ("with NULL", "{NULL,1,NULL}", 23),
            ("text elems", "{hello,world}", 25),
            ("booleans", "{t,f}", 16),
        ];

        for (name, text_input, oid) in cases {
            let original =
                Array::decode_typed(text_input.as_bytes(), Format::Text, oid).expect(name);
            let binary = original.encode(Format::Binary).expect(name);
            let decoded = Array::decode_typed(&binary, Format::Binary, oid).expect(name);
            let text = decoded.encode(Format::Text).expect(name);
            assert_eq!(
                std::str::from_utf8(&text).unwrap(),
                text_input,
                "binary roundtrip: {name}"
            );
        }
    }

    // ── Rejection / validation ───────────────────────────────────────

    #[test]
    fn test_text_decode_rejects_malformed() {
        let bad: Vec<(&str, &str)> = vec![
            ("{1}junk", "trailing garbage"),
            ("{{1,2},{3,4}}", "nested"),
            ("", "empty string"),
            ("1,2,3", "missing braces"),
            ("{1,,2}", "double comma"),
            ("{1,2,}", "trailing comma"),
            ("{,}", "comma only"),
        ];

        for (input, desc) in bad {
            let result = Array::decode_typed(input.as_bytes(), Format::Text, 23);
            assert!(result.is_err(), "should reject {desc}: {input}");
        }
    }

    #[test]
    fn test_text_decode_validates_bounds() {
        assert!(Array::decode_typed(b"[5:7]={1}", Format::Text, 23).is_err());
        assert!(Array::decode_typed(b"[0:2]={1,2}", Format::Text, 23).is_err());
        assert!(Array::decode_typed(b"[5:7]={}", Format::Text, 23).is_err());
        assert!(Array::decode_typed(b"[0:0]={1}", Format::Text, 23).is_ok());
        // Overflow
        assert!(Array::decode_typed(b"[2147483647:0]={1}", Format::Text, 23).is_err());
    }

    #[test]
    fn test_binary_decode_validates_element_count() {
        // Declares 3 elements but only contains 2
        #[rustfmt::skip]
        let short: &[u8] = &[
            0,0,0,1,  0,0,0,0,  0,0,0,23,
            0,0,0,3,  0,0,0,1,
            0,0,0,4, 0,0,0,1,
            0,0,0,4, 0,0,0,2,
        ];
        assert!(Array::decode_typed(short, Format::Binary, 23).is_err());

        // Negative dim.size
        #[rustfmt::skip]
        let neg: &[u8] = &[
            0,0,0,1,  0,0,0,0,  0,0,0,23,
            255,255,255,255,  0,0,0,1,
        ];
        assert!(Array::decode_typed(neg, Format::Binary, 23).is_err());

        // len=-2 (only -1 is valid NULL)
        #[rustfmt::skip]
        let bad_len: &[u8] = &[
            0,0,0,1,  0,0,0,0,  0,0,0,23,
            0,0,0,1,  0,0,0,1,
            255,255,255,254,
        ];
        assert!(Array::decode_typed(bad_len, Format::Binary, 23).is_err());
    }

    #[test]
    fn test_binary_empty_array_wire_format() {
        let decoded = Array::decode_typed(b"{}", Format::Text, 23).unwrap();
        let encoded = decoded.encode(Format::Binary).unwrap();
        assert_eq!(encoded.len(), 12);

        let pg_empty: &[u8] = &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 23];
        let decoded = Array::decode_typed(pg_empty, Format::Binary, 23).unwrap();
        assert!(decoded.elements.is_empty());
        assert_eq!(decoded.element_oid, 23);
    }

    #[test]
    fn test_empty_array_with_explicit_bounds_roundtrips() {
        let input = b"[0:-1]={}";
        let decoded = Array::decode_typed(input, Format::Text, 23).unwrap();

        let text = decoded.encode(Format::Text).unwrap();
        assert_eq!(
            &text[..],
            input,
            "text encoding should preserve explicit empty-array bounds"
        );

        let binary = decoded.encode(Format::Binary).unwrap();
        let from_binary = Array::decode_typed(&binary, Format::Binary, 23).unwrap();
        let text_from_binary = from_binary.encode(Format::Text).unwrap();
        assert_eq!(
            &text_from_binary[..],
            input,
            "binary encoding should preserve explicit empty-array bounds"
        );
    }

    #[test]
    fn test_binary_ndims_zero_rejects_trailing_data() {
        let bad: &[u8] = &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 23, 0, 0, 0, 1, 1];
        assert!(Array::decode_typed(bad, Format::Binary, 23).is_err());
    }

    // ── Equality ─────────────────────────────────────────────────────

    #[test]
    fn test_equality_across_formats() {
        let text = Array::decode_typed(b"{1,2,3}", Format::Text, 23).unwrap();
        let binary = text.encode(Format::Binary).unwrap();
        let from_binary = Array::decode_typed(&binary, Format::Binary, 23).unwrap();
        assert_eq!(text, from_binary);
    }

    // ── Datum integration ────────────────────────────────────────────

    #[test]
    fn test_datum_array_roundtrip() {
        let datum = Datum::new(b"{1,2,3}", DataType::Array(23), Format::Text, false).unwrap();
        match &datum {
            Datum::Array(arr) => {
                assert_eq!(arr.elements.len(), 3);
            }
            other => panic!("expected Datum::Array, got {other:?}"),
        }

        // Encode to binary
        let binary = datum.encode(Format::Binary).unwrap();
        let datum2 = Datum::new(&binary, DataType::Array(23), Format::Binary, false).unwrap();
        let text = datum2.encode(Format::Text).unwrap();
        assert_eq!(std::str::from_utf8(&text).unwrap(), "{1,2,3}");
    }

    #[test]
    fn test_smallint_array_datum_roundtrip() {
        let input = b"{1,-2,32767}";
        let datum = Datum::new(input, DataType::Array(21), Format::Text, false).unwrap();
        let encoded = datum.encode(Format::Text).unwrap();
        assert_eq!(&encoded[..], input);
    }

    #[test]
    fn test_oid_array_datum_roundtrip() {
        let input = b"{1,2500000000}";
        let datum = Datum::new(input, DataType::Array(26), Format::Text, false).unwrap();
        let encoded = datum.encode(Format::Text).unwrap();
        assert_eq!(&encoded[..], input);
    }

    #[test]
    fn test_interval_array_datum_roundtrip() {
        let input = br#"{"1 year 0 mons 0 days 00:00:00.0"}"#;
        let datum = Datum::new(input, DataType::Array(1186), Format::Text, false).unwrap();
        let encoded = datum.encode(Format::Text).unwrap();
        assert_eq!(&encoded[..], input);
    }

    #[test]
    fn test_interval_array_binary_roundtrip() {
        let input = br#"{"1 year 2 mons 3 days 04:05:06.006"}"#;
        let datum = Datum::new(input, DataType::Array(1186), Format::Text, false).unwrap();

        let binary = datum.encode(Format::Binary).unwrap();
        let decoded = Datum::new(&binary, DataType::Array(1186), Format::Binary, false).unwrap();
        let text = decoded.encode(Format::Text).unwrap();

        assert_eq!(
            &text[..],
            input,
            "interval[] should support text->binary->text roundtrip"
        );
    }

    #[test]
    fn test_smallint_array_to_data_row_column_preserves_value() {
        let datum = Datum::new(b"{1,2}", DataType::Array(21), Format::Text, false).unwrap();
        let data = datum.to_data_row_column();
        assert!(
            !data.is_null(),
            "array serialization should not silently become NULL"
        );
        assert_eq!(&data.data[..], b"{1,2}");
    }

    #[test]
    fn test_unknown_element_type_does_not_panic() {
        // Element OID 99999 is unknown — elements become Datum::Unknown
        // to_data_row_column must not panic
        let datum = Datum::new(
            b"{hello,world}",
            DataType::Array(99999),
            Format::Text,
            false,
        )
        .unwrap();
        let data = datum.to_data_row_column();
        assert!(!data.is_null());
    }

    #[test]
    fn test_binary_decode_rejects_mismatched_element_oid() {
        #[rustfmt::skip]
        let payload: &[u8] = &[
            0,0,0,1,  // ndims
            0,0,0,0,  // flags
            0,0,4,19, // varchar element OID (1043)
            0,0,0,1,  // dim size
            0,0,0,1,  // lower bound
            0,0,0,5,  // element len
            b'h', b'e', b'l', b'l', b'o',
        ];

        assert!(
            Array::decode_typed(payload, Format::Binary, 25).is_err(),
            "binary array decode should reject mismatched wire element OIDs"
        );
    }

    #[test]
    fn test_text_array_special_chars_binary_roundtrip() {
        let cases: Vec<&str> = vec![
            r#"{"hello world"}"#,
            r#"{"a,b","c\"d"}"#,
            r#"{"a\\b"}"#,
            r#"{"",NULL,"not null"}"#,
        ];
        for input in cases {
            let decoded = Array::decode_typed(input.as_bytes(), Format::Text, 25).unwrap();
            let binary = decoded.encode(Format::Binary).unwrap();
            let from_binary = Array::decode_typed(&binary, Format::Binary, 25).unwrap();
            let text = from_binary.encode(Format::Text).unwrap();
            assert_eq!(
                std::str::from_utf8(&text).unwrap(),
                input,
                "special char roundtrip: {input}"
            );
        }
    }

    #[test]
    fn test_binary_decode_rejects_overflowing_bounds() {
        #[rustfmt::skip]
        let payload: &[u8] = &[
            0,0,0,1,                // ndims=1
            0,0,0,0,                // flags=0
            0,0,0,23,               // element OID = int4
            0,0,3,232,              // size=1000
            0x7F,0xFF,0xFF,0xFF,    // lower_bound=i32::MAX
        ];
        assert!(
            Array::decode_typed(payload, Format::Binary, 23).is_err(),
            "should reject lower_bound + size overflow"
        );
    }

    #[test]
    fn test_binary_decode_accepts_max_singleton_lower_bound() {
        #[rustfmt::skip]
        let payload: &[u8] = &[
            0,0,0,1,                // ndims=1
            0,0,0,0,                // flags=0
            0,0,0,23,               // element OID = int4
            0,0,0,1,                // size=1
            0x7F,0xFF,0xFF,0xFF,    // lower_bound=i32::MAX
            0,0,0,4,                // element len
            0,0,0,1,                // element value
        ];

        let decoded = Array::decode_typed(payload, Format::Binary, 23)
            .expect("size=1 at i32::MAX lower bound is a valid array");
        let text = decoded.encode(Format::Text).unwrap();
        assert_eq!(&text[..], b"[2147483647:2147483647]={1}");
    }
}
