//! Module: wire_protocol::frontend::function_call
//!
//! Provides parsing and serialization for the FunctionCall message ('F') in the extended protocol.
//!
//! - `FunctionCallFrame`: represents a FunctionCall message with function OID, parameters, and result format.
//! - `Parameter`: reused from bind; distinguishes between text and binary parameter payloads.
//! - `ResultFormat`: reused from bind; indicates text or binary format for result.
//! - `FunctionCallFrameError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `FunctionCallFrame`.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::{error::Error as StdError, fmt, str};

use crate::wire_protocol::shared_property_types::{Parameter, ResultFormat};
use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug)]
pub struct FunctionCallFrame<'a> {
    pub function_oid: u32,
    pub params: Vec<Parameter<'a>>,
    pub result_format: ResultFormat,
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum FunctionCallFrameError {
    Utf8Error(str::Utf8Error),
    UnexpectedEof,
    InvalidLength,
    InvalidFormatCode(i16),
    UnexpectedTag(u8),
}

impl fmt::Display for FunctionCallFrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FunctionCallFrameError::Utf8Error(e) => write!(f, "UTF-8 error: {e}"),
            FunctionCallFrameError::UnexpectedEof => write!(f, "unexpected EOF"),
            FunctionCallFrameError::InvalidLength => write!(f, "invalid length or format code"),
            FunctionCallFrameError::InvalidFormatCode(c) => write!(f, "invalid format code: {c}"),
            FunctionCallFrameError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
        }
    }
}

impl StdError for FunctionCallFrameError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            FunctionCallFrameError::Utf8Error(e) => Some(e),
            _ => None,
        }
    }
}

// -----------------------------------------------------------------------------
// ----- Helpers ---------------------------------------------------------------

#[inline]
fn encode_format_code(buf: &mut BytesMut, is_binary: bool) {
    buf.put_i16(if is_binary { 1 } else { 0 });
}

#[inline]
fn decode_format_code(code: i16) -> Result<bool, FunctionCallFrameError> {
    match code {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(FunctionCallFrameError::InvalidFormatCode(other)),
    }
}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for FunctionCallFrame<'a> {
    type Error = FunctionCallFrameError;

    fn from_bytes(mut bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.remaining() < 5 {
            return Err(FunctionCallFrameError::UnexpectedEof);
        }

        let tag = bytes.get_u8();
        if tag != b'F' {
            return Err(FunctionCallFrameError::UnexpectedTag(tag));
        }

        let len = bytes.get_u32();
        if len < 4 || (len - 4) as usize != bytes.remaining() {
            return Err(FunctionCallFrameError::InvalidLength);
        }

        if bytes.remaining() < 4 {
            return Err(FunctionCallFrameError::UnexpectedEof);
        }
        let function_oid = bytes.get_u32();

        // parameter format codes
        let fmt_count = bytes.get_i16();
        if fmt_count < 0 {
            return Err(FunctionCallFrameError::InvalidLength);
        }

        let mut param_fmts = Vec::with_capacity(fmt_count as usize);
        for _ in 0..fmt_count {
            if bytes.remaining() < 2 {
                return Err(FunctionCallFrameError::UnexpectedEof);
            }
            param_fmts.push(decode_format_code(bytes.get_i16())?);
        }

        // parameters
        if bytes.remaining() < 2 {
            return Err(FunctionCallFrameError::UnexpectedEof);
        }

        let param_count = bytes.get_i16() as usize;
        let mut params = Vec::with_capacity(param_count);
        for idx in 0..param_count {
            if bytes.remaining() < 4 {
                return Err(FunctionCallFrameError::UnexpectedEof);
            }

            let val_len = bytes.get_i32();
            let is_binary = match fmt_count {
                0 => false,
                1 => param_fmts[0],
                _ => param_fmts.get(idx).copied().unwrap_or(false),
            };

            if val_len == -1 {
                params.push(Parameter::Binary(&[]));
                continue;
            }

            if val_len < 0 {
                return Err(FunctionCallFrameError::InvalidLength);
            }

            let len = val_len as usize;
            if bytes.remaining() < len {
                return Err(FunctionCallFrameError::UnexpectedEof);
            }

            let slice = &bytes[..len];
            bytes.advance(len);

            if is_binary {
                params.push(Parameter::Binary(slice));
            } else {
                params.push(Parameter::Text(
                    str::from_utf8(slice).map_err(FunctionCallFrameError::Utf8Error)?,
                ));
            }
        }

        // result format
        if bytes.remaining() < 2 {
            return Err(FunctionCallFrameError::UnexpectedEof);
        }

        let result_code = bytes.get_i16();
        let is_bin = decode_format_code(result_code)?;

        let result_format = if is_bin {
            ResultFormat::Binary
        } else {
            ResultFormat::Text
        };

        if bytes.has_remaining() {
            return Err(FunctionCallFrameError::InvalidLength);
        }

        Ok(FunctionCallFrame {
            function_oid,
            params,
            result_format,
        })
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        let mut body = BytesMut::with_capacity(self.body_size());
        body.put_u32(self.function_oid);

        // param format codes (always per-param)
        body.put_i16(self.params.len() as i16);
        for p in &self.params {
            encode_format_code(&mut body, matches!(p, Parameter::Binary(_)));
        }

        // parameter values
        body.put_i16(self.params.len() as i16);
        for p in &self.params {
            match p {
                Parameter::Text(s) => {
                    body.put_i32(s.len() as i32);
                    body.extend_from_slice(s.as_bytes());
                }
                Parameter::Binary(b) if !b.is_empty() => {
                    body.put_i32(b.len() as i32);
                    body.extend_from_slice(b);
                }
                _ => {
                    body.put_i32(-1);
                }
            }
        }

        // result format
        encode_format_code(
            &mut body,
            matches!(self.result_format, ResultFormat::Binary),
        );

        // wrap with tag + length
        let mut frame = BytesMut::with_capacity(body.len() + 5);
        frame.put_u8(b'F');
        frame.put_u32((body.len() + 4) as u32);
        frame.extend_from_slice(&body);

        Ok(frame.freeze())
    }

    fn body_size(&self) -> usize {
        let mut n = 4; // oid
        n += 2 + self.params.len() * 2; // formats
        n += 2; // param count
        for p in &self.params {
            n += 4;
            match p {
                Parameter::Text(s) => n += s.len(),
                Parameter::Binary(b) => n += b.len(),
            }
        }
        n += 2; // result format
        n
    }
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_text_frame() -> FunctionCallFrame<'static> {
        FunctionCallFrame {
            function_oid: 1234,
            params: vec![Parameter::Text("value")],
            result_format: ResultFormat::Text,
        }
    }

    fn make_binary_null_frame() -> FunctionCallFrame<'static> {
        FunctionCallFrame {
            function_oid: 5678,
            params: vec![Parameter::Binary(&[]), Parameter::Binary(b"\x01\x02")],
            result_format: ResultFormat::Binary,
        }
    }

    #[test]
    fn roundtrip_text_param() {
        let frame = make_text_frame();
        let encoded = frame.to_bytes().unwrap();
        let decoded = FunctionCallFrame::from_bytes(encoded.as_ref()).unwrap();
        assert_eq!(decoded.function_oid, frame.function_oid);
        if let Parameter::Text(t) = &decoded.params[0] {
            assert_eq!(*t, "value");
        } else {
            panic!("expected text param");
        }
        assert!(matches!(decoded.result_format, ResultFormat::Text));
    }

    #[test]
    fn roundtrip_binary_null() {
        let frame = make_binary_null_frame();
        let encoded = frame.to_bytes().unwrap();
        let decoded = FunctionCallFrame::from_bytes(encoded.as_ref()).unwrap();
        assert_eq!(decoded.function_oid, frame.function_oid);
        assert!(matches!(decoded.params[0], Parameter::Binary(ref b) if b.is_empty()));
        assert!(matches!(decoded.params[1], Parameter::Binary(ref b) if b == b"\x01\x02"));
        assert!(matches!(decoded.result_format, ResultFormat::Binary));
    }

    #[test]
    fn deserialize_with_global_format() {
        // Manually craft bytes with fmt_count=1 (all binary)
        let mut body = BytesMut::new();
        body.put_u32(1234); // oid
        body.put_i16(1); // fmt_count
        body.put_i16(1); // binary
        body.put_i16(1); // param_count
        body.put_i32(2); // len
        body.put_slice(b"\x03\x04");
        body.put_i16(0); // result text
        let mut frame_bytes = BytesMut::new();
        frame_bytes.put_u8(b'F');
        frame_bytes.put_u32((body.len() + 4) as u32);
        frame_bytes.extend_from_slice(&body);
        let decoded = FunctionCallFrame::from_bytes(frame_bytes.as_ref()).unwrap();
        assert_eq!(decoded.function_oid, 1234);
        assert!(matches!(decoded.params[0], Parameter::Binary(ref b) if b == b"\x03\x04"));
        assert!(matches!(decoded.result_format, ResultFormat::Text));
    }

    #[test]
    fn deserialize_with_zero_formats() {
        // fmt_count=0 (all text)
        let mut body = BytesMut::new();
        body.put_u32(1234);
        body.put_i16(0); // fmt_count
        body.put_i16(1); // param_count
        body.put_i32(5); // len
        body.put_slice(b"hello");
        body.put_i16(1); // result binary
        let mut frame_bytes = BytesMut::new();
        frame_bytes.put_u8(b'F');
        frame_bytes.put_u32((body.len() + 4) as u32);
        frame_bytes.extend_from_slice(&body);
        let decoded = FunctionCallFrame::from_bytes(frame_bytes.as_ref()).unwrap();
        if let Parameter::Text(t) = &decoded.params[0] {
            assert_eq!(*t, "hello");
        } else {
            panic!("expected text");
        }
        assert!(matches!(decoded.result_format, ResultFormat::Binary));
    }

    #[test]
    fn invalid_tag() {
        let mut bytes = make_text_frame().to_bytes().unwrap().to_vec();
        bytes[0] = b'Q';
        let err = FunctionCallFrame::from_bytes(&bytes).unwrap_err();
        matches!(err, FunctionCallFrameError::UnexpectedTag(_));
    }

    #[test]
    fn invalid_format_code() {
        let mut bytes = make_text_frame().to_bytes().unwrap().to_vec();
        // Corrupt a param format code to 2
        // Offset: tag(1)+len(4)+oid(4)+fmt_count(2)=11, then first fmt i16 at 11-12
        bytes[11] = 0;
        bytes[12] = 2; // 2
        let err = FunctionCallFrame::from_bytes(&bytes).unwrap_err();
        matches!(err, FunctionCallFrameError::InvalidFormatCode(2));
    }

    #[test]
    fn invalid_result_format() {
        let mut bytes = make_text_frame().to_bytes().unwrap().to_vec();
        // Last i16 is result format, set to 3
        let last = bytes.len() - 1;
        bytes[last - 1] = 0;
        bytes[last] = 3;
        let err = FunctionCallFrame::from_bytes(&bytes).unwrap_err();
        matches!(err, FunctionCallFrameError::InvalidFormatCode(3));
    }

    #[test]
    fn unexpected_eof() {
        let bytes = &[b'F', 0, 0, 0, 4][..]; // too short
        let err = FunctionCallFrame::from_bytes(bytes).unwrap_err();
        matches!(err, FunctionCallFrameError::UnexpectedEof);
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
