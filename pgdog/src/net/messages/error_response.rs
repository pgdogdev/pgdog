//! ErrorResponse (B) message.
use std::fmt::Display;

use crate::net::{c_string_buf, messages::code};

use super::prelude::*;

/// ErrorResponse (B) message.
#[derive(Debug, Default)]
pub struct ErrorResponse {
    severity: String,
    code: String,
    message: String,
    detail: Option<String>,
}

impl ErrorResponse {
    /// Authentication error.
    pub fn auth(user: &str, database: &str) -> ErrorResponse {
        ErrorResponse {
            severity: "FATAL".into(),
            code: "28000".into(),
            message: format!(
                "password for user \"{}\" and database \"{}\" is wrong, or the database does not exist",
                user, database
            ),
            detail: None,
        }
    }

    /// Connection error.
    pub fn connection() -> ErrorResponse {
        ErrorResponse {
            severity: "ERROR".into(),
            code: "58000".into(),
            message: "connection pool is down".into(),
            detail: None,
        }
    }
}

impl Display for ErrorResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {} {}", self.severity, self.code, self.message)
    }
}

impl FromBytes for ErrorResponse {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'E');
        let _len = bytes.get_i32();

        let mut error_response = ErrorResponse::default();

        while bytes.has_remaining() {
            let field = bytes.get_u8() as char;
            let value = c_string_buf(&mut bytes);

            match field {
                'S' => error_response.severity = value,
                'C' => error_response.code = value,
                'M' => error_response.message = value,
                'D' => error_response.detail = Some(value),
                _ => continue,
            }
        }

        Ok(error_response)
    }
}

impl ToBytes for ErrorResponse {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let mut payload = Payload::named(self.code());

        payload.put_u8(b'S');
        payload.put_string(&self.severity);

        payload.put_u8(b'C');
        payload.put_string(&self.code);

        payload.put_u8(b'M');
        payload.put_string(&self.message);

        if let Some(ref detail) = self.detail {
            payload.put_u8(b'D');
            payload.put_string(detail);
        }

        payload.put_u8(0);

        Ok(payload.freeze())
    }
}

impl Protocol for ErrorResponse {
    fn code(&self) -> char {
        'E'
    }
}
