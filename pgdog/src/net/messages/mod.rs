//! PostgreSQL wire protocol messages.
pub mod auth;
pub mod backend_key;
pub mod bind;
pub mod command_complete;
pub mod data_row;
pub mod error_response;
pub mod hello;
pub mod parameter_status;
pub mod parse;
pub mod payload;
pub mod prelude;
pub mod query;
pub mod rfq;
pub mod row_description;
pub mod terminate;

pub use auth::{Authentication, Password};
pub use backend_key::BackendKeyData;
pub use bind::Bind;
pub use data_row::{DataRow, ToDataRowColumn};
pub use error_response::ErrorResponse;
pub use hello::Startup;
pub use parameter_status::ParameterStatus;
pub use payload::Payload;
pub use query::Query;
pub use rfq::ReadyForQuery;
pub use row_description::{Field, RowDescription};
pub use terminate::Terminate;

use crate::net::Error;

use bytes::Bytes;

/// Convert a Rust struct to a PostgreSQL wire protocol message.
pub trait ToBytes {
    /// Create the protocol message as an array of bytes.
    /// The message must conform to the spec. No additional manipulation
    /// of the data will take place.
    fn to_bytes(&self) -> Result<Bytes, Error>;
}

/// Convert a PostgreSQL wire protocol message to a Rust struct.
pub trait FromBytes: Sized {
    /// Perform the conversion.
    fn from_bytes(bytes: Bytes) -> Result<Self, Error>;
}

/// PostgreSQL wire protocol message.
pub trait Protocol: ToBytes + FromBytes {
    /// 99% of messages have a letter code.
    fn code(&self) -> char;

    /// Convert to message.
    fn message(&self) -> Result<Message, Error> {
        Ok(Message::new(self.to_bytes()?))
    }
}

/// PostgreSQL protocol message.
#[derive(Debug, Clone)]
pub struct Message {
    payload: Bytes,
}

impl ToBytes for Message {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        Ok(self.payload.clone())
    }
}

impl Protocol for Message {
    fn code(&self) -> char {
        self.payload[0] as char
    }
}

impl FromBytes for Message {
    fn from_bytes(bytes: Bytes) -> Result<Self, Error> {
        Ok(Self { payload: bytes })
    }
}

impl Message {
    /// Create new message from network payload.
    pub fn new(payload: Bytes) -> Self {
        Self { payload }
    }

    /// Take the message payload.
    pub fn payload(&self) -> Bytes {
        self.payload.clone()
    }

    /// Number of bytes in the message.
    pub fn len(&self) -> usize {
        self.payload.len()
    }
}

/// Check that the message we received is what we expected.
/// Return an error otherwise.
macro_rules! code {
    ($code: expr, $expected: expr) => {{
        let code = $code.get_u8() as char;
        let expected = $expected as char;
        if code != expected {
            return Err(crate::net::Error::UnexpectedMessage(expected, code));
        }
    }};
}

pub(crate) use code;
