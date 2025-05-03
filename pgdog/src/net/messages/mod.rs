//! PostgreSQL wire protocol messages.
pub mod auth;
pub mod backend_key;
pub mod bind;
pub mod close;
pub mod close_complete;
pub mod command_complete;
pub mod copy_data;
pub mod data_row;
pub mod data_types;
pub mod describe;
pub mod error_response;
pub mod execute;
pub mod flush;
pub mod hello;
pub mod notice_response;
pub mod parameter_description;
pub mod parameter_status;
pub mod parse;
pub mod parse_complete;
pub mod payload;
pub mod prelude;
pub mod query;
pub mod replication;
pub mod rfq;
pub mod row_description;
pub mod sync;
pub mod terminate;

pub use auth::{Authentication, Password};
pub use backend_key::BackendKeyData;
pub use bind::{Bind, Format, Parameter, ParameterWithFormat};
pub use close::Close;
pub use close_complete::CloseComplete;
pub use command_complete::CommandComplete;
pub use copy_data::CopyData;
pub use data_row::{DataRow, ToDataRowColumn};
pub use data_types::*;
pub use describe::Describe;
pub use error_response::ErrorResponse;
pub use execute::Execute;
pub use flush::Flush;
pub use hello::Startup;
pub use notice_response::NoticeResponse;
pub use parameter_description::ParameterDescription;
pub use parameter_status::ParameterStatus;
pub use parse::Parse;
pub use parse_complete::ParseComplete;
pub use payload::Payload;
pub use query::Query;
pub use rfq::ReadyForQuery;
pub use row_description::{Field, RowDescription};
pub use sync::Sync;
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
pub trait Protocol: ToBytes + FromBytes + std::fmt::Debug {
    /// 99% of messages have a letter code.
    fn code(&self) -> char;

    /// Convert to message.
    fn message(&self) -> Result<Message, Error> {
        Ok(Message::new(self.to_bytes()?))
    }

    /// Message is part of a stream and should not be buffered.
    fn streaming(&self) -> bool {
        false
    }
}

#[derive(Clone, PartialEq, Default, Copy, Debug)]
pub enum Source {
    Backend,
    #[default]
    Frontend,
}

/// PostgreSQL protocol message.
#[derive(Clone, Default, PartialEq)]
pub struct Message {
    payload: Bytes,
    stream: bool,
    source: Source,
}

impl std::fmt::Debug for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.code() {
            'Q' => Query::from_bytes(self.payload()).unwrap().fmt(f),
            'D' => match self.source {
                Source::Backend => DataRow::from_bytes(self.payload()).unwrap().fmt(f),
                Source::Frontend => Describe::from_bytes(self.payload()).unwrap().fmt(f),
            },
            'P' => Parse::from_bytes(self.payload()).unwrap().fmt(f),
            'B' => Bind::from_bytes(self.payload()).unwrap().fmt(f),
            'S' => match self.source {
                Source::Frontend => f.debug_struct("Sync").finish(),
                Source::Backend => ParameterStatus::from_bytes(self.payload()).unwrap().fmt(f),
            },
            '1' => ParseComplete::from_bytes(self.payload()).unwrap().fmt(f),
            '2' => f.debug_struct("BindComplete").finish(),
            '3' => f.debug_struct("CloseComplete").finish(),
            'E' => match self.source {
                Source::Frontend => f.debug_struct("Execute").finish(),
                Source::Backend => ErrorResponse::from_bytes(self.payload()).unwrap().fmt(f),
            },
            'T' => RowDescription::from_bytes(self.payload()).unwrap().fmt(f),
            'Z' => ReadyForQuery::from_bytes(self.payload()).unwrap().fmt(f),
            'C' => match self.source {
                Source::Backend => CommandComplete::from_bytes(self.payload()).unwrap().fmt(f),
                Source::Frontend => Close::from_bytes(self.payload()).unwrap().fmt(f),
            },
            'd' => CopyData::from_bytes(self.payload()).unwrap().fmt(f),
            'W' => f.debug_struct("CopyBothResponse").finish(),
            'I' => f.debug_struct("EmptyQueryResponse").finish(),
            't' => ParameterDescription::from_bytes(self.payload())
                .unwrap()
                .fmt(f),
            'H' => f.debug_struct("Flush").finish(),
            _ => f
                .debug_struct("Message")
                .field("payload", &self.payload())
                .finish(),
        }
    }
}

impl ToBytes for Message {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        Ok(Bytes::clone(&self.payload))
    }
}

impl Protocol for Message {
    fn code(&self) -> char {
        self.payload[0] as char
    }

    fn streaming(&self) -> bool {
        self.stream
    }
}

impl FromBytes for Message {
    fn from_bytes(bytes: Bytes) -> Result<Self, Error> {
        Ok(Self {
            payload: bytes,
            stream: false,
            source: Source::default(),
        })
    }
}

impl Message {
    /// Create new message from network payload.
    pub fn new(payload: Bytes) -> Self {
        Self {
            payload,
            stream: false,
            source: Source::default(),
        }
    }

    /// This message is part of a stream and should be flushed asap.
    pub fn stream(mut self, stream: bool) -> Self {
        self.stream = stream;
        self
    }

    /// Take the message payload.
    pub fn payload(&self) -> Bytes {
        self.payload.clone()
    }

    /// Number of bytes in the message.
    pub fn len(&self) -> usize {
        self.payload.len()
    }

    /// Is the message empty?
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// This message is coming from the backend.
    pub fn backend(mut self) -> Self {
        self.source = Source::Backend;
        self
    }

    /// This message is coming from the frontend.
    pub fn frontend(mut self) -> Self {
        self.source = Source::Frontend;
        self
    }

    /// Where is this message coming from?
    pub fn source(&self) -> Source {
        self.source
    }

    pub fn in_transaction(&self) -> bool {
        self.code() == 'Z' && matches!(self.payload[5] as char, 'T' | 'E')
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
