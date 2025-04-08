use std::io::Cursor;

use bytes::Buf;

use crate::net::{
    Bind, Close, CopyData, Describe, Execute, Flush, FromBytes, Message, Parse, Protocol, Query,
    Sync, ToBytes,
};

#[derive(Debug, Clone)]
pub enum ProtocolMessage {
    Bind(Bind),
    Parse(Parse),
    Describe(Describe),
    Prepare { name: String, statement: String },
    Execute(Execute),
    Close(Close),
    Query(Query),
    Other(Message),
    CopyData(CopyData),
    Sync(Sync),
}

impl ProtocolMessage {
    pub fn extended(&self) -> bool {
        use ProtocolMessage::*;
        matches!(
            self,
            Bind(_) | Parse(_) | Describe(_) | Execute(_) | Sync(_)
        )
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Bind(bind) => bind.len(),
            Self::Parse(parse) => parse.len(),
            Self::Describe(describe) => describe.len(),
            Self::Prepare { statement, .. } => statement.len() + 1 + 1 + 4, // NULL + code + len
            Self::Execute(execute) => execute.len(),
            Self::Close(close) => close.len(),
            Self::Query(query) => query.len(),
            Self::Other(message) => message.len(),
            Self::CopyData(data) => data.len(),
            Self::Sync(sync) => sync.len(),
        }
    }
}

impl Protocol for ProtocolMessage {
    fn code(&self) -> char {
        match self {
            Self::Bind(bind) => bind.code(),
            Self::Parse(parse) => parse.code(),
            Self::Describe(describe) => describe.code(),
            Self::Prepare { .. } => 'Q',
            Self::Execute(execute) => execute.code(),
            Self::Close(close) => close.code(),
            Self::Query(query) => query.code(),
            Self::Other(message) => message.code(),
            Self::CopyData(data) => data.code(),
            Self::Sync(sync) => sync.code(),
        }
    }
}

impl FromBytes for ProtocolMessage {
    fn from_bytes(bytes: bytes::Bytes) -> Result<Self, crate::net::Error> {
        let mut cursor = Cursor::new(&bytes[..]);
        match cursor.get_u8() as char {
            'B' => Ok(Self::Bind(Bind::from_bytes(bytes)?)),
            'P' => Ok(Self::Parse(Parse::from_bytes(bytes)?)),
            'E' => Ok(Self::Execute(Execute::from_bytes(bytes)?)),
            'C' => Ok(Self::Close(Close::from_bytes(bytes)?)),
            'D' => Ok(Self::Describe(Describe::from_bytes(bytes)?)),
            'Q' => Ok(Self::Query(Query::from_bytes(bytes)?)),
            'd' => Ok(Self::CopyData(CopyData::from_bytes(bytes)?)),
            'S' => Ok(Self::Sync(Sync::from_bytes(bytes)?)),
            _ => Ok(Self::Other(Message::from_bytes(bytes)?)),
        }
    }
}

impl ToBytes for ProtocolMessage {
    fn to_bytes(&self) -> Result<bytes::Bytes, crate::net::Error> {
        match self {
            Self::Bind(bind) => bind.to_bytes(),
            Self::Parse(parse) => parse.to_bytes(),
            Self::Describe(describe) => describe.to_bytes(),
            Self::Prepare { statement, .. } => Query::new(statement).to_bytes(),
            Self::Execute(execute) => execute.to_bytes(),
            Self::Close(close) => close.to_bytes(),
            Self::Query(query) => query.to_bytes(),
            Self::Other(message) => message.to_bytes(),
            Self::CopyData(data) => data.to_bytes(),
            Self::Sync(sync) => sync.to_bytes(),
        }
    }
}

impl From<Bind> for ProtocolMessage {
    fn from(value: Bind) -> Self {
        Self::Bind(value)
    }
}

impl From<Parse> for ProtocolMessage {
    fn from(value: Parse) -> Self {
        Self::Parse(value)
    }
}

impl From<Describe> for ProtocolMessage {
    fn from(value: Describe) -> Self {
        Self::Describe(value)
    }
}

impl From<Execute> for ProtocolMessage {
    fn from(value: Execute) -> Self {
        Self::Execute(value)
    }
}

impl From<Close> for ProtocolMessage {
    fn from(value: Close) -> Self {
        Self::Close(value)
    }
}

impl From<Message> for ProtocolMessage {
    fn from(value: Message) -> Self {
        ProtocolMessage::Other(value)
    }
}

impl From<Query> for ProtocolMessage {
    fn from(value: Query) -> Self {
        Self::Query(value)
    }
}

impl From<CopyData> for ProtocolMessage {
    fn from(value: CopyData) -> Self {
        Self::CopyData(value)
    }
}

impl From<Sync> for ProtocolMessage {
    fn from(value: Sync) -> Self {
        Self::Sync(value)
    }
}

impl From<Flush> for ProtocolMessage {
    fn from(value: Flush) -> Self {
        Self::Other(value.message().unwrap())
    }
}
