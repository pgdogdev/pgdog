use bytes::BytesMut;
use tracing::warn;

use crate::net::messages::{CopyData, Message};

use super::super::code;
use super::super::prelude::*;
use super::logical::begin::Begin;
use super::logical::commit::Commit;
use super::logical::delete::Delete;
use super::logical::insert::Insert;
use super::logical::relation::Relation;
use super::logical::stream_start::StreamStart;
use super::logical::truncate::Truncate;
use super::logical::update::Update;

/// XLogData (B) message.
#[derive(Clone)]
pub(crate) struct XLogData {
    pub(crate) starting_point: i64,
    pub(crate) current_end: i64,
    pub(crate) system_clock: i64,
    pub(crate) bytes: Bytes,
}

impl std::fmt::Debug for XLogData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let payload = self.payload();
        if let Some(payload) = payload {
            f.debug_struct("XLogData")
                .field("starting_point", &self.starting_point)
                .field("current_end", &self.current_end)
                .field("system_clock", &self.system_clock)
                .field("payload", &payload)
                .finish()
        } else {
            f.debug_struct("XLogData")
                .field("starting_point", &self.starting_point)
                .field("current_end", &self.current_end)
                .field("system_clock", &self.system_clock)
                .field("bytes", &self.bytes)
                .finish()
        }
    }
}

impl XLogData {
    /// New relation message.
    pub(crate) fn relation(system_clock: i64, relation: &Relation) -> Result<Self, Error> {
        Ok(Self {
            starting_point: 0,
            current_end: 0,
            system_clock: system_clock - 1, // simulates this to be an older message
            bytes: relation.to_bytes()?,
        })
    }

    /// Convert to message.
    pub(crate) fn to_message(&self) -> Result<Message, Error> {
        Ok(Message::new(CopyData::bytes(self.to_bytes()?).to_bytes()?))
    }

    /// Extract payload.
    pub(crate) fn payload(&self) -> Option<XLogPayload> {
        if self.bytes.is_empty() {
            return None;
        }
        match self.bytes[0] as char {
            'R' => Relation::from_bytes(self.bytes.clone())
                .ok()
                .map(XLogPayload::Relation),
            'I' => Insert::from_bytes(self.bytes.clone())
                .ok()
                .map(XLogPayload::Insert),
            'C' => Commit::from_bytes(self.bytes.clone())
                .ok()
                .map(XLogPayload::Commit),
            'B' => Begin::from_bytes(self.bytes.clone())
                .ok()
                .map(XLogPayload::Begin),
            'T' => Truncate::from_bytes(self.bytes.clone())
                .ok()
                .map(XLogPayload::Truncate),
            'U' => Update::from_bytes(self.bytes.clone())
                .ok()
                .map(XLogPayload::Update),
            'D' => Delete::from_bytes(self.bytes.clone())
                .ok()
                .map(XLogPayload::Delete),
            'S' => StreamStart::from_bytes(self.bytes.clone())
                .ok()
                .map(XLogPayload::Start),
            'E' => Some(XLogPayload::End),
            c => {
                warn!("unknown xlog message: {}", c);
                None
            }
        }
    }

    /// Get stored payload of type.
    ///
    /// Caller is responsible to make sure the message has the right code.
    ///
    pub(crate) fn get<T: FromBytes>(&self) -> Option<T> {
        T::from_bytes(self.bytes.clone()).ok()
    }

    /// Length.
    pub(crate) fn len(&self) -> usize {
        self.bytes.len()
    }
}

impl FromBytes for XLogData {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'w');
        let starting_point = bytes.get_i64();
        let current_end = bytes.get_i64();
        let system_clock = bytes.get_i64();

        Ok(Self {
            starting_point,
            current_end,
            system_clock,
            bytes,
        })
    }
}

impl ToBytes for XLogData {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let mut payload = BytesMut::new();
        payload.put_u8(self.code() as u8);
        payload.put_i64(self.starting_point);
        payload.put_i64(self.current_end);
        payload.put_i64(self.system_clock);
        payload.put(self.bytes.clone());
        Ok(payload.freeze())
    }
}

impl Protocol for XLogData {
    fn code(&self) -> char {
        'w'
    }
}

#[derive(Debug, Clone)]
pub(crate) enum XLogPayload {
    Begin(Begin),
    Commit(Commit),
    Insert(Insert),
    Relation(Relation),
    Truncate(Truncate),
    Update(Update),
    Delete(Delete),
    End,
    Start(StreamStart),
}
