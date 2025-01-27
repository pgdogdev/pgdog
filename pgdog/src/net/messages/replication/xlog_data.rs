use bytes::BytesMut;

use super::super::code;
use super::super::prelude::*;
use super::logical::begin::Begin;
use super::logical::commit::Commit;
use super::logical::delete::Delete;
use super::logical::insert::Insert;
use super::logical::relation::Relation;
use super::logical::truncate::Truncate;
use super::logical::update::Update;

/// XLogData (B) messsage.
#[derive(Debug, Clone)]
pub struct XLogData {
    starting_point: i64,
    current_end: i64,
    system_clock: i64,
    bytes: Bytes,
}

impl XLogData {
    /// Extract payload.
    pub fn payload(&self) -> Option<XLogPayload> {
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
            _ => None,
        }
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
pub enum XLogPayload {
    Begin(Begin),
    Commit(Commit),
    Insert(Insert),
    Relation(Relation),
    Truncate(Truncate),
    Update(Update),
    Delete(Delete),
}
