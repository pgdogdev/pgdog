use rmp_serde::{decode, encode, Deserializer, Serializer};
use serde::{Deserialize, Serialize};

/// Message kind.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Payload {
    Startup,
    Healthcheck,
}

/// Message sent via UDP.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Message {
    pub node_id: u64,
    pub payload: Payload,
}

impl Message {
    /// Convert message to bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, encode::Error> {
        let mut buf = vec![];
        self.serialize(&mut Serializer::new(&mut buf))?;
        Ok(buf)
    }

    /// Convert bytes to message.
    pub fn from_bytes(buf: &[u8]) -> Result<Self, decode::Error> {
        Message::deserialize(&mut Deserializer::new(buf))
    }

    pub fn healthcheck(node_id: u64) -> Self {
        Self {
            node_id,
            payload: Payload::Healthcheck,
        }
    }
}
