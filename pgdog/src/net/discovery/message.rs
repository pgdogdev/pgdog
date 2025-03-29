use rmp_serde::{decode, encode, Deserializer, Serializer};
use serde::{Deserialize, Serialize};

use crate::frontend::comms::comms;

/// Message kind.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Payload {
    Healthcheck,
    Stats { clients: u64 },
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

    /// Healthcheck message.
    pub fn healthcheck(node_id: u64) -> Self {
        Self {
            node_id,
            payload: Payload::Healthcheck,
        }
    }

    /// Collect statistics.
    pub fn stats(node_id: u64) -> Self {
        let clients = comms().len() as u64;

        Self {
            node_id,
            payload: Payload::Stats { clients },
        }
    }
}
