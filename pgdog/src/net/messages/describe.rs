//! Describe (F) message.
use std::fmt::Debug;
use std::str::from_utf8;
use std::str::from_utf8_unchecked;

use super::code;
use super::prelude::*;

/// Describe (F) message.
#[derive(Clone)]
pub struct Describe {
    payload: Bytes,
    original: Option<Bytes>,
}

impl Debug for Describe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Describe")
            .field("kind", &self.kind())
            .field("name", &self.statement())
            .finish()
    }
}

impl FromBytes for Describe {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        let original = bytes.clone();
        code!(bytes, 'D');

        from_utf8(&original[6..original.len() - 1])?;

        Ok(Self {
            payload: original.clone(),
            original: Some(original),
        })
    }
}

impl ToBytes for Describe {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        if let Some(ref original) = self.original {
            return Ok(original.clone());
        }

        Ok(self.payload.clone())
    }
}

impl Protocol for Describe {
    fn code(&self) -> char {
        'D'
    }
}

impl Describe {
    pub fn len(&self) -> usize {
        self.payload.len()
    }

    pub fn anonymous(&self) -> bool {
        self.kind() != 'S' || self.statement().is_empty()
    }

    pub fn rename(self, name: impl ToString) -> Self {
        let mut payload = Payload::named('D');
        payload.put_u8(self.kind() as u8);
        payload.put_string(&name.to_string());
        Describe {
            payload: payload.freeze(),
            original: None,
        }
    }

    pub fn new_statement(name: &str) -> Describe {
        let mut payload = Payload::named('D');
        payload.put_u8(b'S');
        payload.put_string(name);
        Describe {
            payload: payload.freeze(),
            original: None,
        }
    }

    pub fn is_statement(&self) -> bool {
        self.kind() == 'S'
    }

    pub fn is_portal(&self) -> bool {
        self.kind() == 'P'
    }

    pub fn new_portal(name: &str) -> Describe {
        let mut payload = Payload::named('D');
        payload.put_u8(b'P');
        payload.put_string(name);
        Describe {
            payload: payload.freeze(),
            original: None,
        }
    }

    #[inline]
    pub(crate) fn statement(&self) -> &str {
        // SAFETY: Name is checked for utf-8 in Bytes::from_bytes
        unsafe { from_utf8_unchecked(&self.payload[6..self.payload.len() - 1]) }
    }

    pub fn kind(&self) -> char {
        self.payload[5] as char
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        backend::pool::{test::pool, Request},
        net::{messages::ErrorResponse, ProtocolMessage},
    };

    #[tokio::test]
    async fn test_describe() {
        let pool = pool();
        let mut conn = pool.get(&Request::default()).await.unwrap();
        let describe = Describe::new_portal("");
        conn.send(&vec![ProtocolMessage::from(describe.message().unwrap())].into())
            .await
            .unwrap();
        let res = conn.read().await.unwrap();
        let err = ErrorResponse::from_bytes(res.to_bytes().unwrap()).unwrap();
        assert_eq!(err.code, "34000");

        let describe = Describe::new_statement("test");
        assert_eq!(describe.len(), describe.to_bytes().unwrap().len());
    }
}
