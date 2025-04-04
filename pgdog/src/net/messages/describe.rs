//! Describe (F) message.
use crate::net::c_string_buf;

use super::code;
use super::prelude::*;

/// Describe (F) message.
#[derive(Debug, Clone)]
pub struct Describe {
    pub kind: char,
    pub statement: String,
}

impl FromBytes for Describe {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'D');
        let _len = bytes.get_i32();
        let kind = bytes.get_u8() as char;
        let statement = c_string_buf(&mut bytes);

        Ok(Self { kind, statement })
    }
}

impl ToBytes for Describe {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let mut payload = Payload::named(self.code());
        payload.put_u8(self.kind as u8);
        payload.put_string(&self.statement);

        Ok(payload.freeze())
    }
}

impl Protocol for Describe {
    fn code(&self) -> char {
        'D'
    }
}

impl Describe {
    pub fn len(&self) -> usize {
        self.statement.len() + 1 + 1 + 1 + 4
    }

    pub fn anonymous(&self) -> bool {
        self.kind != 'S' || self.statement.is_empty()
    }

    pub fn rename(mut self, name: impl ToString) -> Self {
        self.statement = name.to_string();
        self
    }

    pub fn new_statement(name: &str) -> Describe {
        Describe {
            kind: 'S',
            statement: name.to_string(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        backend::pool::{test::pool, Request},
        net::messages::ErrorResponse,
    };

    #[tokio::test]
    async fn test_describe() {
        let pool = pool();
        let mut conn = pool.get(&Request::default()).await.unwrap();
        let describe = Describe {
            kind: 'P',
            statement: "".into(),
        };
        conn.send(vec![describe.message().unwrap()]).await.unwrap();
        let res = conn.read().await.unwrap();
        let err = ErrorResponse::from_bytes(res.to_bytes().unwrap()).unwrap();
        assert_eq!(err.code, "34000");
    }
}
