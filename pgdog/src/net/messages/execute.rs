use std::fmt::Debug;
use std::str::from_utf8;

use crate::net::c_string_buf_len;

use super::code;
use super::prelude::*;

#[derive(Clone)]
pub struct Execute {
    payload: Bytes,
    portal_len: usize,
}

impl Default for Execute {
    fn default() -> Self {
        Self::new()
    }
}

impl Debug for Execute {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Execute")
            .field("portal", &self.portal())
            .finish()
    }
}

impl Execute {
    pub fn new() -> Self {
        let mut payload = Payload::named('E');
        payload.put_string("");
        payload.put_i32(0);
        Self {
            payload: payload.freeze(),
            portal_len: 0,
        }
    }

    pub fn new_portal(name: &str) -> Self {
        let mut payload = Payload::named('E');
        payload.put_string(name);
        payload.put_i32(0);
        Self {
            payload: payload.freeze(),
            portal_len: name.len() + 1,
        }
    }

    pub fn portal(&self) -> &str {
        let start = 5;
        let end = start + self.portal_len - 1; // -1 for terminating NULL.
        let buf = &self.payload[start..end];
        from_utf8(buf).unwrap_or("")
    }

    /// Number of rows to return.
    pub fn max_rows(&self) -> i32 {
        let mut buf = &self.payload[5 + self.portal_len..];
        buf.get_i32()
    }

    pub fn len(&self) -> usize {
        self.payload.len()
    }
}

impl FromBytes for Execute {
    fn from_bytes(bytes: Bytes) -> Result<Self, Error> {
        code!(&bytes[..], 'E');
        let portal_len = c_string_buf_len(&bytes[5..]);
        Ok(Self {
            payload: bytes,
            portal_len,
        })
    }
}

impl ToBytes for Execute {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        Ok(self.payload.clone())
    }
}

impl Protocol for Execute {
    fn code(&self) -> char {
        'E'
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_execute() {
        let mut payload = Payload::named('E');
        payload.put_string("test");
        payload.put_i32(25);
        let msg = payload.freeze();

        let execute = Execute::from_bytes(msg).unwrap();
        assert_eq!(execute.portal(), "test");
        assert_eq!(execute.max_rows(), 25);

        let exec = Execute::new_portal("test1");
        assert_eq!(exec.portal(), "test1");
    }
}
