use std::fmt::Debug;
use std::str::from_utf8;

use crate::net::c_string_buf_len;

use super::code;
use super::prelude::*;

#[derive(Clone, PartialEq)]
pub(crate) struct Execute {
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
    pub(crate) fn new() -> Self {
        let mut payload = Payload::named('E');
        payload.put_string("");
        payload.put_i32(0);
        Self {
            payload: payload.freeze(),
            portal_len: 0,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_portal(name: &str) -> Self {
        let mut payload = Payload::named('E');
        payload.put_string(name);
        payload.put_i32(0);
        Self {
            payload: payload.freeze(),
            portal_len: name.len() + 1,
        }
    }

    /// Create an Execute message for a named portal with a row limit.
    /// A limit of 0 means fetch all rows.
    #[cfg(test)]
    pub(crate) fn new_portal_limit(name: &str, max_rows: i32) -> Self {
        let mut payload = Payload::named('E');
        payload.put_string(name);
        payload.put_i32(max_rows);
        Self {
            payload: payload.freeze(),
            portal_len: name.len() + 1,
        }
    }

    pub(crate) fn portal(&self) -> &str {
        let start = 5;
        let end = start
            + if self.portal_len > 0 {
                self.portal_len - 1
            } else {
                0
            }; // -1 for terminating NULL.
        let buf = &self.payload[start..end];
        from_utf8(buf).unwrap_or("")
    }

    pub(crate) fn len(&self) -> usize {
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
    fn to_bytes(&self) -> Bytes {
        self.payload.clone()
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

        let exec = Execute::new_portal("test1");
        assert_eq!(exec.portal(), "test1");
    }
}
