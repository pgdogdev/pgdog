pub mod decoder;
pub mod discovery;
pub mod error;
pub mod messages;
pub mod parameter;
pub mod stream;
pub mod tls;
pub mod tweaks;

use bytes::{Buf, Bytes};
pub use decoder::Decoder;
pub use error::Error;
pub use messages::*;
pub use parameter::{Parameter, Parameters};
pub use stream::Stream;
pub use tweaks::tweak;

use std::{io::Cursor, marker::Unpin};
use tokio::io::{AsyncRead, AsyncReadExt};

static MAX_C_STRING_LEN: usize = 4096;

/// Read a C-style String from the stream.
///
/// The string will be NULL-terminated. If not, this function
/// will read up to `MAX_C_STRING_LEN` bytes.
///
/// UTF-8 encoding is expected and no other encoding is supported.
///
pub async fn c_string(stream: &mut (impl AsyncRead + Unpin)) -> Result<String, Error> {
    let mut buf = String::new();
    let mut max = MAX_C_STRING_LEN;

    while let Ok(c) = stream.read_u8().await {
        if c != 0 {
            buf.push(c as char);
        } else {
            break;
        }

        max -= 1;
        if max < 1 {
            break;
        }
    }

    Ok(buf)
}

/// Read a C-Style String from the buffer.
///
/// See [`c_string`] for how this works.
pub fn c_string_buf(buf: &mut Bytes) -> String {
    let len = c_string_buf_len(&buf[..]);
    let mut result = String::with_capacity(len);
    while buf.remaining() > 0 {
        let c = buf.get_u8();

        if c != 0 {
            result.push(c as char);
        } else {
            break;
        }
    }

    result
}

/// Get the length of a C-String including terminating NULL.
pub fn c_string_buf_len(buf: &[u8]) -> usize {
    let mut cursor = Cursor::new(buf);
    let mut len = 0;

    while cursor.has_remaining() {
        let c = cursor.get_u8();
        len += 1;

        if c == 0 {
            break;
        }
    }

    len
}

#[cfg(test)]
mod test {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_c_string_buf() {
        let mut buf = Bytes::from("hello\0world\0");
        assert_eq!(c_string_buf(&mut buf), "hello");
        assert_eq!(c_string_buf(&mut buf), "world");
        assert_eq!(c_string_buf(&mut buf), "");
    }

    #[test]
    fn test_c_string_buf_len() {
        let buf = Bytes::from("hello\0test");
        let len = c_string_buf_len(&buf);

        assert_eq!(len, buf.len() - 4);
    }
}
