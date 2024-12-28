pub mod bidirectional;
pub mod connection;
pub mod error;
pub mod messages;
pub mod stream;

pub use bidirectional::Bidirectional;
pub use connection::Connection;
pub use error::Error;
pub use stream::Stream;

use std::marker::Unpin;
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
