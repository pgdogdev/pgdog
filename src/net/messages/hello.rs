//! Client/server connection startup messages.

use crate::net::{c_string, Error};
use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::debug;

use std::marker::Unpin;

use super::{Payload, Protocol, ToBytes};

/// First message a client sends to the server
/// and a server expects from a client.
///
/// See: <https://www.postgresql.org/docs/current/protocol-message-formats.html>
#[derive(Debug)]
pub enum Startup {
    /// SSLRequest (F)
    Ssl,
    /// StartupMessage (F)
    Startup { params: Vec<(String, String)> },
    /// CancelRequet (F)
    Cancel { pid: i32, secret: i32 },
}

impl Startup {
    /// Read Startup message from a stream.
    pub async fn from_stream(stream: &mut (impl AsyncRead + Unpin)) -> Result<Self, Error> {
        let _len = stream.read_i32().await?;
        let code = stream.read_i32().await?;

        debug!("📡 => {}", code);

        match code {
            // SSLRequest (F)
            80877103 => Ok(Startup::Ssl),
            // StartupMessage (F)
            196608 => {
                let mut params = vec![];
                loop {
                    let key = c_string(stream).await?;

                    if key.is_empty() {
                        break;
                    }

                    let value = c_string(stream).await?;
                    params.push((key, value));
                }

                Ok(Startup::Startup { params })
            }
            // CancelRequest (F)
            80877102 => {
                let pid = stream.read_i32().await?;
                let secret = stream.read_i32().await?;

                Ok(Startup::Cancel { pid, secret })
            }

            code => Err(Error::UnsupportedStartup(code)),
        }
    }

    /// Get a startup parameter by name.
    ///
    /// If no such parameter exists, `None` is returned.
    pub fn parameter(&self, name: &str) -> Option<&str> {
        match self {
            Startup::Ssl | Startup::Cancel { .. } => None,
            Startup::Startup { params } => params
                .iter()
                .find(|pair| pair.0 == name)
                .map(|pair| pair.1.as_str()),
        }
    }
}

impl super::ToBytes for Startup {
    fn to_bytes(&self) -> Result<bytes::Bytes, Error> {
        match self {
            Startup::Ssl => {
                let mut buf = BytesMut::new();

                buf.put_i32(8);
                buf.put_i32(80877103);

                Ok(buf.freeze())
            }

            Startup::Cancel { pid, secret } => {
                let mut payload = Payload::new();

                payload.put_i32(80877102);
                payload.put_i32(*pid);
                payload.put_i32(*secret);

                Ok(payload.freeze())
            }

            Startup::Startup { params } => {
                let mut params_buf = BytesMut::new();

                for pair in params {
                    params_buf.put_slice(pair.0.as_bytes());
                    params_buf.put_u8(0);

                    params_buf.put_slice(pair.1.as_bytes());
                    params_buf.put_u8(0);
                }

                let mut payload = Payload::new();

                payload.put_i32(196608);
                payload.put(params_buf);

                Ok(payload.freeze())
            }
        }
    }
}

/// Reply to a SSLRequest (F) message.
#[derive(Debug)]
pub enum SslReply {
    Yes,
    No,
}

impl ToBytes for SslReply {
    fn to_bytes(&self) -> Result<bytes::Bytes, Error> {
        Ok(match self {
            SslReply::Yes => Bytes::from("S"),
            SslReply::No => Bytes::from("N"),
        })
    }
}

impl std::fmt::Display for SslReply {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Yes => "S",
                Self::No => "N",
            }
        )
    }
}

#[cfg(test)]
mod test {
    use crate::net::messages::ToBytes;

    use super::*;
    use bytes::Buf;

    #[test]
    fn test_ssl() {
        let ssl = Startup::Ssl;
        let mut bytes = ssl.to_bytes().unwrap();

        assert_eq!(bytes.get_i32(), 8); // len
        assert_eq!(bytes.get_i32(), 80877103); // request code
    }

    #[tokio::test]
    async fn test_startup() {
        let startup = Startup::Startup {
            params: vec![
                ("user".into(), "postgres".into()),
                ("database".into(), "postgres".into()),
            ],
        };

        let bytes = startup.to_bytes().unwrap();

        assert_eq!(bytes.clone().get_i32(), 40);
    }
}
