//! Startup, SSLRequest messages.

use crate::net::{
    c_string,
    parameter::{ParameterValue, Parameters},
    Error,
};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::debug;

use std::{marker::Unpin, ops::Deref};

use super::{super::Parameter, FromBytes, Payload, Protocol, ToBytes};

/// First message a client sends to the server
/// and a server expects from a client.
///
/// See: <https://www.postgresql.org/docs/current/protocol-message-formats.html>
#[derive(Debug)]
pub enum Startup {
    /// SSLRequest (F)
    Ssl,
    /// StartupMessage (F)
    Startup { params: Parameters },
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
                let mut params = Parameters::default();
                loop {
                    let name = c_string(stream).await?;

                    if name.is_empty() {
                        break;
                    }

                    let value = c_string(stream).await?;

                    if name == "options" {
                        let kvs = value.split("-c");
                        for kv in kvs {
                            let mut nvs = kv.split("=");
                            let name = nvs.next();
                            let value = nvs.next();

                            if let Some(name) = name {
                                if let Some(value) = value {
                                    let name = name.trim().to_string();
                                    let value = value.trim().to_string();
                                    if !name.is_empty() && !value.is_empty() {
                                        params.insert(name, value);
                                    }
                                }
                            }
                        }
                    } else {
                        params.insert(name, value);
                    }
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
            Startup::Startup { params } => params.get(name).and_then(|s| s.as_str()),
        }
    }

    /// Create new startup message from config.
    pub fn new(user: &str, database: &str, mut params: Vec<Parameter>) -> Self {
        params.extend(vec![
            Parameter {
                name: "user".into(),
                value: user.into(),
            },
            Parameter {
                name: "database".into(),
                value: database.into(),
            },
        ]);
        Self::Startup {
            params: params.into(),
        }
    }

    /// Create new startup TLS request.
    pub fn tls() -> Self {
        Self::Ssl
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

                for (name, value) in params.deref() {
                    if let ParameterValue::String(value) = value {
                        params_buf.put_slice(name.as_bytes());
                        params_buf.put_u8(0);

                        params_buf.put(value.as_bytes());
                        params_buf.put_u8(0);
                    }
                }

                let mut payload = Payload::new();

                payload.put_i32(196608);
                payload.put(params_buf);
                payload.put_u8(0); // Terminating null character.

                Ok(payload.freeze())
            }
        }
    }
}

/// Reply to a SSLRequest (F) message.
#[derive(Debug, PartialEq)]
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

impl Protocol for SslReply {
    fn code(&self) -> char {
        match self {
            SslReply::Yes => 'S',
            SslReply::No => 'N',
        }
    }
}

impl FromBytes for SslReply {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        let answer = bytes.get_u8() as char;
        match answer {
            'S' => Ok(SslReply::Yes),
            'N' => Ok(SslReply::No),
            answer => Err(Error::UnexpectedSslReply(answer)),
        }
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
                Parameter {
                    name: "user".into(),
                    value: "postgres".into(),
                },
                Parameter {
                    name: "database".into(),
                    value: "postgres".into(),
                },
            ]
            .into(),
        };

        let bytes = startup.to_bytes().unwrap();

        assert_eq!(bytes.clone().get_i32(), 41);
    }
}
