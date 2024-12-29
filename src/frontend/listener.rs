//! Connection listener.
//!
use std::net::SocketAddr;

use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::stream;

use crate::net::messages::{hello::SslReply, Startup, ToBytes};
use crate::net::messages::{AuthenticationOk, ParameterStatus, ReadyForQuery};
use crate::net::messages::{BackendKeyData, Protocol};
use crate::net::tls::acceptor;
use crate::net::Stream;

use tracing::{debug, info};

use super::{Client, Error};

pub struct Listener {
    addr: String,
    clients: Vec<Client>,
}

impl Listener {
    /// Create new client listener.
    pub fn new(addr: impl ToString) -> Self {
        Self {
            addr: addr.to_string(),
            clients: vec![],
        }
    }

    pub async fn listen(&mut self) -> Result<(), Error> {
        info!("🐕 pgDog listening on {}", self.addr);

        let tls = acceptor().await?;

        let listener = TcpListener::bind(&self.addr).await?;

        while let Ok((stream, addr)) = listener.accept().await {
            info!("🔌 {}", addr);

            let mut stream = Stream::Plain(stream);

            loop {
                let startup = Startup::from_stream(&mut stream).await?;

                match startup {
                    Startup::Ssl => {
                        stream.send_flush(SslReply::Yes).await?;
                        let plain = stream.take()?;
                        let cipher = tls.accept(plain).await?;
                        stream = Stream::Tls(cipher);
                    }

                    Startup::Startup { params } => {
                        stream.send(AuthenticationOk::default()).await?;
                        let params = ParameterStatus::fake();
                        for param in params {
                            stream.send(param).await?;
                        }
                        stream.send(BackendKeyData::new()).await?;
                        stream.send_flush(ReadyForQuery::idle()).await?;

                        self.clients.push(Client::new(stream)?);
                        break;
                    }

                    Startup::Cancel { pid, secret } => {
                        break;
                    }
                }
            }
        }

        Ok(())
    }
}
