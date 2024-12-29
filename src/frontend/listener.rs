//! Connection listener.
//!
use std::net::SocketAddr;

use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};

use crate::net::messages::{hello::SslReply, Startup, ToBytes};
use crate::net::messages::{AuthenticationOk, ParameterStatus, ReadyForQuery};
use crate::net::messages::{BackendKeyData, Protocol};
use crate::net::tls::acceptor;

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
        let listener = TcpListener::bind(&self.addr).await?;

        while let Ok((mut stream, addr)) = listener.accept().await {
            info!("🔌 {}", addr);
            let tls = acceptor().await?;

            loop {
                let startup = Startup::from_stream(&mut stream).await?;

                match startup {
                    Startup::Ssl => {
                        let no = SslReply::No;

                        debug!("📡 <= {}", no);

                        stream.write_all(&no.to_bytes()?).await?;
                        stream.flush().await?;
                    }

                    Startup::Startup { params } => {
                        AuthenticationOk::default().write(&mut stream).await?;
                        let params = ParameterStatus::fake();
                        for param in params {
                            param.write(&mut stream).await?;
                        }
                        BackendKeyData::new().write(&mut stream).await?;
                        ReadyForQuery::idle().write(&mut stream).await?;
                        stream.flush().await?;
                        break;
                    }

                    Startup::Cancel { pid, secret } => {
                        todo!()
                    }
                }
            }
        }

        Ok(())
    }
}
