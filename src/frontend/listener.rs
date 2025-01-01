//! Connection listener.
//!

use std::net::SocketAddr;

use tokio::net::{TcpListener, TcpStream};

use crate::net::messages::{hello::SslReply, Startup};
use crate::net::tls::acceptor;
use crate::net::Stream;

use tracing::info;

use super::{Client, Error};

pub struct Listener {
    addr: String,
}

impl Listener {
    /// Create new client listener.
    pub fn new(addr: impl ToString) -> Self {
        Self {
            addr: addr.to_string(),
        }
    }

    pub async fn listen(&mut self) -> Result<(), Error> {
        info!("🐕 pgDog listening on {}", self.addr);

        let listener = TcpListener::bind(&self.addr).await?;

        while let Ok((stream, addr)) = listener.accept().await {
            info!("🔌 {}", addr);

            tokio::spawn(async move {
                Self::handle_client(stream, addr).await?;
                Ok::<(), Error>(())
            });
        }

        Ok(())
    }

    async fn handle_client(stream: TcpStream, addr: SocketAddr) -> Result<(), Error> {
        let mut stream = Stream::plain(stream);
        let tls = acceptor().await?;

        loop {
            let startup = Startup::from_stream(&mut stream).await?;

            match startup {
                Startup::Ssl => {
                    if let Some(ref tls) = tls {
                        stream.send_flush(SslReply::Yes).await?;
                        let plain = stream.take()?;
                        let cipher = tls.accept(plain).await?;
                        stream = Stream::tls(cipher);
                    } else {
                        stream.send_flush(SslReply::No).await?;
                    }
                }

                Startup::Startup { params } => {
                    tokio::spawn(async move {
                        Client::new(stream, params).await?.spawn().await?;

                        info!("disconnected {}", addr);

                        Ok::<(), Error>(())
                    });

                    break;
                }

                Startup::Cancel { pid, secret } => (),
            }
        }

        Ok(())
    }
}
