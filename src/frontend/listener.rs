//! Connection listener.
//!

use tokio::net::TcpListener;

use crate::net::messages::{hello::SslReply, Startup};
use crate::net::messages::{Authentication, ParameterStatus};
use crate::net::tls::acceptor;
use crate::net::Stream;

use tracing::info;

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

            let mut stream = Stream::plain(stream);

            loop {
                let startup = Startup::from_stream(&mut stream).await?;

                match startup {
                    Startup::Ssl => {
                        stream.send_flush(SslReply::Yes).await?;
                        let plain = stream.take()?;
                        let cipher = tls.accept(plain).await?;
                        stream = Stream::tls(cipher);
                    }

                    Startup::Startup { params } => {
                        stream.send(Authentication::Ok).await?;
                        let params = ParameterStatus::fake();
                        for param in params {
                            stream.send(param).await?;
                        }

                        self.clients.push(Client::new(stream).await?);
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
