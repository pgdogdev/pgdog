//! Connection listener.
//!
use std::net::SocketAddr;

use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};

use crate::net::messages::{hello::SslReply, Startup, ToBytes};

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

        while let Ok((mut stream, _)) = listener.accept().await {
            loop {
                let startup = Startup::from_stream(&mut stream).await?;

                match startup {
                    Startup::Ssl => {
                        let no = SslReply::No.to_bytes()?;

                        stream.write_all(&no).await?;
                        stream.flush().await?;
                    }

                    Startup::Startup { params } => {
                        println!("startup: {:?}", params);
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
