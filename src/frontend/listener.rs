//! Connection listener.
//!
use tokio::net::{TcpListener, TcpStream};

use super::{Client, Error};

pub struct Listener {
    port: u16,
    host: String,
    clients: Vec<Client>,
}

impl Listener {
    pub async fn listen(&mut self) -> Result<(), Error> {
        let listener = TcpListener::bind((self.host.clone(), self.port)).await?;

        while let Ok(stream) = listener.accept().await {
            let client = Client::new(stream.0)?;
            self.clients.push(client);
        }

        Ok(())
    }
}
