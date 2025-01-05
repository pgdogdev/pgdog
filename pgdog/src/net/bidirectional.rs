use bytes::Bytes;
use tokio::sync::mpsc::{Receiver, Sender};

use super::Error;

#[derive(Debug)]
pub enum Message {
    Bytes(Bytes),
    Flush,
}

pub struct Bidirectional {
    tx: Sender<Message>,
    rx: Receiver<Message>,
}

impl Bidirectional {
    pub fn new(tx: Sender<Message>, rx: Receiver<Message>) -> Self {
        Self { tx, rx }
    }

    pub async fn send(&self, message: Message) -> Result<(), Error> {
        if let Ok(_) = self.tx.send(message).await {
            Ok(())
        } else {
            todo!()
        }
    }

    pub async fn recv(&mut self) -> Option<Message> {
        self.rx.recv().await
    }
}
