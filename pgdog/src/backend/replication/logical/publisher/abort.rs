use tokio::sync::mpsc::UnboundedSender;

use super::super::Error;
use super::*;

pub struct AbortSignal {
    tx: UnboundedSender<Result<Table, Error>>,
}

impl AbortSignal {
    pub fn new(tx: UnboundedSender<Result<Table, Error>>) -> Self {
        Self { tx }
    }

    pub async fn aborted(&self) {
        self.tx.closed().await
    }
}
