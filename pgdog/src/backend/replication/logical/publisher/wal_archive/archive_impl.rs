use tokio::sync::mpsc::Sender;
use uuid::Uuid;

use super::{Error, Segment, Uploader};
use crate::net::CopyData;

pub struct WalArchive {
    unique_id: Uuid,
    counter: usize,
    buffer: Vec<CopyData>,
    sender: Sender<Segment>,
    max_buffer: usize,
}

impl WalArchive {
    fn buffered(&self) -> usize {
        self.buffer.iter().map(|data| data.len()).sum()
    }

    pub(crate) fn new(max_workers: usize, s3_bucket: String, s3_prefix: Option<String>) -> Self {
        Self {
            unique_id: Uuid::new_v4(),
            counter: 0,
            buffer: vec![],
            sender: Uploader::launch(max_workers, s3_bucket, s3_prefix),
            max_buffer: 16 * 1024 * 1024,
        }
    }

    pub(crate) async fn handle(&mut self, data: CopyData) -> Result<(), Error> {
        self.buffer.push(data);

        if self.buffered() >= self.max_buffer {
            self.request_upload().await?;
        }

        Ok(())
    }

    async fn request_upload(&mut self) -> Result<(), Error> {
        let buffer = std::mem::take(&mut self.buffer);
        let data = rmp_serde::to_vec(&buffer)?;

        let segment = Segment {
            counter: self.counter,
            prefix: self.unique_id,
            data,
        };
        self.counter += 1;

        self.sender
            .send(segment)
            .await
            .map_err(|_| Error::UploadTaskDead)?;
        Ok(())
    }
}
