use aws_sdk_s3::Client;
use bytes::Bytes;
use tokio::time::{sleep, Duration};
use tracing::{info, warn};

use super::Segment;
use std::sync::Arc;
use tokio::{
    spawn,
    sync::{
        mpsc::{channel, Receiver, Sender},
        Semaphore,
    },
};

pub(super) struct Uploader {
    throttle: Arc<Semaphore>,
    recv: Receiver<Segment>,
    client: Client,
    s3_bucket: String,
    s3_prefix: Option<String>,
}

impl Uploader {
    pub(super) async fn run(&mut self) {
        loop {
            if let Ok(permit) = self.throttle.clone().acquire_owned().await {
                if let Some(segment) = self.recv.recv().await {
                    let client = self.client.clone();
                    let bucket = self.s3_bucket.clone();
                    let prefix = self.s3_prefix.clone();

                    spawn(async move {
                        let key = match &prefix {
                            Some(p) => {
                                format!("{}/{}/{}", p, segment.prefix, segment.counter)
                            }
                            None => format!("{}/{}", segment.prefix, segment.counter),
                        };

                        let data: Bytes = segment.data.into();

                        loop {
                            match client
                                .put_object()
                                .bucket(&bucket)
                                .key(&key)
                                .body(data.clone().into())
                                .send()
                                .await
                            {
                                Ok(_) => {
                                    info!("[archiver] segment {} uploaded to S3", segment.counter);
                                    break;
                                }
                                Err(err) => {
                                    warn!("[acrhiver] failed to upload segment {} to S3: {}, retrying in 1 second", segment.counter, err);
                                    sleep(Duration::from_secs(1)).await;
                                }
                            }
                        }

                        drop(permit);
                    });
                } else {
                    break;
                }
            } else {
                break;
            }
        }
    }

    pub(super) fn launch(
        max_workers: usize,
        s3_bucket: String,
        s3_prefix: Option<String>,
    ) -> Sender<Segment> {
        let (tx, rx) = channel(max_workers);

        spawn(async move {
            let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
            let client = Client::new(&config);

            let mut uploader = Uploader {
                recv: rx,
                throttle: Arc::new(Semaphore::new(max_workers)),
                client,
                s3_bucket,
                s3_prefix,
            };

            uploader.run().await;
        });

        tx
    }
}
