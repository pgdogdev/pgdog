use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("upload task is dead")]
    UploadTaskDead,

    #[error("archive s3 bucket is not configured")]
    BucketNotConfigured,

    #[error("failed to serialize WAL segment: {0}")]
    Serialize(#[from] rmp_serde::encode::Error),
}
