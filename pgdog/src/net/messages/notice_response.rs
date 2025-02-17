use super::{prelude::*, ErrorResponse};

#[derive(Debug)]
pub struct NoticeResponse {
    pub message: ErrorResponse,
}

impl FromBytes for NoticeResponse {
    fn from_bytes(bytes: Bytes) -> Result<Self, Error> {
        Ok(Self {
            message: ErrorResponse::from_bytes(bytes)?,
        })
    }
}
