use bytes::BytesMut;

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

impl ToBytes for NoticeResponse {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let mut message = BytesMut::from(self.message.to_bytes()?);
        message[0] = self.code() as u8;

        Ok(message.freeze())
    }
}

impl From<ErrorResponse> for NoticeResponse {
    fn from(value: ErrorResponse) -> Self {
        Self { message: value }
    }
}

impl Protocol for NoticeResponse {
    fn code(&self) -> char {
        'N'
    }
}
