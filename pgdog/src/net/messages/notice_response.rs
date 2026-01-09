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

#[cfg(test)]
mod test {
    use super::*;
    use crate::net::Payload;

    #[test]
    fn test_notice_response_from_bytes() {
        let mut payload = Payload::named('N');
        payload.put_u8(b'S');
        payload.put_string("NOTICE");
        payload.put_u8(b'V');
        payload.put_string("NOTICE");
        payload.put_u8(b'C');
        payload.put_string("00000");
        payload.put_u8(b'M');
        payload.put_string("test notice message");
        payload.put_u8(b'D');
        payload.put_string("test detail");
        payload.put_u8(b'R');
        payload.put_string("testRoutine");
        payload.put_u8(0);

        let bytes = payload.freeze();
        let notice = NoticeResponse::from_bytes(bytes).unwrap();

        assert_eq!(notice.message.severity, "NOTICE");
        assert_eq!(notice.message.code, "00000");
        assert_eq!(notice.message.message, "test notice message");
        assert_eq!(notice.message.detail, Some("test detail".into()));
        assert_eq!(notice.message.routine, Some("testRoutine".into()));
    }
}
