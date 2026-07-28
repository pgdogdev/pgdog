use bytes::{BufMut, BytesMut};
use tempfile::TempDir;

use super::super::*;
use crate::net::Error;

#[tokio::test]
async fn test_load_complete_and_incomplete_records() {
    let tmp = TempDir::new().unwrap();
    let path = Segment::path(tmp.path(), 1);
    let mut bytes = BytesMut::new();

    bytes.put_u64(1);
    bytes.put_u32(0);
    bytes.put_u8(b'r');
    bytes.put_i32(12);
    bytes.put_u64(42);

    // A partial trailing record is expected after a crash and should be ignored.
    bytes.put_u8(b'r');
    bytes.put_i32(12);
    bytes.put_u32(43);

    tokio::fs::write(&path, bytes).await.unwrap();

    let segment = Segment::load(&path).await.unwrap();
    assert_eq!(segment.counter, 1);
    assert_eq!(segment.size(), 13);
}

#[tokio::test]
async fn test_load_rejects_invalid_record_length() {
    let tmp = TempDir::new().unwrap();
    let path = Segment::path(tmp.path(), 1);
    let mut bytes = BytesMut::new();

    bytes.put_u64(1);
    bytes.put_u32(0);
    bytes.put_u8(b'r');
    bytes.put_i32(3);

    tokio::fs::write(&path, bytes).await.unwrap();

    assert!(matches!(
        Segment::load(&path).await,
        Err(Error::MalformedMessageLength(3))
    ));
}
