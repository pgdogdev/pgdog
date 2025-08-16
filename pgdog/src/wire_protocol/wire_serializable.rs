use bytes::Bytes;
use std::error::Error as StdError;

pub trait WireSerializable<'a>: Sized {
    type Error: StdError + Send + Sync + 'static;

    /// Serialize the object into bytes for wire transmission.
    fn to_bytes(&self) -> Result<Bytes, Self::Error>;

    /// Deserialize from bytes into the object.
    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error>;

    fn body_size(&self) -> usize;
}
