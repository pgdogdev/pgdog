pub mod hello;
pub use hello::Startup;

pub mod payload;
pub use payload::Payload;

use crate::net::Error;

use bytes::Bytes;

pub trait ToBytes {
    fn to_bytes(&self) -> Result<Bytes, Error>;
}
