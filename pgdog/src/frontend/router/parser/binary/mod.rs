//! Binary COPY format.
pub mod header;
pub mod stream;
pub mod tuple;

pub use stream::BinaryStream;
pub use tuple::{Data, Tuple};
