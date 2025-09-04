//! Binary COPY format.
pub mod header;
pub mod stream;
pub mod tuple;

pub(crate) use stream::BinaryStream;
pub(crate) use tuple::Data;
