//! Binary COPY format.
pub(crate) mod header;
pub(crate) mod stream;
pub(crate) mod tuple;

pub(crate) use stream::BinaryStream;
pub(crate) use tuple::Data;
