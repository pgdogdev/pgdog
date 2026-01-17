//! Network errors.

use std::array::TryFromSliceError;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("unexpected payload")]
    UnexpectedPayload,

    #[error("data type not supported for encoding")]
    UnsupportedDataTypeForEncoding,

    #[error("not text encoding")]
    NotTextEncoding,

    #[error("not utf-8")]
    Utf8(#[from] std::str::Utf8Error),

    #[error("not an integer")]
    NotInteger(#[from] std::num::ParseIntError),

    #[error("not a float")]
    NotFloat(#[from] std::num::ParseFloatError),

    #[error("not a uuid")]
    NotUuid(#[from] uuid::Error),

    #[error("not a timestamptz")]
    NotTimestampTz,

    #[error("wrong size slice")]
    WrongSizeSlice(#[from] TryFromSliceError),

    #[error("wrong size binary ({0}) for type")]
    WrongSizeBinary(usize),

    #[error("invalid timestamp components")]
    InvalidTimestamp,

    #[error("array has {0} dimensions, only 1 is supported")]
    ArrayDimensions(usize),

    #[error("not a boolean")]
    NotBoolean,

    #[error("not a pg_lsn")]
    NotPgLsn,

    #[error("lsn decode error")]
    LsnDecode,
}
