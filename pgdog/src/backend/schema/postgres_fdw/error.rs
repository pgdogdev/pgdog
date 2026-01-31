//! Errors for foreign table statement generation.

use std::fmt;
use thiserror::Error;

/// Errors that can occur when building foreign table statements.
#[derive(Debug, Error)]
pub enum Error {
    #[error("no columns provided")]
    NoColumns,
    #[error("format error: {0}")]
    Format(#[from] fmt::Error),
}
