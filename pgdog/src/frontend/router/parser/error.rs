//! Parser errors.

use thiserror::Error;

/// Parser errors.
#[derive(Debug, Error)]
pub enum Error {
    #[error("syntax error")]
    Syntax,
}
