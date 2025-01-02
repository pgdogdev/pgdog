//! Connection pool errors.
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("checkout timeout")]
    CheckoutTimeout,
}
