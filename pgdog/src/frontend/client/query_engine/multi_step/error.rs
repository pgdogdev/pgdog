use thiserror::Error;

use crate::net::ErrorResponse;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Update(#[from] UpdateError),

    #[error("frontend: {0}")]
    Frontend(Box<crate::frontend::Error>),

    #[error("backend: {0}")]
    Backend(#[from] crate::backend::Error),

    #[error("rewrite: {0}")]
    Rewrite(#[from] crate::frontend::router::parser::rewrite::statement::Error),

    #[error("router: {0}")]
    Router(#[from] crate::frontend::router::Error),

    #[error("{0}")]
    Execution(ErrorResponse),

    #[error("net: {0}")]
    Net(#[from] crate::net::Error),
}

#[derive(Debug, Error)]
pub enum UpdateError {
    #[error("sharding key updates are forbidden")]
    Disabled,

    #[error("sharding key update must be executed inside a transaction")]
    TransactionRequired,

    #[error("sharding key update intermediate query has no route")]
    NoRoute,

    #[error("sharding key update changes more than one row ({0})")]
    TooManyRows(usize),
}

impl From<crate::frontend::Error> for Error {
    fn from(value: crate::frontend::Error) -> Self {
        Self::Frontend(Box::new(value))
    }
}
