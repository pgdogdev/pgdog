use thiserror::Error;

use crate::net::ErrorResponse;

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error("{0}")]
    Update(#[from] UpdateError),

    #[error("{0}")]
    Insert(#[from] InsertError),

    #[error("frontend: {0}")]
    Frontend(Box<crate::frontend::Error>),

    #[error("parser: {0}")]
    Parser(#[from] crate::frontend::router::parser::Error),

    #[error("deparse: {0}")]
    Deparse(#[from] pg_raw_parse::Error),

    #[error("backend: {0}")]
    Backend(#[from] crate::backend::Error),

    #[error("rewrite: {0}")]
    Rewrite(#[from] crate::frontend::router::parser::rewrite::statement::Error),

    #[error("router: {0}")]
    Router(#[from] crate::frontend::router::Error),

    #[error("{0}")]
    Execution(Box<ErrorResponse>),

    #[error("net: {0}")]
    Net(#[from] crate::net::Error),
}

impl Error {
    /// Errors the client should see as an `ErrorResponse`.
    /// Otherwise, it's an internal failure and propagates up which closes the connection.
    pub(crate) fn into_client_error(self) -> Result<ErrorResponse, Self> {
        match self {
            Self::Execution(error) => Ok(*error),
            err @ (Self::Update(_) | Self::Insert(_) | Self::Rewrite(_)) => {
                Ok(ErrorResponse::from_err(&err))
            }
            err => Err(err),
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum UpdateError {
    #[error("sharding key updates are forbidden")]
    Disabled,

    /// Parser flagged a sharding key update but the planner can't continue.
    /// If we let it continue, this could cause unintended side effects.
    #[error("sharding key update plan doesn't match the parsed statement")]
    PlanMismatch,

    #[error("sharding key update must be executed inside a transaction")]
    TransactionRequired,

    #[error("sharding key update intermediate query has no route")]
    NoRoute,

    #[error("sharding key update changes more than one row ({0})")]
    TooManyRows(usize),

    #[error("sharding key update would move a row referenced by an ON DELETE foreign key")]
    ForeignKeyOnDelete,

    #[error("sharding key update expected an UPDATE statement")]
    NotAnUpdate,

    #[error("sharding key update step \"{0}\" response is missing or incomplete")]
    MissingStepResponse(&'static str),
}

#[derive(Debug, Error)]
pub(crate) enum InsertError {
    #[error("multi-tuple insert requires multi-shard binding")]
    MultiShardRequired,

    /// Parser flagged a multi insert but the planner can't continue.
    /// If we let it continue, this could cause unintended side effects.
    #[error("multi-tuple insert plan doesn't match the parsed statement")]
    PlanMismatch,

    #[error("cache: {0}")]
    Cache(String),
}

impl From<crate::frontend::Error> for Error {
    fn from(value: crate::frontend::Error) -> Self {
        Self::Frontend(Box::new(value))
    }
}

impl From<ErrorResponse> for Error {
    fn from(value: ErrorResponse) -> Self {
        Self::Execution(Box::new(value))
    }
}
