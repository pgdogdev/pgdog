use thiserror::Error;

use crate::backend;

use super::PlanRequest;

#[derive(Debug, Error)]
pub enum Error {
    #[error("serde: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("deser")]
    Deser,

    #[error("{0}")]
    Backend(#[from] backend::Error),

    #[error("server not in sync")]
    NotInSync,

    #[error("{0}")]
    Net(#[from] crate::net::Error),

    #[error("{0}")]
    Send(#[from] tokio::sync::mpsc::error::SendError<PlanRequest>),

    #[error("plan channel is down")]
    Recv,

    #[error("buffer has nothing to plan")]
    NothingToPlan,
}
