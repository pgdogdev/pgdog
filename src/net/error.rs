//! Frontend errors.

use thiserror::Error;

use super::connection::Message;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Mpsc(#[from] tokio::sync::mpsc::error::SendError<Message>),
}
