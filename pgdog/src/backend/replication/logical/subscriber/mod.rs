use std::sync::Arc;

use crate::{
    backend::{ConnectReason, Error as BackendError, Server, ServerOptions, pool::Shard},
    config::Role,
};

use super::Error;

pub(crate) mod context;
pub(crate) mod copy;
pub(crate) mod parallel_connection;
pub(crate) mod pipeline;
pub(crate) mod stream;

#[cfg(test)]
mod tests;

pub(crate) use context::StreamContext;
pub(crate) use copy::CopySubscriber;
pub(crate) use parallel_connection::ParallelConnection;
pub(crate) use pipeline::PipelinedConnection;

async fn connect_primary(shard: &Shard) -> Result<Server, Error> {
    let pools = shard.pools_with_roles();
    let (_, primary) = pools
        .iter()
        .find(|(role, _)| role == &Role::Primary)
        .ok_or(Error::NoPrimary)?;
    match Box::pin(Server::connect(
        primary.addr(),
        ServerOptions::new_resharding(primary.config()),
        ConnectReason::Resharding,
        Arc::clone(primary.oids()),
    ))
    .await
    {
        Ok(server) => Ok(server),
        Err(error)
            if matches!(
                &error,
                BackendError::ConnectionError(response) | BackendError::ExecutionError(response)
                    if response.code == "42501"
            ) =>
        {
            Err(Error::ReshardingPermissionDenied {
                user: primary.addr().user.clone(),
                database: primary.addr().database_name.clone(),
                source: Box::new(error),
            })
        }
        Err(error) => Err(error.into()),
    }
}
