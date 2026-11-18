use std::sync::Arc;

use crate::{
    backend::{
        ConnectReason, Error as BackendError, Server, ServerOptions,
        pool::{Address, Pool, Shard},
    },
    config::Role,
};

use super::Error;

pub(crate) mod context;
pub(crate) mod copy;
pub(crate) mod parallel_connection;
pub(crate) mod pipeline;
pub(crate) mod replication_origin;
pub(crate) mod stream;

#[cfg(test)]
mod tests;

pub(crate) use context::StreamContext;
pub(crate) use copy::CopySubscriber;
pub(crate) use parallel_connection::ParallelConnection;
pub(crate) use pipeline::PipelinedConnection;

fn primary(shard: &Shard) -> Result<Pool, Error> {
    shard
        .pools_with_roles()
        .into_iter()
        .find(|(role, _)| role == &Role::Primary)
        .map(|(_, pool)| pool)
        .ok_or_else(|| Error::NoPrimary)
}

pub(super) async fn connect_primary(shard: &Shard) -> Result<Server, Error> {
    let primary = primary(shard)?;
    let mut server = Box::pin(Server::connect(
        primary.addr(),
        ServerOptions::new_resharding(primary.config()),
        ConnectReason::Resharding,
        Arc::clone(primary.oids()),
    ))
    .await?;

    // This can't be set via startup parameters on some of the cloud providers due to error
    // `failed: Resharding connection denied for user "user" on database "db":
    // FATAL: 42501 permission denied to set parameter "session_replication_role".`
    // But it works fine if we set the runtime parameter.
    match server
        .execute_checked("SET session_replication_role TO replica")
        .await
    {
        Ok(_) => Ok(server),
        Err(BackendError::ExecutionError(response)) if response.code == "42501" => {
            Err(Error::ReshardingPermissionDenied {
                user: primary.addr().user.clone(),
                database: primary.addr().database_name.clone(),
                source: Box::new(BackendError::ExecutionError(response)),
            })
        }
        Err(error) => Err(error.into()),
    }
}

async fn connect_address(address: &Address) -> Result<Server, Error> {
    Ok(Box::pin(Server::connect(
        address,
        ServerOptions::default(),
        ConnectReason::Resharding,
        Default::default(),
    ))
    .await?)
}
