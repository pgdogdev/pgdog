pub mod copy_statement;
pub mod error;
pub mod publisher;
pub mod status;
pub mod subscriber;

pub use copy_statement::CopyStatement;
pub use error::Error;

pub use publisher::publisher_impl::Publisher;
pub use subscriber::{CopySubscriber, StreamSubscriber};
use tokio::spawn;

use crate::{
    backend::{
        databases::{databases, reload_from_existing},
        schema::sync::{schema_sync, SyncState},
    },
    config::config,
};

pub async fn reshard(source: &str, destination: &str, publication: &str) -> Result<(), Error> {
    let src = databases().schema_owner(source)?;
    let dest = databases().schema_owner(destination)?;

    schema_sync(&src, &dest, publication, SyncState::PreData, false, false)
        .await
        .unwrap();

    // Load schema.
    reload_from_existing()?;

    let dest = databases().schema_owner(destination)?;
    dest.wait_schema_loaded().await;

    let mut publisher = Publisher::new(
        &src,
        publication,
        config().config.general.query_parser_engine,
    );

    publisher.data_sync(&dest, false, None).await?;

    let src = src.clone();
    let dest = dest.clone();
    let publication = publication.to_owned();

    // Do this async.
    spawn(async move {
        schema_sync(&src, &dest, &publication, SyncState::PostData, false, false)
            .await
            .unwrap();
    });

    Ok(())
}
