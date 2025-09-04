//! Log queries to a file.
//!
//! DO NOT USE IN PRODUCTION. This is very slow.
//!
use tokio::{fs::OpenOptions, io::AsyncWriteExt};

use crate::config::config;

use super::{ClientRequest, Error};

/// Log queries.
pub(crate) struct QueryLogger<'a> {
    buffer: &'a ClientRequest,
}

impl<'a> QueryLogger<'a> {
    /// Create new query logger.
    pub(crate) fn new(buffer: &'a ClientRequest) -> Self {
        Self { buffer }
    }

    /// Log queries
    pub(crate) async fn log(&self) -> Result<(), Error> {
        let path = &config().config.general.query_log;

        if let Some(path) = path {
            if let Some(query) = self.buffer.query()? {
                let mut file = OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(path)
                    .await?;
                let line = format!("{}\n", query.trim());
                file.write_all(line.as_bytes()).await?;
            }
        }

        Ok(())
    }
}
