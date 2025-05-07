use std::sync::Arc;

use tokio::select;
use tokio::sync::Notify;
use tokio::{spawn, sync::mpsc::*};
use tracing::error;

use crate::backend::Cluster;
use crate::frontend::{PreparedStatements, Router};
use crate::{backend::pool::Request, frontend::Buffer};

use super::Connection;
use super::Error;

#[derive(Clone, Debug)]
pub(crate) struct MirrorRequest {
    pub(super) request: Request,
    pub(super) buffer: Buffer,
}

impl MirrorRequest {
    pub(crate) fn new(buffer: &Buffer) -> Self {
        Self {
            request: Request::default(),
            buffer: buffer.clone(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct Mirror {
    connection: Connection,
    router: Router,
    cluster: Cluster,
    prepared_statements: PreparedStatements,
}

impl Mirror {
    pub(crate) fn new(cluster: &Cluster) -> Result<MirrorHandler, Error> {
        let connection = Connection::new(cluster.user(), cluster.name(), false)?;
        let shutdown = Arc::new(Notify::new());

        let mut mirror = Self {
            connection,
            router: Router::new(),
            prepared_statements: PreparedStatements::new(),
            cluster: cluster.clone(),
        };

        let (tx, mut rx) = channel(128);
        let handler = MirrorHandler {
            tx,
            shutdown: shutdown.clone(),
        };

        spawn(async move {
            loop {
                select! {
                    _ = shutdown.notified() => {
                        break;
                    }

                    req = rx.recv() => {
                        if let Some(req) = req {
                            // TODO: timeout these.
                            if let Err(err) = mirror.handle(&req).await {
                                error!("mirror error: {}", err);
                                mirror.connection.disconnect();
                            }
                        }
                    }

                    message = mirror.connection.read() => {
                        if let Err(err) = message {
                            error!("mirror error: {}", err);
                            mirror.connection.disconnect();
                        }

                        if mirror.connection.done() {
                            mirror.connection.disconnect();
                            mirror.router.reset();
                        }
                    }
                }
            }
        });

        Ok(handler)
    }

    pub(crate) async fn handle(&mut self, request: &MirrorRequest) -> Result<(), Error> {
        if !self.connection.connected() {
            // TODO: handle parsing errors.
            if let Err(err) = self.router.query(
                &request.buffer,
                &self.cluster,
                &mut self.prepared_statements,
            ) {
                error!("mirror query parse error: {}", err);
                return Ok(()); // Drop request.
            }

            self.connection
                .connect(&request.request, &self.router.route())
                .await?;
        }

        // TODO: handle streaming.
        self.connection
            .handle_buffer(&request.buffer, &mut self.router, false)
            .await?;

        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct MirrorHandler {
    pub(super) tx: Sender<MirrorRequest>,
    shutdown: Arc<Notify>,
}

impl Drop for MirrorHandler {
    fn drop(&mut self) {
        self.shutdown.notify_one();
    }
}
