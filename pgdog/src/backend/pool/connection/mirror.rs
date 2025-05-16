use tokio::select;
use tokio::time::timeout;
use tokio::{spawn, sync::mpsc::*};
use tracing::{debug, error};

use crate::backend::Cluster;
use crate::config::config;
use crate::frontend::client::timeouts::Timeouts;
use crate::frontend::{PreparedStatements, Router, RouterContext};
use crate::net::Parameters;
use crate::state::State;
use crate::{
    backend::pool::{Error as PoolError, Request},
    frontend::Buffer,
};

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
    params: Parameters,
    state: State,
}

impl Mirror {
    pub(crate) fn spawn(cluster: &Cluster) -> Result<MirrorHandler, Error> {
        let connection = Connection::new(cluster.user(), cluster.name(), false)?;

        let mut mirror = Self {
            connection,
            router: Router::new(),
            prepared_statements: PreparedStatements::new(),
            cluster: cluster.clone(),
            state: State::Idle,
            params: Parameters::default(),
        };

        let config = config();

        let query_timeout = Timeouts::from_config(&config.config.general);
        let (tx, mut rx) = channel(config.config.general.mirror_queue);
        let handler = MirrorHandler { tx };

        spawn(async move {
            loop {
                let qt = query_timeout.query_timeout(&mirror.state);
                select! {
                    req = rx.recv() => {
                        if let Some(req) = req {
                            // TODO: timeout these.
                            if let Err(err) = mirror.handle(&req).await {
                                if !matches!(err, Error::Pool(PoolError::Offline | PoolError::AllReplicasDown | PoolError::Banned)) {
                                    error!("mirror error: {}", err);
                                }

                                mirror.connection.force_close();
                                mirror.state = State::Idle;
                            } else {
                                mirror.state = State::Active;
                            }
                        } else {
                            debug!("mirror connection shutting down");
                            break;
                        }
                    }

                    message = timeout(qt, mirror.connection.read()) => {
                        match message {
                            Err(_) => {
                                error!("mirror query timeout");
                                mirror.connection.force_close();
                            }
                            Ok(Err(err)) => {
                                error!("mirror error: {}", err);
                                mirror.connection.disconnect();
                            }
                            Ok(_) => (),
                        }

                        if mirror.connection.done() {
                            mirror.connection.disconnect();
                            mirror.router.reset();
                            mirror.state = State::Idle;
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
            if let Ok(context) = RouterContext::new(
                &request.buffer,
                &self.cluster,
                &mut self.prepared_statements,
                &self.params,
            ) {
                if let Err(err) = self.router.query(context) {
                    error!("mirror query parse error: {}", err);
                    return Ok(()); // Drop request.
                }

                self.connection
                    .connect(&request.request, &self.router.route())
                    .await?;
            }
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
}
