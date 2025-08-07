use rand::{thread_rng, Rng};
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
pub struct MirrorRequest {
    buffer: Vec<Buffer>,
}

#[derive(Debug)]
pub(crate) struct Mirror {
    /// Backend connection.
    connection: Connection,
    /// Query router.
    router: Router,
    /// Destination cluster for the mirrored traffic.
    cluster: Cluster,
    /// Mirror's prepared statements. Should be similar
    /// to client's statements, if exposure is high.
    prepared_statements: PreparedStatements,
    /// Mirror connection parameters (empty).
    params: Parameters,
    /// Mirror state.
    state: State,
}

impl Mirror {
    /// Spawn mirror task in the background.
    ///
    /// # Arguments
    ///
    /// * `cluster`: Destination cluster for mirrored traffic.
    ///
    /// # Return
    ///
    /// Handler for sending queries to the background task.
    ///
    pub fn spawn(cluster: &Cluster) -> Result<MirrorHandler, Error> {
        let connection = Connection::new(cluster.user(), cluster.name(), false, &None)?;
        let config = config();

        let mut mirror = Self {
            connection,
            router: Router::new(),
            prepared_statements: PreparedStatements::new(),
            cluster: cluster.clone(),
            state: State::Idle,
            params: Parameters::default(),
        };

        let query_timeout = Timeouts::from_config(&config.config.general);
        let (tx, mut rx) = channel(config.config.general.mirror_queue);
        let handler = MirrorHandler::new(tx, config.config.general.mirror_exposure);

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

    /// Handle a single mirror request.
    pub async fn handle(&mut self, request: &MirrorRequest) -> Result<(), Error> {
        if !self.connection.connected() {
            let routing_buffer = request.buffer.first().ok_or(Error::MirrorBufferEmpty)?;

            if let Ok(context) = RouterContext::new(
                &routing_buffer,
                &self.cluster,
                &mut self.prepared_statements,
                &self.params,
                false,
            ) {
                if let Err(err) = self.router.query(context) {
                    error!("mirror query parse error: {}", err);
                    return Ok(()); // Drop request.
                }

                self.connection
                    .connect(&Request::default(), &self.router.route())
                    .await?;
            }
        }

        // TODO: handle streaming.
        for buffer in &request.buffer {
            self.connection
                .handle_buffer(buffer, &mut self.router, false)
                .await?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Copy)]
enum MirrorHandlerState {
    Dropping,
    Sending,
    Idle,
}

#[derive(Debug)]
pub(crate) struct MirrorHandler {
    tx: Sender<MirrorRequest>,
    exposure: f32,
    state: MirrorHandlerState,
    buffer: Vec<Buffer>,
}

impl MirrorHandler {
    fn new(tx: Sender<MirrorRequest>, exposure: f32) -> Self {
        Self {
            tx,
            exposure,
            state: MirrorHandlerState::Idle,
            buffer: vec![],
        }
    }

    /// Maybe send request to handler.
    pub fn send(&mut self, buffer: &Buffer) -> bool {
        match self.state {
            MirrorHandlerState::Dropping => false,
            MirrorHandlerState::Idle => {
                let roll = if self.exposure < 1.0 {
                    thread_rng().gen_range(0.0..1.0)
                } else {
                    0.99
                };

                if roll < self.exposure {
                    self.state = MirrorHandlerState::Sending;
                    self.buffer.push(buffer.clone());
                    true
                } else {
                    self.state = MirrorHandlerState::Dropping;
                    false
                }
            }
            MirrorHandlerState::Sending => {
                self.buffer.push(buffer.clone());
                true
            }
        }
    }

    pub fn flush(&mut self) -> bool {
        if self.state == MirrorHandlerState::Dropping {
            self.state = MirrorHandlerState::Idle;
            false
        } else {
            self.state = MirrorHandlerState::Idle;
            self.tx
                .try_send(MirrorRequest {
                    buffer: std::mem::take(&mut self.buffer),
                })
                .is_ok()
        }
    }
}
