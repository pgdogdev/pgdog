use std::time::Duration;

use rand::{thread_rng, Rng};
use tokio::select;
use tokio::time::{sleep, timeout, Instant};
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

/// Simulate original delay between requests.
#[derive(Clone, Debug)]
struct BufferWithDelay {
    delay: Duration,
    buffer: Buffer,
}

#[derive(Clone, Debug)]
pub struct MirrorRequest {
    buffer: Vec<BufferWithDelay>,
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
                &routing_buffer.buffer,
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
            // Simulate original delay between queries.
            sleep(buffer.delay).await;

            self.connection
                .handle_buffer(&buffer.buffer, &mut self.router, false)
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
    buffer: Vec<BufferWithDelay>,
    timer: Instant,
}

impl MirrorHandler {
    fn new(tx: Sender<MirrorRequest>, exposure: f32) -> Self {
        Self {
            tx,
            exposure,
            state: MirrorHandlerState::Idle,
            buffer: vec![],
            timer: Instant::now(),
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
                    self.buffer.push(BufferWithDelay {
                        buffer: buffer.clone(),
                        delay: Duration::ZERO,
                    });
                    self.timer = Instant::now();
                    true
                } else {
                    self.state = MirrorHandlerState::Dropping;
                    false
                }
            }
            MirrorHandlerState::Sending => {
                let now = Instant::now();
                self.buffer.push(BufferWithDelay {
                    delay: now.duration_since(self.timer),
                    buffer: buffer.clone(),
                });
                self.timer = now;
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

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_mirror_exposure() {
        let (tx, rx) = channel(25);
        let mut handle = MirrorHandler::new(tx.clone(), 1.0);

        for _ in 0..25 {
            assert!(
                handle.send(&vec![].into()),
                "did not to mirror with 1.0 exposure"
            );
            assert!(handle.flush(), "flush didn't work with 1.0 exposure");
        }

        assert_eq!(rx.len(), 25);

        let (tx, rx) = channel(25);

        let mut handle = MirrorHandler::new(tx.clone(), 0.5);
        let dropped = (0..25)
            .into_iter()
            .map(|_| handle.send(&vec![].into()) && handle.send(&vec![].into()) && handle.flush())
            .filter(|s| !s)
            .count();
        let received = 25 - dropped;
        assert_eq!(
            rx.len(),
            received,
            "received more than should of with 50% exposure: {}",
            received
        );
        assert!(
            dropped <= 25 && dropped > 20,
            "dropped should be somewhere near 50%, but actually is {}",
            dropped
        );
    }
}
