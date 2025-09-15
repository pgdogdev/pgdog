use crate::backend::Server;

use super::{Error, Guard, Pool, Request};
use crate::backend::pool::WaiterHandle;
use std::sync::atomic::Ordering;
use tokio::{
    sync::oneshot::*,
    time::{timeout, Instant},
};

pub(super) struct Waiting {
    pool: Pool,
    rx: Receiver<Result<Box<Server>, Error>>,
    request: Request,
}

impl Waiting {
    pub(super) fn new(pool: Pool, request: &Request) -> Result<Self, Error> {
        let request = *request;
        let (tx, rx) = channel();

        // Check online status under lock (brief)
        {
            let guard = pool.lock();
            if !guard.online {
                return Err(Error::Offline);
            }
        }

        // Lock-free send to waiting queue
        let waiter = WaiterHandle { request, tx };
        pool.inner()
            .waiting_tx
            .send(waiter)
            .map_err(|_| Error::Offline)?;

        // Update counter
        pool.inner().waiting_count.fetch_add(1, Ordering::Relaxed);

        // Tell maintenance we are in line waiting for a connection.
        pool.comms().request.notify_one();

        Ok(Self { pool, rx, request })
    }

    pub(super) async fn wait(self) -> Result<(Guard, Instant), Error> {
        let checkout_timeout = self.pool.inner().config.checkout_timeout;
        let server = timeout(checkout_timeout, self.rx).await;

        let now = Instant::now();
        match server {
            Ok(Ok(server)) => {
                let server = server?;
                Ok((Guard::new(self.pool.clone(), server, now), now))
            }

            Err(_err) => {
                // Decrement counter on timeout
                self.pool
                    .inner()
                    .waiting_count
                    .fetch_sub(1, Ordering::Relaxed);

                let mut guard = self.pool.lock();
                if !guard.banned() {
                    guard.maybe_ban(now, Error::CheckoutTimeout);
                }
                // No need to call remove_waiter - dropping rx removes from queue
                Err(Error::CheckoutTimeout)
            }

            // Should not be possible.
            // This means someone removed my waiter from the wait queue,
            // indicating a bug in the pool.
            Ok(Err(_)) => Err(Error::CheckoutTimeout),
        }
    }
}
