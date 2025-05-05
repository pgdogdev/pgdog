use crate::backend::Server;

use super::{Error, Guard, Pool, Request};
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

        {
            let mut guard = pool.lock();
            if !guard.online {
                return Err(Error::Offline);
            }
            guard.waiting.push_back(Waiter { request, tx })
        }

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
                let mut guard = self.pool.lock();
                if !guard.banned() {
                    guard.maybe_ban(now, Error::CheckoutTimeout);
                }
                guard.remove_waiter(&self.request.id);
                Err(Error::CheckoutTimeout)
            }

            // Should not be possible.
            // This means someone removed my waiter from the wait queue,
            // indicating a bug in the pool.
            Ok(Err(_)) => Err(Error::CheckoutTimeout),
        }
    }
}

#[derive(Debug)]
pub(super) struct Waiter {
    pub(super) request: Request,
    pub(super) tx: Sender<Result<Box<Server>, Error>>,
}
