use std::ops::Deref;

use crate::backend::Server;

use super::{Error, Guard, Pool, Request};
use tokio::{
    sync::oneshot::*,
    time::{timeout, Instant},
};

pub(super) struct Waiting {
    pool: Pool,
    rx: Option<Receiver<Result<Box<Server>, Error>>>,
    request: Request,
    waiting: bool,
}

impl Drop for Waiting {
    fn drop(&mut self) {
        if self.waiting {
            self.pool.lock().remove_waiter(&self.request.id);
        }
    }
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

        Ok(Self {
            pool,
            rx: Some(rx),
            request,
            waiting: true,
        })
    }

    /// Wait for connection from the pool.
    pub(super) async fn wait(&mut self) -> Result<(Guard, Instant), Error> {
        let checkout_timeout = self.pool.inner().config.checkout_timeout;
        let rx = self.rx.take().expect("waiter rx taken");

        // Make this cancellation-safe.
        let mut wait_guard = WaitGuard::new(self);
        let server = timeout(checkout_timeout, rx).await;
        wait_guard.disarm();
        drop(wait_guard);

        let now = Instant::now();
        match server {
            Ok(Ok(server)) => {
                let server = server?;
                Ok((Guard::new(self.pool.clone(), server, now), now))
            }

            Err(_err) => {
                let mut guard = self.pool.lock();
                guard.remove_waiter(&self.request.id);
                self.pool.inner().health.toggle(false);
                Err(Error::CheckoutTimeout)
            }

            // Should not be possible.
            // This means someone else removed my waiter from the wait queue,
            // indicating a bug in the pool.
            Ok(Err(_)) => Err(Error::CheckoutTimeout),
        }
    }
}

struct WaitGuard<'a> {
    waiting: &'a Waiting,
    armed: bool,
}

impl<'a> Deref for WaitGuard<'a> {
    type Target = &'a Waiting;

    fn deref(&self) -> &Self::Target {
        &self.waiting
    }
}

impl<'a> WaitGuard<'a> {
    fn new(waiting: &'a Waiting) -> Self {
        Self {
            waiting,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for WaitGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            let id = self.waiting.request.id;
            self.waiting.pool.lock().remove_waiter(&id);
        }
    }
}

#[derive(Debug)]
pub(super) struct Waiter {
    pub(super) request: Request,
    pub(super) tx: Sender<Result<Box<Server>, Error>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::pool::Pool;
    use crate::net::messages::BackendKeyData;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_cancellation_safety() {
        let pool = Pool::new_test();
        pool.launch();

        let num_tasks = 10;
        let mut wait_tasks = Vec::new();

        for i in 0..num_tasks {
            let pool_clone = pool.clone();
            let request = Request::new(BackendKeyData::new());
            let mut waiting = Waiting::new(pool_clone, &request).unwrap();

            let wait_task = tokio::spawn(async move { waiting.wait().await });

            wait_tasks.push((wait_task, i));
        }

        {
            let pool_guard = pool.lock();
            assert_eq!(
                pool_guard.waiting.len(),
                num_tasks,
                "All waiters should be in queue"
            );
        }

        sleep(Duration::from_millis(5)).await;

        for (wait_task, i) in wait_tasks {
            if i % 2 == 0 {
                sleep(Duration::from_millis(1)).await;
            }
            wait_task.abort();
        }

        sleep(Duration::from_millis(10)).await;

        let pool_guard = pool.lock();
        assert!(
            pool_guard.waiting.is_empty(),
            "All waiters should be removed from queue on cancellation"
        );
    }
}
