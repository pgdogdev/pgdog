use crate::backend::Server;

use super::{Error, Guard, Pool, Request};
use tokio::{sync::oneshot::*, time::Instant};
use tracing::error;

pub(super) struct Waiting {
    pool: Pool,
    rx: Receiver<Result<Box<Server>, Error>>,
    request: Request,
    waiting: bool,
}

impl Drop for Waiting {
    fn drop(&mut self) {
        if !self.waiting {
            return;
        }

        self.pool.lock().remove_waiter(self.request.id);

        // Verify if the server was actually sent in by the pool,
        // but we haven't read it yet. That should prevent issues
        // with lost checkins due to race with tokio::timeout.
        if let Ok(Ok(server)) = self.rx.try_recv()
            && let Err(err) = self.pool.checkin(server)
        {
            error!("pool checkin error: {} [{}]", err, self.pool.addr());
        }
    }
}

impl Waiting {
    /// Create new waiter.
    ///
    /// N.B. You must call and await `Waiting::wait`, otherwise you'll leak waiters.
    ///
    pub(super) fn new(pool: Pool, request: &Request) -> Result<Self, Error> {
        let request = *request;
        let (tx, rx) = channel();

        let full = {
            let mut guard = pool.lock();
            if !guard.online {
                return Err(Error::Offline);
            }
            if request.read {
                guard.stats.counts.reads += 1;
            } else {
                guard.stats.counts.writes += 1;
            }
            // Recheck availability while registering, since cancellation may
            // have completed after get_internal's first checkout attempt.
            guard.add_waiter(Waiter { request, tx }, request.created_at)?;
            guard.full()
        };

        // Tell maintenance we are in line waiting for a connection.
        if !full {
            pool.comms().request.notify_one();
        }

        Ok(Self {
            pool,
            rx,
            request,
            waiting: true,
        })
    }

    /// Wait for connection from the pool.
    pub(super) async fn wait(&mut self) -> Result<(Guard, Instant), Error> {
        // Can be cancelled. Drop will remove the waiter from the queue
        // and check in a server that was sent but not received.
        let server = (&mut self.rx).await;

        // Disarm the guard. We can't be cancelled beyond this point.
        self.waiting = false;

        let now = Instant::now();
        match server {
            Ok(server) => {
                let server = server?;
                Ok((Guard::new(self.pool.clone(), server, now), now))
            }

            Err(_) => {
                // Should not be possible.
                // This means someone else removed my waiter from the wait queue,
                // indicating a bug in the pool.
                Err(Error::CheckoutTimeout)
            }
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
    use crate::net::messages::FrontendPid;
    use tokio::time::{Duration, sleep, timeout};

    #[tokio::test]
    async fn test_cancellation_safety() {
        let pool = Pool::new_test();
        pool.launch();

        let num_tasks = 10;
        let mut wait_tasks = Vec::new();

        for i in 0..num_tasks {
            let pool_clone = pool.clone();
            let request = Request::unrouted(FrontendPid::new());
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

    #[tokio::test]
    async fn test_timeout_removes_waiter() {
        let config = crate::backend::pool::Config {
            max: 1,
            min: 1,
            checkout_timeout: Duration::from_millis(10),
            ..crate::backend::pool::Config::default()
        };

        let pool = Pool::new(&crate::backend::pool::PoolConfig {
            address: crate::backend::pool::Address {
                host: "127.0.0.1".into(),
                port: 5432,
                database_name: "pgdog".into(),
                user: "pgdog".into(),
                passwords: vec!["pgdog".into()],
                ..Default::default()
            },
            config,
        });
        pool.launch();

        sleep(Duration::from_millis(100)).await;

        let _conn = pool.get(&Request::default()).await.unwrap();

        let request = Request::unrouted(FrontendPid::new());
        let waiter_pool = pool.clone();
        let get_conn = async move {
            let mut waiting = Waiting::new(waiter_pool.clone(), &request).unwrap();
            waiting.wait().await
        };
        let result = timeout(Duration::from_millis(100), get_conn).await;

        assert!(result.is_err());

        let pool_guard = pool.lock();
        assert!(
            pool_guard.waiting.is_empty(),
            "Waiter should be removed on timeout"
        );
    }

    #[tokio::test]
    async fn test_dropped_waiter_after_hand_off_checks_in_server() {
        let pool = Pool::new_test();
        pool.lock().online = true;

        let request = Request::unrouted(FrontendPid::new());
        let mut waiting = Waiting::new(pool.clone(), &request).unwrap();

        {
            let mut wait = Box::pin(waiting.wait());
            assert!(futures::poll!(wait.as_mut()).is_pending());

            pool.lock()
                .put(Box::new(crate::backend::Server::default()), Instant::now());
            assert_eq!(pool.lock().checked_out(), 1);
        }
        drop(waiting);

        let guard = pool.lock();
        assert!(guard.waiting.is_empty());
        assert_eq!(
            guard.checked_out(),
            0,
            "server handed to a dropped waiter must not stay checked out"
        );
        assert_eq!(guard.idle(), 1);
    }

    #[tokio::test]
    async fn test_dropped_waiter_after_immediate_hand_off_checks_in_server() {
        let pool = Pool::new_test();
        pool.lock().online = true;
        pool.lock()
            .put(Box::new(crate::backend::Server::default()), Instant::now());
        assert_eq!(pool.lock().idle(), 1);

        let request = Request::unrouted(FrontendPid::new());
        let waiting = Waiting::new(pool.clone(), &request).unwrap();
        assert_eq!(pool.lock().checked_out(), 1);
        drop(waiting);

        let guard = pool.lock();
        assert!(guard.waiting.is_empty());
        assert_eq!(
            guard.checked_out(),
            0,
            "server handed to a waiter that never polled must not stay checked out"
        );
        assert_eq!(guard.idle(), 1);
    }
}
