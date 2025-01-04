//! Connection pool.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use once_cell::sync::OnceCell;
use parking_lot::lock_api::MutexGuard;
use parking_lot::{Mutex, RawMutex};
use tokio::select;
use tokio::sync::Notify;
use tokio::time::sleep;

use crate::backend::Server;
use crate::net::messages::BackendKeyData;

use super::{Config, Error, Guard, Inner, Monitor};

static POOL: OnceCell<Pool> = OnceCell::new();

/// Get a connection pool handle.
pub fn pool() -> Pool {
    POOL.get_or_init(|| Pool::new("127.0.0.1:5432")).clone()
}

/// Mapping between a client and a server.
#[derive(Debug, Copy, Clone, PartialEq)]
pub(super) struct Mapping {
    /// Client ID.
    pub(super) client: BackendKeyData,
    /// Server ID.
    pub(super) server: BackendKeyData,
}

/// Internal pool notifications.
pub(super) struct Comms {
    /// An idle connection is available in the pool.
    pub(super) ready: Notify,
    /// A client requests a new connection to be open
    /// or waiting for one to be returned to the pool.
    pub(super) request: Notify,
    /// Pool is shutting down.
    pub(super) shutdown: Notify,
    /// Pool is resumed from a pause.
    pub(super) resume: Notify,
    /// Number of references (clones) of this pool.
    /// When this number reaches 0, the maintenance loop is stopped
    /// and the pool is dropped.
    pub(super) ref_count: AtomicUsize,
}

/// Pool state.
pub struct State {
    /// Number of connections checked out.
    pub checked_out: usize,
    /// Number of idle connections.
    pub idle: usize,
    /// Total number of connections managed by the pool.
    pub total: usize,
    /// Is the pool online?
    pub online: bool,
    /// Pool has no idle connections.
    pub empty: bool,
    /// Pool configuration.
    pub config: Config,
    /// The pool is paused.
    pub paused: bool,
    /// Number of clients waiting for a connection.
    pub waiting: usize,
}

struct Waiting {
    pool: Pool,
}

impl Waiting {
    fn new(pool: Pool) -> Self {
        pool.inner.lock().waiting += 1;
        Self { pool }
    }
}

impl Drop for Waiting {
    fn drop(&mut self) {
        self.pool.inner.lock().waiting -= 1;
    }
}

#[derive(Debug)]
pub(super) struct Ban {
    /// When the banw as created.
    pub(super) created_at: Instant,
    /// Why it was created.
    pub(super) reason: Error,
}

impl Ban {
    pub(super) fn expired(&self, now: Instant) -> bool {
        now.duration_since(self.created_at) > Duration::from_secs(300)
    }
}

/// Connection pool.
pub struct Pool {
    inner: Arc<Mutex<Inner>>,
    comms: Arc<Comms>,
    addr: String,
}

impl Clone for Pool {
    fn clone(&self) -> Self {
        let clone = Self {
            inner: self.inner.clone(),
            comms: self.comms.clone(),
            addr: self.addr.clone(),
        };

        self.comms.ref_count.fetch_add(1, Ordering::Relaxed);

        clone
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        let remaining = self.comms.ref_count.fetch_sub(1, Ordering::Relaxed);
        if remaining == 1 {
            self.comms.shutdown.notify_one();
        }
    }
}

impl Pool {
    /// Create new connection pool.
    pub fn new(addr: &str) -> Self {
        let pool = Self {
            inner: Arc::new(Mutex::new(Inner {
                conns: VecDeque::new(),
                taken: Vec::new(),
                config: Config::default(),
                waiting: 0,
                ban: None,
                online: true,
                paused: false,
            })),
            comms: Arc::new(Comms {
                ready: Notify::new(),
                request: Notify::new(),
                shutdown: Notify::new(),
                resume: Notify::new(),
                ref_count: AtomicUsize::new(0),
            }),
            addr: addr.to_owned(),
        };

        // Launch the maintenance loop.
        Monitor::new(&pool);

        pool
    }

    /// Get a connetion from the pool.
    pub async fn get(&self, id: &BackendKeyData) -> Result<Guard, Error> {
        loop {
            // Fast path, idle connection available.
            let checkout_timeout = {
                let mut guard = self.lock();
                if let Some(server) = guard.conns.pop_back() {
                    guard.taken.push(Mapping {
                        client: *id,
                        server: *server.id(),
                    });

                    return Ok(Guard::new(self.clone(), server));
                }

                guard.config.checkout_timeout()
            };

            // Slow path, pool is empty, will create new connection
            // or wait for one to be returned if the pool is maxed out.
            self.comms().request.notify_one();
            let _waiting = Waiting::new(self.clone());

            select! {
                // A connection may be available.
                _ =  self.comms().ready.notified() => {
                    continue;
                }

                // Waited too long, return an error.
                _ = sleep(checkout_timeout) => {
                    return Err(Error::CheckoutTimeout);
                }
            }
        }
    }

    /// Create new identical connection pool.
    pub fn duplicate(&self) -> Pool {
        Pool::new(&self.addr)
    }

    /// Check the connection back into the pool.
    pub(super) fn checkin(&self, server: Server) {
        // Ask for the time before locking.
        // This can take some time on some systems, e.g. EC2.
        let now = Instant::now();

        // Check everything and maybe check the connection
        // into the idle pool.
        self.lock().maybe_check_in(server, now);

        // Notify clients that a connection may be available
        // or at least they should request a new one from the pool again.
        self.comms().ready.notify_one();
    }

    /// Server connection used by the client.
    pub fn peer(&self, id: &BackendKeyData) -> Option<BackendKeyData> {
        self.lock()
            .taken
            .iter()
            .find(|p| p.client == *id)
            .map(|p| p.server)
    }

    /// Send a cancellation request if the client is connected to a server.
    pub async fn cancel(&self, id: &BackendKeyData) -> Result<(), super::super::Error> {
        if let Some(server) = self.peer(id) {
            Server::cancel("127.0.0.1:5432", &server).await?;
        }

        Ok(())
    }

    /// Is this pool banned?
    pub fn banned(&self) -> bool {
        self.lock().ban.is_some()
    }

    /// Pool is available to serve connections.
    pub fn available(&self) -> bool {
        let guard = self.lock();
        !guard.paused && guard.online && guard.ban.is_none()
    }

    /// Ban this connection pool from serving traffic.
    pub fn ban(&self) {
        self.lock().ban = Some(Ban {
            created_at: Instant::now(),
            reason: Error::ManualBan,
        });
    }

    /// Unban this pool from serving traffic.
    pub fn unban(&self) {
        self.lock().ban = None;
    }

    /// Pause pool.
    pub fn pause(&self) {
        self.lock().paused = true;
    }

    /// Wait for pool to resume if it's paused.
    pub async fn wait_resume(&self) {
        if self.inner.lock().paused {
            self.comms().resume.notified().await;
        }
    }

    /// Resume the pool.
    pub fn resume(&self) {
        {
            let mut guard = self.lock();
            guard.paused = false;
            guard.ban = None;
        }

        self.comms().resume.notify_waiters();
    }

    /// Pool exclusive lock.
    #[inline]
    pub(super) fn lock<'a>(&'a self) -> MutexGuard<'a, RawMutex, Inner> {
        self.inner.lock()
    }

    /// Internal notifications.
    #[inline]
    pub(super) fn comms(&self) -> &Comms {
        &self.comms
    }

    /// Pool address.
    pub(crate) fn addr(&self) -> &str {
        &self.addr
    }

    /// Pool state.
    pub fn state(&self) -> State {
        let guard = self.lock();

        State {
            checked_out: guard.checked_out(),
            idle: guard.idle(),
            total: guard.total(),
            online: guard.online,
            empty: guard.idle() == 0,
            config: guard.config,
            paused: guard.paused,
            waiting: guard.waiting,
        }
    }
}
