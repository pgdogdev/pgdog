use super::Pool;

#[derive(Debug)]
pub(super) struct CancelGuard {
    pool: Pool,
    pid: i32,
}

impl CancelGuard {
    pub(super) fn new(pool: &Pool, pid: i32) -> Self {
        Self {
            pool: pool.clone(),
            pid,
        }
    }
}

impl Drop for CancelGuard {
    fn drop(&mut self) {
        self.pool.lock().cancel_done(&self.pid);
    }
}
