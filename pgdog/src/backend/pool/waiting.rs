use super::Pool;

pub(super) struct Waiting {
    pool: Pool,
}

impl Waiting {
    pub(super) fn new(pool: Pool) -> Self {
        pool.lock().waiting += 1;
        Self { pool }
    }
}

impl Drop for Waiting {
    fn drop(&mut self) {
        self.pool.lock().waiting -= 1;
    }
}
