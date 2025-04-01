use super::{Pool, Request};

pub(super) struct Waiting {
    pool: Pool,
}

impl Waiting {
    pub(super) fn new(pool: Pool, request: &Request) -> Self {
        {
            let mut inner = pool.lock();
            inner.waiting.push_back(request.clone());
        }
        Self { pool }
    }
}

impl Drop for Waiting {
    fn drop(&mut self) {
        self.pool.lock().waiting.pop_front();
    }
}
