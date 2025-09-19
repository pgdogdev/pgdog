use dashmap::DashMap;
use once_cell::sync::Lazy;
use parking_lot::Mutex as ParkingMutex;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::Mutex;

static GLOBAL_ACCEPTOR_LOCK: Lazy<ParkingMutex<()>> = Lazy::new(|| ParkingMutex::new(()));

/// Map of per-principal mutexes ensuring only one credential acquisition runs at once.
#[derive(Default)]
pub struct PrincipalLocks {
    locks: Arc<DashMap<String, Arc<Mutex<()>>>>,
}

impl PrincipalLocks {
    pub fn new() -> Self {
        Self {
            locks: Arc::new(DashMap::new()),
        }
    }

    fn get_or_insert(&self, principal: &str) -> Arc<Mutex<()>> {
        self.locks
            .entry(principal.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    pub async fn with_principal<F, Fut, T>(&self, principal: &str, f: F) -> T
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        let lock = self.get_or_insert(principal);
        let _guard = lock.lock().await;
        f().await
    }
}

impl Clone for PrincipalLocks {
    fn clone(&self) -> Self {
        Self {
            locks: Arc::clone(&self.locks),
        }
    }
}

/// Serialize acceptor acquisitions globally to avoid fighting over KRB5_KTNAME.
pub fn with_acceptor_lock<T, F>(f: F) -> T
where
    F: FnOnce() -> T,
{
    let _guard = GLOBAL_ACCEPTOR_LOCK.lock();
    f()
}

#[cfg(test)]
mod tests {
    use super::{with_acceptor_lock, PrincipalLocks};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::task;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn principal_lock_serializes() {
        let locks = PrincipalLocks::new();
        let running = Arc::new(AtomicUsize::new(0));
        let max = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..4 {
            let locks = locks.clone();
            let running = Arc::clone(&running);
            let max = Arc::clone(&max);
            handles.push(task::spawn(async move {
                locks
                    .with_principal("user@REALM", || async {
                        let current = running.fetch_add(1, Ordering::SeqCst) + 1;
                        max.fetch_max(current, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        running.fetch_sub(1, Ordering::SeqCst);
                    })
                    .await;
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        assert_eq!(max.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn acceptor_lock_serializes() {
        let running = Arc::new(AtomicUsize::new(0));
        let max = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..4 {
            let running = Arc::clone(&running);
            let max = Arc::clone(&max);
            handles.push(std::thread::spawn(move || {
                with_acceptor_lock(|| {
                    let current = running.fetch_add(1, Ordering::SeqCst) + 1;
                    max.fetch_max(current, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(10));
                    running.fetch_sub(1, Ordering::SeqCst);
                });
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(max.load(Ordering::SeqCst), 1);
    }
}
