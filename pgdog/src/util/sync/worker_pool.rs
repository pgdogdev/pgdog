use std::num::NonZeroUsize;
use std::ops::Deref;

use parking_lot::Mutex;
use tokio::sync::{AcquireError, Semaphore, SemaphorePermit};

/// Creates a pool of workers that will allow to distribute
/// workload between workers in parallel based on semaphore
/// permit count for the single worker.
///
/// E.g. specify n parallel handles from a single replica,
/// while having a Vec of m replicas achieving
/// n x m parallelization where single replica processes
/// at most n requests.
///
/// # Implementation notes
///
/// - The distribution process will pick the worker with
///   fewer work in progress. If any worker doesn't
///   have task it'll be picked immediately.
///
/// # Warnings
///
/// Don't use for a high number of tasks or workers.
pub(crate) struct WorkerPool<T> {
    pool: Vec<T>,
    permits: Mutex<Vec<usize>>,
    max_permits: NonZeroUsize,
    semaphore: Semaphore,
}

/// Guard to get the current worker and
/// track it's permit. On drop the worker
/// permit is returned to pool
pub(crate) struct Guard<'a, T> {
    pool: &'a WorkerPool<T>,
    index: usize,
    _permit: SemaphorePermit<'a>,
}

impl<T> Deref for Guard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.pool.pool[self.index]
    }
}

impl<T> Drop for Guard<'_, T> {
    fn drop(&mut self) {
        self.pool.permits.lock()[self.index] += 1;
    }
}

#[derive(Debug, Display, Error, From)]
pub(crate) enum Error {
    Semaphore(AcquireError),
}

impl<T> WorkerPool<T> {
    pub(crate) fn new(pool: Vec<T>, max_permits: NonZeroUsize) -> Self {
        assert!(!pool.is_empty(), "worker pool must not be empty");

        let total_permits = pool
            .len()
            .checked_mul(max_permits.get())
            .filter(|&total| total <= Semaphore::MAX_PERMITS)
            .expect("total worker capacity exceeds the semaphore limit");
        let permits = vec![max_permits.get(); pool.len()];

        Self {
            pool,
            permits: Mutex::new(permits),
            max_permits,
            semaphore: Semaphore::new(total_permits),
        }
    }

    fn select_worker(&self) -> usize {
        let mut permits = self.permits.lock();
        let mut max_index = None;
        let mut max_permits = 0;

        for (index, &available) in permits.iter().enumerate() {
            if available > max_permits {
                max_index = Some(index);
                max_permits = available;
                if available == self.max_permits.get() {
                    break;
                }
            }
        }

        let index = max_index.expect("a global permit must have available worker capacity");
        permits[index] -= 1;
        index
    }

    /// Try to acquire a single worker from pool.
    /// Return None if there is no permits available at the moment
    #[allow(unused)]
    pub(crate) fn try_acquire(&self) -> Option<Guard<'_, T>> {
        let permit = self.semaphore.try_acquire().ok()?;
        let index = self.select_worker();

        Some(Guard {
            pool: self,
            index,
            _permit: permit,
        })
    }

    /// Acquire a single worker. If there is no permits
    /// it'll wait for the first available worker.
    pub(crate) async fn acquire(&self) -> Result<Guard<'_, T>, Error> {
        let permit = self.semaphore.acquire().await?;
        let index = self.select_worker();

        Ok(Guard {
            pool: self,
            index,
            _permit: permit,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use futures::{FutureExt, poll};
    use tokio::{sync::Barrier, task::JoinSet, time::timeout};

    use super::*;

    fn acquire_ready<T>(future: impl Future<Output = Result<T, Error>>) -> T {
        future
            .now_or_never()
            .expect("a worker should be available")
            .expect("acquisition should succeed")
    }

    macro_rules! assert_pending {
        ($future:expr $(,)?) => {
            assert!(poll!($future).is_pending())
        };
    }

    #[tokio::test]
    async fn test_prefers_least_busy_worker() {
        let pool = WorkerPool::new(vec![0, 1], NonZeroUsize::new(3).expect("nonzero limit"));

        let first = acquire_ready(pool.acquire());
        let second = acquire_ready(pool.acquire());
        let third = acquire_ready(pool.acquire());
        let fourth = acquire_ready(pool.acquire());

        assert_ne!(*first, *second);
        assert_ne!(*third, *fourth);
    }

    #[tokio::test]
    async fn test_waits_at_capacity_and_reuses_released_worker() {
        let pool = WorkerPool::new(vec![0, 1], NonZeroUsize::new(2).expect("nonzero limit"));
        let mut guards: Vec<_> = (0..4).map(|_| acquire_ready(pool.acquire())).collect();

        for worker in 0..2 {
            assert_eq!(guards.iter().filter(|guard| ***guard == worker).count(), 2);
        }

        let mut waiting = Box::pin(pool.acquire());
        assert_pending!(waiting.as_mut());

        let released = guards.pop().expect("all workers are occupied");
        let worker = *released;
        drop(released);

        let acquired = acquire_ready(waiting);
        assert_eq!(*acquired, worker);

        let mut waiting = Box::pin(pool.acquire());
        assert_pending!(waiting.as_mut());
    }

    #[tokio::test]
    async fn test_cancelled_waiter_does_not_block_next_waiter() {
        let pool = WorkerPool::new(vec![0], NonZeroUsize::new(1).expect("nonzero limit"));
        let held = acquire_ready(pool.acquire());
        let mut cancelled = Box::pin(pool.acquire());
        let mut waiting = Box::pin(pool.acquire());

        assert_pending!(cancelled.as_mut());
        assert_pending!(waiting.as_mut());
        drop(held);
        drop(cancelled);

        let acquired = acquire_ready(waiting);
        assert_eq!(*acquired, 0);

        drop(acquired);
        let acquired = acquire_ready(pool.acquire());
        assert_eq!(*acquired, 0);
    }

    #[tokio::test]
    async fn test_waiting_acquisition_leaves_other_capacity_available() {
        let pool = WorkerPool::new(vec![0, 1], NonZeroUsize::new(1).expect("nonzero limit"));
        let first = acquire_ready(pool.acquire());
        let second = acquire_ready(pool.acquire());
        let mut waiting = Box::pin(pool.acquire());

        assert_pending!(waiting.as_mut());
        drop(first);
        drop(second);

        let selected = acquire_ready(waiting);
        let remaining = acquire_ready(pool.acquire());

        assert_ne!(*selected, *remaining);
        let mut waiting = Box::pin(pool.acquire());
        assert_pending!(waiting.as_mut());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_shares_pool_across_tasks() {
        timeout(Duration::from_secs(5), async {
            let pool = Arc::new(WorkerPool::new(
                vec![0, 1],
                NonZeroUsize::new(2).expect("nonzero limit"),
            ));
            let barrier = Arc::new(Barrier::new(4));
            let mut tasks = JoinSet::new();

            for _ in 0..8 {
                let pool = Arc::clone(&pool);
                let barrier = Arc::clone(&barrier);
                tasks.spawn(async move {
                    let guard = pool.acquire().await.expect("acquisition should succeed");
                    barrier.wait().await;
                    *guard
                });
            }

            let mut counts = [0; 2];
            while let Some(result) = tasks.join_next().await {
                counts[result.expect("worker task should complete")] += 1;
            }
            assert_eq!(counts, [4, 4]);
        })
        .await
        .expect("shared pool tasks should complete without deadlock");
    }

    #[tokio::test]
    async fn test_guard_acquired_inside_static_task_allows_moving_state() {
        async fn hold(state: String, worker: usize, barrier: Arc<Barrier>) -> usize {
            barrier.wait().await;
            barrier.wait().await;
            state.len() + worker
        }

        let pool = Arc::new(WorkerPool::new(
            vec![0],
            NonZeroUsize::new(1).expect("nonzero limit"),
        ));
        let barrier = Arc::new(Barrier::new(2));
        let state = String::from("table");

        let handle = tokio::spawn({
            let pool = Arc::clone(&pool);
            let barrier = Arc::clone(&barrier);
            async move {
                let guard = pool.acquire().await.expect("acquisition should succeed");
                hold(state, *guard, barrier).await
            }
        });

        barrier.wait().await;
        assert!(pool.try_acquire().is_none());
        barrier.wait().await;

        assert_eq!(handle.await.expect("task should complete"), 5);

        let acquired = acquire_ready(pool.acquire());
        assert_eq!(*acquired, 0);
    }

    #[tokio::test]
    async fn test_released_permits_are_reserved_for_waiters() {
        let pool = WorkerPool::new(vec![0, 1], NonZeroUsize::new(1).expect("nonzero limit"));
        let first = acquire_ready(pool.acquire());
        let second = acquire_ready(pool.acquire());
        let mut first_waiter = Box::pin(pool.acquire());
        let mut second_waiter = Box::pin(pool.acquire());

        assert_pending!(first_waiter.as_mut());
        assert_pending!(second_waiter.as_mut());
        drop(first);
        drop(second);

        assert!(pool.try_acquire().is_none());

        let first = acquire_ready(first_waiter);
        let second = acquire_ready(second_waiter);
        assert_ne!(*first, *second);
        assert!(pool.try_acquire().is_none());

        drop(first);
        drop(second);

        let first = pool
            .try_acquire()
            .expect("first worker should be available");
        let second = pool
            .try_acquire()
            .expect("second worker should be available");
        assert_ne!(*first, *second);
        assert!(pool.try_acquire().is_none());
    }

    #[test]
    #[should_panic]
    fn test_rejects_empty_pool() {
        WorkerPool::<usize>::new(Vec::new(), NonZeroUsize::new(1).expect("nonzero limit"));
    }

    #[test]
    #[should_panic]
    fn test_rejects_total_capacity_above_semaphore_limit() {
        WorkerPool::new(
            vec![0, 1],
            NonZeroUsize::new(Semaphore::MAX_PERMITS).expect("nonzero limit"),
        );
    }

    #[test]
    #[should_panic]
    fn test_rejects_total_capacity_overflow() {
        WorkerPool::new(
            vec![0, 1],
            NonZeroUsize::new(usize::MAX).expect("nonzero limit"),
        );
    }
}
