use std::fmt;
use tokio::sync::{Semaphore, SemaphorePermit, SetError, SetOnce, TryAcquireError};

/// A combination of `SetOnce` and `OnceCell`.
/// It allows only a single writer to run, with no contention on future reads,
/// while also allowing callers to wait on the value.
pub(crate) struct SetOnceCell<T> {
    value: SetOnce<T>,
    semaphore: Semaphore,
}

impl<T> SetOnceCell<T> {
    /// Equivalent to [`tokio::sync::OnceCell::get_or_try_init`]
    pub(crate) async fn get_or_try_init<E, F, Fut>(&self, f: F) -> Result<&T, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        if let Some(val) = self.get() {
            Ok(val)
        } else {
            // Ensure only a single writer attempts to write.
            // Err indicates that another task successfully initialized this
            if let Ok(permit) = self.semaphore.acquire().await {
                self.set_value(f().await?, permit);
            }

            Ok(self.get().expect("always initialized"))
        }
    }

    /// Equivalent to [`SetOnce::wait`]
    pub(crate) async fn wait(&self) -> &T {
        self.value.wait().await
    }

    /// Equivalent to [`SetOnce::get`]
    pub(crate) fn get(&self) -> Option<&T> {
        self.value.get()
    }

    /// Equivalent to [`tokio::sync::OnceCell::set`]
    pub(crate) fn set(&self, value: T) -> Result<(), SetError<T>> {
        if self.value.initialized() {
            return Err(SetError::AlreadyInitializedError(value));
        }

        // Ensure no task is currently attempting to initialize this value
        match self.semaphore.try_acquire() {
            Ok(permit) => {
                self.set_value(value, permit);
                Ok(())
            }
            Err(TryAcquireError::NoPermits) => Err(SetError::InitializingError(value)),
            Err(TryAcquireError::Closed) => Err(SetError::AlreadyInitializedError(value)),
        }
    }

    /// Set the value and close the semaphore
    fn set_value(&self, value: T, permit: SemaphorePermit) {
        let Ok(_) = self.value.set(value) else {
            panic!("set_value called when already initialized");
        };
        self.semaphore.close();
        permit.forget();
    }
}

impl<T> Default for SetOnceCell<T> {
    fn default() -> Self {
        Self {
            value: Default::default(),
            semaphore: Semaphore::new(1),
        }
    }
}

impl<T> fmt::Debug for SetOnceCell<T>
where
    SetOnce<T>: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("SetOnceCell").field(&self.value).finish()
    }
}

impl<T> From<T> for SetOnceCell<T> {
    fn from(value: T) -> Self {
        let semaphore = Semaphore::new(0);
        semaphore.close();
        Self {
            value: SetOnce::from(value),
            semaphore,
        }
    }
}
