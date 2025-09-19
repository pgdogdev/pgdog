use once_cell::sync::Lazy;
use parking_lot::{Mutex, MutexGuard};
use std::collections::HashMap;
use std::env;

static ENV_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

/// Scoped guard that applies environment overrides while holding a global lock.
/// Restores previous values (or absence) when dropped.
pub struct ScopedEnv {
    _lock: MutexGuard<'static, ()>,
    previous: HashMap<&'static str, Option<String>>,
}

impl ScopedEnv {
    /// Applies the provided environment overrides while taking the global lock.
    /// Values set to `None` remove the variable for the duration of the guard.
    pub fn set<I, S>(overrides: I) -> Self
    where
        I: IntoIterator<Item = (&'static str, Option<S>)>,
        S: Into<String>,
    {
        let lock = ENV_LOCK.lock();
        let mut previous = HashMap::new();

        for (key, maybe_value) in overrides {
            let prior = env::var(key).ok();
            previous.insert(key, prior);

            match maybe_value {
                Some(value) => env::set_var(key, value.into()),
                None => env::remove_var(key),
            }
        }

        Self {
            _lock: lock,
            previous,
        }
    }
}

impl Drop for ScopedEnv {
    fn drop(&mut self) {
        for (key, prior) in self.previous.drain() {
            match prior {
                Some(value) => env::set_var(key, value),
                None => env::remove_var(key),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ScopedEnv;
    use std::env;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn restores_original_value() {
        env::set_var("PGDOG_TEST_VAR", "initial");

        {
            let _guard = ScopedEnv::set([("PGDOG_TEST_VAR", Some("override"))]);
            assert_eq!(env::var("PGDOG_TEST_VAR").unwrap(), "override");
        }

        assert_eq!(env::var("PGDOG_TEST_VAR").unwrap(), "initial");
        env::remove_var("PGDOG_TEST_VAR");
    }

    #[test]
    fn restores_absence() {
        env::remove_var("PGDOG_TEST_VAR");
        {
            let _guard = ScopedEnv::set([("PGDOG_TEST_VAR", Some("override"))]);
            assert_eq!(env::var("PGDOG_TEST_VAR").unwrap(), "override");
        }
        assert!(env::var("PGDOG_TEST_VAR").is_err());
    }

    #[test]
    fn serializes_access() {
        static KEY: &str = "PGDOG_TEST_VAR";
        env::set_var(KEY, "start");

        let active = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..4 {
            let active = Arc::clone(&active);
            let max_seen = Arc::clone(&max_seen);
            handles.push(thread::spawn(move || {
                let _guard = ScopedEnv::set([(KEY, Some("busy"))]);
                let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                max_seen.fetch_max(now, Ordering::SeqCst);
                thread::sleep(Duration::from_millis(10));
                active.fetch_sub(1, Ordering::SeqCst);
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(max_seen.load(Ordering::SeqCst), 1);
        assert_eq!(env::var(KEY).unwrap(), "start");
    }
}
