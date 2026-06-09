use std::convert::AsRef;
use std::env;
use std::ffi::{OsStr, OsString};

pub(crate) fn set_env_var<T: AsRef<OsStr>>(key: T, value: impl AsRef<OsStr>) -> EnvVarGuard<T> {
    let previous = env::var_os(&key);
    // SAFETY: Our test suite requires cargo-nextest, which uses a
    // process-per-test model and will not run two tests concurrently in the
    // same process
    unsafe { env::set_var(&key, value) };
    EnvVarGuard { key, previous }
}

pub(crate) fn remove_env_var<T: AsRef<OsStr>>(key: T) -> EnvVarGuard<T> {
    let previous = env::var_os(&key);
    // SAFETY: Our test suite requires cargo-nextest, which uses a
    // process-per-test model and will not run two tests concurrently in the
    // same process
    unsafe { env::remove_var(&key) };
    EnvVarGuard { key, previous }
}

#[must_use = "if unused, env var will immediately revert"]
pub(crate) struct EnvVarGuard<T: AsRef<OsStr>> {
    key: T,
    previous: Option<OsString>,
}

impl<T: AsRef<OsStr>> Drop for EnvVarGuard<T> {
    fn drop(&mut self) {
        match self.previous.take() {
            // SAFETY: Our test suite requires cargo-nextest, which uses a
            // process-per-test model and will not run two tests concurrently in
            // the same process
            Some(v) => unsafe { env::set_var(&self.key, v) },
            // SAFETY: Our test suite requires cargo-nextest, which uses a
            // process-per-test model and will not run two tests concurrently in
            // the same process
            None => unsafe { env::remove_var(&self.key) },
        }
    }
}
