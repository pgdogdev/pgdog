use std::convert::AsRef;
use std::env;
use std::ffi::{OsStr, OsString};

pub(crate) fn set_env_var<T: AsRef<OsStr>>(key: T, value: impl AsRef<OsStr>) -> EnvVarGuard<T> {
    let previous = env::var_os(&key);
    env::set_var(&key, value);
    EnvVarGuard { key, previous }
}

pub(crate) fn remove_env_var<T: AsRef<OsStr>>(key: T) -> EnvVarGuard<T> {
    let previous = env::var_os(&key);
    env::remove_var(&key);
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
            Some(v) => env::set_var(&self.key, v),
            None => env::remove_var(&self.key),
        }
    }
}
