use pgdog_config::{Config, User};

use crate::config::config;

/// Check that the user already exists and the passwords match.
pub fn check(user: &User) -> bool {
    let config = config();
    config.users.find(user).is_some_and(|existing| {
        existing
            .password
            .as_deref()
            .zip(user.password.as_deref())
            .is_some_and(|(stored, provided)| {
                crate::util::constant_time_eq(stored.as_bytes(), provided.as_bytes())
            })
    })
}

pub(super) fn can_change(config: &Config) -> bool {
    config.general.passthrough_auth.allows_change()
}
