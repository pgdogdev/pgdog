pub(crate) mod check_db;
pub(crate) mod per_user_lock;

use tracing::debug;

use crate::{
    backend::{
        Error,
        databases::{databases, lock, reload_from_config},
    },
    config::config,
};

use check_db::check_db;
use pgdog_config::{
    User,
    users::{PassthroughUser, PasswordKind},
};

use crate::auth::AuthResult;

enum CheckExistingResult {
    Ok,
    Add,
    WrongPassword,
    NoDatabase,
}

/// Check that a user can be added to the in-memory config
/// by validating its password against the database.
pub(crate) async fn check_or_add(user: PassthroughUser) -> Result<AuthResult, Error> {
    let config = config();
    let allow_password_change = config.config.general.passthrough_auth.allows_change();
    match check_existing(&user)? {
        CheckExistingResult::Ok => Ok(AuthResult::Ok),
        CheckExistingResult::WrongPassword if !allow_password_change => {
            Ok(AuthResult::NoPassthroughPasswordChange)
        }
        CheckExistingResult::NoDatabase
        | CheckExistingResult::WrongPassword
        | CheckExistingResult::Add => {
            let user = config
                .users
                .find(&user)
                .map(|mut existing| {
                    existing.password = Some(user.password.clone());
                    existing
                })
                .unwrap_or(user.into());
            if check_db(&user, || {
                let _guard = lock();
                add(&user)
            })
            .await
            {
                Ok(AuthResult::Ok)
            } else {
                Ok(AuthResult::NoPasswordMatch)
            }
        }
    }
}

/// Add a user back to the in-memory config after a config reload.
/// We assume the user keeps a valid password so we don't check it against the database again.
pub(crate) fn restore_after_reload(user: &PassthroughUser) -> Result<(), Error> {
    let _lock = lock();

    if databases().passthrough_cluster_ready(user) {
        return Ok(());
    }

    let user = if let Some(mut existing) = config().users.find(user) {
        if existing.password.is_none() {
            existing.password = Some(user.password.clone());
        }
        existing
    } else {
        user.clone().into()
    };

    add(&user)?;

    Ok(())
}

/// Check loaded user. This is the fastest path to
/// check if a passthrough user is already configured.
fn check_existing(user: &PassthroughUser) -> Result<CheckExistingResult, Error> {
    match databases()
        .cluster(user)
        .map(|cluster| match cluster.passwords() {
            &[] => CheckExistingResult::Add,
            passwords => {
                if passwords.iter().any(|password| {
                    let passthrough = user.password.as_bytes();
                    if let PasswordKind::Plain(password) = password {
                        crate::util::constant_time_eq(passthrough, password.as_bytes())
                    } else {
                        false
                    }
                }) {
                    CheckExistingResult::Ok
                } else {
                    CheckExistingResult::WrongPassword
                }
            }
        }) {
        Ok(result) => Ok(result),
        Err(Error::NoDatabase(_)) => Ok(CheckExistingResult::NoDatabase),
        Err(err) => Err(err),
    }
}

fn add(user: &User) -> Result<(), Error> {
    use crate::config::set;

    debug!(
        r#"adding user "{}" to database "{}" via passthrough auth"#,
        user.name, user.database
    );

    let mut config = (*config()).clone();
    config.users.add_or_replace(user);
    let config = set(config)?;
    reload_from_config(&config)?;

    Ok(())
}

#[cfg(test)]
mod test;
