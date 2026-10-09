pub(crate) mod check_db;

use tracing::debug;

use crate::{
    backend::{Error, databases::reload_from_existing},
    config::config,
};

use check_db::check_db;
use pgdog_config::{General, User};

use crate::auth::AuthResult;

/// Check that a user can be added to the in-memory config
/// by validating its password against the database.
pub(crate) async fn check_or_add(user: User) -> Result<AuthResult, Error> {
    let config = config();
    let existing = config.users.find(&user);

    if let Some(existing) = existing {
        handle_existing_with_check(user, existing, &config.config.general).await
    } else {
        if check_db(&user).await {
            add_new_user(user)?;
            Ok(AuthResult::Ok)
        } else {
            Ok(AuthResult::NoPassthroughDatabaseCheck)
        }
    }
}

/// Add a user back to the in-memory config after a config reload.
/// We assume the user keeps a valid password so we don't check it against the database again.
pub(crate) fn restore_after_reload(user: User) -> Result<AuthResult, Error> {
    let config = config();
    let existing = config.users.find(&user);

    if let Some(existing) = existing {
        handle_existing(user, existing, &config.config.general)
    } else {
        add_new_user(user)?;
        Ok(AuthResult::Ok)
    }
}

async fn handle_existing_with_check(
    new: User,
    existing: User,
    config: &General,
) -> Result<AuthResult, Error> {
    if existing.password.is_none()
        || (config.passthrough_auth.allows_change() && !passwords_match(&new, &existing))
    {
        if check_db(&new).await {
            update_user(new, existing)?;
            Ok(AuthResult::Ok)
        } else {
            Ok(AuthResult::NoPassthroughDatabaseCheck)
        }
    } else {
        handle_existing(new, existing, config)
    }
}

fn handle_existing(new: User, existing: User, config: &General) -> Result<AuthResult, Error> {
    if existing.password.is_none() {
        update_user(new, existing)?;
        Ok(AuthResult::Ok)
    } else {
        if passwords_match(&new, &existing) {
            Ok(AuthResult::Ok)
        } else if config.passthrough_auth.allows_change() {
            update_user(new, existing)?;
            Ok(AuthResult::Ok)
        } else {
            Ok(AuthResult::NoPassthroughPasswordChange)
        }
    }
}

fn passwords_match(new: &User, existing: &User) -> bool {
    existing
        .password
        .as_deref()
        .zip(new.password.as_deref())
        .is_some_and(|(stored, provided)| {
            crate::util::constant_time_eq(stored.as_bytes(), provided.as_bytes())
        })
}

fn update_user(new: User, mut existing: User) -> Result<(), Error> {
    existing.password = new.password;
    add(existing)?;
    reload_from_existing()?;
    Ok(())
}

fn add_new_user(user: User) -> Result<(), Error> {
    add(user)?;
    reload_from_existing()?;
    Ok(())
}

fn add(user: User) -> Result<(), Error> {
    use crate::config::set;
    use crate::databases::lock;

    debug!(
        r#"adding user "{}" to database "{}" via passthrough auth"#,
        user.name, user.database
    );

    let _lock = lock();
    let mut config = (*config()).clone();
    config.users.add_or_replace(user);
    set(config)?;

    Ok(())
}

#[cfg(test)]
mod test;
