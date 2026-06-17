use super::Error;
use crate::net::{Parameters, Password};

use super::{General, User};

pub fn user_from_params(params: &Parameters, password: &Password) -> Result<User, Error> {
    let user = params
        .get("user")
        .ok_or(Error::IncompleteStartup)?
        .as_str()
        .ok_or(Error::IncompleteStartup)?;
    let database = params.get_default("database", user);
    let password = password.password().ok_or(Error::IncompleteStartup)?;

    Ok(User {
        name: user.to_owned(),
        database: database.to_owned(),
        password: Some(password.to_owned()),
        ..Default::default()
    })
}

/// Build a `User` config record for a JWT-authenticated user.
///
/// The `connection_user` is the PgDog username used in the connection string, `database` is the
/// target database, `jwt_sub` is the username extracted from the JWT token, and `general` provides
/// the JWT-specific server credentials and auto-provision read-only flag.
pub fn user_from_jwt(
    connection_user: &str,
    database: &str,
    jwt_sub: &str,
    general: &General,
) -> User {
    User {
        name: jwt_sub.to_owned(),
        database: database.to_owned(),
        server_user: general.jwt_server_user.clone().or_else(|| {
            // Fall back to the connection_user when no dedicated server user is configured.
            Some(connection_user.to_owned())
        }),
        server_password: general.jwt_server_password.clone(),
        read_only: if general.jwt_user_auto_provision_read_only {
            Some(true)
        } else {
            None
        },
        ..Default::default()
    }
}

// impl User {
//     pub(crate) fn from_params(params: &Parameters, password: &Password) -> Result<Self, Error> {
//         let user = params
//             .get("user")
//             .ok_or(Error::IncompleteStartup)?
//             .as_str()
//             .ok_or(Error::IncompleteStartup)?;
//         let database = params.get_default("database", user);
//         let password = password.password().ok_or(Error::IncompleteStartup)?;

//         Ok(Self {
//             name: user.to_owned(),
//             database: database.to_owned(),
//             password: Some(password.to_owned()),
//             ..Default::default()
//         })
//     }

//     /// New user from user, password and database.
//     pub(crate) fn new(user: &str, password: &str, database: &str) -> Self {
//         Self {
//             name: user.to_owned(),
//             database: database.to_owned(),
//             password: Some(password.to_owned()),
//             ..Default::default()
//         }
//     }
// }
