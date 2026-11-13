use super::Error;
use crate::net::{Parameters, Password};

use pgdog_config::users::PassthroughUser;

pub(crate) fn user_from_params(
    params: &Parameters,
    password: &Password,
) -> Result<PassthroughUser, Error> {
    #[allow(clippy::unnecessary_lazy_evaluations)]
    let user = params
        .get("user")
        .ok_or_else(|| Error::IncompleteStartup)?
        .as_str()
        .ok_or_else(|| Error::IncompleteStartup)?;
    let database = params.get_default("database", user);
    #[allow(clippy::unnecessary_lazy_evaluations)]
    let password = password
        .password()
        .ok_or_else(|| Error::IncompleteStartup)?;

    Ok(PassthroughUser {
        name: user.to_owned(),
        database: database.to_owned(),
        password: password.to_owned(),
    })
}
