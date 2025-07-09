use super::Error;
use crate::net::{Parameters, Password};

use super::User;

impl User {
    pub(crate) fn from_params(params: &Parameters, password: &Password) -> Result<Self, Error> {
        let user = params
            .get("user")
            .ok_or(Error::IncompleteStartup)?
            .as_str()
            .ok_or(Error::IncompleteStartup)?;
        let database = params.get_default("database", user);
        let password = password.password().ok_or(Error::IncompleteStartup)?;

        Ok(Self {
            name: user.to_owned(),
            database: database.to_owned(),
            password: Some(password.to_owned()),
            ..Default::default()
        })
    }

    /// New user from user, password and database.
    pub(crate) fn new(user: &str, password: &str, database: &str) -> Self {
        Self {
            name: user.to_owned(),
            database: database.to_owned(),
            password: Some(password.to_owned()),
            ..Default::default()
        }
    }
}
