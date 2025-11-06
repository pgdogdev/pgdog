//! Parse URL and convert to config struct.
use std::{collections::BTreeSet, env::var, str::FromStr};
use url::Url;

use super::{ConfigAndUsers, Database, Error, User, Users};
use pgdog_config::url::database_name;

impl From<&Url> for User {
    fn from(value: &Url) -> Self {
        let user = value.username();
        let user = if user.is_empty() {
            var("USER").unwrap_or("postgres".into())
        } else {
            user.to_string()
        };
        let password = value.password().unwrap_or("postgres").to_owned();
        User {
            name: user,
            password: Some(password),
            database: database_name(value),
            ..Default::default()
        }
    }
}

impl ConfigAndUsers {
    /// Load from database URLs.
    pub fn databases_from_urls(mut self, urls: &[String]) -> Result<Self, Error> {
        let urls = urls
            .iter()
            .map(|url| Url::parse(url))
            .collect::<Result<Vec<Url>, url::ParseError>>()?;
        let databases = urls
            .iter()
            .map(Database::from)
            .collect::<BTreeSet<_>>() // Make sure we only have unique entries.
            .into_iter()
            .collect::<Vec<_>>();
        let users = urls
            .iter()
            .map(User::from)
            .collect::<BTreeSet<_>>() // Make sure we only have unique entries.
            .into_iter()
            .collect::<Vec<_>>();

        self.users = Users { users, admin: None };
        self.config.databases = databases;

        Ok(self)
    }

    /// Load from mirroring strings.
    pub fn mirroring_from_strings(mut self, mirror_strs: &[String]) -> Result<Self, Error> {
        use super::Mirroring;

        let mirroring = mirror_strs
            .iter()
            .map(|s| Mirroring::from_str(s).map_err(|e| Error::ParseError(e)))
            .collect::<Result<Vec<_>, _>>()?;

        self.config.mirroring = mirroring;

        Ok(self)
    }
}
