//! Parse URL and convert to config struct.
use std::{collections::BTreeSet, env::var};
use url::Url;

use super::{Config, ConfigAndUsers, Database, Error, User, Users};

fn database_name(url: &Url) -> String {
    let database = url.path().chars().skip(1).collect::<String>();
    if database.is_empty() {
        "postgres".into()
    } else {
        database
    }
}

impl From<&Url> for Database {
    fn from(value: &Url) -> Self {
        let host = value
            .host()
            .map(|host| host.to_string())
            .unwrap_or("127.0.0.1".into());
        let port = value.port().unwrap_or(5432);

        Database {
            name: database_name(value),
            host,
            port,
            ..Default::default()
        }
    }
}

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
    pub fn from_urls(urls: &[String]) -> Result<Self, Error> {
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

        Ok(Self {
            users: Users { users },
            config: Config {
                databases,
                ..Default::default()
            },
            ..Default::default()
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_url() {
        let url = Url::parse("postgres://user:password@host:5432/name").unwrap();
        println!("{:#?}", url);
    }
}
