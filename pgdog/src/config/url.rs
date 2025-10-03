//! Parse URL and convert to config struct.
use std::{collections::BTreeSet, env::var, str::FromStr};
use url::Url;

use super::{ConfigAndUsers, Database, Error, PoolerMode, Role, User, Users};

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

        let mut database = Database {
            name: database_name(value),
            host,
            port,
            ..Default::default()
        };

        for (key, val) in value.query_pairs() {
            match key.as_ref() {
                "database_name" => database.database_name = Some(val.to_string()),
                "role" => {
                    if let Ok(role) = Role::from_str(&val) {
                        database.role = role;
                    }
                }
                "shard" => {
                    if let Ok(shard) = val.parse::<usize>() {
                        database.shard = shard;
                    }
                }
                "user" => database.user = Some(val.to_string()),
                "password" => database.password = Some(val.to_string()),
                "pool_size" => {
                    if let Ok(size) = val.parse::<usize>() {
                        database.pool_size = Some(size);
                    }
                }
                "min_pool_size" => {
                    if let Ok(size) = val.parse::<usize>() {
                        database.min_pool_size = Some(size);
                    }
                }
                "pooler_mode" => {
                    if let Ok(mode) = PoolerMode::from_str(&val) {
                        database.pooler_mode = Some(mode);
                    }
                }
                "statement_timeout" => {
                    if let Ok(timeout) = val.parse::<u64>() {
                        database.statement_timeout = Some(timeout);
                    }
                }
                "idle_timeout" => {
                    if let Ok(timeout) = val.parse::<u64>() {
                        database.idle_timeout = Some(timeout);
                    }
                }
                "read_only" => {
                    if let Ok(read_only) = val.parse::<bool>() {
                        database.read_only = Some(read_only);
                    }
                }
                "server_lifetime" => {
                    if let Ok(lifetime) = val.parse::<u64>() {
                        database.server_lifetime = Some(lifetime);
                    }
                }
                _ => {}
            }
        }

        database
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_url() {
        let url = Url::parse("postgres://user:password@host:5432/name").unwrap();
        println!("{:#?}", url);
    }

    #[test]
    fn test_database_name_from_query_param() {
        let url =
            Url::parse("postgres://user:password@host:5432/name?database_name=dbname").unwrap();
        let database = Database::from(&url);

        assert_eq!(database.name, "name");
        assert_eq!(database.database_name, Some("dbname".to_string()));
    }

    #[test]
    fn test_role_from_query_param() {
        let url = Url::parse("postgres://user:password@host:5432/name?role=replica").unwrap();
        let database = Database::from(&url);

        assert_eq!(database.role, super::super::Role::Replica);
    }

    #[test]
    fn test_shard_from_query_param() {
        let url = Url::parse("postgres://user:password@host:5432/name?shard=5").unwrap();
        let database = Database::from(&url);

        assert_eq!(database.shard, 5);
    }

    #[test]
    fn test_numeric_fields_from_query_params() {
        let url = Url::parse("postgres://user:password@host:5432/name?pool_size=10&min_pool_size=2&statement_timeout=5000&idle_timeout=300&server_lifetime=3600").unwrap();
        let database = Database::from(&url);

        assert_eq!(database.pool_size, Some(10));
        assert_eq!(database.min_pool_size, Some(2));
        assert_eq!(database.statement_timeout, Some(5000));
        assert_eq!(database.idle_timeout, Some(300));
        assert_eq!(database.server_lifetime, Some(3600));
    }

    #[test]
    fn test_bool_field_from_query_param() {
        let url = Url::parse("postgres://user:password@host:5432/name?read_only=true").unwrap();
        let database = Database::from(&url);

        assert_eq!(database.read_only, Some(true));
    }

    #[test]
    fn test_pooler_mode_from_query_param() {
        let url =
            Url::parse("postgres://user:password@host:5432/name?pooler_mode=session").unwrap();
        let database = Database::from(&url);

        assert_eq!(
            database.pooler_mode,
            Some(super::super::PoolerMode::Session)
        );
    }

    #[test]
    fn test_string_fields_from_query_params() {
        let url = Url::parse("postgres://user:password@host:5432/name?user=admin&password=secret")
            .unwrap();
        let database = Database::from(&url);

        assert_eq!(database.user, Some("admin".to_string()));
        assert_eq!(database.password, Some("secret".to_string()));
    }

    #[test]
    fn test_multiple_query_params() {
        let url = Url::parse("postgres://user:password@host:5432/name?database_name=realdb&role=replica&shard=3&pool_size=20&read_only=true").unwrap();
        let database = Database::from(&url);

        assert_eq!(database.name, "name");
        assert_eq!(database.database_name, Some("realdb".to_string()));
        assert_eq!(database.role, super::super::Role::Replica);
        assert_eq!(database.shard, 3);
        assert_eq!(database.pool_size, Some(20));
        assert_eq!(database.read_only, Some(true));
    }

    #[test]
    fn test_basic_mirroring_string() {
        let mirror_str = "source_db=primary&destination_db=backup";
        let mirroring = super::super::Mirroring::from_str(mirror_str).unwrap();

        assert_eq!(mirroring.source_db, "primary");
        assert_eq!(mirroring.destination_db, "backup");
        assert_eq!(mirroring.queue_length, None);
        assert_eq!(mirroring.exposure, None);
    }

    #[test]
    fn test_mirroring_with_queue_length() {
        let mirror_str = "source_db=db1&destination_db=db2&queue_length=256";
        let mirroring = super::super::Mirroring::from_str(mirror_str).unwrap();

        assert_eq!(mirroring.source_db, "db1");
        assert_eq!(mirroring.destination_db, "db2");
        assert_eq!(mirroring.queue_length, Some(256));
        assert_eq!(mirroring.exposure, None);
    }

    #[test]
    fn test_mirroring_with_exposure() {
        let mirror_str = "source_db=prod&destination_db=staging&exposure=0.5";
        let mirroring = super::super::Mirroring::from_str(mirror_str).unwrap();

        assert_eq!(mirroring.source_db, "prod");
        assert_eq!(mirroring.destination_db, "staging");
        assert_eq!(mirroring.queue_length, None);
        assert_eq!(mirroring.exposure, Some(0.5));
    }

    #[test]
    fn test_mirroring_with_both_overrides() {
        let mirror_str = "source_db=main&destination_db=backup&queue_length=512&exposure=0.75";
        let mirroring = super::super::Mirroring::from_str(mirror_str).unwrap();

        assert_eq!(mirroring.source_db, "main");
        assert_eq!(mirroring.destination_db, "backup");
        assert_eq!(mirroring.queue_length, Some(512));
        assert_eq!(mirroring.exposure, Some(0.75));
    }

    #[test]
    fn test_config_mirroring_from_strings() {
        let config = ConfigAndUsers::default();
        let mirror_strs = vec![
            "source_db=db1&destination_db=db1_mirror".to_string(),
            "source_db=db2&destination_db=db2_mirror&queue_length=256&exposure=0.5".to_string(),
        ];

        let config = config.mirroring_from_strings(&mirror_strs).unwrap();

        assert_eq!(config.config.mirroring.len(), 2);
        assert_eq!(config.config.mirroring[0].source_db, "db1");
        assert_eq!(config.config.mirroring[0].destination_db, "db1_mirror");
        assert_eq!(config.config.mirroring[0].queue_length, None);
        assert_eq!(config.config.mirroring[0].exposure, None);

        assert_eq!(config.config.mirroring[1].source_db, "db2");
        assert_eq!(config.config.mirroring[1].destination_db, "db2_mirror");
        assert_eq!(config.config.mirroring[1].queue_length, Some(256));
        assert_eq!(config.config.mirroring[1].exposure, Some(0.5));
    }
}
