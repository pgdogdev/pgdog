use pgdog_config::users::PassthroughUser;
use pgdog_stats::User;

/// Convert to a database/user pair.
pub(crate) trait ToUser {
    /// Perform the conversion.
    fn to_user(&self) -> User;
}

impl ToUser for (&str, &str) {
    fn to_user(&self) -> User {
        User {
            user: self.0.to_string(),
            database: self.1.to_string(),
        }
    }
}

impl ToUser for (&str, Option<&str>) {
    fn to_user(&self) -> User {
        User {
            user: self.0.to_string(),
            database: self.1.map_or(self.0.to_string(), |d| d.to_string()),
        }
    }
}

impl ToUser for &pgdog_config::User {
    fn to_user(&self) -> User {
        User {
            user: self.name.clone(),
            database: self.database.clone(),
        }
    }
}

impl ToUser for &PassthroughUser {
    fn to_user(&self) -> User {
        User {
            user: self.name.clone(),
            database: self.database.clone(),
        }
    }
}
