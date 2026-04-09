//! MD5-based authentication.
//!
//! Added for supporting older PostgreSQL clusters (and clients).
//!
use bytes::Bytes;
use md5::Context;
use rand::Rng;

use super::Error;
use crate::net::messages::{Authentication, Password};

#[derive(Debug, Clone)]
pub struct Client<'a> {
    passwords: Vec<String>,
    user: &'a str,
    salt: [u8; 4],
}

impl<'a> Client<'a> {
    /// Create new MD5 client. When multiple passwords are provided, all of
    /// them are accepted by [`Client::check`]; [`Client::encrypted`] and
    /// [`Client::response`] use the first password.
    pub fn new(user: &'a str, passwords: &[String]) -> Self {
        Self {
            passwords: passwords.to_vec(),
            user,
            salt: rand::rng().random(),
        }
    }

    pub fn new_salt(user: &'a str, passwords: &[String], salt: &[u8]) -> Result<Self, Error> {
        Ok(Self {
            user,
            passwords: passwords.to_vec(),
            salt: salt.try_into()?,
        })
    }

    /// Challenge
    pub fn challenge(&self) -> Authentication {
        Authentication::Md5(Bytes::from(self.salt.to_vec()))
    }

    fn encrypt(&self, password: &str) -> String {
        let mut md5 = Context::new();
        md5.consume(password);
        md5.consume(self.user);
        let first_pass = md5.compute();

        let mut md5 = Context::new();
        md5.consume(format!("{:x}", first_pass));
        md5.consume(self.salt);
        format!("md5{:x}", md5.compute())
    }

    pub fn encrypted(&self) -> String {
        self.encrypt(self.passwords.first().map(String::as_str).unwrap_or(""))
    }

    pub fn response(&self) -> Password {
        Password::new_password(self.encrypted())
    }

    /// Check encrypted password against any of the configured passwords.
    pub fn check(&self, encrypted: &str) -> bool {
        self.passwords.iter().any(|p| self.encrypt(p) == encrypted)
    }
}
