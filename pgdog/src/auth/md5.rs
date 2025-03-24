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
    password: &'a str,
    user: &'a str,
    salt: [u8; 4],
}

impl<'a> Client<'a> {
    /// Create new MD5 client.
    pub fn new(user: &'a str, password: &'a str) -> Self {
        Self {
            password,
            user,
            salt: rand::thread_rng().gen(),
        }
    }

    pub fn new_salt(user: &'a str, password: &'a str, salt: &[u8]) -> Result<Self, Error> {
        Ok(Self {
            user,
            password,
            salt: salt.try_into()?,
        })
    }

    /// Challenge
    pub fn challenge(&self) -> Authentication {
        Authentication::Md5(Bytes::from(self.salt.to_vec()))
    }

    pub fn encrypted(&self) -> String {
        let mut md5 = Context::new();
        md5.consume(self.password);
        md5.consume(self.user);
        let first_pass = md5.compute();

        let mut md5 = Context::new();
        md5.consume(format!("{:x}", first_pass));
        md5.consume(self.salt);
        let password = format!("md5{:x}", md5.compute());

        password
    }

    pub fn response(&self) -> Password {
        Password::new_password(self.encrypted())
    }

    /// Check encrypted password against what we have.
    pub fn check(&self, encrypted: &str) -> bool {
        self.encrypted() == encrypted
    }
}
