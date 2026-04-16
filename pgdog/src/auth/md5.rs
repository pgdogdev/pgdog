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

    /// Used for pgdog->postgres auth which can only be
    /// attempted once per connection, so we expect
    /// this code to accept only one password.
    fn encrypted(&self) -> Result<String, Error> {
        Ok(self.encrypt(self.passwords.first().ok_or(Error::ServerSideOnePassword)?))
    }

    pub fn response(&self) -> Result<Password, Error> {
        Ok(Password::new_password(self.encrypted()?))
    }

    /// Check encrypted password against any of the configured passwords.
    pub fn check(&self, encrypted: &str) -> bool {
        self.passwords.iter().any(|p| self.encrypt(p) == encrypted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pw(s: &str) -> String {
        s.to_string()
    }

    /// Reference MD5 password hash exactly as Postgres computes it:
    ///   "md5" || md5_hex(md5_hex(password || user) || salt)
    fn reference_hash(user: &str, password: &str, salt: &[u8]) -> String {
        let inner = format!("{:x}", md5::compute(format!("{password}{user}").as_bytes()));
        let mut outer = inner.into_bytes();
        outer.extend_from_slice(salt);
        format!("md5{:x}", md5::compute(&outer))
    }

    #[test]
    fn new_uses_random_salt() {
        // Two clients should almost always pick different salts.
        let a = Client::new("alice", &[pw("hunter2")]);
        let b = Client::new("alice", &[pw("hunter2")]);
        assert_ne!(
            a.salt, b.salt,
            "two fresh clients should pick independent salts"
        );
    }

    #[test]
    fn new_salt_rejects_wrong_size() {
        // Postgres MD5 salts are exactly 4 bytes.
        assert!(Client::new_salt("alice", &[pw("hunter2")], b"123").is_err());
        assert!(Client::new_salt("alice", &[pw("hunter2")], b"12345").is_err());
        assert!(Client::new_salt("alice", &[pw("hunter2")], b"1234").is_ok());
    }

    #[test]
    fn challenge_carries_the_clients_salt() {
        let salt = [0xAA, 0xBB, 0xCC, 0xDD];
        let client = Client::new_salt("alice", &[pw("hunter2")], &salt).unwrap();
        match client.challenge() {
            Authentication::Md5(bytes) => assert_eq!(bytes.as_ref(), &salt),
            other => panic!("expected Md5 challenge, got {other:?}"),
        }
    }

    #[test]
    fn encrypted_matches_postgres_reference() {
        let salt = [1u8, 2, 3, 4];
        let client = Client::new_salt("alice", &[pw("hunter2")], &salt).unwrap();
        assert_eq!(
            client.encrypted().unwrap(),
            reference_hash("alice", "hunter2", &salt)
        );
    }

    #[test]
    fn encrypted_uses_first_password_when_multiple() {
        let salt = [1u8, 2, 3, 4];
        let client = Client::new_salt("alice", &[pw("first"), pw("second")], &salt).unwrap();
        assert_eq!(
            client.encrypted().unwrap(),
            reference_hash("alice", "first", &salt)
        );
    }

    #[test]
    fn encrypted_errors_when_no_passwords_configured() {
        // `encrypted()` is the server-side path (pgdog -> postgres) and only
        // gets one shot per connection, so an empty password list must error
        // out instead of silently hashing the empty string.
        let salt = [1u8, 2, 3, 4];
        let client = Client::new_salt("alice", &[], &salt).unwrap();
        assert!(matches!(
            client.encrypted(),
            Err(Error::ServerSideOnePassword)
        ));
        assert!(matches!(
            client.response(),
            Err(Error::ServerSideOnePassword)
        ));
    }

    #[test]
    fn response_wraps_encrypted_password() {
        let salt = [1u8, 2, 3, 4];
        let client = Client::new_salt("alice", &[pw("hunter2")], &salt).unwrap();
        // The wire-format password is null-terminated; strip it before comparing.
        let on_wire = client.response().unwrap().password().unwrap().to_string();
        assert_eq!(on_wire.trim_end_matches('\0'), client.encrypted().unwrap());
    }

    #[test]
    fn check_accepts_correct_password() {
        let salt = [9u8, 8, 7, 6];
        let client = Client::new_salt("alice", &[pw("hunter2")], &salt).unwrap();
        let hash = reference_hash("alice", "hunter2", &salt);
        assert!(client.check(&hash));
    }

    #[test]
    fn check_rejects_wrong_password() {
        let salt = [9u8, 8, 7, 6];
        let client = Client::new_salt("alice", &[pw("hunter2")], &salt).unwrap();
        let hash = reference_hash("alice", "wrong", &salt);
        assert!(!client.check(&hash));
    }

    #[test]
    fn check_is_user_specific() {
        // Same password, different user → different MD5; check must reject.
        let salt = [9u8, 8, 7, 6];
        let client = Client::new_salt("alice", &[pw("hunter2")], &salt).unwrap();
        let bob_hash = reference_hash("bob", "hunter2", &salt);
        assert!(!client.check(&bob_hash));
    }

    #[test]
    fn check_is_salt_specific() {
        // Same password and user, different salt → check must reject.
        let salt = [9u8, 8, 7, 6];
        let other_salt = [1u8, 2, 3, 4];
        let client = Client::new_salt("alice", &[pw("hunter2")], &salt).unwrap();
        let other_hash = reference_hash("alice", "hunter2", &other_salt);
        assert!(!client.check(&other_hash));
    }

    #[test]
    fn check_accepts_any_configured_password() {
        let salt = [9u8, 8, 7, 6];
        let client =
            Client::new_salt("alice", &[pw("alpha"), pw("beta"), pw("gamma")], &salt).unwrap();

        for password in ["alpha", "beta", "gamma"] {
            let hash = reference_hash("alice", password, &salt);
            assert!(
                client.check(&hash),
                "expected {password} to be accepted in multi-password mode"
            );
        }

        let bad = reference_hash("alice", "delta", &salt);
        assert!(!client.check(&bad), "delta is not configured");
    }

    #[test]
    fn check_rejects_when_no_passwords_configured() {
        // An empty password list should not authenticate anything — even
        // the hash of the empty string.
        let salt = [9u8, 8, 7, 6];
        let client = Client::new_salt("alice", &[], &salt).unwrap();
        let empty_hash = reference_hash("alice", "", &salt);
        assert!(!client.check(&empty_hash));
    }
}
