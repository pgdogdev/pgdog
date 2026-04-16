//! SCRAM-SHA-256 authentication.
pub mod client;
pub mod error;
pub mod server;
pub mod state;

pub use client::Client;
pub use error::Error;
pub use server::Server;

/// Generate a `SCRAM-SHA-256$iterations:salt$StoredKey:ServerKey` hash string
/// from a plaintext password, suitable for storage in `users.toml` or `pg_shadow`.
pub fn generate_hash(password: &str, iterations: std::num::NonZeroU32, salt: &[u8]) -> String {
    use base64::prelude::*;
    use ring::digest;
    use ring::hmac::{self, HMAC_SHA256};

    let salted_password = scram::hash_password(password, iterations, salt);
    let key = hmac::Key::new(HMAC_SHA256, &salted_password);
    let client_key = hmac::sign(&key, b"Client Key");
    let server_key = hmac::sign(&key, b"Server Key");
    let stored_key = digest::digest(&digest::SHA256, client_key.as_ref());

    format!(
        "SCRAM-SHA-256${}:{}${}:{}",
        iterations,
        BASE64_STANDARD.encode(salt),
        BASE64_STANDARD.encode(stored_key.as_ref()),
        BASE64_STANDARD.encode(server_key.as_ref()),
    )
}
