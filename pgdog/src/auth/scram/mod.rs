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
    use aws_lc_rs::digest;
    use aws_lc_rs::hmac::{self, HMAC_SHA256};
    use base64::prelude::*;

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

/// Verify a plaintext password against a PostgreSQL SCRAM-SHA-256 verifier.
pub fn verify_password(password: &str, verifier: &str) -> bool {
    use std::num::NonZeroU32;

    use base64::prelude::*;

    let Some(rest) = verifier.strip_prefix("SCRAM-SHA-256$") else {
        return false;
    };
    let Some((iterations_and_salt, _keys)) = rest.split_once('$') else {
        return false;
    };
    let Some((iterations, salt)) = iterations_and_salt.split_once(':') else {
        return false;
    };
    let Ok(iterations) = iterations.parse::<u32>() else {
        return false;
    };
    let Some(iterations) = NonZeroU32::new(iterations) else {
        return false;
    };
    let Ok(salt) = BASE64_STANDARD.decode(salt) else {
        return false;
    };

    let candidate = generate_hash(password, iterations, &salt);
    crate::util::constant_time_eq(candidate.as_bytes(), verifier.as_bytes())
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use super::{generate_hash, verify_password};

    #[test]
    fn verifies_plaintext_against_scram_verifier() {
        let verifier = generate_hash(
            "correct-password",
            NonZeroU32::new(4096).expect("iterations are non-zero"),
            b"pgdog_test_salt!",
        );

        assert!(verify_password("correct-password", &verifier));
        assert!(!verify_password("wrong-password", &verifier));
    }

    #[test]
    fn rejects_invalid_scram_verifier() {
        assert!(!verify_password("password", "not-a-scram-verifier"));
        assert!(!verify_password(
            "password",
            "SCRAM-SHA-256$0:c2FsdA==$stored:server"
        ));
    }
}
