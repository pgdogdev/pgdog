//! SCRAM-SHA-256 authentication.
pub(crate) mod client;
pub(crate) mod error;
pub(crate) mod server;
pub(crate) mod state;

pub(crate) use client::Client;
pub(crate) use error::Error;
pub(crate) use server::Server;

/// Generate a `SCRAM-SHA-256$iterations:salt$StoredKey:ServerKey` hash string
/// from a plaintext password, suitable for storage in `users.toml` or `pg_shadow`.
pub(crate) fn generate_hash(
    password: &str,
    iterations: std::num::NonZeroU32,
    salt: &[u8],
) -> String {
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

/// Verify a plaintext password against a PostgreSQL SCRAM-SHA-256 verifier
/// (`SCRAM-SHA-256$iterations:salt$StoredKey:ServerKey`).
///
/// Used when the client already sent its password in cleartext (the plugin
/// fallback, `auth_type = "plain"`) and a SCRAM exchange cannot be started on
/// the same connection. Malformed verifiers never match. This runs PBKDF2 and
/// is CPU-bound: callers on the async runtime should push it to a blocking
/// thread.
pub(crate) fn verify_password(password: &str, verifier: &str) -> bool {
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
        // md5 verifiers are not SCRAM verifiers and never match.
        assert!(!verify_password(
            "password",
            "md532b5f5d0e0a8c1a1b2c3d4e5f60718293"
        ));
    }
}
