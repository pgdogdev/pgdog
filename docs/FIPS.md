# FIPS 140-3

PgDog's cryptography comes from [AWS-LC](https://github.com/aws/aws-lc), through `aws-lc-rs` and Rustls. PgDog does not link OpenSSL, so OpenSSL's FIPS configuration on the host has no effect on it.

## Building

```sh
cargo build -p pgdog --release --features fips
```

The `fips` feature links the AWS-LC FIPS module (`aws-lc-fips-sys`) for both Rustls and PgDog's direct use of AWS-LC. Building it needs CMake, Go and a C compiler.

## Enforcement

The `fips` setting in `[general]` (or the `PGDOG_FIPS` environment variable) controls enforcement:

| Value | Enforced |
|-------|----------|
| `auto` (default) | When PgDog was built with `--features fips`, or the host kernel is in FIPS mode (`/proc/sys/crypto/fips_enabled` is `1`). |
| `required` | Always. |
| `disabled` | Never. A warning is logged if the host is in FIPS mode. |

When enforced, PgDog:

- **Refuses to start or reload** unless it was built with the `fips` feature and AWS-LC reports FIPS mode. On a FIPS host this is a kill switch: a non-FIPS build exits instead of running unvalidated crypto.
- **Checks every TLS configuration** it builds (the client-facing acceptor and every upstream connector, at startup, on reload, and on certificate rotation) with Rustls' `ServerConfig::fips()` / `ClientConfig::fips()`, and refuses ones that aren't FIPS-compliant. A failed reload keeps the previous TLS configuration.
- **Refuses MD5 authentication**: `auth_type = "md5"` fails startup and reload, and an MD5 challenge from a Postgres server fails the connection.
- **Logs a warning** for settings that weaken a FIPS deployment without using non-approved crypto: client TLS not configured or not required, passthrough authentication enabled, or server TLS that isn't `verify_full`.

Regardless of enforcement, security-relevant randomness (query cancellation secrets and SCRAM salts) comes from AWS-LC's system RNG, which is the module's approved DRBG in FIPS builds. Generation failures are errors rather than silent fallbacks. Backend SCRAM state, and its nonce, is only created when the server asks for SASL, so RDS IAM and password logins never generate one.

## Known gaps

- RDS IAM token signing uses `aws-sigv4`, which hashes with RustCrypto SHA-256/HMAC rather than AWS-LC.
- The backend SCRAM client nonce comes from the `scram` crate's `OsRng`; its hashing uses AWS-LC.
