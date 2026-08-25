# Google access-token authentication

`pgdog-google-auth` lets PostgreSQL clients authenticate to PgDog with Google
OAuth 2.0 access tokens, including tokens produced by
[`gcloud auth print-access-token`](https://docs.cloud.google.com/sdk/gcloud/reference/auth/print-access-token).

The plugin sends the token to Google's HTTPS `tokeninfo` endpoint. It checks the
token expiry and can enforce verified email, account, domain, OAuth audience,
and scope restrictions. The token is never forwarded to PostgreSQL.

> [!WARNING]
> Access tokens are bearer credentials. Require TLS between clients and PgDog.
> The plugin enforces TLS by default.

## Build

The main PgDog container build includes the plugin at
`/usr/lib/libpgdog_google_auth.so`. To build it locally:

```bash
cargo build --release -p pgdog-google-auth
```

## Configure PgDog

Enable plugin authentication and point PgDog at the plugin configuration:

```toml
[general]
auth_type = "plugin"
tls_client_required = true

[[plugins]]
name = "pgdog_google_auth"
config = "google-auth.toml"
```

Start from [`config.example.toml`](config.example.toml). At minimum, restrict
the accepted principals with `allowed_domains` or `allowed_emails`.

To let selected users authenticate with PostgreSQL passwords, enable PgDog's
password or passthrough fallback:

```toml
# pgdog.toml
[general]
auth_type = "plugin"
passthrough_auth = "enabled"
tls_client_required = true
```

With `username_claim = "email"`, `strip_email_domain = false`, and
`require_user_match = true`, the plugin claims only full email startup users
matching `allowed_domains` or `allowed_emails`. A non-email user such as
`postgres` returns `Skip` without sending its password to Google. Invalid Google
tokens for claimed email users still return `Deny`, so they cannot downgrade to
password authentication. For true backend passthrough, do not define the
non-email user in `users.toml`.

When `require_user_match = false`, `strip_email_domain = true`, or
`username_claim = "user_id"`, PgDog cannot route by the startup email namespace.
The plugin therefore claims the credential and preserves fail-closed behavior.

By default, the verified Google email becomes the PostgreSQL user and must
match the startup user. For example:

```bash
account="$(gcloud config get-value account)"
PGPASSWORD="$(gcloud auth print-access-token)" \
  psql "host=pgdog.example.com port=6432 dbname=app user=${account} sslmode=require"
```

For preconfigured pools, add the derived user to `users.toml`:

```toml
[[users]]
name = "alice@example.com"
database = "app"
server_user = "pgdog_service"
server_role = "alice"
# Render server_password here from the approved secret manager.
```

Do not commit backend passwords. Render `users.toml` from the approved secret
manager or use auto-provisioning with `server_password_env`.

## Auto-provision users

The plugin can create a pool after it validates a token:

```toml
allowed_domains = ["example.com"]
provision = true
impersonate = true
server_user = "pgdog_service"
server_password_env = "PGDOG_GOOGLE_AUTH_SERVER_PASSWORD"
```

Inject `PGDOG_GOOGLE_AUTH_SERVER_PASSWORD` into the PgDog process from the
approved secret manager. PgDog uses `server_user` to connect to PostgreSQL and
sets the derived Google identity as `server_role`.

The PostgreSQL role must already exist, and the service account must be allowed
to assume it:

```sql
GRANT "alice@example.com" TO pgdog_service;
```

Set `strip_email_domain = true` if PostgreSQL roles use the email local part.
Set `username_claim = "user_id"` if stable numeric Google account IDs are
preferred.

## Security and operations

- The token is sent only to the configured `tokeninfo_url`. Redirects are
  disabled to prevent credential forwarding.
- `tokeninfo_url` must use HTTPS. Plain HTTP is accepted only for loopback test
  servers.
- Request errors are sanitized so logs do not include the token-bearing URL.
- `timeout_ms` bounds token validation. PgDog's `background_workers` setting
  caps concurrent blocking authentication calls.
- Google may rate-limit token introspection. Connection pooling reduces calls
  because validation happens once per client connection.
- A Google or network outage prevents new logins. Existing database sessions
  remain active.
