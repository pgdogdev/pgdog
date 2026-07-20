//! Authentication plugin used by the integration suite.
//!
//! It exercises the `authenticate` extension point added in this PR. The
//! credential the client sends (as a cleartext password) drives the decision:
//!
//! - `"deny"`               => [`AuthDecision::Deny`] with a reason PgDog logs
//!   but never sends to the client.
//! - `"panic"`              => `panic!()`. The plugin runs on PgDog's blocking
//!   pool, which catches the unwind, so PgDog denies the client and stays up.
//! - `"secret-<user>"`      => [`AuthDecision::Allow`] with no derivation; the
//!   client connects to its pre-configured pool.
//! - `"impersonate:<role>"` => [`AuthDecision::Allow`] deriving `<role>`,
//!   impersonating it via `server_role`, and asking PgDog to provision a pool.
//! - anything else          => [`AuthDecision::Skip`] (all-skip => PgDog denies).

use pgdog_plugin::{AuthContext, AuthDecision, AuthGrant, PdStr, Plugin, plugin};

plugin!(TestAuthPlugin);

struct TestAuthPlugin;

/// Backend credentials PgDog uses when a grant provisions a pool. The
/// integration `setup.sh` creates the `pgdog` superuser with this password, and
/// the suite's `setup.sql` grants the impersonated roles to it. A real plugin
/// would read these from its own config; hardcoding keeps the test hermetic.
const SERVER_USER: &str = "pgdog";
const SERVER_PASSWORD: &str = "pgdog";

impl Plugin for TestAuthPlugin {
    extern "C-unwind" fn version() -> PdStr<'static> {
        env!("CARGO_PKG_VERSION").into()
    }

    fn authenticate(context: AuthContext<'_>) -> AuthDecision {
        let credential = &*context.credential;

        if credential == "deny" {
            return AuthDecision::Deny("test deny".into());
        }

        if credential == "panic" {
            panic!("test plugin panic");
        }

        if let Some(role) = credential.strip_prefix("impersonate:") {
            return AuthDecision::Allow(AuthGrant {
                derived_user: Some(role.to_string()),
                server_role: Some(role.to_string()),
                server_user: Some(SERVER_USER.to_string()),
                server_password: Some(SERVER_PASSWORD.to_string()),
                read_only: None,
                provision: true,
            });
        }

        // `secret-<user>` allows the matching client through to its configured pool.
        if credential == format!("secret-{}", &*context.user) {
            return AuthDecision::Allow(AuthGrant::default());
        }

        AuthDecision::Skip
    }
}
