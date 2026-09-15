//! Authentication plugin driver.
//!
//! When `auth_type = "plugin"`, PgDog drives the client wire exchange (asking
//! for a cleartext password) and hands the credential to the loaded plugins.
//! Each plugin answers with an [`AuthDecision`](pgdog_plugin::AuthDecision); the
//! first plugin that does not [`Skip`](pgdog_plugin::AuthDecision::Skip) wins.
//! If every plugin skips, PgDog denies the client: `auth_type = "plugin"` is
//! explicit and there is no fallback to password verification (maintainer
//! decision).
//!
//! Plugins run inside a single [`tokio::task::spawn_blocking`] call (they may
//! block on I/O). Concurrency is bounded by the runtime's blocking pool, whose
//! size is the `background_workers` setting (`max_blocking_threads`); with the
//! default of `0` that pool has a single thread, so plugin calls run one at a
//! time.

use pgdog_plugin::{AuthContext, AuthDecisionTag, AuthField, AuthGrant, PdStr};
use tokio::task::spawn_blocking;
use tracing::{debug, warn};

use crate::auth::AuthResult;
use crate::config::config_quick;
use crate::plugin::plugins;

/// Outcome of running the authentication plugins for a single client.
#[derive(Debug)]
pub(crate) struct PluginAuthOutcome {
    /// Overall result: [`AuthResult::Ok`] on Allow, otherwise a plugin denial.
    pub(crate) result: AuthResult,
    /// Grant returned by the accepting plugin (only set on Allow).
    pub(crate) grant: Option<AuthGrant>,
}

impl PluginAuthOutcome {
    fn allow(grant: AuthGrant) -> Self {
        Self {
            result: AuthResult::Ok,
            grant: Some(grant),
        }
    }

    fn denied() -> Self {
        Self {
            result: AuthResult::PluginDenied,
            grant: None,
        }
    }

    fn invalid_grant() -> Self {
        Self {
            result: AuthResult::PluginInvalidGrant,
            grant: None,
        }
    }

    fn no_decision() -> Self {
        Self {
            result: AuthResult::PluginNoDecision,
            grant: None,
        }
    }
}

/// Authenticate a client through the loaded authentication plugins.
///
/// The owned strings are moved into a single `spawn_blocking` call that builds
/// the [`AuthContext`] (whose [`PdStr`] fields borrow them) and consults the
/// plugins. A task join failure is treated as a denial.
pub(crate) async fn authenticate(
    user: String,
    database: String,
    credential: String,
    client_addr: String,
    tls_identity: Option<String>,
    tls: bool,
) -> PluginAuthOutcome {
    let join = spawn_blocking(move || {
        run(
            &user,
            &database,
            &credential,
            &client_addr,
            tls_identity,
            tls,
        )
    })
    .await;

    match join {
        Ok(outcome) => outcome,
        Err(err) => {
            warn!("authentication plugin task failed: {}", err);
            PluginAuthOutcome::no_decision()
        }
    }
}

/// Fields a plugin streamed back through the authenticate callback.
#[derive(Default)]
struct Collected {
    derived_user: Option<String>,
    server_role: Option<String>,
    server_user: Option<String>,
    server_password: Option<String>,
    error: Option<String>,
}

/// Consult the plugins. First non-Skip decision wins.
///
/// Plugins that do not implement `authenticate` return Skip via the trait
/// default, so no capability check is needed. Iteration order follows
/// `[[plugins]]` configuration order, matching how routing consults plugins.
fn run(
    user: &str,
    database: &str,
    credential: &str,
    client_addr: &str,
    tls_identity: Option<String>,
    tls: bool,
) -> PluginAuthOutcome {
    let Some(plugins) = plugins() else {
        return PluginAuthOutcome::no_decision();
    };

    // Empty identity == absent, matching the PdStr borrow convention.
    let tls_identity = tls_identity.unwrap_or_default();

    // AuthContext is Copy and only borrows these strings, which outlive the loop.
    let context = AuthContext {
        user: PdStr::from(user),
        database: PdStr::from(database),
        credential: PdStr::from(credential),
        client_addr: PdStr::from(client_addr),
        tls_identity: PdStr::from(tls_identity.as_str()),
        tls,
    };

    for (name, plugin) in plugins {
        let mut collected = Collected::default();
        let outcome = plugin.authenticate(context, |field, value| {
            let slot = match field {
                AuthField::DerivedUser => &mut collected.derived_user,
                AuthField::ServerRole => &mut collected.server_role,
                AuthField::ServerUser => &mut collected.server_user,
                AuthField::ServerPassword => &mut collected.server_password,
                AuthField::Error => &mut collected.error,
            };
            *slot = Some(value.to_owned());
        });

        match outcome.tag {
            AuthDecisionTag::Skip => continue,
            AuthDecisionTag::Allow => {
                let grant = AuthGrant {
                    derived_user: collected.derived_user,
                    server_role: collected.server_role,
                    server_user: collected.server_user,
                    server_password: collected.server_password,
                    read_only: outcome.read_only_flag(),
                    provision: outcome.provision,
                };

                if let Some(problem) = invalid_grant(&grant) {
                    warn!(
                        r#"client "{}" denied: plugin "{}" returned an unusable grant: {}"#,
                        user, name, problem
                    );
                    return PluginAuthOutcome::invalid_grant();
                }

                debug!(r#"client "{}" authenticated by plugin "{}""#, user, name);
                return PluginAuthOutcome::allow(grant);
            }
            AuthDecisionTag::Deny => {
                // The reason is logged but never sent to the client. `warn!`
                // follows `log_connections` like the other auth failures, so a
                // client retrying in a loop cannot flood the log; the reason is
                // always available at debug level.
                let reason = collected.error.as_deref().unwrap_or("no reason given");
                if config_quick().config.general.log_connections {
                    warn!(
                        r#"client "{}" denied by plugin "{}": {}"#,
                        user, name, reason
                    );
                } else {
                    debug!(
                        r#"client "{}" denied by plugin "{}": {}"#,
                        user, name, reason
                    );
                }
                return PluginAuthOutcome::denied();
            }
        }
    }

    // Every plugin skipped (or there were none). Deny: no password fallback.
    PluginAuthOutcome::no_decision()
}

/// PostgreSQL's identifier limit, `NAMEDATALEN - 1`.
const MAX_NAME_LENGTH: usize = 63;

/// Why PgDog cannot use a name a plugin returned, if it cannot.
///
/// These names become pool identities, `users.toml` entries and the `role`
/// startup parameter, so they are checked before anything acts on them: an
/// empty one would create a user called `""`, padding makes two identities
/// that look identical in the config file, a control character could be
/// smuggled into it or into the startup packet, and PostgreSQL truncates
/// identifiers past 63 bytes, which would quietly map different identities
/// onto one role.
fn invalid_name(value: &str) -> Option<&'static str> {
    if value.is_empty() {
        Some("is empty")
    } else if value.trim() != value {
        Some("has leading or trailing whitespace")
    } else if value.len() > MAX_NAME_LENGTH {
        Some("is longer than PostgreSQL's 63-byte limit")
    } else if value.contains(char::is_control) {
        Some("contains a control character")
    } else {
        None
    }
}

/// First problem with a grant, if any. A plugin that returns one is buggy, so
/// the login is denied rather than fixed up.
fn invalid_grant(grant: &AuthGrant) -> Option<String> {
    for (field, value) in [
        ("derived_user", &grant.derived_user),
        ("server_user", &grant.server_user),
        ("server_role", &grant.server_role),
    ] {
        if let Some(value) = value.as_deref()
            && let Some(problem) = invalid_name(value)
        {
            return Some(format!("{field} {problem}"));
        }
    }

    // Not an identifier, but it is stored in users.toml and sent to the
    // server, so it cannot be empty or carry control characters either.
    if let Some(password) = grant.server_password.as_deref()
        && (password.is_empty() || password.contains(char::is_control))
    {
        return Some("server_password is empty or contains a control character".into());
    }

    None
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_all_skip_is_no_decision() {
        // With no plugins loaded, the driver denies via PluginNoDecision.
        let outcome = authenticate(
            "alice".into(),
            "pgdog".into(),
            "secret".into(),
            "127.0.0.1:5432".into(),
            None,
            false,
        )
        .await;

        assert_eq!(outcome.result, AuthResult::PluginNoDecision);
        assert!(outcome.grant.is_none());
        assert!(!outcome.result.is_ok());
    }

    #[test]
    fn test_run_no_plugins_denies() {
        let outcome = run("bob", "pgdog", "secret", "127.0.0.1:5432", None, false);
        assert_eq!(outcome.result, AuthResult::PluginNoDecision);
    }

    #[test]
    fn test_invalid_grant_rejects_unusable_names() {
        for (field, value, expected) in [
            ("derived_user", "", "derived_user is empty"),
            (
                "derived_user",
                " alice",
                "derived_user has leading or trailing whitespace",
            ),
            (
                "server_user",
                "svc\u{0}",
                "server_user contains a control character",
            ),
            (
                "server_role",
                "report\ning",
                "server_role contains a control character",
            ),
        ] {
            let mut grant = AuthGrant::default();
            let slot = match field {
                "derived_user" => &mut grant.derived_user,
                "server_user" => &mut grant.server_user,
                _ => &mut grant.server_role,
            };
            *slot = Some(value.into());

            assert_eq!(invalid_grant(&grant).as_deref(), Some(expected));
        }

        let grant = AuthGrant {
            derived_user: Some("a".repeat(MAX_NAME_LENGTH + 1)),
            ..Default::default()
        };
        assert_eq!(
            invalid_grant(&grant).as_deref(),
            Some("derived_user is longer than PostgreSQL's 63-byte limit")
        );

        let grant = AuthGrant {
            server_password: Some(String::new()),
            ..Default::default()
        };
        assert!(invalid_grant(&grant).is_some());
    }

    #[test]
    fn test_invalid_grant_accepts_a_usable_grant() {
        let grant = AuthGrant {
            derived_user: Some("alice@example.com".into()),
            server_role: Some("alice@example.com".into()),
            server_user: Some("pgdog".into()),
            server_password: Some("hunter2".into()),
            read_only: Some(true),
            provision: true,
        };
        assert_eq!(invalid_grant(&grant), None);

        // A grant that sets nothing at all is the common case: the client keeps
        // the startup user and an existing pool.
        assert_eq!(invalid_grant(&AuthGrant::default()), None);

        // 63 bytes is the limit, not one less.
        let grant = AuthGrant {
            derived_user: Some("a".repeat(MAX_NAME_LENGTH)),
            ..Default::default()
        };
        assert_eq!(invalid_grant(&grant), None);
    }

    #[test]
    fn test_outcome_constructors() {
        let grant = AuthGrant {
            derived_user: Some("reporting".into()),
            provision: true,
            ..Default::default()
        };
        let allow = PluginAuthOutcome::allow(grant);
        assert!(allow.result.is_ok());
        assert_eq!(
            allow.grant.and_then(|grant| grant.derived_user).as_deref(),
            Some("reporting")
        );

        let denied = PluginAuthOutcome::denied();
        assert_eq!(denied.result, AuthResult::PluginDenied);
        assert!(!denied.result.is_ok());

        let none = PluginAuthOutcome::no_decision();
        assert_eq!(none.result, AuthResult::PluginNoDecision);
    }
}
