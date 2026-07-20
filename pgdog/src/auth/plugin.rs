//! Authentication plugin driver.
//!
//! When `auth_type = "plugin"`, PgDog drives the client wire exchange (asking
//! for a cleartext password) and hands the credential to the loaded plugins.
//! Each plugin answers with an [`AuthDecision`]; the first plugin that does not
//! [`Skip`](pgdog_plugin::AuthDecision::Skip) wins. If every plugin skips, PgDog
//! denies the client: `auth_type = "plugin"` is explicit and there is no
//! fallback to password verification (maintainer decision).
//!
//! Plugins run inside a single [`tokio::task::spawn_blocking`] call (they may
//! block on I/O), and the number of concurrent authentication calls is capped by
//! a [`Semaphore`] sized from the `background_workers` setting.

use std::sync::OnceLock;

use pgdog_plugin::{AuthContext, AuthDecisionTag, AuthField, AuthGrant, PdStr};
use tokio::sync::Semaphore;
use tracing::{debug, warn};

use crate::auth::AuthResult;
use crate::config::config;
use crate::plugin::plugins;

/// Outcome of running the authentication plugins for a single client.
#[derive(Debug)]
pub struct PluginAuthOutcome {
    /// Overall result: [`AuthResult::Ok`] on Allow, otherwise a plugin denial.
    pub result: AuthResult,
    /// Username the plugin derived for the client, if any.
    pub derived_user: Option<String>,
    /// Grant returned by the accepting plugin (only set on Allow).
    pub grant: Option<AuthGrant>,
    /// Name of the plugin that made the decision, for logging.
    pub plugin_name: Option<String>,
}

impl PluginAuthOutcome {
    fn allow(grant: AuthGrant, plugin_name: String) -> Self {
        Self {
            result: AuthResult::Ok,
            derived_user: grant.derived_user.clone(),
            grant: Some(grant),
            plugin_name: Some(plugin_name),
        }
    }

    fn denied(plugin_name: String) -> Self {
        Self {
            result: AuthResult::PluginDenied,
            derived_user: None,
            grant: None,
            plugin_name: Some(plugin_name),
        }
    }

    fn no_decision() -> Self {
        Self {
            result: AuthResult::PluginNoDecision,
            derived_user: None,
            grant: None,
            plugin_name: None,
        }
    }
}

/// Cap on concurrent authentication-plugin calls. Sized once from
/// `background_workers`; plugins run on the blocking pool so this keeps a burst
/// of logins from exhausting it.
static SEMAPHORE: OnceLock<Semaphore> = OnceLock::new();

fn semaphore() -> &'static Semaphore {
    SEMAPHORE.get_or_init(|| {
        let permits = config().config.general.background_workers.max(1);
        Semaphore::new(permits)
    })
}

/// Authenticate a client through the loaded authentication plugins.
///
/// The owned strings are moved into a single `spawn_blocking` call that builds
/// the [`AuthContext`] (whose [`PdStr`] fields borrow them) and consults the
/// plugins. A task join failure is treated as a denial.
pub async fn authenticate(
    user: String,
    database: String,
    credential: String,
    client_addr: String,
    tls_identity: Option<String>,
    tls: bool,
) -> PluginAuthOutcome {
    let permit = match semaphore().acquire().await {
        Ok(permit) => permit,
        Err(_) => {
            // The semaphore is never closed; this only happens on shutdown.
            warn!("authentication plugin semaphore closed, denying client");
            return PluginAuthOutcome::no_decision();
        }
    };

    let join = tokio::task::spawn_blocking(move || {
        // Hold the permit until the blocking work is done.
        let _permit = permit;
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
/// default, so no capability check is needed. Iteration order follows the
/// plugin registry (a map), matching how routing consults plugins.
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
                    read_only: match outcome.read_only {
                        0 => Some(false),
                        1 => Some(true),
                        _ => None,
                    },
                    provision: outcome.provision,
                };
                debug!(r#"client "{}" authenticated by plugin "{}""#, user, name);
                return PluginAuthOutcome::allow(grant, name.clone());
            }
            AuthDecisionTag::Deny => {
                // The reason is logged but never sent to the client.
                warn!(
                    r#"client "{}" denied by plugin "{}": {}"#,
                    user,
                    name,
                    collected.error.as_deref().unwrap_or("no reason given"),
                );
                return PluginAuthOutcome::denied(name.clone());
            }
        }
    }

    // Every plugin skipped (or there were none). Deny: no password fallback.
    PluginAuthOutcome::no_decision()
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
        assert!(outcome.derived_user.is_none());
        assert!(outcome.plugin_name.is_none());
        assert!(!outcome.result.is_ok());
    }

    #[test]
    fn test_run_no_plugins_denies() {
        let outcome = run("bob", "pgdog", "secret", "127.0.0.1:5432", None, false);
        assert_eq!(outcome.result, AuthResult::PluginNoDecision);
    }

    #[tokio::test]
    async fn test_semaphore_sized_from_config() {
        // The semaphore is created lazily and bounded (>= 1 permit). Acquiring
        // and releasing a permit must succeed.
        let permit = semaphore().acquire().await;
        assert!(permit.is_ok());
        assert!(semaphore().available_permits() >= 1);
    }

    #[test]
    fn test_outcome_constructors() {
        let grant = AuthGrant {
            derived_user: Some("reporting".into()),
            provision: true,
            ..Default::default()
        };
        let allow = PluginAuthOutcome::allow(grant, "jwt".into());
        assert!(allow.result.is_ok());
        assert_eq!(allow.derived_user.as_deref(), Some("reporting"));
        assert_eq!(allow.plugin_name.as_deref(), Some("jwt"));

        let denied = PluginAuthOutcome::denied("jwt".into());
        assert_eq!(denied.result, AuthResult::PluginDenied);
        assert!(!denied.result.is_ok());

        let none = PluginAuthOutcome::no_decision();
        assert_eq!(none.result, AuthResult::PluginNoDecision);
    }
}
