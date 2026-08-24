//! Google OAuth 2.0 access-token authentication for PgDog.

mod config;
mod token_info;

use std::{path::Path, sync::Arc};

use config::RuntimeConfig;
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use pgdog_plugin::{AuthContext, AuthDecision, Config as PluginConfig, PdStr, Plugin, plugin};
use tracing::{error, info};

plugin!(GoogleAuthPlugin);

struct GoogleAuthPlugin;

static RUNTIME: Lazy<RwLock<Option<Arc<RuntimeConfig>>>> = Lazy::new(|| RwLock::new(None));

impl Plugin for GoogleAuthPlugin {
    extern "C-unwind" fn version() -> PdStr<'static> {
        env!("CARGO_PKG_VERSION").into()
    }

    extern "C-unwind" fn config(config: PluginConfig<'_>) -> bool {
        let path = (!config.plugin_config.is_empty()).then(|| Path::new(&*config.plugin_config));

        match RuntimeConfig::load(path) {
            Ok(runtime) => {
                info!("[pgdog_google_auth] configured Google access-token authentication");
                *RUNTIME.write() = Some(Arc::new(runtime));
                true
            }
            Err(err) => {
                error!("[pgdog_google_auth] configuration failed: {err}");
                *RUNTIME.write() = None;
                false
            }
        }
    }

    fn authenticate(context: AuthContext<'_>) -> AuthDecision {
        let Some(runtime) = RUNTIME.read().clone() else {
            return AuthDecision::Deny("Google authentication plugin is not configured".into());
        };

        match token_info::authenticate(&runtime, &context.user, &context.credential, context.tls) {
            Ok(grant) => AuthDecision::Allow(grant),
            Err(err) => AuthDecision::Deny(err.to_string()),
        }
    }
}
