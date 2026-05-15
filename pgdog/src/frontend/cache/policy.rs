use crate::config::{config, CachePolicy};
use crate::frontend::ClientRequest;
use crate::net::parameter::ParameterValue;
use crate::net::Parameters;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CacheDirective {
    Cache {
        ttl_seconds: Option<u64>,
    },
    ForceCache {
        ttl_seconds: Option<u64>,
    },
    #[default]
    NoCache,
}

pub enum CacheDecision {
    Skip,
    Cache(u64),
    ForceCache(u64),
}

const KEY: &str = "pgdog.cache";

pub async fn resolve(
    client_request: &ClientRequest,
    params: &Parameters,
    is_read: bool,
) -> CacheDecision {
    let cache_config = &config().config.general.cache;

    if !is_read {
        return CacheDecision::Skip;
    }

    let cache_directive = get_cache_directive(client_request, params);
    match cache_directive {
        Some(CacheDirective::NoCache) => return CacheDecision::Skip,
        Some(CacheDirective::Cache { ttl_seconds }) => {
            return CacheDecision::Cache(ttl_seconds.unwrap_or(cache_config.ttl))
        },
        Some(CacheDirective::ForceCache { ttl_seconds }) => {
            return CacheDecision::ForceCache(ttl_seconds.unwrap_or(cache_config.ttl))
        }
        _ => (),
    }

    match cache_config.policy {
        CachePolicy::NoCache => CacheDecision::Skip,
        CachePolicy::Cache => CacheDecision::Cache(cache_config.ttl),
    }
}

// Comment hint has priority over connection parameter
fn get_cache_directive(
    client_request: &ClientRequest,
    params: &Parameters,
) -> Option<CacheDirective> {
    client_request
        .ast
        .as_ref()
        .and_then(|ast| ast.comment_cache)
        .or_else(|| extract_parameter_directive(params))
}

fn extract_parameter_directive(params: &Parameters) -> Option<CacheDirective> {
    let value = params.get(KEY)?;
    let s = match value {
        ParameterValue::String(v) => v.as_str().trim(),
        _ => return None,
    };

    match s {
        "no_cache" => return Some(CacheDirective::NoCache),
        "force_cache" => return Some(CacheDirective::ForceCache { ttl_seconds: None }),
        "cache" => return Some(CacheDirective::Cache { ttl_seconds: None }),
        _ => (),
    }

    if let Some(ttl) = s
        .strip_prefix("force_cache")
        .or_else(|| s.strip_prefix("cache"))
        .map(|s| s.trim_start())
        .and_then(|s| s.strip_prefix("ttl="))
        .and_then(|t| t.trim().parse::<u64>().ok())
    {
        let ttl_seconds = Some(ttl);
        if s.starts_with("force_cache") {
            return Some(CacheDirective::ForceCache { ttl_seconds });
        } else {
            return Some(CacheDirective::Cache { ttl_seconds });
        }
    }

    None
}
