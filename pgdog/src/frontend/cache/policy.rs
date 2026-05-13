use super::stats::QueryStatsTracker;
use crate::config::{config, CachePolicy};
use crate::frontend::ClientRequest;
use crate::net::parameter::ParameterValue;
use crate::net::Parameters;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CacheDirective {
    Cache {
        ttl_seconds: Option<u64>,
    },
    #[default]
    NoCache,
}

pub enum CacheDecision {
    Skip,
    Cache(Option<u64>),
}

impl CacheDecision {
    pub fn should_cache(&self) -> bool {
        matches!(self, CacheDecision::Cache(_))
    }

    pub fn ttl(&self) -> Option<u64> {
        match self {
            CacheDecision::Cache(ttl) => *ttl,
            _ => None,
        }
    }
}

const KEY: &str = "pgdog.cache";

pub async fn resolve(
    client_request: &ClientRequest,
    params: &Parameters,
    is_read: bool,
    cache_key_hash: u64,
    stats: &QueryStatsTracker,
) -> CacheDecision {
    let cache_config = &config().config.general.cache;

    if !is_read {
        return CacheDecision::Skip;
    }

    let cache_directive = get_cache_directive(client_request, params);
    match cache_directive {
        Some(CacheDirective::NoCache) => return CacheDecision::Skip,
        Some(CacheDirective::Cache { ttl_seconds }) => {
            return CacheDecision::Cache(ttl_seconds.or(Some(cache_config.ttl())))
        }
        _ => (),
    }

    match cache_config.policy() {
        CachePolicy::NoCache => CacheDecision::Skip,
        CachePolicy::Cache => CacheDecision::Cache(Some(cache_config.ttl())),
        CachePolicy::Auto => auto_decision(cache_key_hash, stats).await,
    }
}

async fn auto_decision(cache_key_hash: u64, stats: &QueryStatsTracker) -> CacheDecision {
    let query_stats = stats.get(cache_key_hash).await;
    if query_stats.hit_count > query_stats.miss_count && query_stats.avg_result_size() < 1_000_000 {
        CacheDecision::Cache(None)
    } else {
        CacheDecision::Skip
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
        .map(|ast| ast.comment_cache)
        .flatten()
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
        "cache" => return Some(CacheDirective::Cache { ttl_seconds: None }),
        _ => (),
    }

    if let Some(ttl) = s
        .strip_prefix("cache")
        .map(|s| s.trim_start())
        .map(|s| s.strip_prefix("ttl="))
        .flatten()
        .and_then(|t| t.trim().parse::<u64>().ok())
    {
        return Some(CacheDirective::Cache {
            ttl_seconds: Some(ttl),
        });
    }

    None
}
