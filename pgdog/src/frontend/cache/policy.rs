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
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn extract(s: &str) -> Option<CacheDirective> {
        let mut params = Parameters::default();
        params.insert("pgdog.cache", s);
        extract_parameter_directive(&params)
    }

    #[test]
    fn no_cache_directive() {
        assert_eq!(extract("no_cache"), Some(CacheDirective::NoCache));
    }

    #[test]
    fn cache_directive_no_ttl() {
        assert_eq!(
            extract("cache"),
            Some(CacheDirective::Cache { ttl_seconds: None })
        );
    }

    #[test]
    fn cache_directive_with_ttl() {
        assert_eq!(
            extract("cache ttl=60"),
            Some(CacheDirective::Cache {
                ttl_seconds: Some(60)
            })
        );
    }

    #[test]
    fn cache_directive_with_large_ttl() {
        assert_eq!(
            extract("cache ttl=86400"),
            Some(CacheDirective::Cache {
                ttl_seconds: Some(86400)
            })
        );
    }

    #[test]
    fn force_cache_no_ttl() {
        assert_eq!(
            extract("force_cache"),
            Some(CacheDirective::ForceCache { ttl_seconds: None })
        );
    }

    #[test]
    fn force_cache_with_ttl() {
        assert_eq!(
            extract("force_cache ttl=120"),
            Some(CacheDirective::ForceCache {
                ttl_seconds: Some(120)
            })
        );
    }

    #[test]
    fn garbage_input_returns_none() {
        assert_eq!(extract("garbage"), None);
    }

    #[test]
    fn invalid_ttl_letters_returns_none() {
        assert_eq!(extract("cache ttl=abc"), None);
    }

    #[test]
    fn empty_ttl_returns_none() {
        assert_eq!(extract("cache ttl="), None);
    }

    #[test]
    fn ttl_zero_is_valid() {
        // 0 is a valid u64, even if semantically it means "expire immediately"
        assert_eq!(
            extract("cache ttl=0"),
            Some(CacheDirective::Cache {
                ttl_seconds: Some(0)
            })
        );
    }

    #[test]
    fn missing_key_returns_none() {
        let params = Parameters::default();
        assert_eq!(extract_parameter_directive(&params), None);
    }

    #[test]
    fn force_cache_invalid_ttl_returns_none() {
        assert_eq!(extract("force_cache ttl=bad"), None);
    }

    #[test]
    fn force_cache_empty_ttl_returns_none() {
        assert_eq!(extract("force_cache ttl="), None);
    }

    #[test]
    fn whitespace_trimmed_around_value() {
        // The value stored in the param is retrieved with .trim() in extract_parameter_directive
        let mut params = Parameters::default();
        params.insert("pgdog.cache", "  no_cache  ");
        assert_eq!(
            extract_parameter_directive(&params),
            Some(CacheDirective::NoCache)
        );
    }
}
