use core::fmt;

use pgdog_config::{Cache as CacheConfig, CachePolicy};
use tracing::debug;

use super::stats::QueryStatsTracker;

use crate::net::parameter::ParameterValue;
use crate::net::Parameters;
use once_cell::sync::Lazy;
use regex::Regex;

static CACHE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"pgdog_cache: *(no-cache|cache(?:\s+ttl\s*=\s*([0-9]+))?)?"#).unwrap()
});

/// Cache directive from SQL comment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CacheDirective {
    #[default]
    None,
    Cache {
        ttl_seconds: Option<u64>,
    },
    NoCache,
}

impl CacheDirective {
    pub fn is_cache(&self) -> bool {
        matches!(self, CacheDirective::Cache { .. })
    }

    pub fn is_no_cache(&self) -> bool {
        matches!(self, CacheDirective::NoCache)
    }

    pub fn ttl(&self) -> Option<u64> {
        match self {
            CacheDirective::Cache { ttl_seconds } => *ttl_seconds,
            _ => None,
        }
    }
}

pub trait CachePolicyExtractor: Send + Sync + fmt::Debug {
    fn extract(&self, query: &str, params: &Parameters) -> CacheDirective;
}

#[derive(Debug)]
pub struct CommentCacheExtractor;

impl CachePolicyExtractor for CommentCacheExtractor {
    fn extract(&self, query: &str, _params: &Parameters) -> CacheDirective {
        for cap in CACHE.captures_iter(query) {
            if let Some(action) = cap.get(1) {
                let action = action.as_str();
                if action == "no-cache" {
                    return CacheDirective::NoCache;
                } else if action.starts_with("cache") {
                    let ttl = cap.get(2).and_then(|m| m.as_str().parse::<u64>().ok());
                    return CacheDirective::Cache { ttl_seconds: ttl };
                }
            } else {
                return CacheDirective::Cache { ttl_seconds: None };
            }
        }
        CacheDirective::None
    }
}

#[derive(Debug)]
pub struct ParameterCacheExtractor {
    key: String,
}

impl ParameterCacheExtractor {
    pub fn new() -> Self {
        Self {
            key: "pgdog.cache".to_string(),
        }
    }
}

impl CachePolicyExtractor for ParameterCacheExtractor {
    fn extract(&self, _query: &str, params: &Parameters) -> CacheDirective {
        let value = match params.get(&self.key) {
            Some(p) => p,
            None => return CacheDirective::None,
        };

        let s = match value {
            ParameterValue::String(v) => v.as_str(),
            _ => return CacheDirective::None,
        };

        match s {
            "no-cache" => CacheDirective::NoCache,
            "cache" => CacheDirective::Cache { ttl_seconds: None },
            _ => {
                if let Some(ttl) = s
                    .strip_prefix("cache ttl=")
                    .and_then(|t| t.trim().parse::<u64>().ok())
                {
                    CacheDirective::Cache {
                        ttl_seconds: Some(ttl),
                    }
                } else if let Some(ttl) = s
                    .strip_prefix("cache ttl =")
                    .and_then(|t| t.trim().parse::<u64>().ok())
                {
                    CacheDirective::Cache {
                        ttl_seconds: Some(ttl),
                    }
                } else {
                    CacheDirective::None
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct CachePolicyDispatcher {
    extractors: Vec<Box<dyn CachePolicyExtractor>>,
}

impl CachePolicyDispatcher {
    pub fn new() -> Self {
        Self {
            extractors: Vec::new(),
        }
    }

    pub fn add_extractor(&mut self, extractor: Box<dyn CachePolicyExtractor>) {
        self.extractors.push(extractor);
    }

    pub fn extract(&self, query: &str, params: &Parameters) -> CacheDirective {
        for extractor in &self.extractors {
            let result = extractor.extract(query, params);
            if result != CacheDirective::None {
                debug!("Cache directive for query {} is {:?}", query, result);
                return result;
            }
        }
        CacheDirective::None
    }

    pub fn is_empty(&self) -> bool {
        self.extractors.is_empty()
    }
}

pub struct CachePolicyResolver;

impl CachePolicyResolver {
    pub async fn resolve(
        cache_directive: CacheDirective,
        cache_config: &CacheConfig,
        is_read: bool,
        cache_key_hash: u64,
        stats: &QueryStatsTracker,
    ) -> CacheDecision {
        if !is_read {
            return CacheDecision::Skip;
        }

        if let CacheDirective::NoCache = cache_directive {
            return CacheDecision::Skip;
        }

        if let CacheDirective::Cache { ttl_seconds } = cache_directive {
            return CacheDecision::Cache(ttl_seconds.or(Some(cache_config.ttl())));
        }

        match cache_config.policy() {
            CachePolicy::NoCache => CacheDecision::Skip,
            CachePolicy::Cache => CacheDecision::Cache(Some(cache_config.ttl())),
            CachePolicy::Auto => Self::auto_decision(cache_key_hash, stats).await,
        }
    }

    async fn auto_decision(cache_key_hash: u64, stats: &QueryStatsTracker) -> CacheDecision {
        let query_stats = stats.get(cache_key_hash).await;

        if query_stats.hit_count > query_stats.miss_count
            && query_stats.avg_result_size() < 1_000_000
        {
            CacheDecision::Cache(None)
        } else {
            CacheDecision::Skip
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_skip_for_writes() {
        let cache_config = CacheConfig {
            enabled: Some(true),
            policy: Some(CachePolicy::Cache),
            ttl: None,
            redis_url: None,
            max_result_size: None,
        };
        let decision = CachePolicyResolver::resolve(
            CacheDirective::None,
            &cache_config,
            false,
            0xAABBCCDD,
            &QueryStatsTracker::default(),
        )
        .await;
        assert!(!decision.should_cache());
    }

    #[tokio::test]
    async fn test_no_cache_directive() {
        let cache_config = CacheConfig {
            enabled: Some(true),
            policy: Some(CachePolicy::Cache),
            ttl: None,
            redis_url: None,
            max_result_size: None,
        };
        let decision = CachePolicyResolver::resolve(
            CacheDirective::NoCache,
            &cache_config,
            true,
            0xAABBCCDD,
            &QueryStatsTracker::default(),
        )
        .await;
        assert!(!decision.should_cache());
    }

    #[tokio::test]
    async fn test_cache_directive_with_ttl() {
        let cache_config = CacheConfig {
            enabled: Some(true),
            policy: Some(CachePolicy::NoCache),
            ttl: None,
            redis_url: None,
            max_result_size: None,
        };
        let decision = CachePolicyResolver::resolve(
            CacheDirective::Cache {
                ttl_seconds: Some(120),
            },
            &cache_config,
            true,
            0xAABBCCDD,
            &QueryStatsTracker::default(),
        )
        .await;
        assert!(decision.should_cache());
        assert_eq!(decision.ttl(), Some(120));
    }

    #[test]
    fn test_comment_extractor_no_cache() {
        let extractor = CommentCacheExtractor;
        let params = Parameters::default();
        let directive =
            extractor.extract("SELECT * FROM users /* pgdog_cache: no-cache */", &params);
        assert!(matches!(directive, CacheDirective::NoCache));
    }

    #[test]
    fn test_comment_extractor_cache_default_ttl() {
        let extractor = CommentCacheExtractor;
        let params = Parameters::default();
        let directive = extractor.extract("SELECT * FROM users /* pgdog_cache: cache */", &params);
        match directive {
            CacheDirective::Cache { ttl_seconds } => assert!(ttl_seconds.is_none()),
            _ => panic!("Expected Cache directive"),
        }
    }

    #[test]
    fn test_comment_extractor_cache_with_ttl() {
        let extractor = CommentCacheExtractor;
        let params = Parameters::default();
        let directive = extractor.extract(
            "SELECT * FROM users /* pgdog_cache: cache ttl=60 */",
            &params,
        );
        match directive {
            CacheDirective::Cache { ttl_seconds } => assert_eq!(ttl_seconds, Some(60)),
            _ => panic!("Expected Cache directive"),
        }
    }

    #[test]
    fn test_comment_extractor_no_directive() {
        let extractor = CommentCacheExtractor;
        let params = Parameters::default();
        let directive = extractor.extract("SELECT * FROM users", &params);
        assert!(matches!(directive, CacheDirective::None));
    }

    #[test]
    fn test_parameter_extractor_no_cache() {
        let extractor = ParameterCacheExtractor::new();
        let mut params = Parameters::default();
        params.insert("pgdog.cache", "no-cache");
        let directive = extractor.extract("SELECT * FROM users", &params);
        assert!(matches!(directive, CacheDirective::NoCache));
    }

    #[test]
    fn test_parameter_extractor_cache() {
        let extractor = ParameterCacheExtractor::new();
        let mut params = Parameters::default();
        params.insert("pgdog.cache", "cache");
        let directive = extractor.extract("SELECT * FROM users", &params);
        match directive {
            CacheDirective::Cache { ttl_seconds } => assert!(ttl_seconds.is_none()),
            _ => panic!("Expected Cache directive"),
        }
    }

    #[test]
    fn test_parameter_extractor_cache_with_ttl() {
        let extractor = ParameterCacheExtractor::new();
        let mut params = Parameters::default();
        params.insert("pgdog.cache", "cache ttl=120");
        let directive = extractor.extract("SELECT * FROM users", &params);
        match directive {
            CacheDirective::Cache { ttl_seconds } => assert_eq!(ttl_seconds, Some(120)),
            _ => panic!("Expected Cache directive"),
        }
    }

    #[test]
    fn test_parameter_extractor_no_param() {
        let extractor = ParameterCacheExtractor::new();
        let params = Parameters::default();
        let directive = extractor.extract("SELECT * FROM users", &params);
        assert!(matches!(directive, CacheDirective::None));
    }

    #[test]
    fn test_dispatcher_comment_wins() {
        let comment_extractor = CommentCacheExtractor;
        let parameter_extractor = ParameterCacheExtractor::new();

        let mut dispatcher = CachePolicyDispatcher::new();
        dispatcher.add_extractor(Box::new(comment_extractor));
        dispatcher.add_extractor(Box::new(parameter_extractor));

        let mut params = Parameters::default();
        params.insert("pgdog.cache", "no-cache");

        let directive = dispatcher.extract("SELECT * /* pgdog_cache: cache ttl=60 */", &params);
        match directive {
            CacheDirective::Cache { ttl_seconds } => assert_eq!(ttl_seconds, Some(60)),
            _ => panic!("Expected comment to win"),
        }
    }

    #[test]
    fn test_dispatcher_parameter_fallback() {
        let comment_extractor = CommentCacheExtractor;
        let parameter_extractor = ParameterCacheExtractor::new();

        let mut dispatcher = CachePolicyDispatcher::new();
        dispatcher.add_extractor(Box::new(comment_extractor));
        dispatcher.add_extractor(Box::new(parameter_extractor));

        let mut params = Parameters::default();
        params.insert("pgdog.cache", "no-cache");

        let directive = dispatcher.extract("SELECT * FROM users", &params);
        assert!(matches!(directive, CacheDirective::NoCache));
    }
}
