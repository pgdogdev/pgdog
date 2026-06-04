use crate::frontend::ClientRequest;
use crate::net::parameter::ParameterValue;
use crate::net::Parameters;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CacheMode {
    Cache,
    ForceCache,
    #[default]
    NoCache,
}

/// A fully-parsed cache directive.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CacheDirective {
    pub mode: Option<CacheMode>,
    pub ttl_seconds: Option<u64>,
}

const KEY: &str = "pgdog.cache";

impl CacheDirective {
    /// Parse a space-separated token string like `"force_cache ttl=60"`.
    pub fn parse(s: &str) -> Self {
        let mut directive = CacheDirective::default();

        for token in s.split_whitespace() {
            if let Some((key, val)) = token.split_once('=') {
                match key {
                    "ttl" => {
                        directive.ttl_seconds = val.parse::<u64>().ok();
                    }
                    _ => {}
                }
            } else {
                match token {
                    "no_cache" => {
                        directive.mode = Some(CacheMode::NoCache);
                    }
                    "force_cache" => {
                        directive.mode = Some(CacheMode::ForceCache);
                    }
                    "cache" => {
                        directive.mode = Some(CacheMode::Cache);
                    }
                    _ => {}
                }
            }
        }

        directive
    }

    fn from_params(params: &Parameters) -> Self {
        match params.get(KEY) {
            Some(ParameterValue::String(v)) => CacheDirective::parse(v.as_str().trim()),
            _ => return CacheDirective::default(),
        }
    }

    pub fn or(self, fallback: Self) -> Self {
        let Self { mode, ttl_seconds } = self;
        Self {
            mode: mode.or(fallback.mode),
            ttl_seconds: ttl_seconds.or(fallback.ttl_seconds),
        }
    }
}

pub fn resolve(client_request: &ClientRequest, params: &Parameters) -> CacheDirective {
    let comment_directive = client_request
        .ast
        .as_ref()
        .and_then(|ast| ast.comment_cache.clone())
        .unwrap_or_default();
    let param_directive = CacheDirective::from_params(params);

    comment_directive.or(param_directive)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> CacheDirective {
        CacheDirective::parse(s)
    }

    fn directive(mode: CacheMode, ttl: Option<u64>) -> CacheDirective {
        CacheDirective {
            mode: Some(mode),
            ttl_seconds: ttl,
        }
    }

    #[test]
    fn no_cache_directive() {
        assert_eq!(parse("no_cache"), directive(CacheMode::NoCache, None));
    }

    #[test]
    fn cache_directive_no_ttl() {
        assert_eq!(parse("cache"), directive(CacheMode::Cache, None));
    }

    #[test]
    fn cache_directive_with_ttl() {
        assert_eq!(parse("cache ttl=60"), directive(CacheMode::Cache, Some(60)));
    }

    #[test]
    fn cache_directive_with_large_ttl() {
        assert_eq!(
            parse("cache ttl=86400"),
            directive(CacheMode::Cache, Some(86400))
        );
    }

    #[test]
    fn force_cache_no_ttl() {
        assert_eq!(parse("force_cache"), directive(CacheMode::ForceCache, None));
    }

    #[test]
    fn force_cache_with_ttl() {
        assert_eq!(
            parse("force_cache ttl=120"),
            directive(CacheMode::ForceCache, Some(120))
        );
    }

    #[test]
    fn garbage_input_returns_all_none() {
        assert_eq!(parse("garbage"), CacheDirective::default());
    }

    #[test]
    fn invalid_ttl_does_not_set_ttl() {
        assert_eq!(parse("cache ttl=abc"), directive(CacheMode::Cache, None));
    }

    #[test]
    fn force_cache_invalid_ttl_does_not_set_ttl() {
        assert_eq!(
            parse("force_cache ttl=bad"),
            directive(CacheMode::ForceCache, None)
        );
    }

    #[test]
    fn empty_ttl_does_not_set_ttl() {
        assert_eq!(parse("cache ttl="), directive(CacheMode::Cache, None));
    }

    #[test]
    fn ttl_zero_is_valid() {
        assert_eq!(parse("cache ttl=0"), directive(CacheMode::Cache, Some(0)));
    }

    #[test]
    fn missing_key_returns_all_none() {
        let params = Parameters::default();
        assert_eq!(
            CacheDirective::from_params(&params),
            CacheDirective::default()
        );
    }

    #[test]
    fn whitespace_trimmed_around_value() {
        let mut params = Parameters::default();
        params.insert("pgdog.cache", "  no_cache  ");
        assert_eq!(
            CacheDirective::from_params(&params),
            directive(CacheMode::NoCache, None)
        );
    }

    // --- position-agnostic parsing ---

    #[test]
    fn ttl_before_mode() {
        assert_eq!(
            parse("ttl=17 force_cache"),
            directive(CacheMode::ForceCache, Some(17))
        );
    }

    #[test]
    fn ttl_only_no_mode() {
        assert_eq!(
            parse("ttl=42"),
            CacheDirective {
                mode: None,
                ttl_seconds: Some(42)
            }
        );
    }

    #[test]
    fn mode_ttl_extra_unknown_token() {
        assert_eq!(
            parse("cache ttl=5 foo=bar"),
            directive(CacheMode::Cache, Some(5))
        );
    }
}
