use std::collections::HashMap;
use std::env;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// OpenTelemetry push exporter settings.
///
/// When `endpoint` is set, PgDog periodically POSTs OTLP JSON metrics
/// to the configured URL.
#[derive(JsonSchema, Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct Otel {
    /// Full URL of the OTLP metrics ingest endpoint
    /// (e.g. `https://otlp.us5.datadoghq.com/v1/metrics`).
    /// When not set, the push exporter is disabled.
    ///
    /// Env: `OTEL_EXPORTER_OTLP_ENDPOINT`
    #[serde(default = "Otel::endpoint")]
    pub endpoint: Option<String>,

    /// HTTP headers sent with each OTLP push request.
    ///
    /// In TOML:
    /// ```toml
    /// [otel.headers]
    /// DD-API-KEY = "abc123"
    /// X-Custom = "foo"
    /// ```
    ///
    /// Env: `OTEL_EXPORTER_OTLP_HEADERS` (comma-separated `key=value` pairs)
    #[serde(default = "Otel::headers")]
    pub headers: HashMap<String, String>,

    /// Datadog API key. Convenience shorthand that adds a `DD-API-KEY` header
    /// to OTLP push requests.
    ///
    /// Env: `DD_API_KEY`
    #[serde(default = "Otel::datadog_api_key")]
    pub datadog_api_key: Option<String>,

    /// How often, in milliseconds, to push metrics to the OTLP endpoint.
    ///
    /// _Default:_ `10000`
    ///
    /// Env: `OTEL_METRIC_EXPORT_INTERVAL`
    #[serde(default = "Otel::push_interval")]
    pub push_interval: u64,
}

impl Otel {
    fn env_option_string(env_var: &str) -> Option<String> {
        env::var(env_var).ok().filter(|s| !s.is_empty())
    }

    fn endpoint() -> Option<String> {
        Self::env_option_string("OTEL_EXPORTER_OTLP_ENDPOINT")
    }

    fn headers() -> HashMap<String, String> {
        let mut map = HashMap::new();
        if let Some(raw) = Self::env_option_string("OTEL_EXPORTER_OTLP_HEADERS") {
            for pair in raw.split(',') {
                let pair = pair.trim();
                if let Some((k, v)) = pair.split_once('=') {
                    map.insert(k.trim().to_owned(), v.trim().to_owned());
                }
            }
        }
        map
    }

    fn datadog_api_key() -> Option<String> {
        Self::env_option_string("DD_API_KEY")
    }

    fn push_interval() -> u64 {
        env::var("OTEL_METRIC_EXPORT_INTERVAL")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10_000)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn headers_from_toml() {
        let toml = r#"
            endpoint = "https://example.com/v1/metrics"

            [headers]
            DD-API-KEY = "abc123"
            X-Custom = "foo"
        "#;

        let otel: Otel = toml::from_str(toml).expect("parse");
        assert_eq!(
            otel.endpoint.as_deref(),
            Some("https://example.com/v1/metrics")
        );
        assert_eq!(otel.headers.get("DD-API-KEY").unwrap(), "abc123");
        assert_eq!(otel.headers.get("X-Custom").unwrap(), "foo");
        assert_eq!(otel.headers.len(), 2);
    }

    #[test]
    fn default_has_no_headers() {
        let otel: Otel = toml::from_str("").expect("parse");
        assert!(otel.headers.is_empty());
        assert!(otel.endpoint.is_none());
        assert!(otel.datadog_api_key.is_none());
        assert_eq!(otel.push_interval, 10_000);
    }

    #[test]
    fn full_config_section() {
        let toml = r#"
            [otel]
            endpoint = "https://otlp.us5.datadoghq.com/v1/metrics"
            datadog_api_key = "my-key"
            push_interval = 5000

            [otel.headers]
            Authorization = "Bearer token"
        "#;

        let config: crate::Config = toml::from_str(toml).expect("parse");
        assert_eq!(
            config.otel.endpoint.as_deref(),
            Some("https://otlp.us5.datadoghq.com/v1/metrics")
        );
        assert_eq!(config.otel.datadog_api_key.as_deref(), Some("my-key"));
        assert_eq!(config.otel.push_interval, 5000);
        assert_eq!(
            config.otel.headers.get("Authorization").unwrap(),
            "Bearer token"
        );
    }
}
