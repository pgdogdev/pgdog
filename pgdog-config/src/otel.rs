use std::collections::HashMap;
use std::env;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::otel_temporality::OtelTemporalityPreference;

const DEFAULT_PUSH_INTERVAL: u64 = 10_000;

/// OpenTelemetry push exporter settings.
///
/// When `endpoint` is set, PgDog periodically POSTs OTLP JSON metrics
/// to the configured URL.
///
/// <https://docs.pgdog.dev/configuration/pgdog.toml/otel/>
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

    /// Prefix added to all metric names emitted by the OTEL exporter.
    ///
    /// **Note:** Trailing `.` and `_` are stripped before PgDog appends metric names, so
    /// `pgdog`, `pgdog.`, and `pgdog_` all emit `pgdog.clients`.
    ///
    /// _Default:_ `pgdog`
    ///
    /// Env: `PGDOG_OTEL_NAMESPACE`
    ///
    /// <https://docs.pgdog.dev/configuration/pgdog.toml/otel/#namespace>
    #[serde(default = "Otel::namespace")]
    pub namespace: Option<String>,

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
    #[schemars(default = "Otel::schema_default_push_interval")]
    pub push_interval: u64,

    /// Describes how the exported metric points should be described.
    ///
    /// See https://opentelemetry.io/docs/specs/otel/metrics/data-model/#metric-points
    ///
    /// _Default:_ `Cumulative`, or `Delta` when `datadog_api_key` is set.
    ///
    /// Env: `OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE`
    #[serde(default = "Otel::temporality_preference")]
    #[schemars(default = "Otel::schema_default_temporality_preference")]
    pub temporality_preference: Option<OtelTemporalityPreference>,
}

impl Otel {
    pub fn effective_temporality_preference(&self) -> OtelTemporalityPreference {
        self.temporality_preference
            .unwrap_or(if self.datadog_api_key.is_some() {
                OtelTemporalityPreference::Delta
            } else {
                OtelTemporalityPreference::Cumulative
            })
    }

    fn env_option_string(env_var: &str) -> Option<String> {
        env::var(env_var).ok().filter(|s| !s.is_empty())
    }

    fn endpoint() -> Option<String> {
        Self::env_option_string("OTEL_EXPORTER_OTLP_ENDPOINT")
    }

    fn namespace() -> Option<String> {
        Self::env_option_string("PGDOG_OTEL_NAMESPACE")
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
            .unwrap_or(DEFAULT_PUSH_INTERVAL)
    }

    fn temporality_preference() -> Option<OtelTemporalityPreference> {
        env::var("OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE")
            .ok()
            .and_then(|v| v.parse().ok())
    }

    fn schema_default_push_interval() -> u64 {
        DEFAULT_PUSH_INTERVAL
    }

    fn schema_default_temporality_preference() -> Option<OtelTemporalityPreference> {
        Some(OtelTemporalityPreference::Cumulative)
    }

    /// Schema-only default for the whole `Otel` object, used so the top-level
    /// `default` block in the generated JSON schema matches the per-field
    /// documented defaults instead of the raw derived `Default` (0 / null).
    pub fn schema_default() -> Self {
        Self {
            push_interval: Self::schema_default_push_interval(),
            temporality_preference: Self::schema_default_temporality_preference(),
            ..Self::default()
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::set_env_var;

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
        assert_eq!(otel.push_interval, DEFAULT_PUSH_INTERVAL);
        assert!(otel.temporality_preference.is_none());
    }

    #[test]
    fn endpoint_toml_wins_over_env() {
        let _guard = set_env_var("OTEL_EXPORTER_OTLP_ENDPOINT", "https://env.example/v1");
        let toml = r#"endpoint = "https://toml.example/v1""#;
        let otel: Otel = toml::from_str(toml).expect("parse");
        assert_eq!(otel.endpoint.as_deref(), Some("https://toml.example/v1"));
    }

    #[test]
    fn push_interval_env_used_when_toml_absent() {
        let _guard = set_env_var("OTEL_METRIC_EXPORT_INTERVAL", "7500");
        let otel: Otel = toml::from_str("").expect("parse");
        assert_eq!(otel.push_interval, 7500);
    }

    #[test]
    fn temporality_preference_env_parsed() {
        let _guard = set_env_var("OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE", "Delta");
        let otel: Otel = toml::from_str("").expect("parse");
        assert_eq!(
            otel.temporality_preference,
            Some(OtelTemporalityPreference::Delta)
        );
    }

    #[test]
    fn full_config_section() {
        let toml = r#"
            [otel]
            endpoint = "https://otlp.us5.datadoghq.com/v1/metrics"
            namespace = "pgdog_"
            datadog_api_key = "my-key"
            push_interval = 5000
            temporality_preference = "Delta"

            [otel.headers]
            Authorization = "Bearer token"
        "#;

        let config: crate::Config = toml::from_str(toml).expect("parse");
        assert_eq!(
            config.otel.endpoint.as_deref(),
            Some("https://otlp.us5.datadoghq.com/v1/metrics")
        );
        assert_eq!(config.otel.namespace.as_deref(), Some("pgdog_"));
        assert_eq!(config.otel.datadog_api_key.as_deref(), Some("my-key"));
        assert_eq!(config.otel.push_interval, 5000);
        assert_eq!(
            config.otel.temporality_preference,
            Some(OtelTemporalityPreference::Delta)
        );
        assert_eq!(
            config.otel.headers.get("Authorization").unwrap(),
            "Bearer token"
        );
    }

    #[test]
    fn effective_temporality_defaults_to_delta_with_datadog_key() {
        let mut otel = Otel::default();
        assert_eq!(
            otel.effective_temporality_preference(),
            OtelTemporalityPreference::Cumulative
        );

        otel.datadog_api_key = Some("abc".into());
        assert_eq!(
            otel.effective_temporality_preference(),
            OtelTemporalityPreference::Delta
        );

        otel.temporality_preference = Some(OtelTemporalityPreference::Cumulative);
        assert_eq!(
            otel.effective_temporality_preference(),
            OtelTemporalityPreference::Cumulative
        );
    }

    #[test]
    fn namespace_from_env() {
        let _guard = set_env_var("PGDOG_OTEL_NAMESPACE", "pgdog_");

        let otel: Otel = toml::from_str("").expect("parse");
        assert_eq!(otel.namespace.as_deref(), Some("pgdog_"));
    }
}
