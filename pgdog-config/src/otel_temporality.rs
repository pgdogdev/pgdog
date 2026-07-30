use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Aggregation temporality used when exporting OTLP metric points.
///
/// <https://docs.pgdog.dev/configuration/pgdog.toml/otel/#temporality_preference>
// Note: Derive FromStr is case insensitive, matching OTEL behavior, though serde Deserialize
// see https://docs.rs/derive_more/latest/derive_more/derive.FromStr.html#empty-enums
#[derive(
    derive_more::FromStr, Debug, Clone, Copy, PartialEq, Eq, Default, JsonSchema, Serialize,
)]
pub enum OtelTemporalityPreference {
    /// Points report the value accumulated since the exporter started.
    #[default]
    Cumulative,

    /// Points report the change since the last export.
    Delta,
    /// Delta for sums, cumulative for histograms; minimizes exporter memory.
    LowMemory,
}

// Use case insensitive deserialization to match env var behavior
impl<'de> Deserialize<'de> for OtelTemporalityPreference {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use std::str::FromStr;

        let s = String::deserialize(deserializer)?;

        // this from_str is case insensitive
        Self::from_str(&s.to_ascii_lowercase()).map_err(|_| {
            serde::de::Error::unknown_variant(&s, &["Cumulative", "Delta", "LowMemory"])
        })
    }
}
