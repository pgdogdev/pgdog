use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Aggregation temporality used when exporting OTLP metric points.
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

#[cfg(test)]
mod test {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn default_is_cumulative() {
        assert_eq!(
            OtelTemporalityPreference::default(),
            OtelTemporalityPreference::Cumulative,
        );
    }

    #[test]
    fn from_str_is_case_insensitive() {
        let cases = [
            ("cumulative", OtelTemporalityPreference::Cumulative),
            ("CUMULATIVE", OtelTemporalityPreference::Cumulative),
            ("Cumulative", OtelTemporalityPreference::Cumulative),
            ("delta", OtelTemporalityPreference::Delta),
            ("DELTA", OtelTemporalityPreference::Delta),
            ("Delta", OtelTemporalityPreference::Delta),
            ("lowmemory", OtelTemporalityPreference::LowMemory),
            ("LOWMEMORY", OtelTemporalityPreference::LowMemory),
            ("LowMemory", OtelTemporalityPreference::LowMemory),
        ];

        for (input, expected) in cases {
            assert_eq!(
                OtelTemporalityPreference::from_str(input).unwrap(),
                expected,
                "input {input:?}",
            );
        }
    }

    #[test]
    fn from_str_rejects_unknown_variant() {
        assert!(OtelTemporalityPreference::from_str("nope").is_err());
        assert!(OtelTemporalityPreference::from_str("").is_err());
    }

    #[derive(Debug, Deserialize)]
    struct Wrap {
        t: OtelTemporalityPreference,
    }

    #[test]
    fn deserialize_is_case_insensitive() {
        for (raw, expected) in [
            ("delta", OtelTemporalityPreference::Delta),
            ("DELTA", OtelTemporalityPreference::Delta),
            ("LowMemory", OtelTemporalityPreference::LowMemory),
            ("lowmemory", OtelTemporalityPreference::LowMemory),
            ("Cumulative", OtelTemporalityPreference::Cumulative),
        ] {
            let toml = format!("t = \"{raw}\"");
            let w: Wrap = toml::from_str(&toml).expect("deserialize");
            assert_eq!(w.t, expected, "input {raw:?}");
        }
    }

    #[test]
    fn deserialize_rejects_unknown_variant() {
        let err = toml::from_str::<Wrap>("t = \"histogram\"").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("histogram"), "message was: {msg}");
        assert!(msg.contains("Cumulative"), "message was: {msg}");
    }
}
