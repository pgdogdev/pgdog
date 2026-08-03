//! Human-readable time values, e.g. `"5s"` or `"1h5m15s"`.
//!
//! Time settings accept either a plain number, in the unit documented on the
//! field, or a duration string built from `ms`, `s`, `m`, `h` and `d` components.

use std::time::Duration;

use schemars::JsonSchema;
use serde::Deserialize;
use serde::de::{self, Deserializer};
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum Error {
    #[error(r#""{0}" is not a valid duration, expected a number or e.g. "250ms", "5s", "1h5m15s""#)]
    Invalid(String),

    #[error(r#"duration "{0}" is too large"#)]
    Overflow(String),

    #[error(r#"duration "{0}" is not a whole number of seconds"#)]
    NotWholeSeconds(String),
}

const UNITS: &[(&str, u64)] = &[
    ("ms", 1),
    ("s", 1_000),
    ("m", 60 * 1_000),
    ("h", 60 * 60 * 1_000),
    ("d", 24 * 60 * 60 * 1_000),
];

/// Units are required; use [`parse_millis`] or [`parse_seconds`] to also accept bare numbers.
pub fn parse(value: &str) -> Result<Duration, Error> {
    let input = value.trim();
    if input.is_empty() {
        return Err(Error::Invalid(input.into()));
    }

    let mut rest = input;
    let mut millis = 0_u64;

    while !rest.is_empty() {
        let (number, tail) = rest.split_at(
            rest.find(|c: char| !c.is_ascii_digit())
                .unwrap_or(rest.len()),
        );
        let tail = tail.trim_start();
        let (unit, tail) = tail.split_at(
            tail.find(|c: char| !c.is_ascii_alphabetic())
                .unwrap_or(tail.len()),
        );

        let number = number
            .trim()
            .parse::<u64>()
            .map_err(|_| Error::Invalid(input.into()))?;
        let (_, multiplier) = UNITS
            .iter()
            .find(|(name, _)| *name == unit)
            .ok_or_else(|| Error::Invalid(input.into()))?;

        millis = number
            .checked_mul(*multiplier)
            .and_then(|value| millis.checked_add(value))
            .ok_or_else(|| Error::Overflow(input.into()))?;
        rest = tail.trim_start();
    }

    Ok(Duration::from_millis(millis))
}

pub fn parse_millis(value: &str) -> Result<u64, Error> {
    if let Ok(millis) = value.trim().parse::<u64>() {
        return Ok(millis);
    }

    Ok(parse(value)?.as_millis() as u64)
}

pub fn parse_seconds(value: &str) -> Result<u64, Error> {
    if let Ok(seconds) = value.trim().parse::<u64>() {
        return Ok(seconds);
    }

    let duration = parse(value)?;
    if duration.subsec_millis() != 0 {
        return Err(Error::NotWholeSeconds(value.trim().into()));
    }

    Ok(duration.as_secs())
}

/// Time value: a number, in the unit documented on the field, or a duration
/// string built from `ms`, `s`, `m`, `h` and `d` components, e.g. `"1h5m15s"`.
#[derive(Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum TimeValue {
    /// Number of milliseconds, or seconds for fields documented in seconds.
    Number(#[schemars(with = "u64")] i64),
    /// Duration string, e.g. `"250ms"`, `"5s"`, `"1h5m15s"`.
    Text(String),
}

impl TimeValue {
    fn resolve<E: de::Error>(self, parse: fn(&str) -> Result<u64, Error>) -> Result<u64, E> {
        match self {
            TimeValue::Number(number) => u64::try_from(number)
                .map_err(|_| E::custom(format!("duration cannot be negative: {}", number))),
            TimeValue::Text(text) => parse(&text).map_err(E::custom),
        }
    }
}

pub fn millis<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
    TimeValue::deserialize(deserializer)?.resolve(parse_millis)
}

pub fn millis_optional<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<u64>, D::Error> {
    Option::<TimeValue>::deserialize(deserializer)?
        .map(|value| value.resolve(parse_millis))
        .transpose()
}

pub fn seconds<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
    TimeValue::deserialize(deserializer)?.resolve(parse_seconds)
}

pub fn seconds_optional<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<u64>, D::Error> {
    Option::<TimeValue>::deserialize(deserializer)?
        .map(|value| value.resolve(parse_seconds))
        .transpose()
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::util::human_duration;

    #[test]
    fn test_parse_single_component() {
        assert_eq!(parse("250ms"), Ok(Duration::from_millis(250)));
        assert_eq!(parse("5s"), Ok(Duration::from_secs(5)));
        assert_eq!(parse("15m"), Ok(Duration::from_secs(900)));
        assert_eq!(parse("2h"), Ok(Duration::from_secs(7200)));
        assert_eq!(parse("3d"), Ok(Duration::from_secs(259_200)));
        assert_eq!(parse("0s"), Ok(Duration::ZERO));
    }

    #[test]
    fn test_parse_compound() {
        assert_eq!(parse("1h5m15s"), Ok(Duration::from_secs(3915)));
        assert_eq!(parse("1h 5m 15s"), Ok(Duration::from_secs(3915)));
        assert_eq!(
            parse(" 1d12h30m500ms "),
            Ok(Duration::from_millis(131_400_500))
        );
        assert_eq!(parse("5s5s"), Ok(Duration::from_secs(10)));
        assert_eq!(parse("15s1h"), Ok(Duration::from_secs(3615)));
    }

    #[test]
    fn test_parse_invalid() {
        for value in [
            "", "  ", "5", "s", "5x", "1.5s", "5s!", "-5s", "5 s x", "ms",
        ] {
            assert!(matches!(parse(value), Err(Error::Invalid(_))), "{}", value);
        }

        assert!(matches!(
            parse("99999999999999999999s"),
            Err(Error::Invalid(_))
        ));
        assert!(matches!(
            parse("9999999999999999d"),
            Err(Error::Overflow(_))
        ));
    }

    #[test]
    fn test_parse_millis() {
        assert_eq!(parse_millis("5000"), Ok(5000));
        assert_eq!(parse_millis("5s"), Ok(5000));
        assert_eq!(parse_millis("0"), Ok(0));
        assert!(parse_millis("5 000").is_err());
    }

    #[test]
    fn test_parse_seconds() {
        assert_eq!(parse_seconds("60"), Ok(60));
        assert_eq!(parse_seconds("1m"), Ok(60));
        assert_eq!(parse_seconds("1h5m15s"), Ok(3915));
        assert_eq!(
            parse_seconds("500ms"),
            Err(Error::NotWholeSeconds("500ms".into()))
        );
    }

    #[test]
    fn test_optional_null_and_missing() {
        #[derive(Deserialize)]
        struct Settings {
            #[serde(default, deserialize_with = "millis_optional")]
            timeout: Option<u64>,
        }

        let missing: Settings = serde_json::from_str("{}").unwrap();
        assert_eq!(missing.timeout, None);

        let null: Settings = serde_json::from_str(r#"{"timeout": null}"#).unwrap();
        assert_eq!(null.timeout, None);

        let set: Settings = serde_json::from_str(r#"{"timeout": "5s"}"#).unwrap();
        assert_eq!(set.timeout, Some(5_000));
    }

    #[test]
    fn test_deserialize_errors() {
        #[derive(Deserialize, Debug)]
        struct Settings {
            #[serde(deserialize_with = "millis")]
            timeout: u64,
        }

        let parsed: Settings = toml::from_str(r#"timeout = "5s""#).unwrap();
        assert_eq!(parsed.timeout, 5_000);

        let err = toml::from_str::<Settings>(r#"timeout = -1"#)
            .unwrap_err()
            .to_string();
        assert!(err.contains("duration cannot be negative: -1"), "{}", err);

        let err = toml::from_str::<Settings>(r#"timeout = "5 minutes""#)
            .unwrap_err()
            .to_string();
        assert!(err.contains("is not a valid duration"), "{}", err);
    }

    #[test]
    fn test_human_duration_round_trip() {
        for millis in [
            0, 1, 999, 1_000, 1_500, 60_000, 300_000, 3_600_000, 86_400_000,
        ] {
            let duration = Duration::from_millis(millis);
            assert_eq!(parse(&human_duration(duration)), Ok(duration));
        }
    }
}
