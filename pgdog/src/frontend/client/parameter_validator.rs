//! Validate that the params the client attempts
//! to SET are valid without talking to Postgres.

use std::collections::HashSet;

use once_cell::sync::Lazy;
use pg_internals::guc::{GUC, Guc, Value};

use crate::{frontend::SetParam, net::ErrorResponse};

static TZ: Lazy<HashSet<&'static str>> =
    Lazy::new(|| chrono_tz::TZ_VARIANTS.iter().map(|tz| tz.name()).collect());

/// Ergonomic interface around the GUC.
pub(super) struct ParameterValidator<'a> {
    params: &'a [SetParam],
}

impl<'a> ParameterValidator<'a> {
    /// Create parameter validator for all parameters.
    pub(super) fn new(params: &'a [SetParam]) -> Self {
        Self { params }
    }

    /// Run paramete validation. If any of the parameters are invalid,
    /// an error is returned.
    pub(super) fn validate(&self) -> Option<ErrorResponse> {
        for param in self.params {
            let name = match param.name.as_str() {
                // Postgres is weird.
                "timezone" => "TimeZone",
                "intervalstyle" => "IntervalStyle",
                "datestyle" => "DateStyle",
                name => name,
            };

            let Some(value) = param.value.as_ref().and_then(|param| param.as_str()) else {
                continue;
            };

            let valid = if name == "TimeZone" {
                TZ.contains(value)
            } else {
                GUC.get(name)
                    .is_none_or(|guc| self.validate_value(guc, value))
            };

            if !valid {
                let mut error = ErrorResponse {
                    code: "22023".into(),
                    message: format!("invalid value for parameter \"{name}\": \"{value}\""),
                    ..Default::default()
                };
                if let Some(guc) = GUC.get(name).filter(|guc| !guc.options.is_empty()) {
                    error.hint = Some(format!("Available values: {}.", guc.options.join(", ")));
                    error.file = Some("guc.c".into());
                    // Source location from the PostgreSQL compatibility response.
                    error.line = Some("3278".into());
                    error.routine = Some("parse_and_validate_value".into());
                }
                return Some(error);
            }
        }

        None
    }

    fn validate_value(&self, guc: &Guc, value: &str) -> bool {
        if !guc.options.is_empty() {
            return guc
                .options
                .iter()
                .any(|option| option.eq_ignore_ascii_case(value));
        }

        match guc.parse_numeric(value) {
            Some(Value::Int(value)) => {
                guc.min
                    .is_none_or(|min| matches!(min, Value::Int(min) if value >= min))
                    && guc
                        .max
                        .is_none_or(|max| matches!(max, Value::Int(max) if value <= max))
            }
            Some(Value::Real(value)) => {
                value.is_finite()
                    && guc
                        .min
                        .is_none_or(|min| matches!(min, Value::Real(min) if value >= min))
                    && guc
                        .max
                        .is_none_or(|max| matches!(max, Value::Real(max) if value <= max))
            }
            // A failed numeric parse is invalid; other GUC types are unrestricted here.
            _ => !matches!(guc.boot_val, Value::Int(_) | Value::Real(_)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parameters::ParameterValue;

    fn validate(name: &str, value: &str) -> Option<ErrorResponse> {
        ParameterValidator::new(&[SetParam {
            name: name.into(),
            value: Some(value.into()),
            local: false,
        }])
        .validate()
    }

    #[test]
    fn timezone() {
        for name in ["timezone", "TimeZone"] {
            for value in ["UTC", "America/New_York", "Europe/Paris"] {
                assert!(validate(name, value).is_none());
            }
            let error = validate(name, "invalid/timezone").expect("invalid time zone");
            assert_eq!(error.severity, "ERROR");
            assert_eq!(error.code, "22023");
            assert_eq!(
                error.message,
                "invalid value for parameter \"TimeZone\": \"invalid/timezone\""
            );
        }
    }

    #[test]
    fn enum_options() {
        for name in ["intervalstyle", "IntervalStyle"] {
            for value in ["postgres", "iso_8601", "SQL_STANDARD"] {
                assert!(validate(name, value).is_none());
            }
            for value in ["", "invalid", "post"] {
                assert_eq!(
                    validate(name, value).expect("invalid enum option").code,
                    "22023"
                );
            }
        }
    }

    #[test]
    fn integer_bounds() {
        for value in [
            "-15", "0", "3", " +3 ", "1.5", "2.5", "3.49", "-15.4", "0x3", "03", "3e0",
        ] {
            assert!(validate("extra_float_digits", value).is_none(), "{value}");
        }
        for value in [
            "-16",
            "4",
            "3.5",
            "-15.5",
            "0x4",
            "04",
            "NaN",
            "invalid",
            "",
            "2147483648",
            "1\0",
            "3ms",
        ] {
            assert!(validate("extra_float_digits", value).is_some(), "{value}");
        }
    }

    #[test]
    fn integer_units_and_bounds() {
        for (name, value) in [
            ("statement_timeout", "5s"),
            ("statement_timeout", "12345ms"),
            ("statement_timeout", "1.5 min"),
            ("statement_timeout", " 5 ms \t"),
            ("statement_timeout", "24d"),
            ("statement_timeout", "2147483647ms"),
            ("work_mem", "16MB"),
            ("work_mem", "65536B"),
            ("work_mem", "63.5kB"),
            ("work_mem", "0.0625MB"),
        ] {
            assert!(validate(name, value).is_none(), "{name} = {value}");
        }
        for value in [
            "invalidms",
            "NaNms",
            "1e309ms",
            "5invalid",
            "5MB",
            "5MS",
            "-1ms",
            "25d",
            "2147483648ms",
            "5ms junk",
            "5\0ms",
        ] {
            assert!(validate("statement_timeout", value).is_some(), "{value}");
        }
        for value in ["63kB", "0.01MB", "2TB", "5s"] {
            assert!(validate("work_mem", value).is_some(), "{value}");
        }
    }

    #[test]
    fn real_units_and_bounds() {
        // vacuum_cost_delay is measured in milliseconds, with bounds 0..100.
        for value in ["0", "0.5ms", "0.1s", "100000us", "0.1004s"] {
            assert!(validate("vacuum_cost_delay", value).is_none(), "{value}");
        }
        for value in ["0.101s", "0.1006s", "-1ms", "1MB", "NaNms", "inf", "1\0ms"] {
            assert!(validate("vacuum_cost_delay", value).is_some(), "{value}");
        }
    }

    #[test]
    fn real_bounds() {
        for value in ["0", "0.5", "1", "1e-2"] {
            assert!(validate("geqo_selection_bias", value).is_some(), "{value}");
        }
        for value in ["1.5", "1.75", "2", " 2.0 "] {
            assert!(validate("geqo_selection_bias", value).is_none(), "{value}");
        }
        for value in ["2.1", "NaN", "inf", "-inf", "1e309", "invalid", ""] {
            assert!(validate("geqo_selection_bias", value).is_some(), "{value}");
        }
    }

    #[test]
    fn optional_bounds() {
        let validator = ParameterValidator::new(&[]);
        for (name, min, max, below, inside, above) in [
            (
                "extra_float_digits",
                Value::Int(1),
                Value::Int(2),
                "0",
                "1",
                "3",
            ),
            (
                "geqo_selection_bias",
                Value::Real(1.0),
                Value::Real(2.0),
                "0.5",
                "1.5",
                "2.5",
            ),
        ] {
            let mut guc = *GUC.get(name).expect("known numeric parameter");
            guc.min = Some(min);
            guc.max = None;
            assert!(!validator.validate_value(&guc, below));
            assert!(validator.validate_value(&guc, inside));
            assert!(validator.validate_value(&guc, above));

            guc.min = None;
            guc.max = Some(max);
            assert!(validator.validate_value(&guc, below));
            assert!(validator.validate_value(&guc, inside));
            assert!(!validator.validate_value(&guc, above));

            guc.max = None;
            assert!(validator.validate_value(&guc, below));
            assert!(validator.validate_value(&guc, above));
            assert!(!validator.validate_value(&guc, "invalid"));
        }
    }

    #[test]
    fn unconstrained_values() {
        assert!(validate("application_name", "any application").is_none());
        assert!(validate("custom.parameter", "any value").is_none());
        assert!(ParameterValidator::new(&[]).validate().is_none());

        let mut params = vec![
            SetParam {
                name: "timezone".into(),
                value: None,
                local: false,
            },
            SetParam {
                name: "search_path".into(),
                value: Some(ParameterValue::Tuple(vec!["public".into(), "app".into()])),
                local: false,
            },
        ];
        assert!(ParameterValidator::new(&params).validate().is_none());

        params.push(SetParam {
            name: "extra_float_digits".into(),
            value: Some("4".into()),
            local: true,
        });
        assert!(ParameterValidator::new(&params).validate().is_some());
    }
}
