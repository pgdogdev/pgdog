use std::collections::HashSet;

use once_cell::sync::Lazy;
use pg_internals::guc::GUC_NAMES;

use crate::{
    net::ErrorResponse,
    parameters::{ParameterName, ParameterNameRef},
};

static KNOWN_NAMES: Lazy<HashSet<ParameterName>> = Lazy::new(|| {
    GUC_NAMES
        .iter()
        .copied()
        // Old settings no longer in use.
        .chain(["sort_mem", "vacuum_mem", "ssl_ecdh_curve"])
        .map(ParameterName::new)
        .collect()
});

pub(super) fn validate(name: &str) -> Option<ErrorResponse> {
    if KNOWN_NAMES.contains(ParameterNameRef::new(name)) {
        return None;
    }
    if !name.contains('.') {
        return Some(ErrorResponse {
            code: "42704".into(),
            message: format!("unrecognized configuration parameter \"{name}\""),
            ..Default::default()
        });
    }
    // Match PostgreSQL's valid_custom_variable_name: each component starts
    // with a letter, underscore, or high-bit byte; digits and $ may follow.
    let valid = name.split('.').all(|component| {
        !component.is_empty()
            && component.bytes().enumerate().all(|(index, byte)| {
                byte.is_ascii_alphabetic()
                    || byte == b'_'
                    || !byte.is_ascii()
                    || (index > 0 && (byte.is_ascii_digit() || byte == b'$'))
            })
    });
    if valid {
        None
    } else {
        Some(ErrorResponse {
            code: "42602".into(),
            message: format!("invalid configuration parameter name \"{name}\""),
            detail: Some(
                "Custom parameter names must be two or more simple identifiers separated by dots."
                    .into(),
            ),
            ..Default::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_and_custom_names_remain_valid() {
        for name in [
            "DateStyle",
            "DATESTYLE",
            "WORK_MEM",
            "min_dynamic_shared_memory",
            "sort_mem",
            "vacuum_mem",
            "ssl_ecdh_curve",
            "app.setting",
            "app.nested.setting",
            "pgdog.shard",
            "pgdog.sharding_key",
            "pgdog.pin",
            "_app.setting2$",
            "app.é",
        ] {
            assert!(validate(name).is_none(), "{name}");
        }
    }

    #[test]
    fn unknown_single_part_names_are_rejected() {
        for name in ["no_such_variable", "", "some setting"] {
            let error = validate(name).expect("unknown name");
            assert_eq!(error.code, "42704");
            assert_eq!(
                error.message,
                format!("unrecognized configuration parameter \"{name}\"")
            );
            assert!(error.detail.is_none());
        }
    }

    #[test]
    fn malformed_custom_names_are_rejected() {
        for name in [
            ".setting",
            "app.",
            "app..setting",
            "1app.setting",
            "app.1setting",
            "app.bad-name",
            "app.$name",
        ] {
            let error = validate(name).expect("invalid custom name");
            assert_eq!(error.code, "42602");
            assert_eq!(
                error.message,
                format!("invalid configuration parameter name \"{name}\"")
            );
            assert_eq!(
                error.detail.as_deref(),
                Some(
                    "Custom parameter names must be two or more simple identifiers separated by dots."
                )
            );
        }
    }
}
