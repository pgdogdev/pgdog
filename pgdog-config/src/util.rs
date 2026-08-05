use std::time::Duration;

use rand::{RngExt, distr::Alphanumeric};

pub fn human_duration_optional(duration: Option<Duration>) -> String {
    if let Some(duration) = duration {
        human_duration(duration)
    } else {
        "default".into()
    }
}

/// Get a human-readable duration for amounts that
/// a human would use.
pub fn human_duration(duration: Duration) -> String {
    let second = 1000;
    let minute = second * 60;
    let hour = minute * 60;
    let day = hour * 24;
    let week = day * 7;
    // Ok that's enough.

    let ms = duration.as_millis();
    let ms_fmt = |ms: u128, unit: u128, name: &str| -> String {
        if !ms.is_multiple_of(unit) {
            format!("{}ms", ms)
        } else {
            format!("{}{}", ms / unit, name)
        }
    };

    if ms < second {
        format!("{}ms", ms)
    } else if ms < minute {
        ms_fmt(ms, second, "s")
    } else if ms < hour {
        ms_fmt(ms, minute, "m")
    } else if ms < day {
        ms_fmt(ms, hour, "h")
    } else if ms < week {
        ms_fmt(ms, day, "d")
    } else {
        ms_fmt(ms, 1, "ms")
    }
}

/// Generate a random string of length n.
pub fn random_string(n: usize) -> String {
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(n)
        .map(char::from)
        .collect()
}

/// Fold a configured identifier the way PostgreSQL's parser folds
/// identifiers in SQL, so values from `pgdog.toml` compare equal to
/// the ones the query parser produces.
///
/// - Unquoted identifiers are lower-cased: `Orders` -> `orders`
/// - Quoted identifiers keep their case, lose the surrounding quotes,
///   and un-escape doubled quotes: `"Or""ders"` -> `Or"ders`
///
/// Folding is ASCII-only, matching PostgreSQL's behaviour for
/// multi-byte encodings such as UTF-8.
pub fn normalize_identifier(identifier: &str) -> String {
    match identifier
        .strip_prefix('"')
        .and_then(|inner| inner.strip_suffix('"'))
    {
        Some(inner) => inner.replace("\"\"", "\""),
        None => identifier.to_ascii_lowercase(),
    }
}

/// Swap field values using tmp pattern: source -> tmp, dest -> source, tmp -> dest.
#[macro_export]
macro_rules! swap_field {
    ($iter:expr_2021, $field:ident, $source:expr_2021, $destination:expr_2021, $tmp:expr_2021) => {
        $iter.for_each(|item| {
            if item.$field == $source {
                item.$field = $tmp.clone();
            }
        });
        $iter.for_each(|item| {
            if item.$field == $destination {
                item.$field = $source.to_owned();
            }
        });
        $iter.for_each(|item| {
            if item.$field == $tmp {
                item.$field = $destination.to_owned();
            }
        });
    };
}

#[cfg(test)]
mod test_normalize_identifier {
    use super::normalize_identifier;

    #[test]
    fn test_unquoted_is_lowercased() {
        assert_eq!(normalize_identifier("Orders"), "orders");
        assert_eq!(normalize_identifier("ORDERS"), "orders");
        assert_eq!(normalize_identifier("orders"), "orders");
        assert_eq!(normalize_identifier("Tenant_Id"), "tenant_id");
    }

    #[test]
    fn test_quoted_preserves_case_and_drops_quotes() {
        assert_eq!(normalize_identifier(r#""Orders""#), "Orders");
        assert_eq!(normalize_identifier(r#""ORDERS""#), "ORDERS");
        assert_eq!(normalize_identifier(r#""orders""#), "orders");
    }

    #[test]
    fn test_quoted_unescapes_doubled_quotes() {
        assert_eq!(normalize_identifier(r#""Or""ders""#), r#"Or"ders"#);
        assert_eq!(
            normalize_identifier(r#""He said ""hi""""#),
            r#"He said "hi""#
        );
    }

    #[test]
    fn test_folding_is_ascii_only() {
        // PostgreSQL folds only ASCII in multi-byte encodings, so the
        // accented character is left alone while ECOLE is lowered.
        assert_eq!(normalize_identifier("ÉCOLE"), "École");
    }

    #[test]
    fn test_edge_cases() {
        assert_eq!(normalize_identifier(""), "");
        assert_eq!(normalize_identifier(r#""""#), "");
        // A lone quote is not a valid quoted identifier; treat it as unquoted.
        assert_eq!(normalize_identifier(r#""Orders"#), r#""orders"#);
    }
}
