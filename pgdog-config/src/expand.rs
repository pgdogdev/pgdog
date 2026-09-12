//! Environment variable expansion in configuration files.

use std::borrow::Cow;
use std::env::var;

use serde::de::DeserializeOwned;

use crate::Error;

/// Start of a variable reference.
const OPEN: &str = "${";

/// Expand `${VAR}` references in a configuration file against the process
/// environment.
///
/// Only the braced form is a reference: a bare `$VAR`, a `${` that's malformed
/// or unterminated, and a reference to a variable that isn't set are all literal
/// text, so values that merely contain a `$` (passwords, most commonly) survive
/// untouched. Write `$${VAR}` for a literal `${VAR}`, and `${VAR:-value}` to
/// supply a fallback.
///
/// **Note:** expansion happens on the document source, before it's parsed, so a
/// variable is interpolated as TOML rather than as a string. `${PASSWORD}` in
/// value position needs surrounding quotes, and a value containing `"` or a
/// newline changes how the rest of the document parses.
pub fn expand(source: &str) -> Cow<'_, str> {
    if !source.contains(OPEN) {
        return Cow::Borrowed(source);
    }

    let mut expanded = String::with_capacity(source.len());
    let mut rest = source;

    while let Some(start) = rest.find(OPEN) {
        let body = &rest[start + OPEN.len()..];

        // A reference is `${`, a valid name, an optional `:-fallback`, and `}`.
        // Anything else is literal text: emit through the `${` and rescan right
        // after it, so a stray `${` in one value can't swallow a real reference
        // later in the document.
        let reference = body.find('}').and_then(|end| {
            let (name, fallback) = match body[..end].split_once(":-") {
                Some((name, fallback)) => (name, Some(fallback)),
                None => (&body[..end], None),
            };
            is_name(name).then_some((name, fallback, end))
        });
        let Some((name, fallback, end)) = reference else {
            expanded.push_str(&rest[..start + OPEN.len()]);
            rest = body;
            continue;
        };

        let stop = start + OPEN.len() + end + 1;
        if rest[..start].ends_with('$') {
            // `$${VAR}` escapes the reference: drop the `$` and keep the
            // reference as written, whether or not the variable is set.
            expanded.push_str(&rest[..start - 1]);
            expanded.push_str(&rest[start..stop]);
        } else {
            expanded.push_str(&rest[..start]);
            match var(name).ok().as_deref().or(fallback) {
                Some(value) => expanded.push_str(value),
                // Unset with no fallback: the reference stays as written.
                None => expanded.push_str(&rest[start..stop]),
            }
        }
        rest = &rest[stop..];
    }

    expanded.push_str(rest);
    Cow::Owned(expanded)
}

/// Is this a shell variable name, i.e. letters, digits and underscores, not
/// starting with a digit?
fn is_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Parse a TOML configuration document, expanding environment variables first.
pub trait FromToml: DeserializeOwned {
    /// Parse `source` as TOML, [`expand`]ing environment variables first.
    ///
    /// # Errors
    ///
    /// Returns [`Error::MissingField`] if the expanded document isn't valid TOML
    /// or doesn't match the shape of `Self`.
    fn from_toml(source: &str) -> Result<Self, Error> {
        let expanded = expand(source);
        toml::from_str(&expanded).map_err(|err| Error::config(&expanded, err))
    }
}

impl<T: DeserializeOwned> FromToml for T {}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::{remove_env_var, set_env_var};
    use crate::{Config, Users};

    #[test]
    fn test_expand() {
        let _set = set_env_var("PGDOG_TEST_VAR", "expanded");
        let _unset = remove_env_var("PGDOG_TEST_MISSING");

        assert_eq!(expand("${PGDOG_TEST_VAR}"), "expanded");
        assert_eq!(expand("${PGDOG_TEST_VAR}/db"), "expanded/db");
        assert_eq!(
            expand("a${PGDOG_TEST_VAR}b${PGDOG_TEST_VAR}"),
            "aexpandedbexpanded"
        );
        assert_eq!(expand("${PGDOG_TEST_MISSING}"), "${PGDOG_TEST_MISSING}");
        assert_eq!(expand("${PGDOG_TEST_MISSING:-fallback}"), "fallback");
        assert_eq!(expand("${PGDOG_TEST_VAR:-fallback}"), "expanded");
    }

    #[test]
    fn test_expand_leaves_unbraced_alone() {
        let _set = set_env_var("PGDOG_TEST_VAR", "expanded");

        assert_eq!(expand("$PGDOG_TEST_VAR/db"), "$PGDOG_TEST_VAR/db");
        assert_eq!(expand("sup$rsecret"), "sup$rsecret");
        assert_eq!(expand("p$$w0rd"), "p$$w0rd");
    }

    #[test]
    fn test_expand_leaves_malformed_alone() {
        let _set = set_env_var("PGDOG_TEST_VAR", "expanded");

        assert_eq!(expand("${PGDOG_TEST_VAR"), "${PGDOG_TEST_VAR");
        assert_eq!(expand("${PGDOG TEST VAR}"), "${PGDOG TEST VAR}");
        assert_eq!(expand("${}"), "${}");
        assert_eq!(expand("${1VAR}"), "${1VAR}");
    }

    #[test]
    fn test_expand_escape() {
        let _set = set_env_var("PGDOG_TEST_VAR", "expanded");
        let _unset = remove_env_var("PGDOG_TEST_MISSING");

        assert_eq!(expand("$${PGDOG_TEST_VAR}"), "${PGDOG_TEST_VAR}");
        // The escape doesn't depend on the variable being set.
        assert_eq!(expand("$${PGDOG_TEST_MISSING}"), "${PGDOG_TEST_MISSING}");
        // Only a well-formed reference needs escaping; a `$` before anything
        // else is literal.
        assert_eq!(expand("a$${b"), "a$${b");
        assert_eq!(expand("p$${a b}q"), "p$${a b}q");
    }

    #[test]
    fn test_expand_scans_past_stray_reference() {
        let _set = set_env_var("PGDOG_TEST_VAR", "expanded");

        // A stray `${` in one value must not swallow a real reference later
        // in the document.
        assert_eq!(
            expand("password = \"ab${cd\"\nhost = \"${PGDOG_TEST_VAR}\""),
            "password = \"ab${cd\"\nhost = \"expanded\""
        );
    }

    #[test]
    fn test_from_toml_expands() {
        let _password = set_env_var("PGDOG_TEST_PASSWORD", "not a real secret");
        let _timeout = set_env_var("PGDOG_TEST_SHUTDOWN_TIMEOUT", "1_000");

        let source = r#"
[admin]
password = "${PGDOG_TEST_PASSWORD}"

[general]
shutdown_timeout = ${PGDOG_TEST_SHUTDOWN_TIMEOUT}
"#;

        let config = Config::from_toml(source).unwrap();
        assert_eq!(config.admin.password, "not a real secret");
        assert_eq!(config.general.shutdown_timeout, 1_000);
    }

    #[test]
    fn test_from_toml_leaves_unset_alone() {
        let _unset = remove_env_var("PGDOG_TEST_MISSING");

        let source = r#"
[[users]]
name = "pgdog"
database = "pgdog"
password = "${PGDOG_TEST_MISSING}"
"#;

        let users = Users::from_toml(source).unwrap();
        assert_eq!(
            users.users[0].password.as_deref(),
            Some("${PGDOG_TEST_MISSING}")
        );
    }

    #[test]
    fn test_from_toml_reports_errors() {
        let err = Config::from_toml("[general]\nnot_a_field = 1\n").unwrap_err();
        assert!(matches!(err, Error::MissingField(..)), "{err:?}");
    }
}
