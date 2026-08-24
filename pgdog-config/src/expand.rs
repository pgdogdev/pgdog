//! Environment variable expansion in configuration files.

use std::borrow::Cow;
use std::convert::Infallible;
use std::env::var;

use serde::de::DeserializeOwned;

use crate::Error;

/// Expand `$VAR` and `${VAR}` references in a configuration file against the
/// process environment.
///
/// References to variables that aren't set are left in the document verbatim, so
/// values that merely contain a `$` (passwords, most commonly) survive
/// untouched. Write `$$` for a literal `$`, and `${VAR:-value}` to supply a
/// fallback.
///
/// **Note:** expansion happens on the document source, before it's parsed, so a
/// variable is interpolated as TOML rather than as a string. `${PASSWORD}` in
/// value position needs surrounding quotes, and a value containing `"` or a
/// newline changes how the rest of the document parses.
pub fn expand(source: &str) -> Cow<'_, str> {
    shellexpand::env_with_context(source, |name| Ok::<_, Infallible>(var(name).ok()))
        .expect("lookup is infallible")
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
        assert_eq!(expand("$PGDOG_TEST_VAR/db"), "expanded/db");
        assert_eq!(expand("${PGDOG_TEST_MISSING}"), "${PGDOG_TEST_MISSING}");
        assert_eq!(expand("${PGDOG_TEST_MISSING:-fallback}"), "fallback");
        assert_eq!(expand("sup$rsecret"), "sup$rsecret");
        assert_eq!(expand("p$$w0rd"), "p$w0rd");
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
