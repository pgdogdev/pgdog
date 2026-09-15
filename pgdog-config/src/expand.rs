//! Environment variable and file expansion in configuration files.

use std::borrow::Cow;
use std::env::var;
use std::fs::read_to_string;

use serde::de::DeserializeOwned;

use crate::Error;

/// Start of a variable reference.
const OPEN: &str = "${";

/// Prefix of an environment variable reference.
const ENV: &str = "env.";

/// Prefix of a file reference.
const FILE: &str = "file.";

/// A parsed `${env.NAME}` or `${file.PATH}` reference.
enum Reference<'a> {
    Env(&'a str),
    File(&'a str),
}

/// Expand `${env.VAR}` and `${file.PATH}` references in a configuration file.
///
/// `${env.VAR}` interpolates the environment variable `VAR` from the process
/// environment. `${file.PATH}` interpolates the contents of the file at
/// `PATH`, with trailing newlines trimmed; a relative path resolves against
/// the current working directory.
///
/// Only the braced, prefixed form is a reference: a bare `$VAR`, an unprefixed
/// `${VAR}`, a `${` that's malformed or unterminated, and an `env.` reference
/// to a variable that isn't set are all literal text, so values that merely
/// contain a `$` (passwords, most commonly) survive untouched. Write
/// `$${env.VAR}` for a literal `${env.VAR}`, and `${env.VAR:-value}` or
/// `${file.PATH:-value}` to supply a fallback.
///
/// **Note:** expansion happens on the document source, before it's parsed, so a
/// reference is interpolated as TOML rather than as a string. `${env.PASSWORD}`
/// in value position needs surrounding quotes, and a value containing `"` or a
/// newline changes how the rest of the document parses.
///
/// # Errors
///
/// Returns [`Error::FileReference`] if a `file.` reference without a fallback
/// names a file that can't be read.
pub fn expand(source: &str) -> Result<Cow<'_, str>, Error> {
    if !source.contains(OPEN) {
        return Ok(Cow::Borrowed(source));
    }

    let mut expanded = String::with_capacity(source.len());
    let mut rest = source;

    while let Some(start) = rest.find(OPEN) {
        let body = &rest[start + OPEN.len()..];

        // A reference is `${`, `env.` plus a valid name or `file.` plus a
        // plausible path, an optional `:-fallback`, and `}`. Anything else is
        // literal text: emit through the `${` and rescan right after it, so a
        // stray `${` in one value can't swallow a real reference later in the
        // document.
        let reference = body.find('}').and_then(|end| {
            let (target, fallback) = match body[..end].split_once(":-") {
                Some((target, fallback)) => (target, Some(fallback)),
                None => (&body[..end], None),
            };
            let reference = if let Some(name) = target.strip_prefix(ENV) {
                is_name(name).then_some(Reference::Env(name))
            } else if let Some(path) = target.strip_prefix(FILE) {
                is_path(path).then_some(Reference::File(path))
            } else {
                None
            }?;
            Some((reference, fallback, end))
        });
        let Some((reference, fallback, end)) = reference else {
            expanded.push_str(&rest[..start + OPEN.len()]);
            rest = body;
            continue;
        };

        let stop = start + OPEN.len() + end + 1;
        if rest[..start].ends_with('$') {
            // `$${env.VAR}` escapes the reference: drop the `$` and keep the
            // reference as written, whether or not it resolves.
            expanded.push_str(&rest[..start - 1]);
            expanded.push_str(&rest[start..stop]);
        } else {
            expanded.push_str(&rest[..start]);
            match reference {
                Reference::Env(name) => match var(name).ok().as_deref().or(fallback) {
                    Some(value) => expanded.push_str(value),
                    // Unset with no fallback: the reference stays as written.
                    None => expanded.push_str(&rest[start..stop]),
                },
                Reference::File(path) => match read_to_string(path) {
                    // Mounted secrets conventionally end with a newline that
                    // would corrupt the surrounding TOML.
                    Ok(contents) => expanded.push_str(contents.trim_end_matches(['\r', '\n'])),
                    Err(err) => match fallback {
                        Some(value) => expanded.push_str(value),
                        None => return Err(Error::FileReference(path.into(), err)),
                    },
                },
            }
        }
        rest = &rest[stop..];
    }

    expanded.push_str(rest);
    Ok(Cow::Owned(expanded))
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

/// Is this a plausible file path, i.e. non-empty with no whitespace or
/// reference syntax? Anything else is literal text, not a reference.
fn is_path(path: &str) -> bool {
    !path.is_empty()
        && path
            .chars()
            .all(|c| !c.is_whitespace() && c != '$' && c != '{')
}

/// Parse a TOML configuration document, expanding variable references first.
pub trait FromToml: DeserializeOwned {
    /// Parse `source` as TOML, [`expand`]ing variable references first.
    ///
    /// # Errors
    ///
    /// Returns [`Error::FileReference`] if a `file.` reference can't be read,
    /// or [`Error::MissingField`] if the expanded document isn't valid TOML or
    /// doesn't match the shape of `Self`.
    fn from_toml(source: &str) -> Result<Self, Error> {
        let expanded = expand(source)?;
        toml::from_str(&expanded).map_err(|err| Error::config(&expanded, err))
    }
}

impl<T: DeserializeOwned> FromToml for T {}

#[cfg(test)]
mod test {
    use std::io::Write;

    use tempfile::NamedTempFile;

    use super::*;
    use crate::test_utils::{remove_env_var, set_env_var};
    use crate::{Config, Users};

    fn expanded(source: &str) -> String {
        expand(source).unwrap().into_owned()
    }

    fn secret_file(contents: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(contents.as_bytes()).unwrap();
        file
    }

    #[test]
    fn test_expand_env() {
        let _set = set_env_var("PGDOG_TEST_VAR", "expanded");
        let _unset = remove_env_var("PGDOG_TEST_MISSING");

        assert_eq!(expanded("${env.PGDOG_TEST_VAR}"), "expanded");
        assert_eq!(expanded("${env.PGDOG_TEST_VAR}/db"), "expanded/db");
        assert_eq!(
            expanded("a${env.PGDOG_TEST_VAR}b${env.PGDOG_TEST_VAR}"),
            "aexpandedbexpanded"
        );
        assert_eq!(
            expanded("${env.PGDOG_TEST_MISSING}"),
            "${env.PGDOG_TEST_MISSING}"
        );
        assert_eq!(expanded("${env.PGDOG_TEST_MISSING:-fallback}"), "fallback");
        assert_eq!(expanded("${env.PGDOG_TEST_VAR:-fallback}"), "expanded");
    }

    #[test]
    fn test_expand_file() {
        let file = secret_file("not a real secret\n");
        let path = file.path().display();

        assert_eq!(expanded(&format!("${{file.{path}}}")), "not a real secret");
        assert_eq!(
            expanded(&format!("a${{file.{path}}}b")),
            "anot a real secretb"
        );
        // A fallback covers a file that can't be read, set or not.
        assert_eq!(
            expanded(&format!("${{file.{path}:-fallback}}")),
            "not a real secret"
        );
        assert_eq!(
            expanded("${file./pgdog/no/such/file:-fallback}"),
            "fallback"
        );
    }

    #[test]
    fn test_expand_file_trims_trailing_newlines() {
        let trailing = secret_file("secret\r\n\n");
        assert_eq!(
            expanded(&format!("${{file.{}}}", trailing.path().display())),
            "secret"
        );

        // Only trailing newlines are trimmed, not interior ones or spaces.
        let interior = secret_file("a\nb ");
        assert_eq!(
            expanded(&format!("${{file.{}}}", interior.path().display())),
            "a\nb "
        );
    }

    #[test]
    fn test_expand_file_missing_is_error() {
        let err = expand("${file./pgdog/no/such/file}").unwrap_err();
        assert!(matches!(err, Error::FileReference(..)), "{err:?}");
    }

    #[test]
    fn test_expand_leaves_unbraced_alone() {
        let _set = set_env_var("PGDOG_TEST_VAR", "expanded");

        assert_eq!(expanded("$PGDOG_TEST_VAR/db"), "$PGDOG_TEST_VAR/db");
        assert_eq!(expanded("sup$rsecret"), "sup$rsecret");
        assert_eq!(expanded("p$$w0rd"), "p$$w0rd");
    }

    #[test]
    fn test_expand_leaves_malformed_alone() {
        let _set = set_env_var("PGDOG_TEST_VAR", "expanded");

        // An unprefixed reference is literal text.
        assert_eq!(expanded("${PGDOG_TEST_VAR}"), "${PGDOG_TEST_VAR}");
        assert_eq!(expanded("${env.PGDOG_TEST_VAR"), "${env.PGDOG_TEST_VAR");
        assert_eq!(expanded("${env.PGDOG TEST VAR}"), "${env.PGDOG TEST VAR}");
        assert_eq!(expanded("${env.}"), "${env.}");
        assert_eq!(expanded("${env.1VAR}"), "${env.1VAR}");
        assert_eq!(expanded("${file.}"), "${file.}");
        assert_eq!(expanded("${file.a b}"), "${file.a b}");
        assert_eq!(expanded("${file.a${b}"), "${file.a${b}");
    }

    #[test]
    fn test_expand_escape() {
        let _set = set_env_var("PGDOG_TEST_VAR", "expanded");
        let _unset = remove_env_var("PGDOG_TEST_MISSING");

        assert_eq!(expanded("$${env.PGDOG_TEST_VAR}"), "${env.PGDOG_TEST_VAR}");
        // The escape doesn't depend on the reference resolving.
        assert_eq!(
            expanded("$${env.PGDOG_TEST_MISSING}"),
            "${env.PGDOG_TEST_MISSING}"
        );
        assert_eq!(
            expanded("$${file./pgdog/no/such/file}"),
            "${file./pgdog/no/such/file}"
        );
        // Only a well-formed reference needs escaping; a `$` before anything
        // else is literal.
        assert_eq!(expanded("a$${env.b"), "a$${env.b");
        assert_eq!(expanded("p$${env.a b}q"), "p$${env.a b}q");
    }

    #[test]
    fn test_expand_scans_past_stray_reference() {
        let _set = set_env_var("PGDOG_TEST_VAR", "expanded");

        // A stray `${` in one value must not swallow a real reference later
        // in the document.
        assert_eq!(
            expanded("password = \"ab${cd\"\nhost = \"${env.PGDOG_TEST_VAR}\""),
            "password = \"ab${cd\"\nhost = \"expanded\""
        );
    }

    #[test]
    fn test_from_toml_expands() {
        let _password = set_env_var("PGDOG_TEST_PASSWORD", "not a real secret");
        let timeout = secret_file("1_000\n");

        let source = format!(
            r#"
[admin]
password = "${{env.PGDOG_TEST_PASSWORD}}"

[general]
shutdown_timeout = ${{file.{}}}
"#,
            timeout.path().display()
        );

        let config = Config::from_toml(&source).unwrap();
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
password = "${env.PGDOG_TEST_MISSING}"
"#;

        let users = Users::from_toml(source).unwrap();
        assert_eq!(
            users.users[0].password.as_deref(),
            Some("${env.PGDOG_TEST_MISSING}")
        );
    }

    #[test]
    fn test_from_toml_reports_missing_file() {
        let err =
            Users::from_toml("[[users]]\nname = \"${file./pgdog/no/such/file}\"\n").unwrap_err();
        assert!(matches!(err, Error::FileReference(..)), "{err:?}");
    }

    #[test]
    fn test_from_toml_reports_errors() {
        let err = Config::from_toml("[general]\nnot_a_field = 1\n").unwrap_err();
        assert!(matches!(err, Error::MissingField(..)), "{err:?}");
    }
}
