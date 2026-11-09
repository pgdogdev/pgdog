//! Options of admin commands, in Postgres' `WITH (name 'value', flag)` style.

use super::error::Error;

/// What a resharding admin command does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunMode {
    /// Start a run. With `cleanup_on_cancel` false, a stop pauses it.
    Run { cleanup_on_cancel: bool },
    /// Abort the unfinished run with the same key.
    Abort,
}

impl RunMode {
    pub(crate) fn cleanup_on_cancel(self) -> bool {
        !matches!(
            self,
            Self::Run {
                cleanup_on_cancel: false
            }
        )
    }
}

/// Options after a command's positional arguments. Each command takes the
/// options it knows, then calls [`Options::finish`] to reject the rest.
#[derive(Debug, Default)]
pub(crate) struct Options<'a> {
    options: Vec<(&'a str, Option<&'a str>)>,
}

impl<'a> Options<'a> {
    /// Split a command into its words and the options in `(...)` or
    /// `WITH (...)` at the end. Quoted values can't contain commas.
    pub(crate) fn parse(sql: &'a str) -> Result<(Vec<&'a str>, Self), Error> {
        let Some((head, tail)) = sql.split_once('(') else {
            return Ok((sql.split_whitespace().collect(), Self::default()));
        };
        let body = tail
            .trim_end()
            .strip_suffix(')')
            .ok_or_else(|| Error::Syntax)?;

        let mut words = head.split_whitespace().collect::<Vec<_>>();
        if words.last() == Some(&"with") {
            words.pop();
        }

        let mut options = Self::default();
        if body.trim().is_empty() {
            return Ok((words, options));
        }
        for option in body.split(',') {
            let (name, value) = match option.trim().split_once(char::is_whitespace) {
                Some((name, value)) => (name, Some(unquote(value.trim())?)),
                None => (option.trim(), None),
            };
            if name.is_empty() || options.options.iter().any(|(n, _)| *n == name) {
                return Err(Error::InvalidOption(name.to_owned()));
            }
            options.options.push((name, value));
        }

        Ok((words, options))
    }

    /// Take a flag, e.g. `sync_only`. It can't have a value.
    pub(crate) fn flag(&mut self, name: &str) -> Result<bool, Error> {
        match self.take(name) {
            None => Ok(false),
            Some(None) => Ok(true),
            Some(Some(_)) => Err(Error::InvalidOption(name.to_owned())),
        }
    }

    /// Take an option with a value, e.g. `slot 'x'`.
    pub(crate) fn value(&mut self, name: &str) -> Result<Option<&'a str>, Error> {
        match self.take(name) {
            None => Ok(None),
            Some(Some(value)) => Ok(Some(value)),
            Some(None) => Err(Error::InvalidOption(name.to_owned())),
        }
    }

    pub(crate) fn run_mode(&mut self) -> Result<RunMode, Error> {
        let abort = self.flag("abort")?;
        let cleanup_on_cancel = self.take("cleanup_on_cancel");
        if abort && cleanup_on_cancel.is_some() {
            return Err(Error::InvalidOption("abort with cleanup_on_cancel".into()));
        }
        if abort {
            return Ok(RunMode::Abort);
        }
        match cleanup_on_cancel {
            None | Some(None) | Some(Some("true" | "on")) => Ok(RunMode::Run {
                cleanup_on_cancel: true,
            }),
            Some(Some("false" | "off")) => Ok(RunMode::Run {
                cleanup_on_cancel: false,
            }),
            Some(Some(_)) => Err(Error::InvalidOption("cleanup_on_cancel".into())),
        }
    }

    /// Reject options the command didn't take.
    pub(crate) fn finish(self) -> Result<(), Error> {
        match self.options.first() {
            Some((name, _)) => Err(Error::InvalidOption((*name).to_owned())),
            None => Ok(()),
        }
    }

    fn take(&mut self, name: &str) -> Option<Option<&'a str>> {
        let index = self.options.iter().position(|(n, _)| *n == name)?;
        Some(self.options.swap_remove(index).1)
    }
}

fn unquote(value: &str) -> Result<&str, Error> {
    match value.strip_prefix('\'') {
        Some(quoted) => quoted.strip_suffix('\'').ok_or_else(|| Error::Syntax),
        None if value.contains(char::is_whitespace) => Err(Error::Syntax),
        None => Ok(value),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parses_words_and_options() {
        for sql in [
            "copy_data a b (slot 'x', sync_only, size 10)",
            "copy_data a b with ( slot 'x' ,sync_only,  size 10 )",
        ] {
            let (words, mut options) = Options::parse(sql).unwrap();
            assert_eq!(words, ["copy_data", "a", "b"], "{sql}");
            assert_eq!(options.value("slot").unwrap(), Some("x"), "{sql}");
            assert_eq!(options.value("size").unwrap(), Some("10"), "{sql}");
            assert!(options.flag("sync_only").unwrap(), "{sql}");
            assert!(!options.flag("missing").unwrap(), "{sql}");
            options.finish().unwrap();
        }

        let (words, options) = Options::parse("copy_data a b").unwrap();
        assert_eq!(words, ["copy_data", "a", "b"]);
        options.finish().unwrap();
    }

    const CLEANUP: RunMode = RunMode::Run {
        cleanup_on_cancel: true,
    };
    const PAUSE: RunMode = RunMode::Run {
        cleanup_on_cancel: false,
    };

    #[test]
    fn parses_run_mode() {
        for (sql, mode) in [
            ("reshard a b", CLEANUP),
            ("reshard a b ()", CLEANUP),
            ("reshard a b (cleanup_on_cancel)", CLEANUP),
            ("reshard a b (cleanup_on_cancel true)", CLEANUP),
            ("reshard a b (cleanup_on_cancel 'true')", CLEANUP),
            ("reshard a b (cleanup_on_cancel on)", CLEANUP),
            ("reshard a b (cleanup_on_cancel false)", PAUSE),
            ("reshard a b with (cleanup_on_cancel off)", PAUSE),
            ("reshard a b (cleanup_on_cancel 'false')", PAUSE),
            ("reshard a b (abort)", RunMode::Abort),
        ] {
            let (_, mut options) = Options::parse(sql).unwrap();
            assert_eq!(options.run_mode().unwrap(), mode, "{sql}");
            options.finish().unwrap();
        }

        for (sql, name) in [
            ("reshard a b (cleanup_on_cancel maybe)", "cleanup_on_cancel"),
            ("reshard a b (cleanup_on_cancel 1)", "cleanup_on_cancel"),
            ("reshard a b (abort true)", "abort"),
            (
                "reshard a b (abort, cleanup_on_cancel)",
                "abort with cleanup_on_cancel",
            ),
            (
                "reshard a b (cleanup_on_cancel false, abort)",
                "abort with cleanup_on_cancel",
            ),
            (
                "reshard a b (abort, cleanup_on_cancel true)",
                "abort with cleanup_on_cancel",
            ),
        ] {
            let (_, mut options) = Options::parse(sql).unwrap();
            assert!(
                matches!(options.run_mode(), Err(Error::InvalidOption(n)) if n == name),
                "{sql}"
            );
        }

        let (_, mut options) =
            Options::parse("reshard a b (cleanup_on_cancel false, other)").unwrap();
        assert_eq!(options.run_mode().unwrap(), PAUSE);
        assert!(matches!(options.finish(), Err(Error::InvalidOption(n)) if n == "other"));
    }

    #[test]
    fn rejects_invalid_options() {
        for sql in [
            "copy_data a b (slot 'x'",
            "copy_data a b (slot 'x)",
            "copy_data a b (slot x y)",
        ] {
            assert!(matches!(Options::parse(sql), Err(Error::Syntax)), "{sql}");
        }

        for sql in [
            "copy_data a b (slot 'x', slot 'y')",
            "copy_data a b (a, , b)",
        ] {
            assert!(
                matches!(Options::parse(sql), Err(Error::InvalidOption(_))),
                "{sql}"
            );
        }

        let (_, mut options) = Options::parse("copy_data a b (slot, flag 'x', other)").unwrap();
        assert!(matches!(
            options.value("slot"),
            Err(Error::InvalidOption(_))
        ));
        assert!(matches!(options.flag("flag"), Err(Error::InvalidOption(_))));
        assert!(matches!(options.finish(), Err(Error::InvalidOption(name)) if name == "other"));
    }
}
