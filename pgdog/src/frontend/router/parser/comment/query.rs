use std::borrow::Borrow;
use std::ops::Deref;

/// A query with any leading/trailing block comment stripped.
///
/// Borrows the original query when no stripping was needed, and owns the
/// trimmed copy otherwise.
#[derive(Debug, Clone)]
pub enum QueryWithoutComment<'a> {
    Original(&'a str),
    Stripped(String),
}

impl PartialEq for QueryWithoutComment<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}

impl PartialEq<&str> for QueryWithoutComment<'_> {
    fn eq(&self, other: &&str) -> bool {
        self.deref() == *other
    }
}

impl Borrow<str> for QueryWithoutComment<'_> {
    fn borrow(&self) -> &str {
        self.deref()
    }
}

impl Default for QueryWithoutComment<'_> {
    fn default() -> Self {
        QueryWithoutComment::Original("")
    }
}

impl<'a> Deref for QueryWithoutComment<'a> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Original(original) => original,
            Self::Stripped(stripped) => stripped.as_str(),
        }
    }
}
