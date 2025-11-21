#[derive(Debug, PartialEq, Eq)]
pub enum Parameter<'a> {
    Text(&'a str),
    Binary(&'a [u8]),
}

impl<'a> Parameter<'a> {
    /// Decode this parameter as a UTF-8 string, if possible.
    /// Returns `Some(&str)` for text or valid UTF-8 binary, else `None`.
    pub fn as_str(&self) -> Option<&'a str> {
        match self {
            Parameter::Text(s) => Some(s),
            Parameter::Binary(b) => std::str::from_utf8(b).ok(),
        }
    }
}
