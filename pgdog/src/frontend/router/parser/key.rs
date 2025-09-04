//! Sharding key in a query.

#[derive(Debug, PartialEq)]
pub(crate) enum Key {
    /// Parameter, like $1, $2, referring to a value
    /// sent in a separate Bind message.
    Parameter { pos: usize, array: bool },
    /// A constant value, e.g. "1", "2", or "'value'"
    /// which can be parsed from the query text.
    Constant { value: String, array: bool },
    /// Null check on a column.
    Null,
}
