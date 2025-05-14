//! Sharding key in a query.

#[derive(Debug, PartialEq)]
pub enum Key {
    /// Parameter, like $1, $2, referring to a value
    /// sent in a separate Bind message.
    Parameter(usize),
    /// A constant value, e.g. "1", "2", or "'value'"
    /// which can be parsed from the query text.
    Constant(String),
    /// Null check on a column.
    Null,
}
