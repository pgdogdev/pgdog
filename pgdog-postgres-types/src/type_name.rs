//! PostgreSQL type name.

use std::fmt;
use std::sync::Arc;

/// Name of a PostgreSQL data type.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TypeName(pub Arc<Vec<String>>);

impl From<Vec<String>> for TypeName {
    fn from(names: Vec<String>) -> Self {
        Self(Arc::new(names))
    }
}

impl FromIterator<String> for TypeName {
    fn from_iter<T: IntoIterator<Item = String>>(iter: T) -> Self {
        Self(Arc::new(iter.into_iter().collect()))
    }
}

impl fmt::Display for TypeName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0.join("."))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conversions() {
        let data_type = TypeName::from(vec![String::from("pg_catalog"), String::from("int8")]);

        assert_eq!(*data_type.0, ["pg_catalog", "int8"]);
        assert_eq!(data_type.to_string(), "pg_catalog.int8");
    }
}
