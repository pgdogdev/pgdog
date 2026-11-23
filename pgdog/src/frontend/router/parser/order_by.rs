//! Sorting columns extracted from the query.

use std::fmt::Debug;

use crate::net::messages::Vector;

/// Placement of NULLs requested by `NULLS FIRST` / `NULLS LAST`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum Nulls {
    /// Not specified: Postgres sorts NULLs last for `ASC`, first for `DESC`.
    #[default]
    Default,
    First,
    Last,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum OrderBy {
    Asc(usize, Nulls),
    Desc(usize, Nulls),
    AscColumn(String, Nulls),
    DescColumn(String, Nulls),
    AscVectorL2Column(String, Vector),
    AscVectorL2(usize, Vector),
}

impl OrderBy {
    pub(crate) fn helper_column_name(sort_key_position: usize) -> String {
        format!("__pgdog_order_col{sort_key_position}")
    }

    /// ORDER BY x ASC
    pub(crate) fn asc(&self) -> bool {
        matches!(
            self,
            OrderBy::Asc(..)
                | OrderBy::AscColumn(..)
                | OrderBy::AscVectorL2Column(_, _)
                | OrderBy::AscVectorL2(_, _)
        )
    }

    /// NULL placement as written in the query.
    pub(crate) fn nulls(&self) -> Nulls {
        match self {
            OrderBy::Asc(_, nulls)
            | OrderBy::Desc(_, nulls)
            | OrderBy::AscColumn(_, nulls)
            | OrderBy::DescColumn(_, nulls) => *nulls,
            OrderBy::AscVectorL2Column(_, _) | OrderBy::AscVectorL2(_, _) => Nulls::Default,
        }
    }

    /// NULLs sort before non-NULL values.
    pub(crate) fn nulls_first(&self) -> bool {
        match self.nulls() {
            Nulls::First => true,
            Nulls::Last => false,
            Nulls::Default => !self.asc(),
        }
    }

    /// Column index.
    pub(crate) fn index(&self) -> Option<usize> {
        match self {
            OrderBy::Asc(column, _) => Some(*column - 1),
            OrderBy::Desc(column, _) => Some(*column - 1),
            OrderBy::AscVectorL2(column, _) => Some(*column - 1),
            _ => None,
        }
    }

    /// ORDER BY clause contains a vector.
    pub(crate) fn vector(&self) -> Option<(&Vector, &String)> {
        match self {
            OrderBy::AscVectorL2Column(name, vector) => Some((vector, name)),
            _ => None,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_nulls_first_defaults_match_postgres() {
        assert!(!OrderBy::Asc(1, Nulls::Default).nulls_first());
        assert!(OrderBy::Desc(1, Nulls::Default).nulls_first());
        assert!(!OrderBy::AscColumn("a".into(), Nulls::Default).nulls_first());
        assert!(OrderBy::DescColumn("a".into(), Nulls::Default).nulls_first());
    }

    #[test]
    fn test_nulls_first_explicit_overrides_direction() {
        assert!(OrderBy::Asc(1, Nulls::First).nulls_first());
        assert!(!OrderBy::Desc(1, Nulls::Last).nulls_first());
        assert!(OrderBy::AscColumn("a".into(), Nulls::First).nulls_first());
        assert!(!OrderBy::DescColumn("a".into(), Nulls::Last).nulls_first());
    }
}
