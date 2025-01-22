#[derive(Clone, Debug)]
pub enum OrderBy {
    Asc(usize),
    Desc(usize),
    AscColumn(String),
    DescColumn(String),
}

impl OrderBy {
    /// ORDER BY x ASC
    pub fn asc(&self) -> bool {
        matches!(self, OrderBy::Asc(_) | OrderBy::AscColumn(_))
    }

    /// Column index.
    pub fn index(&self) -> Option<usize> {
        match self {
            OrderBy::Asc(column) => Some(*column - 1),
            OrderBy::Desc(column) => Some(*column - 1),
            _ => None,
        }
    }

    /// Get column name.
    pub fn name(&self) -> Option<&str> {
        match self {
            OrderBy::AscColumn(ref name) => Some(name.as_str()),
            OrderBy::DescColumn(ref name) => Some(name.as_str()),
            _ => None,
        }
    }
}
