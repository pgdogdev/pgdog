use crate::net::{row_description::Field, DataRow, RowDescription, ToDataRowColumn};
use std::collections::HashSet;

#[derive(Clone, Debug)]
pub(crate) struct NamedRow {
    filter: HashSet<String>,
    rd: RowDescription,
    data_row: DataRow,
}

impl NamedRow {
    pub(crate) fn new(fields: &[Field], filter: &HashSet<String>) -> Self {
        let fields = fields
            .iter()
            .filter(|f| filter.contains(&f.name) || filter.is_empty())
            .cloned()
            .collect::<Vec<_>>();
        Self {
            rd: RowDescription::new(&fields),
            filter: filter.clone(),
            data_row: DataRow::new(),
        }
    }

    pub(crate) fn data_row(&mut self) -> DataRow {
        let dr = self.data_row.clone();
        self.data_row = DataRow::new();

        dr
    }

    pub(crate) fn add(&mut self, name: &str, data: impl ToDataRowColumn) -> &mut Self {
        if self.filter.is_empty() || self.filter.contains(name) {
            self.data_row.add(data);
        }

        self
    }

    pub(crate) fn row_description(&self) -> &RowDescription {
        &self.rd
    }
}
