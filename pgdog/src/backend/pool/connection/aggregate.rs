//! Aggregate buffer.

use std::collections::{HashMap, VecDeque};

use crate::{
    frontend::router::parser::{Aggregate, AggregateFunction, AggregateTarget},
    net::messages::{DataRow, Datum, RowDescription},
};

use super::Error;

/// GROUP BY <columns>
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct Grouping {
    columns: Vec<(usize, Datum)>,
}

impl Grouping {
    fn new(row: &DataRow, group_by: &[usize], rd: &RowDescription) -> Result<Self, Error> {
        let mut columns = vec![];
        for idx in group_by {
            let column = row.get_column(idx.clone(), rd)?;
            if let Some(column) = column {
                columns.push((idx.clone(), column.value));
            }
        }

        Ok(Self { columns })
    }
}

/// The aggregate accumulator.
#[derive(Debug)]
struct Accumulator<'a> {
    target: &'a AggregateTarget,
    datum: Datum,
}

impl<'a> Accumulator<'a> {
    pub fn from_aggregate(aggregate: &'a Aggregate) -> Vec<Self> {
        aggregate
            .targets()
            .iter()
            .map(|target| match target.function() {
                AggregateFunction::Count => Accumulator {
                    target,
                    datum: Datum::Bigint(0),
                },
                AggregateFunction::Max => Accumulator {
                    target,
                    datum: Datum::Bigint(i64::MIN),
                },
                AggregateFunction::Min => Accumulator {
                    target,
                    datum: Datum::Bigint(i64::MAX),
                },
                _ => Accumulator {
                    target,
                    datum: Datum::Null,
                },
            })
            .collect()
    }

    fn accumulate(&mut self, row: &DataRow, rd: &RowDescription) -> Result<(), Error> {
        let column = row.get_column(self.target.column(), rd)?;
        if let Some(column) = column {
            match self.target.function() {
                AggregateFunction::Count => self.datum = self.datum.clone() + column.value,
                AggregateFunction::Max => {
                    if self.datum < column.value {
                        self.datum = column.value;
                    }
                }
                AggregateFunction::Min => {
                    if self.datum > column.value {
                        self.datum = column.value;
                    }
                }
                _ => (),
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
pub(super) struct Aggregates<'a> {
    rows: &'a VecDeque<DataRow>,
    mappings: HashMap<Grouping, Vec<Accumulator<'a>>>,
    rd: &'a RowDescription,
    aggregate: &'a Aggregate,
}

impl<'a> Aggregates<'a> {
    pub(super) fn new(
        rows: &'a VecDeque<DataRow>,
        rd: &'a RowDescription,
        aggregate: &'a Aggregate,
    ) -> Self {
        Self {
            rows,
            rd,
            mappings: HashMap::new(),
            aggregate,
        }
    }

    pub(super) fn aggregate(mut self) -> Result<VecDeque<DataRow>, Error> {
        for row in self.rows {
            let grouping = Grouping::new(&row, &self.aggregate.group_by(), &self.rd)?;
            let entry = self
                .mappings
                .entry(grouping)
                .or_insert_with(|| Accumulator::from_aggregate(&self.aggregate));

            for aggregate in entry {
                aggregate.accumulate(&row, &self.rd)?;
            }
        }

        let mut rows = VecDeque::new();
        for (grouping, accumulator) in self.mappings {
            let mut row = DataRow::new();
            for (idx, datum) in grouping.columns {
                row.insert(idx, datum);
            }
            for acc in accumulator {
                row.insert(acc.target.column(), acc.datum);
            }
            rows.push_back(row);
        }

        Ok(rows)
    }
}
