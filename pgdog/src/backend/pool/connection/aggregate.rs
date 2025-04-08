//! Aggregate buffer.

use std::collections::{HashMap, VecDeque};

use crate::{
    frontend::router::parser::{Aggregate, AggregateFunction, AggregateTarget},
    net::{
        messages::{DataRow, Datum},
        Decoder,
    },
};

use super::Error;

/// GROUP BY <columns>
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct Grouping {
    columns: Vec<(usize, Datum)>,
}

impl Grouping {
    fn new(row: &DataRow, group_by: &[usize], decoder: &Decoder) -> Result<Self, Error> {
        let mut columns = vec![];
        for idx in group_by {
            let column = row.get_column(*idx, decoder)?;
            if let Some(column) = column {
                columns.push((*idx, column.value));
            }
        }

        Ok(Self { columns })
    }
}

/// The aggregate accumulator.
///
/// This transforms distributed aggregate functions
/// into a single value.
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
                _ => Accumulator {
                    target,
                    datum: Datum::Null,
                },
            })
            .collect()
    }

    /// Transform COUNT(*), MIN, MAX, etc., from multiple shards into a single value.
    fn accumulate(&mut self, row: &DataRow, decoder: &Decoder) -> Result<(), Error> {
        let column = row
            .get_column(self.target.column(), decoder)?
            .ok_or(Error::DecoderRowError)?;
        match self.target.function() {
            AggregateFunction::Count => self.datum = self.datum.clone() + column.value,
            AggregateFunction::Max => {
                if !self.datum.is_null() {
                    if self.datum < column.value {
                        self.datum = column.value;
                    }
                } else {
                    self.datum = column.value;
                }
            }
            AggregateFunction::Min => {
                if !self.datum.is_null() {
                    if self.datum > column.value {
                        self.datum = column.value;
                    }
                } else {
                    self.datum = column.value;
                }
            }
            AggregateFunction::Sum => {
                if !self.datum.is_null() {
                    self.datum = self.datum.clone() + column.value;
                } else {
                    self.datum = column.value;
                }
            }
            _ => (),
        }

        Ok(())
    }
}

#[derive(Debug)]
pub(super) struct Aggregates<'a> {
    rows: &'a VecDeque<DataRow>,
    mappings: HashMap<Grouping, Vec<Accumulator<'a>>>,
    decoder: &'a Decoder,
    aggregate: &'a Aggregate,
}

impl<'a> Aggregates<'a> {
    pub(super) fn new(
        rows: &'a VecDeque<DataRow>,
        decoder: &'a Decoder,
        aggregate: &'a Aggregate,
    ) -> Self {
        Self {
            rows,
            decoder,
            mappings: HashMap::new(),
            aggregate,
        }
    }

    pub(super) fn aggregate(mut self) -> Result<VecDeque<DataRow>, Error> {
        for row in self.rows {
            let grouping = Grouping::new(row, self.aggregate.group_by(), self.decoder)?;
            let entry = self
                .mappings
                .entry(grouping)
                .or_insert_with(|| Accumulator::from_aggregate(self.aggregate));

            for aggregate in entry {
                aggregate.accumulate(row, self.decoder)?;
            }
        }

        let mut rows = VecDeque::new();
        for (grouping, accumulator) in self.mappings {
            //
            // Aggregate rules in Postgres dictate that the only
            // columns present in the row are either:
            //
            // 1. part of the GROUP BY, which means they are
            //    stored in the grouping
            // 2. are aggregate functions, which means they
            //    are stored in the accumulator
            //
            let mut row = DataRow::new();
            for (idx, datum) in grouping.columns {
                row.insert(idx, datum.encode(self.decoder.format(idx))?);
            }
            for acc in accumulator {
                row.insert(
                    acc.target.column(),
                    acc.datum.encode(self.decoder.format(acc.target.column()))?,
                );
            }
            rows.push_back(row);
        }

        Ok(rows)
    }
}
