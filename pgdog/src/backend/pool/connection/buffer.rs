//! Buffer messages to sort and aggregate them later.

use std::{cmp::Ordering, collections::VecDeque};

use crate::{
    frontend::router::parser::{Aggregate, AggregateTarget, OrderBy},
    net::messages::{DataRow, Datum, FromBytes, Message, Protocol, RowDescription, ToBytes},
};

/// Sort and aggregate rows received from multiple shards.
#[derive(Default, Debug)]
pub(super) struct Buffer {
    buffer: VecDeque<DataRow>,
    full: bool,
}

impl Buffer {
    /// Add message to buffer.
    pub(super) fn add(&mut self, message: Message) -> Result<(), super::Error> {
        let dr = DataRow::from_bytes(message.to_bytes()?)?;

        self.buffer.push_back(dr);

        Ok(())
    }

    /// Mark the buffer as full. It will start returning messages now.
    /// Caller is responsible for sorting the buffer if needed.
    pub(super) fn full(&mut self) {
        self.full = true;
    }

    /// Sort the buffer.
    pub(super) fn sort(&mut self, columns: &[OrderBy], rd: &RowDescription) {
        // Calculate column indecies once, since
        // fetching indecies by name is O(n).
        let mut cols = vec![];
        for column in columns {
            if let Some(index) = column.index() {
                cols.push(Some((index, column.asc())));
            } else if let Some(name) = column.name() {
                if let Some(index) = rd.field_index(name) {
                    cols.push(Some((index, column.asc())));
                } else {
                    cols.push(None);
                }
            } else {
                cols.push(None);
            };
        }

        // Sort rows.
        let order_by = move |a: &DataRow, b: &DataRow| -> Ordering {
            for col in cols.iter().flatten() {
                let (index, asc) = col;
                let left = a.get_column(*index, rd);
                let right = b.get_column(*index, rd);

                let ordering = match (left, right) {
                    (Ok(Some(left)), Ok(Some(right))) => {
                        if *asc {
                            left.value.partial_cmp(&right.value)
                        } else {
                            right.value.partial_cmp(&left.value)
                        }
                    }

                    _ => Some(Ordering::Equal),
                };

                if ordering != Some(Ordering::Equal) {
                    return ordering.unwrap_or(Ordering::Equal);
                }
            }

            Ordering::Equal
        };

        self.buffer.make_contiguous().sort_by(order_by);
    }

    /// Execute aggregate functions.
    ///
    /// This function is the entrypoint for aggregation, so if you're reading this,
    /// understand that this will be a WIP for a while. Some (many) assumptions are made
    /// about queries and they will be tested (and adjusted) over time.
    ///
    /// Some aggregates will require query rewriting. This information will need to be passed in,
    /// and extra columns fetched from Postgres removed from the final result.
    pub(super) fn aggregate(
        &mut self,
        aggregates: &[Aggregate],
        rd: &RowDescription,
    ) -> Result<(), super::Error> {
        let buffer: VecDeque<DataRow> = self.buffer.drain(0..).collect();
        let mut result = DataRow::new();

        for aggregate in aggregates {
            match aggregate {
                // COUNT(*) are summed across shards. This is the easiest of the aggregates,
                // yet it's probably the most common one.
                //
                // TODO: If there is a GROUP BY clause, we need to sum across specified columns.
                Aggregate::Count(AggregateTarget::Star(index)) => {
                    let mut count = Datum::Bigint(0);
                    for row in &buffer {
                        let column = row.get_column(*index, rd)?;
                        if let Some(column) = column {
                            count = count + column.value;
                        }
                    }

                    result.insert(*index, count);
                }

                Aggregate::Max(AggregateTarget::Star(index)) => {
                    let mut max = Datum::Bigint(i64::MIN);
                    for row in &buffer {
                        let column = row.get_column(*index, rd)?;
                        if let Some(column) = column {
                            if max < column.value {
                                max = column.value;
                            }
                        }
                    }

                    result.insert(*index, max);
                }

                Aggregate::Min(AggregateTarget::Star(index)) => {
                    let mut min = Datum::Bigint(i64::MAX);
                    for row in &buffer {
                        let column = row.get_column(*index, rd)?;
                        if let Some(column) = column {
                            if min > column.value {
                                min = column.value;
                            }
                        }
                    }

                    result.insert(*index, min);
                }
                _ => (),
            }
        }

        if !result.is_empty() {
            self.buffer.push_back(result);
        } else {
            self.buffer = buffer;
        }

        Ok(())
    }

    /// Take messages from buffer.
    pub(super) fn take(&mut self) -> Option<Message> {
        if self.full {
            self.buffer.pop_front().and_then(|s| s.message().ok())
        } else {
            None
        }
    }

    pub(super) fn len(&self) -> usize {
        self.buffer.len()
    }

    #[allow(dead_code)]
    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::net::messages::{Field, Format};

    #[test]
    fn test_sort_buffer() {
        let mut buf = Buffer::default();
        let rd = RowDescription::new(&[Field::bigint("one"), Field::text("two")]);
        let columns = [OrderBy::Asc(1), OrderBy::Desc(2)];

        for i in 0..25_i64 {
            let mut dr = DataRow::new();
            dr.add(25 - i).add((25 - i).to_string());
            buf.add(dr.message().unwrap()).unwrap();
        }

        buf.sort(&columns, &rd);
        buf.full();

        let mut i = 1;
        while let Some(message) = buf.take() {
            let dr = DataRow::from_bytes(message.to_bytes().unwrap()).unwrap();
            let one = dr.get::<i64>(0, Format::Text).unwrap();
            let two = dr.get::<String>(1, Format::Text).unwrap();
            assert_eq!(one, i);
            assert_eq!(two, i.to_string());
            i += 1;
        }

        assert_eq!(i, 26);
    }

    #[test]
    fn test_aggregate_buffer() {
        let mut buf = Buffer::default();
        let rd = RowDescription::new(&[Field::bigint("count")]);
        let agg = [Aggregate::Count(AggregateTarget::Star(0))];

        for _ in 0..6 {
            let mut dr = DataRow::new();
            dr.add(15_i64);
            buf.add(dr.message().unwrap()).unwrap();
        }

        buf.aggregate(&agg, &rd).unwrap();
        buf.full();

        assert_eq!(buf.len(), 1);
        let row = buf.take().unwrap();
        let dr = DataRow::from_bytes(row.to_bytes().unwrap()).unwrap();
        let count = dr.get::<i64>(0, Format::Text).unwrap();
        assert_eq!(count, 15 * 6);
    }
}
