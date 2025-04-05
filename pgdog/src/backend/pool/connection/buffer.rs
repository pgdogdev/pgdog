//! Buffer messages to sort and aggregate them later.

use std::{cmp::Ordering, collections::VecDeque};

use crate::{
    frontend::router::parser::{Aggregate, OrderBy},
    net::{
        messages::{DataRow, FromBytes, Message, Protocol, ToBytes, Vector},
        Decoder,
    },
};

use super::Aggregates;

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

    pub(super) fn reset(&mut self) {
        self.buffer.clear();
        self.full = false;
    }

    /// Sort the buffer.
    pub(super) fn sort(&mut self, columns: &[OrderBy], decoder: &Decoder) {
        // Calculate column indices once, since
        // fetching indices by name is O(number of columns).
        let mut cols = vec![];
        for column in columns {
            match column {
                OrderBy::Asc(_) => cols.push(column.clone()),
                OrderBy::AscColumn(name) => {
                    if let Some(index) = decoder.rd().field_index(name) {
                        cols.push(OrderBy::Asc(index + 1));
                    }
                }
                OrderBy::Desc(_) => cols.push(column.clone()),
                OrderBy::DescColumn(name) => {
                    if let Some(index) = decoder.rd().field_index(name) {
                        cols.push(OrderBy::Desc(index + 1));
                    }
                }
                OrderBy::AscVectorL2(_, _) => cols.push(column.clone()),
                OrderBy::AscVectorL2Column(name, vector) => {
                    if let Some(index) = decoder.rd().field_index(name) {
                        cols.push(OrderBy::AscVectorL2(index + 1, vector.clone()));
                    }
                }
            };
        }

        // Sort rows.
        let order_by = move |a: &DataRow, b: &DataRow| -> Ordering {
            for col in cols.iter() {
                let index = col.index();
                let asc = col.asc();
                let index = if let Some(index) = index {
                    index
                } else {
                    continue;
                };
                let left = a.get_column(index, decoder);
                let right = b.get_column(index, decoder);

                let ordering = match (left, right) {
                    (Ok(Some(left)), Ok(Some(right))) => {
                        // Handle the special vector case.
                        if let OrderBy::AscVectorL2(_, vector) = col {
                            let left: Option<Vector> = left.value.try_into().ok();
                            let right: Option<Vector> = right.value.try_into().ok();

                            if let (Some(left), Some(right)) = (left, right) {
                                let left = left.distance_l2(vector);
                                let right = right.distance_l2(vector);

                                left.partial_cmp(&right)
                            } else {
                                Some(Ordering::Equal)
                            }
                        } else if asc {
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
        aggregate: &Aggregate,
        decoder: &Decoder,
    ) -> Result<(), super::Error> {
        let buffer: VecDeque<DataRow> = std::mem::take(&mut self.buffer);
        if aggregate.is_empty() {
            self.buffer = buffer;
        } else {
            let aggregates = Aggregates::new(&buffer, decoder, aggregate);
            let result = aggregates.aggregate()?;

            if !result.is_empty() {
                self.buffer = result;
            } else {
                self.buffer = buffer;
            }
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
    use crate::net::{Field, Format, RowDescription};

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

        let decoder = Decoder::from(&rd);

        buf.sort(&columns, &decoder);
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
        let agg = Aggregate::new_count(0);

        for _ in 0..6 {
            let mut dr = DataRow::new();
            dr.add(15_i64);
            buf.add(dr.message().unwrap()).unwrap();
        }

        buf.aggregate(&agg, &Decoder::from(&rd)).unwrap();
        buf.full();

        assert_eq!(buf.len(), 1);
        let row = buf.take().unwrap();
        let dr = DataRow::from_bytes(row.to_bytes().unwrap()).unwrap();
        let count = dr.get::<i64>(0, Format::Text).unwrap();
        assert_eq!(count, 15 * 6);
    }

    #[test]
    fn test_aggregate_buffer_group_by() {
        let mut buf = Buffer::default();
        let rd = RowDescription::new(&[Field::bigint("count"), Field::text("email")]);
        let agg = Aggregate::new_count_group_by(0, &[1]);
        let emails = ["test@test.com", "admin@test.com"];

        for email in emails {
            for _ in 0..6 {
                let mut dr = DataRow::new();
                dr.add(15_i64);
                dr.add(email);
                buf.add(dr.message().unwrap()).unwrap();
            }
        }

        buf.aggregate(&agg, &Decoder::from(&rd)).unwrap();
        buf.full();

        assert_eq!(buf.len(), 2);
        for _ in &emails {
            let row = buf.take().unwrap();
            let dr = DataRow::from_bytes(row.to_bytes().unwrap()).unwrap();
            let count = dr.get::<i64>(0, Format::Text).unwrap();
            assert_eq!(count, 15 * 6);
        }
    }
}
