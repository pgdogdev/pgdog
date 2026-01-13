//! Buffer messages to sort and aggregate them later.

use std::{
    cmp::Ordering,
    collections::{HashSet, VecDeque},
};

use crate::{
    frontend::router::parser::{
        rewrite::statement::aggregate::AggregateRewritePlan, Aggregate, DistinctBy, DistinctColumn,
        OrderBy,
    },
    net::{
        messages::{DataRow, FromBytes, Message, Protocol, ToBytes, Vector},
        Decoder,
    },
};

use super::Aggregates;

/// Sort and aggregate rows received from multiple shards.
#[derive(Default, Debug, Clone)]
pub(super) struct Buffer {
    buffer: VecDeque<DataRow>,
    full: bool,
    distinct: HashSet<DataRow>,
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
                    // TODO: Error out instead of silently not sorting.
                }
                OrderBy::Desc(_) => cols.push(column.clone()),
                OrderBy::DescColumn(name) => {
                    if let Some(index) = decoder.rd().field_index(name) {
                        cols.push(OrderBy::Desc(index + 1));
                    }
                    // TODO: Error out instead of silently not sorting.
                }
                OrderBy::AscVectorL2(_, _) => cols.push(column.clone()),
                OrderBy::AscVectorL2Column(name, vector) => {
                    if let Some(index) = decoder.rd().field_index(name) {
                        cols.push(OrderBy::AscVectorL2(index + 1, vector.clone()));
                    }
                    // TODO: Error out instead of silently not sorting.
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
        plan: &AggregateRewritePlan,
    ) -> Result<(), super::Error> {
        let buffer: VecDeque<DataRow> = std::mem::take(&mut self.buffer);
        let mut rows = if aggregate.is_empty() {
            buffer
        } else {
            let aggregates = Aggregates::new(&buffer, decoder, aggregate, plan);
            let result = aggregates.aggregate()?;

            if result.is_empty() {
                buffer
            } else {
                result
            }
        };

        Self::drop_helper_columns(&mut rows, plan);
        self.buffer = rows;

        Ok(())
    }

    fn drop_helper_columns(rows: &mut VecDeque<DataRow>, plan: &AggregateRewritePlan) {
        if plan.drop_columns().is_empty() {
            return;
        }

        let mut drop = plan.drop_columns().to_vec();
        drop.sort_unstable();
        drop.dedup();

        for row in rows.iter_mut() {
            row.drop_columns(&drop);
        }
    }

    pub(super) fn distinct(&mut self, distinct: &Option<DistinctBy>, decoder: &Decoder) {
        if let Some(distinct) = distinct {
            match distinct {
                DistinctBy::Row => {
                    self.buffer.retain(|row| self.distinct.insert(row.clone()));
                }

                DistinctBy::Columns(ref columns) => {
                    self.buffer.retain(|row| {
                        let mut dr = DataRow::new();
                        for col in columns {
                            match col {
                                DistinctColumn::Index(index) => {
                                    if let Some(data) = row.column(*index) {
                                        dr.add(data);
                                    }
                                }

                                DistinctColumn::Name(name) => {
                                    if let Some(index) = decoder.rd().field_index(name) {
                                        if let Some(data) = row.column(index) {
                                            dr.add(data);
                                        }
                                    }
                                }
                            }
                        }

                        self.distinct.insert(dr)
                    });
                }
            }
        }
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
    use crate::net::{Datum, Field, Format, RowDescription};
    use bytes::Bytes;

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

        buf.aggregate(&agg, &Decoder::from(&rd), &AggregateRewritePlan::default())
            .unwrap();
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

        buf.aggregate(&agg, &Decoder::from(&rd), &AggregateRewritePlan::default())
            .unwrap();
        buf.full();

        assert_eq!(buf.len(), 2);
        for _ in &emails {
            let row = buf.take().unwrap();
            let dr = DataRow::from_bytes(row.to_bytes().unwrap()).unwrap();
            let count = dr.get::<i64>(0, Format::Text).unwrap();
            assert_eq!(count, 15 * 6);
        }
    }

    #[test]
    fn test_sort_buffer_with_timestamps() {
        let mut buf = Buffer::default();
        let rd = RowDescription::new(&[Field::timestamp("created_at"), Field::text("name")]);
        let columns = [OrderBy::Asc(1)]; // Sort by timestamp column

        // Add timestamps in random order
        let timestamps = [
            "2025-01-15 10:30:45.123456",
            "2025-01-14 09:15:30.000000",
            "2025-01-16 14:45:00.987654",
            "2025-01-13 08:00:00.000000",
            "2025-01-15 10:30:45.123455", // 1 microsecond before first
        ];

        for (i, ts) in timestamps.iter().enumerate() {
            let mut dr = DataRow::new();
            dr.add(ts.to_string()).add(format!("item_{}", i));
            buf.add(dr.message().unwrap()).unwrap();
        }

        let decoder = Decoder::from(&rd);

        buf.sort(&columns, &decoder);
        buf.full();

        // Verify timestamps are sorted
        let expected_order = [
            "2025-01-13 08:00:00.000000",
            "2025-01-14 09:15:30.000000",
            "2025-01-15 10:30:45.123455",
            "2025-01-15 10:30:45.123456",
            "2025-01-16 14:45:00.987654",
        ];

        for expected in expected_order {
            let message = buf.take().expect("Should have message");
            let dr = DataRow::from_bytes(message.to_bytes().unwrap()).unwrap();
            let ts = dr.get::<String>(0, Format::Text).unwrap();
            assert_eq!(ts, expected);
        }
    }

    #[test]
    fn test_sort_buffer_with_numeric() {
        let mut buf = Buffer::default();
        let rd = RowDescription::new(&[Field::numeric("price"), Field::text("product")]);
        let columns = [OrderBy::Desc(1)]; // Sort by numeric column descending

        // Add numeric values in random order
        let prices = [
            "199.99", "50.25", "1000.00", "75.50", "199.98", // Very close to first value
            "0.99", "1000.01", // Slightly more than 1000
        ];

        for (i, price) in prices.iter().enumerate() {
            let mut dr = DataRow::new();
            dr.add(price.to_string()).add(format!("product_{}", i));
            buf.add(dr.message().unwrap()).unwrap();
        }

        let decoder = Decoder::from(&rd);

        buf.sort(&columns, &decoder);
        buf.full();

        // Verify numeric values are sorted in descending order
        let expected_order = [
            "1000.01", "1000.00", "199.99", "199.98", "75.50", "50.25", "0.99",
        ];

        for expected in expected_order {
            let message = buf.take().expect("Should have message");
            let dr = DataRow::from_bytes(message.to_bytes().unwrap()).unwrap();
            let price = dr.get::<String>(0, Format::Text).unwrap();
            assert_eq!(price, expected);
        }
    }

    // Helper function to create PostgreSQL binary NUMERIC data
    fn create_binary_numeric(value: &str) -> Vec<u8> {
        use crate::net::messages::bind::Format;
        use crate::net::messages::data_types::{FromDataType, Numeric};
        use rust_decimal::Decimal;
        use std::str::FromStr;

        // Use our actual Numeric implementation
        let decimal = Decimal::from_str(value).unwrap();
        let numeric = Numeric::from(decimal);
        numeric.encode(Format::Binary).unwrap().to_vec()
    }

    #[test]
    fn test_sort_buffer_with_numeric_binary() {
        let mut buf = Buffer::default();
        let rd = RowDescription::new(&[Field::numeric_binary("price"), Field::text("product")]);
        let columns = [OrderBy::Desc(1)]; // Sort by numeric column descending

        // Test values with their expected binary representations
        let test_cases = [
            ("199.99", "199.99"),
            ("50.25", "50.25"),
            ("1000.00", "1000.00"),
            ("75.50", "75.50"),
            ("199.98", "199.98"),
            ("0.99", "0.99"),
            ("1000.01", "1000.01"),
        ];

        for (i, (price, _)) in test_cases.iter().enumerate() {
            let mut dr = DataRow::new();
            let binary_data = create_binary_numeric(price);
            dr.add(Bytes::from(binary_data))
                .add(format!("product_{}", i));
            buf.add(dr.message().unwrap()).unwrap();
        }

        let decoder = Decoder::from(&rd);
        buf.sort(&columns, &decoder);
        buf.full();

        let expected_order = [
            "1000.01", "1000.00", "199.99", "199.98", "75.50", "50.25", "0.99",
        ];

        for expected in expected_order {
            let message = buf.take().expect("Should have message");
            let dr = DataRow::from_bytes(message.to_bytes().unwrap()).unwrap();
            // Get the numeric value and convert to string for comparison
            let column = dr.get_column(0, &decoder).unwrap().unwrap();
            if let Datum::Numeric(numeric) = column.value {
                assert_eq!(numeric.to_string(), expected);
            } else {
                panic!("Expected Numeric datum, got {:?}", column.value);
            }
        }
    }

    #[test]
    fn test_sort_buffer_with_numeric_edge_cases() {
        let mut buf = Buffer::default();
        let rd = RowDescription::new(&[Field::numeric("value"), Field::text("description")]);
        let columns = [OrderBy::Asc(1)]; // Sort by numeric column ascending

        // Test edge cases: negative numbers, very large numbers, very small decimals, zero
        let values = [
            "-999.99",
            "0",
            "0.001",
            "999999999999.99",
            "-0.001",
            "123.4500000",
            "123.45",
        ];

        for (i, value) in values.iter().enumerate() {
            let mut dr = DataRow::new();
            dr.add(value.to_string()).add(format!("case_{}", i));
            buf.add(dr.message().unwrap()).unwrap();
        }

        let decoder = Decoder::from(&rd);
        buf.sort(&columns, &decoder);
        buf.full();

        // Expected order: ascending numeric sort
        // Note: equal values maintain input order (stable sort)
        let expected_order = [
            "-999.99",
            "-0.001",
            "0",
            "0.001",
            "123.4500000",
            "123.45",
            "999999999999.99",
        ];

        for expected in expected_order {
            let message = buf.take().expect("Should have message");
            let dr = DataRow::from_bytes(message.to_bytes().unwrap()).unwrap();
            let value = dr.get::<String>(0, Format::Text).unwrap();
            assert_eq!(value, expected);
        }
    }

    #[test]
    fn test_distinct() {
        let mut buf = Buffer::default();
        let rd = RowDescription::new(&[Field::bigint("id"), Field::text("email")]);
        let decoder = Decoder::from(&rd);

        for email in ["test@test.com", "apples@test.com", "domain@test.com"] {
            for i in 0..5 {
                let mut dr = DataRow::new();
                dr.add(i as i64);
                dr.add(email);
                buf.add(dr.message().unwrap()).unwrap();
            }
        }

        let mut distinct_row = buf.clone();
        distinct_row.distinct(&Some(DistinctBy::Row), &decoder);

        assert_eq!(distinct_row.buffer.len(), 15);

        for distinct in [
            DistinctColumn::Index(0),
            DistinctColumn::Name("id".to_string()),
        ] {
            let mut distinct_id = buf.clone();
            distinct_id.distinct(&Some(DistinctBy::Columns(vec![distinct])), &decoder);
            assert_eq!(distinct_id.buffer.len(), 5);
        }

        for distinct in [
            DistinctColumn::Index(1),
            DistinctColumn::Name("email".to_string()),
        ] {
            let mut distinct_id = buf.clone();
            distinct_id.distinct(&Some(DistinctBy::Columns(vec![distinct])), &decoder);
            assert_eq!(distinct_id.buffer.len(), 3);
        }

        let mut buf = Buffer::default();

        for email in ["test@test.com", "apples@test.com", "domain@test.com"] {
            for _ in 0..5 {
                let mut dr = DataRow::new();
                dr.add(5_i64);
                dr.add(email);
                buf.add(dr.message().unwrap()).unwrap();
            }
        }

        assert_eq!(buf.buffer.len(), 15);
        buf.distinct(&Some(DistinctBy::Row), &decoder);

        assert_eq!(buf.buffer.len(), 3);
    }
}
