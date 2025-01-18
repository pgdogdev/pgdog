//! Buffer messages to sort them later.

use std::{cmp::Ordering, collections::VecDeque};

use crate::{
    frontend::router::route::OrderBy,
    net::messages::{DataRow, FromBytes, Message, Protocol, RowDescription, ToBytes},
};

/// Sort rows received from multiple shards.
#[derive(Default)]
pub(super) struct SortBuffer {
    buffer: VecDeque<DataRow>,
    full: bool,
}

impl SortBuffer {
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
        // Calculate column indecies once since
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
            for index in &cols {
                let (index, asc) = if let Some((index, asc)) = index {
                    (*index, *asc)
                } else {
                    continue;
                };
                let ordering = if let Some(field) = rd.field(index) {
                    let text = field.is_text();
                    if field.is_int() {
                        let a = a.get_int(index, text);
                        let b = b.get_int(index, text);
                        if asc {
                            a.partial_cmp(&b)
                        } else {
                            b.partial_cmp(&a)
                        }
                    } else if field.is_float() {
                        let a = a.get_float(index, text);
                        let b = b.get_float(index, text);
                        if asc {
                            a.partial_cmp(&b)
                        } else {
                            b.partial_cmp(&a)
                        }
                    } else {
                        continue;
                    }
                } else {
                    continue;
                };

                if ordering != Some(Ordering::Equal) {
                    return ordering.unwrap_or(Ordering::Equal);
                }
            }
            Ordering::Equal
        };

        self.buffer.make_contiguous().sort_by(order_by);
    }

    /// Take messages from buffer.
    pub(super) fn take(&mut self) -> Option<Message> {
        if self.full {
            self.buffer.pop_front().and_then(|s| s.message().ok())
        } else {
            None
        }
    }
}
