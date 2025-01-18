//! Buffer messages to sort them later.

use std::{cmp::Ordering, collections::VecDeque};

use crate::{
    frontend::router::route::OrderBy,
    net::messages::{DataRow, FromBytes, Message, Protocol, RowDescription, ToBytes},
};

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
        let order_by = move |a: &DataRow, b: &DataRow| -> Ordering {
            for column in columns {
                // TODO: move this out of the sort loop,
                // the field_index call is O(n) number of fields in RowDescription
                let index = if let Some(index) = column.index() {
                    index
                } else if let Some(name) = column.name() {
                    if let Some(index) = rd.field_index(name) {
                        index
                    } else {
                        continue;
                    }
                } else {
                    continue;
                };
                let ordering = if let Some(field) = rd.field(index) {
                    let text = field.is_text();
                    if field.is_int() {
                        let a = a.get_int(index, text);
                        let b = b.get_int(index, text);
                        if column.asc() {
                            a.partial_cmp(&b)
                        } else {
                            b.partial_cmp(&a)
                        }
                    } else if field.is_float() {
                        let a = a.get_float(index, text);
                        let b = b.get_float(index, text);
                        if column.asc() {
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
            self.buffer.pop_front().map(|s| s.message().ok()).flatten()
        } else {
            None
        }
    }
}
