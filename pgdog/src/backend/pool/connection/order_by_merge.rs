use std::{cmp::Ordering, collections::VecDeque};

use crate::{
    frontend::router::parser::{Limit, OrderBy},
    net::{
        Decoder,
        messages::{DataRow, Message, Protocol, Vector},
    },
};

use pgdog_postgres_types::Datum;

#[derive(Debug, Clone)]
pub(super) struct OrderByMergeState {
    columns: Vec<OrderBy>,
    per_shard: Vec<VecDeque<DataRow>>,
    done: Vec<bool>,
    ready: VecDeque<DataRow>,
    offset: usize,
    limit: Option<usize>,
    skipped: usize,
    emitted: usize,
    exhausted: bool,
}

impl OrderByMergeState {
    pub(super) fn new(shards: usize, columns: Vec<OrderBy>, limit: &Limit) -> Self {
        let mut per_shard = Vec::with_capacity(shards);
        per_shard.resize_with(shards, VecDeque::new);
        Self {
            columns,
            per_shard,
            done: vec![false; shards],
            ready: VecDeque::new(),
            offset: limit.offset.unwrap_or(0),
            limit: limit.limit,
            skipped: 0,
            emitted: 0,
            exhausted: false,
        }
    }

    pub(super) fn push_row(&mut self, shard: usize, row: DataRow, decoder: &Decoder) {
        if self.exhausted {
            return;
        }
        if let Some(queue) = self.per_shard.get_mut(shard) {
            queue.push_back(row);
            self.drain_ready(decoder);
        }
    }

    pub(super) fn mark_done(&mut self, shard: usize, decoder: &Decoder) {
        if let Some(done) = self.done.get_mut(shard) {
            *done = true;
            self.drain_ready(decoder);
        }
    }

    pub(super) fn take_ready(&mut self) -> Option<Message> {
        self.ready.pop_front().and_then(|row| row.message().ok())
    }

    pub(super) fn output_rows(&self) -> usize {
        self.emitted
    }

    pub(super) fn ready_len(&self) -> usize {
        self.ready.len()
    }

    fn can_emit_next(&self) -> bool {
        let mut has_candidate = false;
        for (queue, done) in self.per_shard.iter().zip(self.done.iter()) {
            if queue.is_empty() && !done {
                return false;
            }
            has_candidate |= !queue.is_empty();
        }
        has_candidate
    }

    fn choose_shard(&self, decoder: &Decoder) -> Option<usize> {
        let mut best: Option<usize> = None;
        for (idx, queue) in self.per_shard.iter().enumerate() {
            let Some(head) = queue.front() else {
                continue;
            };
            let replace = best
                .and_then(|best_idx| {
                    self.per_shard
                        .get(best_idx)
                        .and_then(|best_queue| best_queue.front())
                        .map(|best_head| {
                            compare_rows(head, best_head, &self.columns, decoder) == Ordering::Less
                        })
                })
                .unwrap_or(true);
            if replace {
                best = Some(idx);
            }
        }
        best
    }

    fn drain_ready(&mut self, decoder: &Decoder) {
        if self.exhausted {
            return;
        }
        loop {
            if let Some(limit) = self.limit
                && self.emitted >= limit
            {
                self.exhausted = true;
                for queue in &mut self.per_shard {
                    queue.clear();
                }
                return;
            }
            if !self.can_emit_next() {
                return;
            }
            let Some(shard) = self.choose_shard(decoder) else {
                return;
            };
            let Some(row) = self.per_shard[shard].pop_front() else {
                return;
            };
            if self.skipped < self.offset {
                self.skipped += 1;
                continue;
            }
            self.emitted += 1;
            self.ready.push_back(row);
        }
    }
}

pub(super) fn normalize_order_by(columns: &[OrderBy], decoder: &Decoder) -> Vec<OrderBy> {
    let mut normalized = vec![];
    for column in columns {
        match column {
            OrderBy::Asc(_) => normalized.push(column.clone()),
            OrderBy::AscColumn(name) => {
                if let Some(index) = decoder.rd().field_index(name) {
                    normalized.push(OrderBy::Asc(index + 1));
                }
                // TODO: Error out instead of silently not sorting.
            }
            OrderBy::Desc(_) => normalized.push(column.clone()),
            OrderBy::DescColumn(name) => {
                if let Some(index) = decoder.rd().field_index(name) {
                    normalized.push(OrderBy::Desc(index + 1));
                }
                // TODO: Error out instead of silently not sorting.
            }
            OrderBy::AscVectorL2(_, _) => normalized.push(column.clone()),
            OrderBy::AscVectorL2Column(name, vector) => {
                if let Some(index) = decoder.rd().field_index(name) {
                    normalized.push(OrderBy::AscVectorL2(index + 1, vector.clone()));
                }
                // TODO: Error out instead of silently not sorting.
            }
        };
    }
    normalized
}

pub(super) fn compare_rows(
    a: &DataRow,
    b: &DataRow,
    cols: &[OrderBy],
    decoder: &Decoder,
) -> Ordering {
    cols.iter()
        .filter_map(|col| {
            let index = col.index();
            let asc = col.asc();
            let index = index?;
            let left = a.get_column(index, decoder);
            let right = b.get_column(index, decoder);

            match (left, right) {
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
                    } else {
                        // FIXME(sage): We don't handle ASC NULLS FIRST or
                        // DESC NULLS LAST we should either error or add
                        // support rather than silently do the wrong sorting
                        match (&left.value, &right.value, asc) {
                            (Datum::Null, Datum::Null, _) => Some(Ordering::Equal),
                            (Datum::Null, _, true) => Some(Ordering::Greater),
                            (_, Datum::Null, true) => Some(Ordering::Less),
                            (Datum::Null, _, false) => Some(Ordering::Less),
                            (_, Datum::Null, false) => Some(Ordering::Greater),
                            (a, b, true) => a.partial_cmp(b),
                            (a, b, false) => b.partial_cmp(a),
                        }
                    }
                }

                _ => Some(Ordering::Equal),
            }
        })
        .reduce(Ordering::then)
        .unwrap_or(Ordering::Equal)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::net::messages::FromBytes;
    use crate::net::{Field, Format, RowDescription};

    #[test]
    fn test_order_by_merge_honors_offset_and_limit() {
        let rd = RowDescription::new(&[Field::bigint("id")]);
        let decoder = Decoder::from(&rd);
        let mut merge = OrderByMergeState::new(
            2,
            normalize_order_by(&[OrderBy::Asc(1)], &decoder),
            &Limit {
                limit: Some(2),
                offset: Some(1),
            },
        );

        for value in [1_i64, 3_i64] {
            let mut row = DataRow::new();
            row.add(value);
            merge.push_row(0, row, &decoder);
        }
        for value in [2_i64, 4_i64] {
            let mut row = DataRow::new();
            row.add(value);
            merge.push_row(1, row, &decoder);
        }

        merge.mark_done(0, &decoder);
        merge.mark_done(1, &decoder);

        // Global order is 1,2,3,4 -> offset 1 + limit 2 => 2,3
        let out = DataRow::from_bytes(merge.take_ready().unwrap().to_bytes()).unwrap();
        assert_eq!(out.get::<i64>(0, Format::Text).unwrap(), 2_i64);
        let out = DataRow::from_bytes(merge.take_ready().unwrap().to_bytes()).unwrap();
        assert_eq!(out.get::<i64>(0, Format::Text).unwrap(), 3_i64);
        assert!(merge.take_ready().is_none());
        assert_eq!(merge.output_rows(), 2);
    }

    #[test]
    fn test_order_by_merge_waits_for_all_shards_heads() {
        let rd = RowDescription::new(&[Field::bigint("id")]);
        let decoder = Decoder::from(&rd);
        let mut merge = OrderByMergeState::new(
            2,
            normalize_order_by(&[OrderBy::Asc(1)], &decoder),
            &Limit {
                limit: None,
                offset: None,
            },
        );

        let mut row = DataRow::new();
        row.add(1_i64);
        merge.push_row(0, row, &decoder);

        // Can't emit yet: shard 1 has not produced a head row or finished.
        assert!(merge.take_ready().is_none());

        let mut row = DataRow::new();
        row.add(2_i64);
        merge.push_row(1, row, &decoder);
        let out = DataRow::from_bytes(merge.take_ready().unwrap().to_bytes()).unwrap();
        assert_eq!(out.get::<i64>(0, Format::Text).unwrap(), 1_i64);

        // Still can't emit row 2: shard 0 is neither done nor has another head row.
        merge.mark_done(1, &decoder);
        assert!(merge.take_ready().is_none());

        // Once shard 0 is done, shard 1's remaining head can be emitted.
        merge.mark_done(0, &decoder);
        let out = DataRow::from_bytes(merge.take_ready().unwrap().to_bytes()).unwrap();
        assert_eq!(out.get::<i64>(0, Format::Text).unwrap(), 2_i64);
    }
}
