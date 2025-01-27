use fnv::FnvHashMap as HashMap;
use std::{f32::consts::PI, mem::take};

use super::{insert::InsertBuffer, Error};
use crate::net::messages::replication::{xlog_data::XLogPayload, Begin, Commit, Relation};

#[derive(Debug)]
pub struct OperationBuffer {
    relation: Relation,
    insert: InsertBuffer,
}

impl OperationBuffer {
    fn new(relation: Relation) -> Self {
        Self {
            relation,
            insert: InsertBuffer::default(),
        }
    }

    fn len(&self) -> usize {
        self.insert.len()
    }

    fn insert_sql(&self) -> Result<String, Error> {
        self.insert.to_sql(&self.relation)
    }

    fn clear(&mut self) {
        self.insert.clear();
    }
}

#[derive(Debug, Default)]
pub struct Buffer {
    begin: Option<Begin>,
    commit: Option<Commit>,
    buffers: HashMap<i32, OperationBuffer>,
    lsn: i64,
    timestamp: i64,
}

impl Buffer {
    pub fn handle(&mut self, message: XLogPayload) -> Result<Vec<Query>, Error> {
        match message {
            XLogPayload::Begin(begin) => {
                self.begin = Some(begin);
            }
            XLogPayload::Commit(commit) => {
                self.commit = Some(commit);
            }
            XLogPayload::Relation(relation) => {
                self.buffers
                    .entry(relation.oid)
                    .or_insert_with(|| OperationBuffer::new(relation));
            }
            XLogPayload::Insert(insert) => {
                let buffer = self.buffer(insert.oid)?;
                buffer.insert.add(insert);
            }
            XLogPayload::Update(update) => {}
            XLogPayload::Delete(delete) => {}
            XLogPayload::Truncate(truncate) => {}
        }

        Ok(vec![])
    }

    #[inline]
    fn buffer(&mut self, oid: i32) -> Result<&mut OperationBuffer, Error> {
        self.buffers.get_mut(&oid).ok_or(Error::NoRelationMessage)
    }

    #[inline]
    fn len(&self) -> usize {
        let mut len = 0;
        for (_, buffer) in &self.buffers {
            len += buffer.len();
        }

        len
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn queries(&mut self) -> Result<Vec<Query>, Error> {
        let mut queries = vec![];
        if self.is_empty() {
            return Ok(queries);
        }

        if let Some(_begin) = self.begin.take() {
            queries.push(Query {
                query: "BEGIN".into(),
                shard: None,
            });
        }

        // TODO: shard
        for (_, buffer) in &self.buffers {
            queries.push(Query {
                query: buffer.insert_sql()?,
                shard: None,
            });
        }

        if let Some(commit) = self.commit.take() {
            queries.push(Query {
                query: "COMMIT".into(),
                shard: None,
            });
            self.lsn = commit.end_lsn;
            self.timestamp = commit.commit_timestamp;
            for (_, buffer) in &mut self.buffers {
                buffer.clear();
            }
        }

        Ok(queries)
    }
}

#[derive(Debug, Clone)]
pub struct Query {
    pub query: String,
    pub shard: Option<usize>,
}
