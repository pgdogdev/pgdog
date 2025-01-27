use fnv::FnvHashMap as HashMap;
use std::mem::take;

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
}

#[derive(Debug, Default)]
pub struct Buffer {
    begin: Option<Begin>,
    commit: Option<Commit>,
    relation: Option<Relation>,
    messages: Vec<XLogPayload>,
    buffers: HashMap<i32, OperationBuffer>,
    state: State,
}

impl Buffer {
    pub fn handle(&mut self, message: XLogPayload) -> Result<(), Error> {
        match message {
            XLogPayload::Begin(begin) => {
                self.begin = Some(begin);
                self.state = State::Buffering;
            }
            XLogPayload::Commit(commit) => {
                self.commit = Some(commit);
                self.state = State::Commit;
            }
            XLogPayload::Relation(relation) => {
                self.buffers
                    .entry(relation.oid)
                    .or_insert_with(|| OperationBuffer::new(relation.clone()));
                self.relation = Some(relation);

                self.state = State::Buffering;
            }
            _ => {
                self.messages.push(message);
                self.state = State::Message;
            }
        }

        Ok(())
    }

    pub fn query(&mut self) -> Result<Vec<Query>, Error> {
        if self.relation.is_none() {
            return Ok(vec![]);
        }

        // Wait until commit or another tuple arrives.
        // This is an optimization to not use explicit transactions
        // when only one tuple is sent.
        if self.messages.len() == 1 && self.commit.is_none() {
            return Ok(vec![]);
        }

        let mut queries = vec![];

        // More than one row change requires an explicit transaction.
        //
        if self.messages.len() > 1 {
            if let Some(_) = self.begin.take() {
                queries.push(Query {
                    query: "BEGIN".into(),
                    shard: None,
                });
            }
        }

        for message in take(&mut self.messages) {
            // TODO: convert messages to queries
        }

        if let Some(ref commit) = self.commit {
            queries.push(Query {
                query: "COMMIT".into(),
                shard: None,
            });
        }

        Ok(queries)
    }
}

#[derive(Debug, PartialEq, Default)]
enum State {
    #[default]
    Buffering,
    Message,
    Commit,
}

#[derive(Debug, Clone)]
pub struct Query {
    query: String,
    shard: Option<usize>,
}
