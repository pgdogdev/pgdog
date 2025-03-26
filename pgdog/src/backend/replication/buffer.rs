use fnv::FnvHashMap as HashMap;
use fnv::FnvHashSet as HashSet;
use std::collections::VecDeque;

use crate::backend::ShardingSchema;
use crate::frontend::router::parser::Shard;
use crate::frontend::router::sharding::shard_str;
use crate::net::messages::FromBytes;
use crate::net::messages::Protocol;
use crate::net::messages::ToBytes;
use crate::net::messages::{
    replication::{xlog_data::XLogPayload, Relation, XLogData},
    CopyData, Message,
};

use super::{Error, ReplicationConfig};

/// We are putting vectors on a single shard only.
static CENTROID_PROBES: usize = 1;

#[derive(Debug)]
pub struct Buffer {
    replication_config: ReplicationConfig,
    begin: Option<XLogData>,
    message: Option<XLogData>,
    relations: HashMap<i32, Relation>,
    sent_relations: HashSet<i32>,
    shard: Shard,
    oid: Option<i32>,
    buffer: VecDeque<Message>,
    sharding_schema: ShardingSchema,
}

impl Buffer {
    /// New replication buffer.
    pub fn new(
        shard: Shard,
        cluster: &ReplicationConfig,
        sharding_schema: &ShardingSchema,
    ) -> Self {
        Self {
            begin: None,
            message: None,
            relations: HashMap::default(),
            sent_relations: HashSet::default(),
            shard,
            oid: None,
            buffer: VecDeque::new(),
            replication_config: cluster.clone(),
            sharding_schema: sharding_schema.clone(),
        }
    }

    /// Buffer message maybe. If message isn't buffered,
    /// it's sent to the client. Some messages are skipped,
    /// like Insert/Update/Delete that don't belong to the shard.
    pub fn handle(&mut self, message: Message) -> Result<(), Error> {
        let data = match message.code() {
            'd' => CopyData::from_bytes(message.to_bytes()?)?,
            _ => {
                self.buffer.push_back(message);
                return Ok(());
            }
        };

        if let Some(xlog_data) = data.xlog_data() {
            if let Some(payload) = xlog_data.payload() {
                match &payload {
                    XLogPayload::Begin(_) => {
                        self.begin = Some(xlog_data);
                    }
                    XLogPayload::Commit(_) => {
                        self.message = Some(xlog_data);
                        return self.flush();
                    }
                    XLogPayload::Relation(relation) => {
                        self.relations.insert(relation.oid, relation.clone());
                        self.oid = Some(relation.oid);
                    }
                    XLogPayload::Update(update) => {
                        let (table, columns) = self.sharding_key(update.oid)?;
                        let column = self
                            .replication_config
                            .sharded_column(table, &columns)
                            .and_then(|column| update.column(column.position))
                            .and_then(|column| column.as_str());
                        if let Some(column) = column {
                            let shard =
                                shard_str(column, &self.sharding_schema, &vec![], CENTROID_PROBES);
                            if self.shard == shard {
                                self.message = Some(xlog_data);
                                return self.flush();
                            }
                        } else {
                            self.message = Some(xlog_data);
                            return self.flush();
                        }
                    }
                    XLogPayload::Insert(insert) => {
                        let (table, columns) = self.sharding_key(insert.oid)?;
                        let column = self
                            .replication_config
                            .sharded_column(table, &columns)
                            .and_then(|column| insert.column(column.position))
                            .and_then(|column| column.as_str());
                        if let Some(column) = column {
                            let shard =
                                shard_str(column, &self.sharding_schema, &vec![], CENTROID_PROBES);
                            if self.shard == shard {
                                self.message = Some(xlog_data);
                                return self.flush();
                            }
                        } else {
                            self.message = Some(xlog_data);
                            return self.flush();
                        }
                    }
                    _ => {
                        self.message = Some(xlog_data);
                        return self.flush();
                    }
                }
            } else {
                self.buffer.push_back(message);
            }
        } else {
            self.buffer.push_back(message);
        }

        Ok(())
    }

    /// Retrieve one message from the buffer, if any is stored.
    pub fn message(&mut self) -> Option<Message> {
        self.buffer.pop_front()
    }

    /// Flush partial transaction to buffer. Client will receive
    /// these messages next time it calls [`Self::message`].
    fn flush(&mut self) -> Result<(), Error> {
        // Start transaction if we haven't already.
        if let Some(begin) = self.begin.take() {
            self.buffer.push_back(begin.to_message()?);
        }

        // Message that triggered the flush.
        let message = self.message.take().ok_or(Error::NoMessage)?;

        // Make sure we send a Relation message identifying the table
        // we're sending changes for.
        let oid = self.oid.ok_or(Error::NoRelationMessage)?;
        if !self.sent_relations.contains(&oid) {
            let relation = self.relations.get(&oid).ok_or(Error::NoRelationMessage)?;
            // Rewind the clock on the Relation message to simulate
            // like Postgres sent it in this transaction.
            let xlog_data = XLogData::relation(message.system_clock, relation)?;
            self.buffer.push_back(xlog_data.to_message()?);
            self.sent_relations.insert(oid);
        }

        self.buffer.push_back(message.to_message()?.stream(true));

        Ok(())
    }

    fn sharding_key(&self, oid: i32) -> Result<(&str, Vec<&str>), Error> {
        let relation = self.relations.get(&oid).ok_or(Error::NoRelationMessage)?;
        let columns = relation.columns();
        let name = relation.name();

        Ok((name, columns))
    }
}
