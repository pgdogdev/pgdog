use fnv::FnvHashMap as HashMap;
use fnv::FnvHashSet as HashSet;
use std::collections::VecDeque;

use crate::frontend::router::sharding::shard_str;
use crate::net::messages::FromBytes;
use crate::net::messages::Protocol;
use crate::net::messages::ToBytes;
use crate::{
    backend::Cluster,
    net::messages::{
        replication::{xlog_data::XLogPayload, Relation, XLogData},
        CopyData, Message,
    },
};

use super::{Error, ReplicationConfig};

#[derive(Debug)]
pub struct Buffer {
    cluster: ReplicationConfig,
    begin: Option<XLogData>,
    message: Option<XLogData>,
    relations: HashMap<i32, Relation>,
    sent_relations: HashSet<i32>,
    shard: Option<usize>,
    oid: Option<i32>,
    buffer: VecDeque<Message>,
}

impl Buffer {
    pub fn new(shard: Option<usize>, cluster: &ReplicationConfig) -> Self {
        Self {
            begin: None,
            message: None,
            relations: HashMap::default(),
            sent_relations: HashSet::default(),
            shard,
            oid: None,
            buffer: VecDeque::new(),
            cluster: cluster.clone(),
        }
    }

    /// Buffer message possibly.
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
                        return self.forward();
                    }
                    XLogPayload::Relation(relation) => {
                        self.relations.insert(relation.oid, relation.clone());
                        self.oid = Some(relation.oid);
                    }
                    XLogPayload::Insert(insert) => {
                        let (table, columns) = self.sharding_key(insert.oid)?;
                        let column = self
                            .cluster
                            .sharded_column(table, &columns)
                            .map(|column| insert.column(column))
                            .flatten()
                            .map(|column| column.as_str())
                            .flatten();
                        if let Some(column) = column {
                            let shard = shard_str(column, self.cluster.shards());
                            if self.shard == shard {
                                self.message = Some(xlog_data);
                                return self.forward();
                            }
                        } else {
                            self.message = Some(xlog_data);
                            return self.forward();
                        }
                    }
                    _ => {
                        self.message = Some(xlog_data);
                        return self.forward();
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

    pub fn message(&mut self) -> Option<Message> {
        self.buffer.pop_front()
    }

    fn forward(&mut self) -> Result<(), Error> {
        if let Some(begin) = self.begin.take() {
            self.buffer.push_back(begin.to_message()?);
        }
        let message = self.message.take().ok_or(Error::NoMessage)?;
        let oid = self.oid.ok_or(Error::NoRelationMessage)?;
        if self.sent_relations.get(&oid).is_none() {
            let relation = self.relations.get(&oid).ok_or(Error::NoRelationMessage)?;
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
