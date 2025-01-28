use crate::{
    backend::{replication::Buffer, Cluster},
    net::messages::{CopyData, FromBytes, Message, Protocol, ToBytes},
};

use super::Error;

#[derive(Debug)]
pub struct Replication {
    pub shard: usize,
    buffer: Buffer,
}

impl Replication {}
