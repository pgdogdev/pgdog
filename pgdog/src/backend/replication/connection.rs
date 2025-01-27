use crate::backend::{pool::Guard, Server};

use super::Buffer;

#[derive(Debug)]
pub struct Connection {
    buffer: Buffer,
    shards: Vec<Server>,
}
