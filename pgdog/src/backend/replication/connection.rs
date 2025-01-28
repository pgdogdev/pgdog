use crate::backend::{pool::Guard, Server};

#[derive(Debug)]
pub struct Connection {
    shards: Vec<Server>,
}
