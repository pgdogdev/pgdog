use crate::net::{Message, PgLsn};

#[derive(Debug, Clone)]
pub enum Event {
    Notify(Message),
    Lsn(PgLsn),
}
