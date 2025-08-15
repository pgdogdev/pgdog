use std::ops::Deref;

use crate::net::{Parse, Query};

#[derive(Debug, Clone)]
pub enum BufferedQuery {
    Query(Query),
    Prepared(Parse),
}

impl BufferedQuery {
    pub fn query(&self) -> &str {
        match self {
            Self::Query(query) => query.query(),
            Self::Prepared(parse) => parse.query(),
        }
    }

    pub fn extended(&self) -> bool {
        matches!(self, Self::Prepared(_))
    }

    pub fn simple(&self) -> bool {
        matches!(self, Self::Query(_))
    }
}

impl Deref for BufferedQuery {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.query()
    }
}
