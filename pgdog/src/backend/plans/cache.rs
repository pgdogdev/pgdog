use std::{collections::HashMap, sync::Arc, time::Instant};

use pg_query::normalize;

use crate::net::{Parse, Query};

use super::{PlanRequest, QueryPlan};

#[derive(Debug, Clone)]
pub struct PlanCache {
    cache: HashMap<Key, Value>,
}

impl PlanCache {
    pub(crate) fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }

    pub(crate) fn insert(&mut self, key: impl Into<Key>, plan: QueryPlan, created_at: Instant) {
        self.cache.insert(
            key.into(),
            Value::Plan {
                plan: Arc::new(plan),
                created_at,
            },
        );
    }

    pub(crate) fn requested(&mut self, key: impl Into<Key>, created_at: Instant) {
        self.cache.insert(key.into(), Value::Request(created_at));
    }

    pub(crate) fn clear(&mut self) {
        self.cache.clear();
    }

    pub(crate) fn remove(&mut self, key: impl Into<Key>) {
        self.cache.remove(&key.into());
    }

    pub(crate) fn get(&self, key: &Key) -> Option<Value> {
        self.cache.get(key).cloned()
    }

    pub(crate) fn get_mut(&mut self, key: &Key) -> Option<&mut Value> {
        self.cache.get_mut(key)
    }
}

#[derive(PartialEq, Debug, Clone, Eq, Hash)]
pub struct Key {
    key: Arc<String>,
}

impl From<&Query> for Key {
    fn from(value: &Query) -> Self {
        let normal = normalize(value.query());
        Key {
            key: Arc::new(value.query().to_string()),
        }
    }
}

impl From<&Parse> for Key {
    fn from(value: &Parse) -> Self {
        Key {
            key: value.query_ref(),
        }
    }
}

impl From<&PlanRequest> for Key {
    fn from(value: &PlanRequest) -> Self {
        match value {
            PlanRequest::Query(query) => query.into(),
            PlanRequest::Prepared { parse, .. } => parse.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Value {
    Plan {
        plan: Arc<QueryPlan>,
        created_at: Instant,
    },
    Request(Instant),
}

impl Value {
    pub(crate) fn age(&mut self, now: Instant) {
        match self {
            Value::Plan { created_at, .. } => *created_at = now,
            _ => (),
        }
    }
}
