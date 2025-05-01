use std::{
    collections::{BinaryHeap, HashMap},
    sync::Arc,
};

use once_cell::sync::Lazy;
use tokio::time::Instant;

use crate::net::{Parse, Query};

use super::{PlanRequest, QueryPlan};

static BEGINNING_OF_TIME: Lazy<Instant> = Lazy::new(Instant::now);

#[derive(Debug, Clone)]
pub struct PlanCache {
    cache: HashMap<Key, QueryPlan>,
    age: BinaryHeap<AgedKey>,
}

impl PlanCache {
    pub(crate) fn new() -> Self {
        Self {
            cache: HashMap::new(),
            age: BinaryHeap::new(),
        }
    }

    pub(crate) fn insert(&mut self, key: impl Into<Key> + Clone, plan: QueryPlan) {
        self.age.push(AgedKey::new(key.clone()));
        self.cache.insert(key.into(), plan);
    }
}

#[derive(PartialEq, Debug, Clone, Eq, Hash)]
pub struct Key {
    key: Arc<String>,
}

impl From<&Query> for Key {
    fn from(value: &Query) -> Self {
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

impl From<PlanRequest> for Key {
    fn from(value: PlanRequest) -> Self {
        match value {
            PlanRequest::Query(ref query) => query.into(),
            PlanRequest::Prepared { ref parse, .. } => parse.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AgedKey {
    age: i64,
    key: Key,
}

impl AgedKey {
    pub(crate) fn new(key: impl Into<Key>) -> Self {
        let beginning_of_time = *BEGINNING_OF_TIME;
        let age = Instant::now().duration_since(beginning_of_time).as_millis() as i64;
        let age = -age; // Queries made earlier need to have a bigger number for the max heap.

        Self {
            age,
            key: key.into(),
        }
    }
}

impl Ord for AgedKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.age.cmp(&other.age)
    }
}

impl PartialOrd for AgedKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for AgedKey {
    fn eq(&self, other: &Self) -> bool {
        self.key.eq(&other.key)
    }
}

impl Eq for AgedKey {}
