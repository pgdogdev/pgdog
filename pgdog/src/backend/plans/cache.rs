use std::{collections::HashMap, sync::Arc};

use crate::net::{Parse, Query};

use super::QueryPlan;

#[derive(Debug, Clone)]
pub struct PlanCache {
    cache: HashMap<Key, QueryPlan>,
}

#[derive(PartialEq, Debug, Clone, Eq, Hash)]
struct Key {
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
