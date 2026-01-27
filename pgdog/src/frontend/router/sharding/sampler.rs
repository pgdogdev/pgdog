use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use dashmap::DashMap;
use once_cell::sync::Lazy;
use parking_lot::Mutex;

#[derive(Debug, Clone)]
pub struct Sampler {
    keys: HashMap<Arc<String>, usize>,
    usage: HashMap<usize, HashSet<Arc<String>>>,
    min_usage: usize,
}

impl Sampler {
    pub(crate) fn sample(&mut self, key: impl ToString) {
        let key = Arc::new(key.to_string());
        if let Some(usage) = self.keys.get(&key) {
            if let Some(entry) = self.usage.get_mut(usage) {
                entry.remove(&key);
            }

            if self.min_usage == *usage {
                self.min_usage += 1;
            }

            let usage = *usage + 1;
            let entry = self.usage.entry(usage).or_default();
            entry.insert(key);
        } else {
        }
    }
}
