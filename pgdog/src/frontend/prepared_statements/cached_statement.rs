use crate::stats::memory::MemoryUsage;

pub(crate) type Counter = usize;

pub(crate) fn global_name(counter: Counter) -> String {
    format!("__pgdog_{}", counter)
}

#[derive(Debug, Copy, Clone)]
pub struct CachedStmt {
    pub counter: Counter,
    pub used: usize,
}

impl MemoryUsage for CachedStmt {
    #[inline]
    fn memory_usage(&self) -> usize {
        self.counter.memory_usage() + self.used.memory_usage()
    }
}

impl CachedStmt {
    pub fn name(&self) -> String {
        global_name(self.counter)
    }
}
