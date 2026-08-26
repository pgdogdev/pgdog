use crate::stats::memory::MemoryUsage;

pub(crate) type Counter = usize;

pub(crate) fn global_name(counter: Counter) -> String {
    format!("__pgdog_{}", counter)
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct CachedStmt {
    pub(crate) counter: Counter,
    pub(crate) used: usize,
}

impl MemoryUsage for CachedStmt {
    #[inline]
    fn memory_usage(&self) -> usize {
        self.counter.memory_usage() + self.used.memory_usage()
    }
}

impl CachedStmt {
    pub(crate) fn name(&self) -> String {
        global_name(self.counter)
    }
}
