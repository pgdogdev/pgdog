use crate::net::Message;

/// Cache context to use in QueryEngineContext.
#[derive(Default)]
pub struct CacheContext {
    pub cache_miss: Option<(u64, Option<u64>)>,
    pub response_buffer: Vec<Message>,
}

impl CacheContext {
    /// Capture a response message for caching.
    pub fn capture_response(&mut self, message: Message) {
        if self.cache_miss.is_some() {
            self.response_buffer.push(message);
        }
    }
}
