use crate::{
    frontend::cache::integration::CacheMiss,
    net::{messages::Protocol, Message},
};

/// Cache context to use in QueryEngineContext.
#[derive(Default)]
pub struct CacheContext {
    pub cache_miss: Option<CacheMiss>,
    pub response_buffer: Vec<Message>,
    pub had_error: bool,
}

impl CacheContext {
    /// Capture a response message for caching.
    pub fn capture_response(&mut self, message: Message) {
        if self.cache_miss.is_some() {
            if message.code() == 'E' {
                self.had_error = true;
            }
            self.response_buffer.push(message);
        }
    }

    /// Reset the cache context for a new query.
    pub fn reset(&mut self) {
        self.cache_miss = None;
        self.response_buffer.clear();
        self.had_error = false;
    }
}
