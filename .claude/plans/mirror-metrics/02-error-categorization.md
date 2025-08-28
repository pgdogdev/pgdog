# Sub-Plan 02: Error Categorization

## Objective
Add error type tracking and categorization logic to properly classify and count different types of mirror errors (connection, query, timeout, buffer_full).

## Dependencies
- Sub-plan 01: Core Stats Structure (must be completed first)

## Failing Tests to Write First

```rust
// pgdog/src/stats/mirror.rs (add to test module)
#[cfg(test)]
mod error_tests {
    use super::*;
    
    #[test]
    fn test_error_type_enum() {
        // Ensure all error types are defined
        let _ = MirrorErrorType::Connection;
        let _ = MirrorErrorType::Query;
        let _ = MirrorErrorType::Timeout;
        let _ = MirrorErrorType::BufferFull;
    }
    
    #[test]
    fn test_record_error_by_type() {
        let stats = MirrorStats::default();
        
        stats.record_error("db1", MirrorErrorType::Connection);
        assert_eq!(stats.errors_connection.load(Ordering::Relaxed), 1);
        assert_eq!(stats.errors_query.load(Ordering::Relaxed), 0);
        
        stats.record_error("db1", MirrorErrorType::Query);
        assert_eq!(stats.errors_query.load(Ordering::Relaxed), 1);
        
        stats.record_error("db1", MirrorErrorType::Timeout);
        assert_eq!(stats.errors_timeout.load(Ordering::Relaxed), 1);
        
        stats.record_error("db1", MirrorErrorType::BufferFull);
        assert_eq!(stats.errors_buffer_full.load(Ordering::Relaxed), 1);
    }
    
    #[test]
    fn test_consecutive_errors_tracking() {
        let stats = MirrorStats::default();
        
        stats.record_error("db1", MirrorErrorType::Connection);
        assert_eq!(stats.consecutive_errors.load(Ordering::Relaxed), 1);
        
        stats.record_error("db1", MirrorErrorType::Query);
        assert_eq!(stats.consecutive_errors.load(Ordering::Relaxed), 2);
        
        stats.record_success("db1", 100);
        assert_eq!(stats.consecutive_errors.load(Ordering::Relaxed), 0); // Reset on success
        
        stats.record_error("db1", MirrorErrorType::Timeout);
        assert_eq!(stats.consecutive_errors.load(Ordering::Relaxed), 1);
    }
    
    #[test]
    fn test_last_error_timestamp() {
        let stats = MirrorStats::default();
        
        assert!(stats.last_error.read().is_none());
        
        let before = Instant::now();
        std::thread::sleep(Duration::from_millis(10));
        stats.record_error("db1", MirrorErrorType::Connection);
        let after = Instant::now();
        
        let last_error = stats.last_error.read().unwrap();
        assert!(last_error > before);
        assert!(last_error < after);
    }
    
    #[test]
    fn test_database_error_stats() {
        let stats = MirrorStats::default();
        
        stats.record_error("db1", MirrorErrorType::Connection);
        stats.record_error("db1", MirrorErrorType::Query);
        stats.record_error("db2", MirrorErrorType::Timeout);
        
        let db1_stats = stats.database_stats.get("db1").unwrap();
        assert_eq!(db1_stats.errors.load(Ordering::Relaxed), 2);
        
        let db2_stats = stats.database_stats.get("db2").unwrap();
        assert_eq!(db2_stats.errors.load(Ordering::Relaxed), 1);
    }
    
    #[test]
    fn test_categorize_error_from_string() {
        assert_eq!(categorize_error("connection refused"), MirrorErrorType::Connection);
        assert_eq!(categorize_error("connection reset by peer"), MirrorErrorType::Connection);
        assert_eq!(categorize_error("no route to host"), MirrorErrorType::Connection);
        assert_eq!(categorize_error("broken pipe"), MirrorErrorType::Connection);
        
        assert_eq!(categorize_error("syntax error at or near"), MirrorErrorType::Query);
        assert_eq!(categorize_error("column does not exist"), MirrorErrorType::Query);
        assert_eq!(categorize_error("relation does not exist"), MirrorErrorType::Query);
        assert_eq!(categorize_error("permission denied"), MirrorErrorType::Query);
        
        assert_eq!(categorize_error("operation timed out"), MirrorErrorType::Timeout);
        assert_eq!(categorize_error("timeout expired"), MirrorErrorType::Timeout);
        assert_eq!(categorize_error("deadline exceeded"), MirrorErrorType::Timeout);
        
        assert_eq!(categorize_error("buffer full"), MirrorErrorType::BufferFull);
        assert_eq!(categorize_error("channel full"), MirrorErrorType::BufferFull);
        assert_eq!(categorize_error("queue capacity exceeded"), MirrorErrorType::BufferFull);
    }
    
    #[test]
    fn test_categorize_error_from_error_type() {
        use std::io::{Error, ErrorKind};
        
        let connection_err = Error::new(ErrorKind::ConnectionRefused, "test");
        assert_eq!(categorize_io_error(&connection_err), MirrorErrorType::Connection);
        
        let timeout_err = Error::new(ErrorKind::TimedOut, "test");
        assert_eq!(categorize_io_error(&timeout_err), MirrorErrorType::Timeout);
        
        let broken_pipe = Error::new(ErrorKind::BrokenPipe, "test");
        assert_eq!(categorize_io_error(&broken_pipe), MirrorErrorType::Connection);
    }
    
    #[test]
    fn test_total_errors_calculation() {
        let stats = MirrorStats::default();
        
        stats.record_error("db1", MirrorErrorType::Connection);
        stats.record_error("db1", MirrorErrorType::Query);
        stats.record_error("db1", MirrorErrorType::Timeout);
        stats.record_error("db1", MirrorErrorType::BufferFull);
        
        assert_eq!(stats.total_errors(), 4);
    }
    
    #[test]
    fn test_error_rate_calculation() {
        let stats = MirrorStats::default();
        
        // No requests yet
        assert_eq!(stats.error_rate(), 0.0);
        
        // Add some successful requests
        for _ in 0..95 {
            stats.increment_total();
            stats.record_success("db1", 50);
        }
        
        // Add some errors
        for _ in 0..5 {
            stats.increment_total();
            stats.record_error("db1", MirrorErrorType::Query);
        }
        
        assert!((stats.error_rate() - 0.05).abs() < 0.001); // ~5% error rate
    }
}
```

## Implementation Steps

1. **Define the MirrorErrorType enum**
   ```rust
   #[derive(Debug, Clone, Copy, PartialEq, Eq)]
   pub enum MirrorErrorType {
       Connection,
       Query,
       Timeout,
       BufferFull,
   }
   ```

2. **Implement record_error method**
   ```rust
   impl MirrorStats {
       pub fn record_error(&self, database: &str, error_type: MirrorErrorType) {
           self.requests_total.fetch_add(1, Ordering::Relaxed);
           
           match error_type {
               MirrorErrorType::Connection => {
                   self.errors_connection.fetch_add(1, Ordering::Relaxed);
               }
               MirrorErrorType::Query => {
                   self.errors_query.fetch_add(1, Ordering::Relaxed);
               }
               MirrorErrorType::Timeout => {
                   self.errors_timeout.fetch_add(1, Ordering::Relaxed);
               }
               MirrorErrorType::BufferFull => {
                   self.errors_buffer_full.fetch_add(1, Ordering::Relaxed);
               }
           }
           
           // Update database-specific error count
           self.database_stats
               .entry(database.to_string())
               .or_insert_with(DatabaseMirrorStats::default)
               .errors.fetch_add(1, Ordering::Relaxed);
           
           // Track consecutive errors
           self.consecutive_errors.fetch_add(1, Ordering::Relaxed);
           
           // Update last error timestamp
           *self.last_error.write() = Some(Instant::now());
       }
   }
   ```

3. **Implement error categorization functions**
   ```rust
   pub fn categorize_error(error_msg: &str) -> MirrorErrorType {
       let error_lower = error_msg.to_lowercase();
       
       // Connection errors
       if error_lower.contains("connection refused")
           || error_lower.contains("connection reset")
           || error_lower.contains("no route to host")
           || error_lower.contains("broken pipe")
           || error_lower.contains("connection closed") {
           return MirrorErrorType::Connection;
       }
       
       // Query errors
       if error_lower.contains("syntax error")
           || error_lower.contains("does not exist")
           || error_lower.contains("permission denied")
           || error_lower.contains("invalid")
           || error_lower.contains("violation") {
           return MirrorErrorType::Query;
       }
       
       // Timeout errors
       if error_lower.contains("timeout")
           || error_lower.contains("timed out")
           || error_lower.contains("deadline exceeded") {
           return MirrorErrorType::Timeout;
       }
       
       // Buffer full errors
       if error_lower.contains("buffer full")
           || error_lower.contains("channel full")
           || error_lower.contains("queue")
           || error_lower.contains("capacity") {
           return MirrorErrorType::BufferFull;
       }
       
       // Default to query error
       MirrorErrorType::Query
   }
   
   pub fn categorize_io_error(error: &std::io::Error) -> MirrorErrorType {
       use std::io::ErrorKind;
       
       match error.kind() {
           ErrorKind::ConnectionRefused 
           | ErrorKind::ConnectionReset
           | ErrorKind::ConnectionAborted
           | ErrorKind::NotConnected
           | ErrorKind::BrokenPipe => MirrorErrorType::Connection,
           
           ErrorKind::TimedOut => MirrorErrorType::Timeout,
           
           ErrorKind::PermissionDenied => MirrorErrorType::Query,
           
           _ => {
               // Fall back to string categorization
               categorize_error(&error.to_string())
           }
       }
   }
   ```

4. **Add helper methods for error statistics**
   ```rust
   impl MirrorStats {
       pub fn total_errors(&self) -> u64 {
           self.errors_connection.load(Ordering::Relaxed)
               + self.errors_query.load(Ordering::Relaxed)
               + self.errors_timeout.load(Ordering::Relaxed)
               + self.errors_buffer_full.load(Ordering::Relaxed)
       }
       
       pub fn error_rate(&self) -> f64 {
           let total = self.requests_total.load(Ordering::Relaxed);
           if total == 0 {
               return 0.0;
           }
           
           let errors = self.total_errors();
           errors as f64 / total as f64
       }
   }
   ```

5. **Update record_success to ensure consecutive_errors resets**
   ```rust
   // In record_success method, ensure this line exists:
   self.consecutive_errors.store(0, Ordering::Relaxed);
   ```

## Success Criteria

- [ ] All error categorization tests pass
- [ ] Error types correctly identified from string messages
- [ ] Error types correctly identified from IO errors  
- [ ] Consecutive error tracking works properly
- [ ] Error timestamps updated correctly
- [ ] Database-specific error counts maintained
- [ ] Total errors and error rate calculations correct
- [ ] Thread-safe error recording
- [ ] `cargo check` shows no warnings

## Notes

- Build upon the structure from sub-plan 01
- Focus only on error tracking logic
- Don't integrate with actual mirror code yet
- Keep categorization logic simple but extensible
- Prepare for integration in sub-plan 03