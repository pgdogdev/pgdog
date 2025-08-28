# Sub-Plan 06: Migration Readiness Calculator

## Objective
Implement the migration readiness assessment algorithm that analyzes mirror metrics to determine if it's safe to perform a database migration, with confidence scoring and detailed reasoning.

## Dependencies
- Sub-plan 01: Core Stats Structure (completed)
- Sub-plan 02: Error Categorization (completed)
- Sub-plan 03: Mirror Integration (completed)
- Sub-plan 04: OpenMetrics Implementation (completed)
- Sub-plan 05: HTTP Endpoint (completed)

## Failing Tests to Write First

```rust
// pgdog/src/stats/mirror_readiness.rs (new test module)
#[cfg(test)]
mod test {
    use super::*;
    use crate::stats::mirror::{MirrorStats, MirrorErrorType};
    
    #[test]
    fn test_readiness_criteria_defaults() {
        let criteria = ReadinessCriteria::default();
        
        assert_eq!(criteria.max_error_rate, 0.01);  // 1%
        assert_eq!(criteria.max_latency_ms, 1000.0);  // 1 second
        assert_eq!(criteria.min_runtime_hours, 24.0);  // 24 hours
        assert_eq!(criteria.min_requests, 100_000);  // 100k requests
    }
    
    #[test]
    fn test_readiness_metrics_calculation() {
        let stats = MirrorStats::default();
        
        // Simulate traffic
        for _ in 0..95 {
            stats.increment_total();
            stats.record_success("test_db", 50);
        }
        for _ in 0..5 {
            stats.increment_total();
            stats.record_error("test_db", MirrorErrorType::Query);
        }
        
        let metrics = ReadinessMetrics::from_stats(&stats);
        
        assert!((metrics.error_rate - 0.05).abs() < 0.001);  // 5% error rate
        assert!((metrics.success_rate - 0.95).abs() < 0.001);  // 95% success rate
        assert_eq!(metrics.avg_latency_ms, 50.0);
        assert_eq!(metrics.total_requests, 100);
    }
    
    #[test]
    fn test_readiness_passes_all_criteria() {
        let stats = MirrorStats::default();
        
        // Simulate good traffic
        for _ in 0..100_000 {
            stats.increment_total();
            stats.record_success("test_db", 100);
        }
        
        let criteria = ReadinessCriteria {
            max_error_rate: 0.01,
            max_latency_ms: 200.0,
            min_runtime_hours: 0.0,  // For testing
            min_requests: 100_000,
        };
        
        let readiness = stats.calculate_readiness(&criteria);
        
        assert!(readiness.ready);
        assert_eq!(readiness.confidence_score, 100.0);
        assert!(readiness.reasons.contains(&"All migration criteria met".to_string()));
    }
    
    #[test]
    fn test_readiness_fails_error_rate() {
        let stats = MirrorStats::default();
        
        // Simulate high error rate (10%)
        for _ in 0..90 {
            stats.increment_total();
            stats.record_success("test_db", 50);
        }
        for _ in 0..10 {
            stats.increment_total();
            stats.record_error("test_db", MirrorErrorType::Connection);
        }
        
        let criteria = ReadinessCriteria {
            max_error_rate: 0.05,  // 5% max
            max_latency_ms: 1000.0,
            min_runtime_hours: 0.0,
            min_requests: 100,
        };
        
        let readiness = stats.calculate_readiness(&criteria);
        
        assert!(!readiness.ready);
        assert!(readiness.confidence_score < 100.0);
        assert!(readiness.reasons.iter().any(|r| r.contains("Error rate")));
        assert!(readiness.reasons.iter().any(|r| r.contains("10.00%")));
    }
    
    #[test]
    fn test_readiness_fails_latency() {
        let stats = MirrorStats::default();
        
        // Simulate high latency
        for _ in 0..1000 {
            stats.increment_total();
            stats.record_success("test_db", 2000);  // 2 seconds
        }
        
        let criteria = ReadinessCriteria {
            max_error_rate: 0.01,
            max_latency_ms: 1000.0,  // 1 second max
            min_runtime_hours: 0.0,
            min_requests: 1000,
        };
        
        let readiness = stats.calculate_readiness(&criteria);
        
        assert!(!readiness.ready);
        assert!(readiness.reasons.iter().any(|r| r.contains("Average latency")));
        assert!(readiness.reasons.iter().any(|r| r.contains("2000ms")));
    }
    
    #[test]
    fn test_readiness_fails_min_requests() {
        let stats = MirrorStats::default();
        
        // Only 100 requests
        for _ in 0..100 {
            stats.increment_total();
            stats.record_success("test_db", 50);
        }
        
        let criteria = ReadinessCriteria {
            max_error_rate: 0.01,
            max_latency_ms: 1000.0,
            min_runtime_hours: 0.0,
            min_requests: 10_000,  // Requires 10k
        };
        
        let readiness = stats.calculate_readiness(&criteria);
        
        assert!(!readiness.ready);
        assert!(readiness.reasons.iter().any(|r| r.contains("Total requests")));
        assert!(readiness.reasons.iter().any(|r| r.contains("100 below minimum 10000")));
    }
    
    #[test]
    fn test_confidence_score_calculation() {
        let stats = MirrorStats::default();
        
        // Mixed results
        for _ in 0..900 {
            stats.increment_total();
            stats.record_success("test_db", 500);  // 500ms latency
        }
        for _ in 0..100 {
            stats.increment_total();
            stats.record_error("test_db", MirrorErrorType::Query);
        }
        
        let criteria = ReadinessCriteria {
            max_error_rate: 0.15,  // 15% (passing)
            max_latency_ms: 600.0,  // 600ms (passing)
            min_runtime_hours: 0.0,
            min_requests: 500,  // (passing)
        };
        
        let readiness = stats.calculate_readiness(&criteria);
        
        // Should pass but with reduced confidence
        assert!(readiness.ready);
        assert!(readiness.confidence_score < 100.0);
        assert!(readiness.confidence_score > 50.0);
    }
    
    #[test]
    fn test_readiness_from_config() {
        use crate::config::General;
        
        let mut general = General::default();
        general.mirror_max_error_rate = 0.02;
        general.mirror_max_latency_ms = 2000;
        general.mirror_min_runtime_hours = 48;
        general.mirror_min_requests = 50_000;
        
        let criteria = ReadinessCriteria::from_config(&general);
        
        assert_eq!(criteria.max_error_rate, 0.02);
        assert_eq!(criteria.max_latency_ms, 2000.0);
        assert_eq!(criteria.min_runtime_hours, 48.0);
        assert_eq!(criteria.min_requests, 50_000);
    }
}

// pgdog/src/admin/mirror_readiness.rs (test module)
#[cfg(test)]
mod test {
    use super::*;
    
    #[tokio::test]
    async fn test_show_mirror_readiness_format() {
        let stats = MirrorStats::instance();
        
        // Add some test data
        for _ in 0..1000 {
            stats.increment_total();
            stats.record_success("test_db", 100);
        }
        
        let output = show_mirror_readiness().await.unwrap();
        
        // Check output format
        assert!(output.contains("=== Mirror Migration Readiness Report ==="));
        assert!(output.contains("Status:"));
        assert!(output.contains("Confidence Score:"));
        assert!(output.contains("Metrics:"));
        assert!(output.contains("Error Rate:"));
        assert!(output.contains("Success Rate:"));
        assert!(output.contains("Avg Latency:"));
        assert!(output.contains("Assessment:"));
    }
    
    #[tokio::test]
    async fn test_readiness_admin_command() {
        let stats = MirrorStats::instance();
        
        // Simulate sufficient traffic for readiness
        for _ in 0..100_000 {
            stats.increment_total();
            stats.record_success("prod_db", 50);
        }
        
        let output = show_mirror_readiness().await.unwrap();
        
        assert!(output.contains("Status: READY"));
        assert!(output.contains("100.0%"));  // confidence
        assert!(output.contains("All migration criteria met"));
    }
}
```

## Implementation Steps

1. **Create readiness module**
   
   File: `pgdog/src/stats/mirror_readiness.rs` (new file)
   
   ```rust
   use crate::stats::mirror::MirrorStats;
   use crate::config::General;
   use std::sync::atomic::Ordering;
   
   #[derive(Debug, Clone)]
   pub struct ReadinessCriteria {
       pub max_error_rate: f64,
       pub max_latency_ms: f64,
       pub min_runtime_hours: f64,
       pub min_requests: u64,
   }
   
   impl Default for ReadinessCriteria {
       fn default() -> Self {
           Self {
               max_error_rate: 0.01,     // 1%
               max_latency_ms: 1000.0,   // 1 second
               min_runtime_hours: 24.0,  // 24 hours
               min_requests: 100_000,    // 100k requests
           }
       }
   }
   
   impl ReadinessCriteria {
       pub fn from_config(config: &General) -> Self {
           Self {
               max_error_rate: config.mirror_max_error_rate,
               max_latency_ms: config.mirror_max_latency_ms as f64,
               min_runtime_hours: config.mirror_min_runtime_hours as f64,
               min_requests: config.mirror_min_requests,
           }
       }
   }
   
   #[derive(Debug)]
   pub struct MigrationReadiness {
       pub ready: bool,
       pub confidence_score: f64,
       pub reasons: Vec<String>,
       pub metrics: ReadinessMetrics,
   }
   
   #[derive(Debug)]
   pub struct ReadinessMetrics {
       pub error_rate: f64,
       pub avg_latency_ms: f64,
       pub success_rate: f64,
       pub uptime_hours: f64,
       pub total_requests: u64,
       pub consecutive_successes: u64,
   }
   
   impl ReadinessMetrics {
       pub fn from_stats(stats: &MirrorStats) -> Self {
           let total = stats.requests_total.load(Ordering::Relaxed);
           let mirrored = stats.requests_mirrored.load(Ordering::Relaxed);
           let errors = stats.total_errors();
           
           let error_rate = if total > 0 {
               errors as f64 / total as f64
           } else {
               0.0
           };
           
           let success_rate = if total > 0 {
               (mirrored.saturating_sub(errors)) as f64 / total as f64
           } else {
               0.0
           };
           
           let avg_latency_ms = if stats.latency_count.load(Ordering::Relaxed) > 0 {
               stats.latency_sum_ms.load(Ordering::Relaxed) as f64 
                   / stats.latency_count.load(Ordering::Relaxed) as f64
           } else {
               0.0
           };
           
           let uptime_hours = stats.last_success.read().elapsed().as_secs() as f64 / 3600.0;
           
           Self {
               error_rate,
               avg_latency_ms,
               success_rate,
               uptime_hours,
               total_requests: total,
               consecutive_successes: mirrored.saturating_sub(errors),
           }
       }
   }
   ```

2. **Implement readiness calculation**
   
   File: `pgdog/src/stats/mirror_readiness.rs` (continue)
   
   ```rust
   impl MirrorStats {
       pub fn calculate_readiness(&self, criteria: &ReadinessCriteria) -> MigrationReadiness {
           let metrics = ReadinessMetrics::from_stats(self);
           
           let mut ready = true;
           let mut reasons = vec![];
           let mut confidence_score = 100.0;
           
           // Check error rate (30% weight)
           if metrics.error_rate > criteria.max_error_rate {
               ready = false;
               confidence_score -= 30.0;
               reasons.push(format!(
                   "Error rate {:.2}% exceeds threshold {:.2}%",
                   metrics.error_rate * 100.0,
                   criteria.max_error_rate * 100.0
               ));
           } else if metrics.error_rate > criteria.max_error_rate * 0.5 {
               // Reduce confidence if error rate is > 50% of threshold
               confidence_score -= 10.0;
           }
           
           // Check latency (20% weight)
           if metrics.avg_latency_ms > criteria.max_latency_ms {
               ready = false;
               confidence_score -= 20.0;
               reasons.push(format!(
                   "Average latency {:.0}ms exceeds threshold {:.0}ms",
                   metrics.avg_latency_ms,
                   criteria.max_latency_ms
               ));
           } else if metrics.avg_latency_ms > criteria.max_latency_ms * 0.75 {
               // Reduce confidence if latency is > 75% of threshold
               confidence_score -= 5.0;
           }
           
           // Check minimum runtime (25% weight)
           if metrics.uptime_hours < criteria.min_runtime_hours {
               ready = false;
               confidence_score -= 25.0;
               reasons.push(format!(
                   "Runtime {:.1} hours below minimum {} hours",
                   metrics.uptime_hours,
                   criteria.min_runtime_hours
               ));
           }
           
           // Check minimum requests (25% weight)
           if metrics.total_requests < criteria.min_requests {
               ready = false;
               confidence_score -= 25.0;
               reasons.push(format!(
                   "Total requests {} below minimum {}",
                   metrics.total_requests,
                   criteria.min_requests
               ));
           } else if metrics.total_requests < criteria.min_requests * 2 {
               // Reduce confidence if requests are < 2x minimum
               confidence_score -= 5.0;
           }
           
           // Check for recent errors
           let consecutive_errors = self.consecutive_errors.load(Ordering::Relaxed);
           if consecutive_errors > 100 {
               confidence_score -= 10.0;
               reasons.push(format!(
                   "High consecutive error count: {}",
                   consecutive_errors
               ));
           }
           
           if ready && reasons.is_empty() {
               reasons.push("All migration criteria met".to_string());
           }
           
           MigrationReadiness {
               ready,
               confidence_score: confidence_score.max(0.0),
               reasons,
               metrics,
           }
       }
   }
   ```

3. **Add to stats module**
   
   File: `pgdog/src/stats/mod.rs`
   
   ```rust
   pub mod mirror_readiness;
   
   pub use mirror_readiness::{
       MigrationReadiness,
       ReadinessCriteria,
       ReadinessMetrics,
   };
   ```

4. **Create admin command**
   
   File: `pgdog/src/admin/mirror_readiness.rs` (new file)
   
   ```rust
   use crate::stats::mirror::MirrorStats;
   use crate::stats::mirror_readiness::ReadinessCriteria;
   use crate::config::config;
   use crate::admin::error::Error;
   
   pub async fn show_mirror_readiness() -> Result<String, Error> {
       let stats = MirrorStats::instance();
       let criteria = ReadinessCriteria::from_config(&config().config.general);
       let readiness = stats.calculate_readiness(&criteria);
       
       let mut output = String::new();
       output.push_str("=== Mirror Migration Readiness Report ===\n\n");
       
       // Status line with color coding (if terminal supports it)
       output.push_str(&format!("Status: {}\n", 
           if readiness.ready { "READY" } else { "NOT READY" }
       ));
       output.push_str(&format!("Confidence Score: {:.1}%\n\n", 
           readiness.confidence_score
       ));
       
       // Metrics section
       output.push_str("Metrics:\n");
       output.push_str(&format!("  Error Rate: {:.3}%\n", 
           readiness.metrics.error_rate * 100.0
       ));
       output.push_str(&format!("  Success Rate: {:.3}%\n", 
           readiness.metrics.success_rate * 100.0
       ));
       output.push_str(&format!("  Avg Latency: {:.0}ms\n", 
           readiness.metrics.avg_latency_ms
       ));
       output.push_str(&format!("  Uptime: {:.1} hours\n", 
           readiness.metrics.uptime_hours
       ));
       output.push_str(&format!("  Total Requests: {}\n", 
           readiness.metrics.total_requests
       ));
       output.push_str(&format!("  Consecutive Successes: {}\n\n", 
           readiness.metrics.consecutive_successes
       ));
       
       // Criteria section
       output.push_str("Criteria:\n");
       output.push_str(&format!("  Max Error Rate: {:.2}%\n", 
           criteria.max_error_rate * 100.0
       ));
       output.push_str(&format!("  Max Latency: {:.0}ms\n", 
           criteria.max_latency_ms
       ));
       output.push_str(&format!("  Min Runtime: {:.0} hours\n", 
           criteria.min_runtime_hours
       ));
       output.push_str(&format!("  Min Requests: {}\n\n", 
           criteria.min_requests
       ));
       
       // Assessment section
       output.push_str("Assessment:\n");
       for reason in &readiness.reasons {
           output.push_str(&format!("  - {}\n", reason));
       }
       
       // Recommendation
       output.push_str("\nRecommendation:\n");
       if readiness.ready && readiness.confidence_score >= 95.0 {
           output.push_str("  ✓ Safe to proceed with migration\n");
       } else if readiness.ready && readiness.confidence_score >= 80.0 {
           output.push_str("  ⚠ Migration possible but review warnings\n");
       } else if readiness.confidence_score >= 60.0 {
           output.push_str("  ⚠ Address issues before migration\n");
       } else {
           output.push_str("  ✗ Do not migrate - significant issues detected\n");
       }
       
       Ok(output)
   }
   ```

5. **Add admin command handler**
   
   File: `pgdog/src/admin/parser.rs` (update existing)
   
   ```rust
   // Add to parse_admin function
   match tokens.next() {
       Some("SHOW") => match tokens.next() {
           Some("MIRROR") => match tokens.next() {
               Some("READINESS") => {
                   return Ok(Command::ShowMirrorReadiness);
               }
               _ => {}
           },
           // ... existing SHOW commands
       },
       // ... existing commands
   }
   
   // Add enum variant
   pub enum Command {
       // ... existing variants
       ShowMirrorReadiness,
   }
   ```

6. **Add configuration fields**
   
   File: `pgdog/src/config/mod.rs`
   
   ```rust
   // Add to General struct
   #[serde(default = "General::mirror_max_error_rate")]
   pub mirror_max_error_rate: f64,
   
   #[serde(default = "General::mirror_max_latency_ms")]
   pub mirror_max_latency_ms: u64,
   
   #[serde(default = "General::mirror_min_runtime_hours")]
   pub mirror_min_runtime_hours: u64,
   
   #[serde(default = "General::mirror_min_requests")]
   pub mirror_min_requests: u64,
   
   // Add default implementations
   fn mirror_max_error_rate() -> f64 {
       0.01  // 1%
   }
   
   fn mirror_max_latency_ms() -> u64 {
       1000  // 1 second
   }
   
   fn mirror_min_runtime_hours() -> u64 {
       24  // 24 hours
   }
   
   fn mirror_min_requests() -> u64 {
       100_000  // 100k requests
   }
   ```

## Success Criteria

- [ ] ReadinessCriteria correctly loads from config or defaults
- [ ] ReadinessMetrics accurately calculates from MirrorStats
- [ ] Confidence score algorithm properly weights different factors
- [ ] Readiness assessment provides clear, actionable reasons
- [ ] Admin command "SHOW MIRROR READINESS" works correctly
- [ ] Output format is clear and informative
- [ ] Edge cases handled (no traffic, all errors, etc.)
- [ ] Configuration values properly used and validated
- [ ] Thread-safe access to stats during calculation
- [ ] `cargo check` shows no warnings

## Notes

- This is the final user-facing feature of the mirror metrics
- Focus on clarity of output and actionable insights
- The confidence score helps users make informed decisions
- Consider future enhancements like historical trending
- Integration tests will validate the complete flow in sub-plan 07