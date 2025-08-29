# Sub-Plan 04: OpenMetrics Implementation

**STATUS: ✅ COMPLETE**
**Completed: 2025-08-28**

OpenMetric trait fully implemented with proper measurements

## Objective
Implement the OpenMetric trait for MirrorStats to expose metrics in Prometheus-compatible format, integrating with PgDog's existing metrics system.

## Dependencies
- Sub-plan 01: Core Stats Structure (completed)
- Sub-plan 02: Error Categorization (completed)  
- Sub-plan 03: Mirror Integration (completed)

## Failing Tests to Write First

```rust
// pgdog/src/stats/mirror.rs (add to test module)
#[cfg(test)]
mod openmetric_tests {
    use super::*;
    use crate::stats::open_metric::{OpenMetric, Measurement};
    
    #[test]
    fn test_mirror_stats_implements_openmetric() {
        let stats = MirrorStats::default();
        
        // Should implement OpenMetric trait
        assert_eq!(stats.name(), "mirror");
        assert_eq!(stats.metric_type(), "gauge");
        assert_eq!(stats.help(), Some("Mirror traffic statistics and health metrics".to_string()));
        assert_eq!(stats.unit(), None);
    }
    
    #[test]
    fn test_measurements_include_request_counters() {
        let stats = MirrorStats::default();
        
        // Add some data
        stats.increment_total();
        stats.increment_total();
        stats.record_success("test_db", 100);
        stats.increment_dropped();
        
        let measurements = stats.measurements();
        
        // Find specific measurements
        let total = measurements.iter()
            .find(|m| m.labels.contains(&("type".into(), "total".into())))
            .expect("Should have total measurement");
        assert_eq!(total.measurement, 2.into());
        
        let mirrored = measurements.iter()
            .find(|m| m.labels.contains(&("type".into(), "mirrored".into())))
            .expect("Should have mirrored measurement");
        assert_eq!(mirrored.measurement, 1.into());
        
        let dropped = measurements.iter()
            .find(|m| m.labels.contains(&("type".into(), "dropped".into())))
            .expect("Should have dropped measurement");
        assert_eq!(dropped.measurement, 1.into());
    }
    
    #[test]
    fn test_measurements_include_error_metrics() {
        let stats = MirrorStats::default();
        
        stats.record_error("db1", MirrorErrorType::Connection);
        stats.record_error("db1", MirrorErrorType::Query);
        stats.record_error("db1", MirrorErrorType::Query);
        
        let measurements = stats.measurements();
        
        let conn_errors = measurements.iter()
            .find(|m| m.labels.contains(&("error_type".into(), "connection".into())))
            .expect("Should have connection error measurement");
        assert_eq!(conn_errors.measurement, 1.into());
        
        let query_errors = measurements.iter()
            .find(|m| m.labels.contains(&("error_type".into(), "query".into())))
            .expect("Should have query error measurement");
        assert_eq!(query_errors.measurement, 2.into());
    }
    
    #[test]
    fn test_measurements_include_latency_metrics() {
        let stats = MirrorStats::default();
        
        stats.record_success("db1", 100);
        stats.record_success("db1", 200);
        stats.record_success("db1", 300);
        
        let measurements = stats.measurements();
        
        let avg_latency = measurements.iter()
            .find(|m| m.labels.contains(&("stat".into(), "avg_latency_ms".into())))
            .expect("Should have average latency measurement");
        assert_eq!(avg_latency.measurement, 200.0.into()); // (100+200+300)/3
        
        let max_latency = measurements.iter()
            .find(|m| m.labels.contains(&("stat".into(), "max_latency_ms".into())))
            .expect("Should have max latency measurement");
        assert_eq!(max_latency.measurement, 300.into());
    }
    
    #[test]
    fn test_measurements_include_health_metrics() {
        let stats = MirrorStats::default();
        
        stats.record_error("db1", MirrorErrorType::Connection);
        stats.record_error("db1", MirrorErrorType::Query);
        
        let measurements = stats.measurements();
        
        let consecutive_errors = measurements.iter()
            .find(|m| m.labels.contains(&("health".into(), "consecutive_errors".into())))
            .expect("Should have consecutive errors measurement");
        assert_eq!(consecutive_errors.measurement, 2.into());
        
        let last_success = measurements.iter()
            .find(|m| m.labels.contains(&("health".into(), "last_success_seconds_ago".into())))
            .expect("Should have last success age measurement");
        // Should be some positive value
        assert!(last_success.measurement.as_integer().unwrap() >= 0);
    }
    
    #[test]
    fn test_measurements_include_per_database_stats() {
        let stats = MirrorStats::default();
        
        stats.record_success("db1", 100);
        stats.record_success("db1", 150);
        stats.record_error("db1", MirrorErrorType::Query);
        stats.record_success("db2", 200);
        
        let measurements = stats.measurements();
        
        let db1_mirrored = measurements.iter()
            .find(|m| {
                m.labels.contains(&("database".into(), "db1".into())) &&
                m.labels.contains(&("metric".into(), "mirrored".into()))
            })
            .expect("Should have db1 mirrored measurement");
        assert_eq!(db1_mirrored.measurement, 2.into());
        
        let db1_errors = measurements.iter()
            .find(|m| {
                m.labels.contains(&("database".into(), "db1".into())) &&
                m.labels.contains(&("metric".into(), "errors".into()))
            })
            .expect("Should have db1 errors measurement");
        assert_eq!(db1_errors.measurement, 1.into());
        
        let db2_mirrored = measurements.iter()
            .find(|m| {
                m.labels.contains(&("database".into(), "db2".into())) &&
                m.labels.contains(&("metric".into(), "mirrored".into()))
            })
            .expect("Should have db2 mirrored measurement");
        assert_eq!(db2_mirrored.measurement, 1.into());
    }
    
    #[test]
    fn test_openmetric_format_output() {
        let stats = MirrorStats::default();
        stats.record_success("test", 100);
        stats.record_error("test", MirrorErrorType::Connection);
        
        // Convert to Metric and format
        let metric = Metric::new(stats);
        let output = metric.to_string();
        
        // Check format includes proper OpenMetrics structure
        assert!(output.contains("# HELP mirror"));
        assert!(output.contains("# TYPE mirror gauge"));
        assert!(output.contains("mirror{type=\"total\"}"));
        assert!(output.contains("mirror{type=\"mirrored\"}"));
        assert!(output.contains("mirror{error_type=\"connection\"}"));
    }
}
```

## Implementation Steps

1. **Import OpenMetric trait and types**
   
   File: `pgdog/src/stats/mirror.rs`
   
   ```rust
   use crate::stats::open_metric::{OpenMetric, Measurement, Value};
   ```

2. **Implement OpenMetric trait for MirrorStats**
   
   File: `pgdog/src/stats/mirror.rs`
   
   ```rust
   impl OpenMetric for MirrorStats {
       fn name(&self) -> String {
           "mirror".to_string()
       }
       
       fn measurements(&self) -> Vec<Measurement> {
           let mut measurements = vec![];
           
           // Request metrics
           measurements.push(Measurement {
               labels: vec![("type".into(), "total".into())],
               measurement: Value::Integer(self.requests_total.load(Ordering::Relaxed) as i64),
           });
           
           measurements.push(Measurement {
               labels: vec![("type".into(), "mirrored".into())],
               measurement: Value::Integer(self.requests_mirrored.load(Ordering::Relaxed) as i64),
           });
           
           measurements.push(Measurement {
               labels: vec![("type".into(), "dropped".into())],
               measurement: Value::Integer(self.requests_dropped.load(Ordering::Relaxed) as i64),
           });
           
           // Error metrics by type
           measurements.push(Measurement {
               labels: vec![("error_type".into(), "connection".into())],
               measurement: Value::Integer(self.errors_connection.load(Ordering::Relaxed) as i64),
           });
           
           measurements.push(Measurement {
               labels: vec![("error_type".into(), "query".into())],
               measurement: Value::Integer(self.errors_query.load(Ordering::Relaxed) as i64),
           });
           
           measurements.push(Measurement {
               labels: vec![("error_type".into(), "timeout".into())],
               measurement: Value::Integer(self.errors_timeout.load(Ordering::Relaxed) as i64),
           });
           
           measurements.push(Measurement {
               labels: vec![("error_type".into(), "buffer_full".into())],
               measurement: Value::Integer(self.errors_buffer_full.load(Ordering::Relaxed) as i64),
           });
           
           // Latency metrics (only if we have data)
           let count = self.latency_count.load(Ordering::Relaxed);
           if count > 0 {
               let sum = self.latency_sum_ms.load(Ordering::Relaxed);
               let avg = sum as f64 / count as f64;
               
               measurements.push(Measurement {
                   labels: vec![("stat".into(), "avg_latency_ms".into())],
                   measurement: Value::Float(avg),
               });
               
               measurements.push(Measurement {
                   labels: vec![("stat".into(), "max_latency_ms".into())],
                   measurement: Value::Integer(self.latency_max_ms.load(Ordering::Relaxed) as i64),
               });
           }
           
           // Health metrics
           measurements.push(Measurement {
               labels: vec![("health".into(), "consecutive_errors".into())],
               measurement: Value::Integer(self.consecutive_errors.load(Ordering::Relaxed) as i64),
           });
           
           let last_success_age = self.last_success.read().elapsed().as_secs();
           measurements.push(Measurement {
               labels: vec![("health".into(), "last_success_seconds_ago".into())],
               measurement: Value::Integer(last_success_age as i64),
           });
           
           // Per-database metrics
           for entry in self.database_stats.iter() {
               let (database, stats) = entry.pair();
               
               measurements.push(Measurement {
                   labels: vec![
                       ("database".into(), database.clone()),
                       ("metric".into(), "mirrored".into()),
                   ],
                   measurement: Value::Integer(stats.mirrored.load(Ordering::Relaxed) as i64),
               });
               
               measurements.push(Measurement {
                   labels: vec![
                       ("database".into(), database.clone()),
                       ("metric".into(), "errors".into()),
                   ],
                   measurement: Value::Integer(stats.errors.load(Ordering::Relaxed) as i64),
               });
               
               // Add average latency if available
               let avg_lat = stats.avg_latency_ms.load(Ordering::Relaxed);
               if avg_lat > 0 {
                   measurements.push(Measurement {
                       labels: vec![
                           ("database".into(), database.clone()),
                           ("metric".into(), "avg_latency_ms".into()),
                       ],
                       measurement: Value::Integer(avg_lat as i64),
                   });
               }
           }
           
           measurements
       }
       
       fn unit(&self) -> Option<String> {
           None
       }
       
       fn metric_type(&self) -> String {
           "gauge".to_string()
       }
       
       fn help(&self) -> Option<String> {
           Some("Mirror traffic statistics and health metrics".to_string())
       }
   }
   ```

3. **Add convenience method for Metric wrapper**
   
   File: `pgdog/src/stats/mirror.rs`
   
   ```rust
   impl MirrorStats {
       /// Create a Metric instance for OpenMetrics formatting
       pub fn as_metric(&self) -> Metric<MirrorStats> {
           Metric::new(self.clone())
       }
   }
   ```

4. **Ensure Clone is implemented for use with Metric**
   
   File: `pgdog/src/stats/mirror.rs`
   
   ```rust
   #[derive(Clone)]
   pub struct MirrorStats {
       // existing fields...
   }
   
   // Or implement manually if needed:
   impl Clone for MirrorStats {
       fn clone(&self) -> Self {
           // Clone is safe for Arc and atomic types
           Self {
               requests_total: AtomicU64::new(self.requests_total.load(Ordering::Relaxed)),
               // ... clone other fields
           }
       }
   }
   ```

5. **Create integration test for metrics endpoint**
   
   File: `integration/mirror/openmetrics_test.sh`
   
   ```bash
   #!/bin/bash
   
   echo "Testing OpenMetrics format for mirror stats..."
   
   # Start PgDog
   cargo build --release
   ./target/release/pgdog --config integration/mirror/pgdog.toml &
   PGDOG_PID=$!
   
   sleep 3
   
   # Generate some mirror traffic
   for i in {1..10}; do
       psql -h localhost -p 6432 -U pgdog -d primary -c "SELECT $i"
   done
   
   sleep 1
   
   # Fetch and validate metrics
   METRICS=$(curl -s http://localhost:9090/metrics)
   
   # Check for mirror metrics in OpenMetrics format
   echo "$METRICS" | grep -E "^# HELP mirror"
   echo "$METRICS" | grep -E "^# TYPE mirror gauge"
   echo "$METRICS" | grep -E "^mirror\{type=\"total\"\}"
   echo "$METRICS" | grep -E "^mirror\{type=\"mirrored\"\}"
   echo "$METRICS" | grep -E "^mirror\{error_type=\".*\"\}"
   
   # Cleanup
   kill $PGDOG_PID
   
   echo "OpenMetrics format test completed"
   ```

6. **Test with Prometheus scraping**
   
   File: `integration/mirror/prometheus_test.yml`
   
   ```yaml
   # Test prometheus config
   global:
     scrape_interval: 5s
   
   scrape_configs:
     - job_name: 'pgdog_mirror_test'
       static_configs:
         - targets: ['localhost:9090']
       metrics_path: /metrics
   ```

## Success Criteria

- [ ] MirrorStats implements OpenMetric trait correctly
- [ ] All metric types are properly exposed (counters, gauges, etc.)
- [ ] Labels are correctly formatted for each metric
- [ ] Per-database metrics appear with proper labels
- [ ] Metrics format is valid OpenMetrics/Prometheus compatible
- [ ] Integration with existing Metric wrapper works
- [ ] Prometheus can successfully scrape the metrics
- [ ] No missing or malformed measurements
- [ ] `cargo check` shows no warnings
- [ ] Performance impact is negligible (< 1ms to generate metrics)

## Notes

- OpenMetric trait is already defined in PgDog's codebase
- Must match existing metric format conventions
- Focus on compatibility with Prometheus
- Keep measurements efficient - this runs on every scrape
- Next sub-plan will integrate with HTTP endpoint