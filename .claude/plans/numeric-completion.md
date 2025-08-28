# Numeric Type Completion Implementation Plan

## Objective
Fix the numeric type sorting issue in pgdog to ensure numeric values are compared arithmetically rather than lexicographically, enabling proper ORDER BY functionality for NUMERIC columns.

## Current State Summary
- ✅ Numeric type exists using rust_decimal (pgdog/src/net/messages/data_types/numeric.rs)
- ✅ Field::numeric() constructor exists and correctly sets OID 1700
- ✅ Text encoding/decoding works properly
- ✅ Numeric struct has correct Ord/PartialOrd implementations
- ❌ Datum enum uses derived PartialOrd which causes lexicographic comparison
- ❌ Binary encoding returns error (deferred - not blocking)
- ❌ Test `test_sort_buffer_with_numeric` fails due to incorrect sorting

## Root Cause
The `Datum` enum in `pgdog/src/net/messages/data_types/mod.rs` derives `PartialOrd`, which performs structural comparison rather than semantic comparison. When comparing `Datum::Numeric(a)` with `Datum::Numeric(b)`, it should compare the contained Numeric values, not use the default enum ordering.

## Implementation Steps

### 1. Remove derived PartialOrd and Ord from Datum enum
**File:** `pgdog/src/net/messages/data_types/mod.rs`
- Line 30: Change from `#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]` 
- To: `#[derive(Debug, Clone, PartialEq, Eq, Hash)]`

### 2. Implement custom PartialOrd for Datum
**File:** `pgdog/src/net/messages/data_types/mod.rs`
- Add after the Datum enum definition (around line 139):
```rust
impl PartialOrd for Datum {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use Datum::*;
        
        match (self, other) {
            // Null handling
            (Null, Null) => Some(Ordering::Equal),
            (Null, _) => Some(Ordering::Less),
            (_, Null) => Some(Ordering::Greater),
            
            // Same type comparisons
            (Bigint(a), Bigint(b)) => a.partial_cmp(b),
            (Integer(a), Integer(b)) => a.partial_cmp(b),
            (SmallInt(a), SmallInt(b)) => a.partial_cmp(b),
            (Text(a), Text(b)) => a.partial_cmp(b),
            (Boolean(a), Boolean(b)) => a.partial_cmp(b),
            (Timestamp(a), Timestamp(b)) => a.partial_cmp(b),
            (TimestampTz(a), TimestampTz(b)) => a.partial_cmp(b),
            (Numeric(a), Numeric(b)) => a.partial_cmp(b),
            (Interval(a), Interval(b)) => a.partial_cmp(b),
            (Uuid(a), Uuid(b)) => a.partial_cmp(b),
            (Vector(a), Vector(b)) => a.partial_cmp(b),
            (Unknown(a), Unknown(b)) => a.partial_cmp(b),
            
            // Cross-type numeric comparisons (optional, for compatibility)
            (Integer(a), Bigint(b)) => (*a as i64).partial_cmp(b),
            (Bigint(a), Integer(b)) => a.partial_cmp(&(*b as i64)),
            (SmallInt(a), Integer(b)) => (*a as i32).partial_cmp(b),
            (Integer(a), SmallInt(b)) => a.partial_cmp(&(*b as i32)),
            (SmallInt(a), Bigint(b)) => (*a as i64).partial_cmp(b),
            (Bigint(a), SmallInt(b)) => a.partial_cmp(&(*b as i64)),
            
            // Different types - no natural ordering
            _ => None,
        }
    }
}
```

### 3. Implement Ord for Datum
**File:** `pgdog/src/net/messages/data_types/mod.rs`
- Add after the PartialOrd implementation:
```rust
impl Ord for Datum {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}
```

### 4. Add necessary imports
**File:** `pgdog/src/net/messages/data_types/mod.rs`
- Ensure `use std::cmp::Ordering;` is present at the top of the file

## Testing Strategy

1. **Run the existing test:**
   ```bash
   cargo nextest run test_sort_buffer_with_numeric
   ```
   Expected: Test should pass with numeric values sorted in correct descending order:
   - 1000.01, 1000.00, 199.99, 199.98, 75.50, 50.25, 0.99

2. **Verify no regression:**
   ```bash
   cargo nextest run --test-threads=1
   ```
   Expected: All existing tests should continue to pass

3. **Format code:**
   ```bash
   cargo fmt
   ```

## Success Criteria

✅ Test `test_sort_buffer_with_numeric` passes  
✅ Numeric values sort arithmetically (1000.01 > 199.99)  
✅ No regression in existing tests  
✅ Code compiles without warnings  
✅ Code is properly formatted

## Notes
- Binary encoding for Numeric type is intentionally left incomplete (returns error) as it's not blocking current functionality
- NaN support is deferred as documented in the code comments
- The implementation leverages rust_decimal's Ord implementation for precise numeric comparison