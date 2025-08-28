# Float/Double Type Separation Plan

## Problem Statement
The current implementation has two critical issues:
1. It incorrectly reuses the `Numeric` type for PostgreSQL's `REAL` (float4) and `DOUBLE PRECISION` (float8) types. While this works in text format (where floats are represented as strings), it's critically incorrect for binary format where these types have completely different representations.
2. The `Numeric` type itself lacks proper support for special values (NaN, +Infinity, -Infinity) which are valid in PostgreSQL's NUMERIC type.

## Type Distinctions

### PostgreSQL Type Mappings
- **NUMERIC** (OID 1700): Exact decimal arithmetic, variable precision
  - Binary format: PostgreSQL's custom decimal format
  - Text format: Decimal string representation
  
- **REAL** (OID 700): 32-bit IEEE 754 floating-point
  - Binary format: 4 bytes, IEEE 754 single precision
  - Text format: Float string representation
  
- **DOUBLE PRECISION** (OID 701): 64-bit IEEE 754 floating-point
  - Binary format: 8 bytes, IEEE 754 double precision
  - Text format: Float string representation

## Current Issues

### 1. Type Confusion in Datum::new
**File:** `pgdog/src/net/messages/data_types/mod.rs:112-113`
```rust
DataType::Numeric | DataType::DoublePrecision | DataType::Real => {
    Ok(Datum::Numeric(Numeric::decode(bytes, encoding)?))
}
```
All three types are incorrectly mapped to `Datum::Numeric`.

### 2. Missing Float/Double Variants in Datum Enum
The `Datum` enum lacks proper variants for floating-point types.

### 3. Incorrect Binary Encoding/Decoding
The `Numeric` type's binary format is completely different from IEEE 754 floats, causing incorrect data interpretation.

## Implementation Plan

### Phase 1: Add Float Type Implementations

#### 1.1 Create float.rs
**File:** `pgdog/src/net/messages/data_types/float.rs`
```rust
use super::*;

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Float(f32);

impl FromDataType for Float {
    fn decode(bytes: &[u8], encoding: Format) -> Result<Self, Error> {
        match encoding {
            Format::Binary => {
                if bytes.len() != 4 {
                    return Err(Error::InvalidDataType);
                }
                let mut buf = bytes;
                Ok(Float(buf.get_f32()))
            }
            Format::Text => {
                let s = std::str::from_utf8(bytes)?;
                Ok(Float(s.parse()?))
            }
        }
    }
    
    fn encode(&self, encoding: Format) -> Result<Bytes, Error> {
        match encoding {
            Format::Binary => {
                let mut bytes = BytesMut::with_capacity(4);
                bytes.put_f32(self.0);
                Ok(bytes.freeze())
            }
            Format::Text => {
                Ok(Bytes::from(self.0.to_string()))
            }
        }
    }
}
```

#### 1.2 Create double.rs
**File:** `pgdog/src/net/messages/data_types/double.rs`
```rust
use super::*;

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Double(f64);

impl FromDataType for Double {
    fn decode(bytes: &[u8], encoding: Format) -> Result<Self, Error> {
        match encoding {
            Format::Binary => {
                if bytes.len() != 8 {
                    return Err(Error::InvalidDataType);
                }
                let mut buf = bytes;
                Ok(Double(buf.get_f64()))
            }
            Format::Text => {
                let s = std::str::from_utf8(bytes)?;
                Ok(Double(s.parse()?))
            }
        }
    }
    
    fn encode(&self, encoding: Format) -> Result<Bytes, Error> {
        match encoding {
            Format::Binary => {
                let mut bytes = BytesMut::with_capacity(8);
                bytes.put_f64(self.0);
                Ok(bytes.freeze())
            }
            Format::Text => {
                Ok(Bytes::from(self.0.to_string()))
            }
        }
    }
}
```

### Phase 2: Update Datum Enum

#### 2.1 Add Float/Double Variants
**File:** `pgdog/src/net/messages/data_types/mod.rs`
```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Datum {
    // ... existing variants ...
    /// REAL (float4)
    Float(Float),
    /// DOUBLE PRECISION (float8)
    Double(Double),
    /// NUMERIC (exact decimal)
    Numeric(Numeric),
    // ... rest ...
}
```

#### 2.2 Update Datum::new
```rust
match data_type {
    // ... existing cases ...
    DataType::Real => Ok(Datum::Float(Float::decode(bytes, encoding)?)),
    DataType::DoublePrecision => Ok(Datum::Double(Double::decode(bytes, encoding)?)),
    DataType::Numeric => Ok(Datum::Numeric(Numeric::decode(bytes, encoding)?)),
    // ... rest ...
}
```

#### 2.3 Update ToDataRowColumn
```rust
match self {
    // ... existing cases ...
    Float(val) => val.to_data_row_column(),
    Double(val) => val.to_data_row_column(),
    Numeric(val) => val.to_data_row_column(),
    // ... rest ...
}
```

### Phase 3: Add Field Constructors

**File:** `pgdog/src/net/messages/row_description.rs`
```rust
impl Field {
    pub fn float(name: &str) -> Self {
        Field {
            name: name.to_string(),
            table_oid: 0,
            column_id: 0,
            type_oid: 700,  // REAL
            type_size: 4,
            type_modifier: -1,
            format: Format::Text,
        }
    }
    
    pub fn double(name: &str) -> Self {
        Field {
            name: name.to_string(),
            table_oid: 0,
            column_id: 0,
            type_oid: 701,  // DOUBLE PRECISION
            type_size: 8,
            type_modifier: -1,
            format: Format::Text,
        }
    }
    
    pub fn float_binary(name: &str) -> Self {
        Field {
            name: name.to_string(),
            table_oid: 0,
            column_id: 0,
            type_oid: 700,
            type_size: 4,
            type_modifier: -1,
            format: Format::Binary,
        }
    }
    
    pub fn double_binary(name: &str) -> Self {
        Field {
            name: name.to_string(),
            table_oid: 0,
            column_id: 0,
            type_oid: 701,
            type_size: 8,
            type_modifier: -1,
            format: Format::Binary,
        }
    }
}
```

### Phase 4: Handle Special Float Values

Both Float and Double types need to handle:
- Infinity (`inf`, `-inf`)
- NaN (Not a Number)
- Scientific notation (e.g., `1.23e-10`)

### Phase 5: Testing

#### 5.1 Unit Tests
- Test binary encoding/decoding for float4 and float8
- Test text parsing for various formats
- Test special values (NaN, Infinity)
- Test sorting behavior with floats vs numerics

#### 5.2 Integration Tests
- Verify compatibility with PostgreSQL wire protocol
- Test COPY operations with float columns
- Test ORDER BY with mixed numeric types

## Migration Considerations

1. **Backward Compatibility**: Existing code using Numeric for floats will break
2. **Performance**: Native float operations will be faster than decimal arithmetic
3. **Precision**: Float/Double have different precision characteristics than Numeric

## Success Criteria

✅ Float and Double types properly decode PostgreSQL binary format  
✅ Text format handles all float representations (including special values)  
✅ Datum enum correctly distinguishes between Float, Double, and Numeric  
✅ All existing tests pass  
✅ New tests verify float/double binary compatibility  
✅ ORDER BY works correctly with all numeric types

## Notes

- IEEE 754 special values (NaN, ±Infinity) need special handling
- PostgreSQL uses network byte order (big-endian) for binary format
- Consider adding conversion methods between Float/Double/Numeric for compatibility