use std::ffi::CStr;

use pg_internals::{
    constants::{
        BLCKSZ, GUC_UNIT, GUC_UNIT_BLOCKS, GUC_UNIT_BYTE, GUC_UNIT_KB, GUC_UNIT_MB, GUC_UNIT_MIN,
        GUC_UNIT_MS, GUC_UNIT_S, GUC_UNIT_XBLOCKS, XLOG_BLCKSZ,
    },
    guc::{Guc, Value},
};

/// PostgreSQL reports range failures using the parsed value in base units.
pub(super) fn range_error(guc: &Guc, value: &str) -> Option<String> {
    let (value, min, max) = match (guc.parse_numeric(value)?, guc.min?, guc.max?) {
        (Value::Int(value), Value::Int(min), Value::Int(max)) if value < min || value > max => {
            (value.to_string(), min.to_string(), max.to_string())
        }
        (Value::Real(value), Value::Real(min), Value::Real(max)) if value < min || value > max => {
            (format_real(value), format_real(min), format_real(max))
        }
        _ => return None,
    };
    let unit = match guc.unit_flags & GUC_UNIT {
        GUC_UNIT_BYTE => " B".into(),
        GUC_UNIT_KB => " kB".into(),
        GUC_UNIT_MB => " MB".into(),
        GUC_UNIT_BLOCKS => format!(" {}kB", BLCKSZ / 1024),
        GUC_UNIT_XBLOCKS => format!(" {}kB", XLOG_BLCKSZ / 1024),
        GUC_UNIT_MS => " ms".into(),
        GUC_UNIT_S => " s".into(),
        GUC_UNIT_MIN => " min".into(),
        _ => String::new(),
    };
    Some(format!(
        "{value}{unit} is outside the valid range for parameter \"{}\" ({min}{unit} .. {max}{unit})",
        guc.name
    ))
}

fn format_real(value: f64) -> String {
    // PostgreSQL spells infinities itself, then uses snprintf("%g") for finite
    // values: six significant digits and scientific notation for large bounds.
    if value.is_infinite() {
        return if value.is_sign_negative() {
            "-Infinity"
        } else {
            "Infinity"
        }
        .into();
    }
    let mut buffer = [0u8; 64];
    // SAFETY: the writable buffer has the supplied size, the format is a static
    // C string, and f64 matches the double required by %g. No pointers escape.
    unsafe {
        libc::snprintf(
            buffer.as_mut_ptr().cast(),
            buffer.len(),
            c"%g".as_ptr(),
            value,
        );
    }
    CStr::from_bytes_until_nul(&buffer)
        .expect("snprintf terminates the nonempty buffer")
        .to_string_lossy()
        .into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn real_format_matches_postgres_diagnostics() {
        for (value, expected) in [
            (10000.0, "10000"),
            (100.125, "100.125"),
            (1.23456789, "1.23457"),
            (1e6, "1e+06"),
            (1e-5, "1e-05"),
            (f64::MAX, "1.79769e+308"),
            (f64::INFINITY, "Infinity"),
            (f64::NEG_INFINITY, "-Infinity"),
        ] {
            assert_eq!(format_real(value), expected);
        }
    }
}
