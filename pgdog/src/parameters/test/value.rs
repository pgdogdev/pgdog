use super::*;
use crate::net::ToBytes;

#[test]
fn test_parameter_value_to_bytes_string() {
    let value = ParameterValue::String("test".into());
    let bytes = value.to_bytes();

    assert_eq!(&bytes[..], b"test\0");
}

#[test]
fn test_parameter_value_to_bytes_tuple() {
    let value = ParameterValue::Tuple(vec!["a".into(), "b".into()]);
    let bytes = value.to_bytes();

    assert_eq!(&bytes[..], b"a, b\0");
}

#[test]
fn test_parameter_value_display_string() {
    let value = ParameterValue::String("test".into());
    assert_eq!(format!("{}", value), r#""test""#);
}

#[test]
fn test_parameter_value_display_tuple() {
    let value = ParameterValue::Tuple(vec!["$user".into(), "public".into()]);
    assert_eq!(format!("{}", value), r#""$user", "public""#);
}

#[test]
fn test_parameter_value_display_already_quoted() {
    // If value is already quoted, it should strip quotes and re-quote
    let value = ParameterValue::String(r#""already quoted""#.into());
    assert_eq!(format!("{}", value), r#""already quoted""#);
}

#[test]
fn test_json_parameter_value() {
    assert_eq!(
            ParameterValue::String(r#"{"sampling_state":"1","span_id":"2a9abb846bb02bfe","trace_id":"6b9e798174650d2f6e8262ec175f241f"}"#.into()).to_string(),
            r#"'{"sampling_state":"1","span_id":"2a9abb846bb02bfe","trace_id":"6b9e798174650d2f6e8262ec175f241f"}'"#
        );
}

#[test]
fn test_empty_parameter_value() {
    assert_eq!(ParameterValue::String("".into()).to_string(), "''");
}

#[test]
fn test_clear_resets_hash() {
    let mut params = Parameters::default();
    params.insert("application_name", "test_app");
    params.insert("TimeZone", "UTC");

    // Verify params are not identical to empty (hash differs)
    assert!(!params.identical(&Parameters::default()));

    params.clear();

    // After clear, hash should be reset to match empty Parameters
    assert!(params.identical(&Parameters::default()));
}
