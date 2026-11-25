//! Provide additional logic and validation for the deserialization.
//!
//! Approach is inspired by [cargo-util-schemas](https://github.com/rust-lang/cargo/blob/master/crates/cargo-util-schemas/src/manifest/mod.rs) repo.
//!
//! See [`deserialize_with_unknown_fields`] for more details.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use serde::de::IgnoredAny;
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// The deserialized value and the paths of all keys that no field recognized.
#[derive(Debug, Clone, PartialEq)]
pub struct Deserialized<T> {
    pub value: T,
    pub unknown_fields: Vec<String>,
}

/// Trait to specify what fields where unknown while deserializing the object
///
/// For objects that have any `serde(flatten)` in their tree (ancestors included)
/// this should return the list of this unparsed values with proper prefix for their names.
/// This is for the resolving [issue](https://github.com/dtolnay/serde-ignored/issues/10)
/// and catch unknown fields that was not resolved automatically by [serde_ignored].
///
/// If the object doesn't contain any `serde(flatten)` in the subtree, that trait should
/// be just derived without implementation code.
pub trait WithUnknownFields {
    /// Unknown field paths, relative to `self`. Parents add their own prefix.
    fn unknown_fields(&self) -> impl Iterator<Item = String> + '_ {
        std::iter::empty()
    }
}

/// Deserialize with any [Deserializer] and catch any fields that were not identified by
/// utilizing [WithUnknownFields].
pub fn deserialize_with_unknown_fields<'de, D, T>(
    deserializer: D,
) -> Result<Deserialized<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + WithUnknownFields,
{
    let mut unknown_fields = Vec::new();
    let value: T =
        serde_ignored::deserialize(deserializer, |path| unknown_fields.push(path.to_string()))?;
    unknown_fields.extend(value.unknown_fields());

    Ok(Deserialized {
        value,
        unknown_fields,
    })
}

/// Represent any unknown fields that is not parsed.
///
/// Add it as a last field with `serde(flatten)` attribute,
/// and return its [WithUnknownFields::unknown_fields] from the parent's
/// implementation with the proper prefix.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(transparent)]
pub struct UnknownFields(BTreeMap<String, IgnoredAny>);

impl WithUnknownFields for UnknownFields {
    fn unknown_fields(&self) -> impl Iterator<Item = String> + '_ {
        self.0.keys().cloned()
    }
}

impl PartialEq for UnknownFields {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

impl Eq for UnknownFields {}

impl PartialOrd for UnknownFields {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for UnknownFields {
    fn cmp(&self, _: &Self) -> Ordering {
        Ordering::Equal
    }
}

impl Serialize for UnknownFields {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_map(Some(0))?.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Deserialize, PartialEq)]
    struct Root {
        name: String,
        #[serde(default)]
        nested: Nested,
        #[serde(default)]
        items: Vec<Nested>,
    }

    #[derive(Debug, Default, Deserialize, Serialize, PartialEq)]
    struct Nested {
        value: u32,
    }

    #[derive(Debug, Deserialize, Serialize, PartialEq)]
    struct Flattened {
        name: String,
        #[serde(flatten)]
        nested: Nested,
        #[serde(flatten)]
        unknown: UnknownFields,
    }

    #[derive(Debug, Deserialize, PartialEq)]
    struct FlattenedWithoutCatchAll {
        name: String,
        #[serde(flatten)]
        nested: Nested,
    }

    impl WithUnknownFields for Root {}

    impl WithUnknownFields for FlattenedWithoutCatchAll {}

    impl WithUnknownFields for Flattened {
        fn unknown_fields(&self) -> impl Iterator<Item = String> + '_ {
            self.unknown.unknown_fields()
        }
    }

    #[derive(Debug, Deserialize, PartialEq)]
    struct Parent {
        #[serde(default)]
        entries: Vec<Flattened>,
    }

    impl WithUnknownFields for Parent {
        fn unknown_fields(&self) -> impl Iterator<Item = String> + '_ {
            self.entries.iter().enumerate().flat_map(|(index, entry)| {
                entry
                    .unknown_fields()
                    .map(move |field| format!("entries.{index}.{field}"))
            })
        }
    }

    fn expected() -> Root {
        Root {
            name: "prod".into(),
            nested: Nested { value: 1 },
            items: vec![Nested { value: 2 }],
        }
    }

    fn sorted(mut fields: Vec<String>) -> Vec<String> {
        fields.sort();
        fields
    }

    #[test]
    fn toml_reports_unknown_field_paths() {
        let source = r#"
name = "prod"
unknown_root = 1

[nested]
value = 1
unknown_nested = true

[unknown_section]
key = 1

[[items]]
value = 2
unknown_item = "x"
"#;
        let usual: Root = toml::from_str(source).unwrap();
        let strict: Deserialized<Root> =
            deserialize_with_unknown_fields(toml::Deserializer::new(source)).unwrap();

        assert_eq!(usual, expected());
        assert_eq!(strict.value, expected());
        assert_eq!(
            sorted(strict.unknown_fields),
            [
                "items.0.unknown_item",
                "nested.unknown_nested",
                "unknown_root",
                "unknown_section",
            ]
        );
    }

    #[test]
    fn json_reports_unknown_field_paths() {
        let source = r#"{
            "name": "prod",
            "unknown_root": 1,
            "nested": { "value": 1, "unknown_nested": true },
            "unknown_section": { "key": 1 },
            "items": [{ "value": 2, "unknown_item": "x" }]
        }"#;
        let usual: Root = serde_json::from_str(source).unwrap();
        let mut deserializer = serde_json::Deserializer::from_str(source);
        let strict: Deserialized<Root> =
            deserialize_with_unknown_fields(&mut deserializer).unwrap();
        deserializer.end().unwrap();

        assert_eq!(usual, expected());
        assert_eq!(strict.value, expected());
        assert_eq!(
            sorted(strict.unknown_fields),
            [
                "items.0.unknown_item",
                "nested.unknown_nested",
                "unknown_root",
                "unknown_section",
            ]
        );
    }

    #[test]
    fn known_fields_report_nothing() {
        let source = r#"
name = "prod"

[nested]
value = 1

[[items]]
value = 2
"#;
        let usual: Root = toml::from_str(source).unwrap();
        let strict: Deserialized<Root> =
            deserialize_with_unknown_fields(toml::Deserializer::new(source)).unwrap();

        assert_eq!(usual, expected());
        assert_eq!(strict.value, expected());
        assert!(strict.unknown_fields.is_empty());
    }

    #[test]
    fn parse_errors_are_returned() {
        let source = r#"name = 1"#;
        assert!(toml::from_str::<Root>(source).is_err());
        assert!(
            deserialize_with_unknown_fields::<_, Root>(toml::Deserializer::new(source)).is_err()
        );
    }

    #[test]
    fn serde_ignored_misses_unknown_fields_next_to_flatten() {
        let source = r#"
name = "prod"
value = 1
unknown_flattened = true
"#;
        let usual: FlattenedWithoutCatchAll = toml::from_str(source).unwrap();
        let strict: Deserialized<FlattenedWithoutCatchAll> =
            deserialize_with_unknown_fields(toml::Deserializer::new(source)).unwrap();

        assert_eq!(usual, strict.value);
        assert_eq!(strict.value.nested, Nested { value: 1 });
        assert!(strict.unknown_fields.is_empty());
    }

    #[test]
    fn unknown_fields_catch_flattened_unknown_fields() {
        let source = r#"
name = "prod"
value = 1
unknown_flattened = true
"#;
        let usual: Flattened = toml::from_str(source).unwrap();
        let strict: Deserialized<Flattened> =
            deserialize_with_unknown_fields(toml::Deserializer::new(source)).unwrap();

        assert_eq!(usual, strict.value);
        assert_eq!(strict.value.name, "prod");
        assert_eq!(strict.value.nested, Nested { value: 1 });
        assert_eq!(strict.unknown_fields, ["unknown_flattened"]);
        assert_eq!(
            strict.value.unknown.unknown_fields().collect::<Vec<_>>(),
            ["unknown_flattened"]
        );

        let serialized = toml::to_string(&strict.value).unwrap();
        assert!(!serialized.contains("unknown_flattened"), "{serialized}");
    }

    #[test]
    fn unknown_keys_report_prefixed_paths_for_nested_entries() {
        let source = r#"
unknown_parent = 1

[[entries]]
name = "first"
value = 1

[[entries]]
name = "second"
value = 2
unknown_flattened = true
another_unknown = "x"
"#;
        let usual: Parent = toml::from_str(source).unwrap();
        let strict: Deserialized<Parent> =
            deserialize_with_unknown_fields(toml::Deserializer::new(source)).unwrap();

        assert_eq!(usual, strict.value);
        assert_eq!(strict.value.entries[1].nested, Nested { value: 2 });
        assert_eq!(
            sorted(strict.unknown_fields),
            [
                "entries.1.another_unknown",
                "entries.1.unknown_flattened",
                "unknown_parent",
            ]
        );
    }
}
