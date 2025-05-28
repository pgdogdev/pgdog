use std::collections::HashMap;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::frontend::router::parser::Shard;

// =============================================================================
// Serialization Helper Module
// =============================================================================

/// Helper module for (de)serializing maps with usize keys as strings
mod usize_map_keys_as_strings {
    use super::*;

    pub fn serialize<S, V>(map: &HashMap<usize, V>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        V: Serialize,
    {
        let string_map: HashMap<String, &V> = map.iter().map(|(k, v)| (k.to_string(), v)).collect();
        string_map.serialize(serializer)
    }

    pub fn deserialize<'de, D, V>(deserializer: D) -> Result<HashMap<usize, V>, D::Error>
    where
        D: Deserializer<'de>,
        V: Deserialize<'de>,
    {
        let string_map = HashMap::<String, V>::deserialize(deserializer)?;
        string_map
            .into_iter()
            .map(|(s, v)| {
                s.parse::<usize>()
                    .map(|k| (k, v))
                    .map_err(serde::de::Error::custom)
            })
            .collect()
    }
}

// =============================================================================
// Core Sharding Types
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ShardingMethod {
    #[default]
    Hash,
    Range,
    List,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ShardRange {
    pub start: Option<i64>,
    pub end: Option<i64>,
    #[serde(default)]
    pub no_max: bool,
    #[serde(default)]
    pub no_min: bool,
}

impl ShardRange {
    /// Check if a value falls within this range
    pub fn contains(&self, value: i64) -> bool {
        // Check lower bound
        if !self.no_min {
            if let Some(start) = self.start {
                if value < start {
                    return false;
                }
            }
        }

        // Check upper bound
        if !self.no_max {
            if let Some(end) = self.end {
                if value >= end {
                    // Using >= for exclusive upper bound
                    return false;
                }
            }
        }

        true
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ShardList {
    pub values: Vec<i64>,
}

impl ShardList {
    /// Check if a value is contained in this list
    pub fn contains(&self, value: i64) -> bool {
        self.values.contains(&value)
    }
}

// =============================================================================
// Shard Map Types
// =============================================================================

/// A map of shard IDs to their range definitions
#[derive(Debug, Clone, PartialEq)]
pub struct ShardRangeMap(pub HashMap<usize, ShardRange>);

impl ShardRangeMap {
    pub fn new() -> Self {
        Self::default()
    }

    /// Find the shard key for a given value based on range containment
    pub fn find_shard_key(&self, value: i64) -> Option<Shard> {
        for (shard_id, range) in &self.0 {
            if range.contains(value) {
                return Some(Shard::Direct(*shard_id));
            }
        }
        None
    }
}

impl Default for ShardRangeMap {
    fn default() -> Self {
        Self(HashMap::new())
    }
}

impl Serialize for ShardRangeMap {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        usize_map_keys_as_strings::serialize(&self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for ShardRangeMap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(ShardRangeMap(usize_map_keys_as_strings::deserialize(
            deserializer,
        )?))
    }
}

/// A map of shard IDs to their list definitions
#[derive(Debug, Clone, PartialEq)]
pub struct ShardListMap(pub HashMap<usize, ShardList>);

impl ShardListMap {
    pub fn new() -> Self {
        Self::default()
    }

    /// Find the shard key for a given value based on list containment
    pub fn find_shard_key(&self, value: i64) -> Option<Shard> {
        for (shard_id, list) in &self.0 {
            if list.contains(value) {
                return Some(Shard::Direct(*shard_id));
            }
        }
        None
    }
}

impl Default for ShardListMap {
    fn default() -> Self {
        Self(HashMap::new())
    }
}

impl Serialize for ShardListMap {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        usize_map_keys_as_strings::serialize(&self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for ShardListMap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(ShardListMap(usize_map_keys_as_strings::deserialize(
            deserializer,
        )?))
    }
}
