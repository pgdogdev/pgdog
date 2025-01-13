//! SHOW CONFIG command.

use crate::{
    backend::databases::databases,
    config::config,
    net::messages::{DataRow, Field, Protocol, RowDescription},
    util::human_duration,
};

use std::time::Duration;

use super::prelude::*;

pub struct ShowConfig;

#[async_trait]
impl Command for ShowConfig {
    fn name(&self) -> String {
        "SHOW".into()
    }

    fn parse(_sql: &str) -> Result<Self, Error> {
        Ok(Self {})
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let config = config();
        let _databases = databases();

        let mut messages =
            vec![RowDescription::new(&[Field::text("name"), Field::text("value")]).message()?];

        // Reflection using JSON.
        let general = serde_json::to_value(&config.config.general)?;
        if let Some(map) = general.as_object() {
            for (key, value) in map {
                let mut dr = DataRow::new();
                dr.add(key.as_str()).add(pretty_value(key.as_str(), value)?);
                messages.push(dr.message()?);
            }
        }

        Ok(messages)
    }
}

/// Format the value in a human-readable way.
fn pretty_value(name: &str, value: &serde_json::Value) -> Result<String, serde_json::Error> {
    let s = serde_json::to_string(value)?;

    let value =
        if name.contains("_timeout") || name.contains("_interval") || name.contains("_delay") {
            match s.parse::<u64>() {
                Ok(v) => human_duration(Duration::from_millis(v)),
                Err(_) => s,
            }
        } else if s == "null" {
            "not configured".to_string()
        } else {
            s.replace("\"", "")
        };

    Ok(value)
}
