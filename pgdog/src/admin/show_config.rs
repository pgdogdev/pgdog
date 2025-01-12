//! SHOW CONFIG command.

use crate::{
    backend::databases::databases,
    config::config,
    net::messages::{DataRow, Field, Protocol, RowDescription},
};

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

    let value = if name.contains("_timeout") || name.contains("_interval") {
        match s.parse::<u64>() {
            Ok(v) => {
                let second = 1000;
                let minute = second * 60;
                let hour = minute * 60;
                let day = hour * 24;
                if v < second {
                    format!("{}ms", v)
                } else if v < minute && v % second == 0 {
                    format!("{}s", v / second)
                } else if v < hour && v % minute == 0 {
                    format!("{}m", v / minute)
                } else if v < day && v / hour == 0 {
                    format!("{}d", v)
                } else {
                    format!("{}ms", v)
                }
            }
            Err(_) => s,
        }
    } else if s == "null" {
        "not configured".to_string()
    } else {
        s.replace("\"", "")
    };

    Ok(value)
}
