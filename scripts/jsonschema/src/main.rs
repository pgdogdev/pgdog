use std::fs::File;
use std::path::PathBuf;
use std::sync::OnceLock;

use anyhow::Context;
use schemars::{Schema, schema_for};

use pgdog_config::Config;
use pgdog_config::Users;

static WORKSPACE_ROOT: OnceLock<PathBuf> = OnceLock::new();

fn write_schema(name: &str, schema: Schema) -> anyhow::Result<()> {
    let root = WORKSPACE_ROOT.get_or_init(workspace_root::get_workspace_root);
    let schema_path = root.join(".schema").join(format!("{name}.schema.json"));

    let file = File::create(&schema_path).context("Failed to create file")?;
    serde_json::to_writer_pretty(file, &schema).context("serde_json conversion")?;

    Ok(())
}

fn main() -> anyhow::Result<()> {
    write_schema("pgdog", schema_for!(Config))?;
    write_schema("users", schema_for!(Users))?;

    Ok(())
}
