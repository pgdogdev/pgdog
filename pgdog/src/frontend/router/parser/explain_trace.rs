use pgdog_config::Role;

use crate::frontend::router::parser::route::Shard;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExplainTrace {
    summary: ExplainSummary,
    steps: Vec<ExplainEntry>,
}

impl ExplainTrace {
    pub fn new(summary: ExplainSummary, steps: Vec<ExplainEntry>) -> Self {
        Self { summary, steps }
    }

    pub fn summary(&self) -> &ExplainSummary {
        &self.summary
    }

    pub fn steps(&self) -> &[ExplainEntry] {
        &self.steps
    }

    pub fn render_lines(&self) -> Vec<String> {
        let mut lines = vec![String::new(), "PgDog Routing:".to_string()];
        lines.push(format!(
            "  Summary: shard={} role={}",
            self.summary.shard,
            if self.summary.read {
                "replica"
            } else {
                "primary"
            }
        ));

        for entry in &self.steps {
            lines.push(entry.render_line());
        }

        lines
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExplainSummary {
    pub shard: Shard,
    pub read: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainEntry {
    pub shard: Option<Shard>,
    pub description: String,
}

impl ExplainEntry {
    pub fn new(shard: Option<Shard>, description: impl Into<String>) -> Self {
        Self {
            shard,
            description: description.into(),
        }
    }

    fn render_line(&self) -> String {
        match &self.shard {
            Some(Shard::Direct(shard)) => {
                format!("  Shard {}: {}", shard, self.description)
            }
            Some(Shard::Multi(shards)) => {
                format!("  Shards {:?}: {}", shards, self.description)
            }
            Some(Shard::All) => format!("  All shards: {}", self.description),
            None => format!("  Note: {}", self.description),
        }
    }
}

/// EXPLAIN recorder.
#[derive(Debug, Default)]
pub struct ExplainRecorder {
    entries: Vec<ExplainEntry>,
    comment: Option<ExplainEntry>,
    plugin: Option<ExplainEntry>,
}

impl ExplainRecorder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_entry(&mut self, shard: Option<Shard>, description: impl Into<String>) {
        self.entries.push(ExplainEntry::new(shard, description));
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.comment = None;
        self.plugin = None;
    }

    pub fn record_comment_override(&mut self, shard: Shard, role: Option<Role>) {
        let mut description = match shard {
            Shard::Direct(_) | Shard::Multi(_) | Shard::All => {
                format!("manual override to shard={}", shard)
            }
        };

        if let Some(role) = role {
            description.push_str(&format!(" role={}", role));
        }

        self.comment = Some(ExplainEntry::new(Some(shard), description));
    }

    pub fn record_plugin_override(
        &mut self,
        plugin: impl Into<String>,
        shard: Option<Shard>,
        read: Option<bool>,
    ) {
        let mut description = format!("plugin {} adjusted routing", plugin.into());
        if let Some(shard) = &shard {
            description.push_str(&format!(" shard={}", shard));
        }
        if let Some(read) = read {
            description.push_str(&format!(
                " role={}",
                if read { "replica" } else { "primary" }
            ));
        }
        self.plugin = Some(ExplainEntry::new(shard, description));
    }

    pub fn finalize(mut self, summary: ExplainSummary) -> ExplainTrace {
        if let Some(comment) = self.comment.take() {
            self.entries.insert(0, comment);
        }

        if let Some(plugin) = self.plugin.take() {
            self.entries.push(plugin);
        }

        if self.entries.is_empty() {
            let description = match summary.shard {
                Shard::All => "no sharding key matched; broadcasting".to_string(),
                Shard::Multi(ref shards) if !shards.is_empty() => {
                    format!("multiple shards matched: {:?}", shards)
                }
                Shard::Multi(_) => "multiple shards matched".to_string(),
                Shard::Direct(_) => "direct routing without recorded hints".to_string(),
            };
            self.entries.push(ExplainEntry::new(None, description));
        }

        ExplainTrace::new(summary, self.entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_lines_formats_summary_and_entries() {
        let trace = ExplainTrace::new(
            ExplainSummary {
                shard: Shard::Direct(2),
                read: true,
            },
            vec![ExplainEntry::new(
                Some(Shard::Direct(2)),
                "matched sharding key",
            )],
        );

        let lines = trace.render_lines();
        assert_eq!(lines[0], "");
        assert_eq!(lines[1], "PgDog Routing:");
        assert_eq!(lines[2], "  Summary: shard=2 role=replica");
        assert_eq!(lines[3], "  Shard 2: matched sharding key");
    }

    #[test]
    fn finalize_inserts_comment_and_plugin_entries() {
        let mut recorder = ExplainRecorder::new();
        recorder.record_entry(Some(Shard::Direct(7)), "matched sharding key");
        recorder.record_comment_override(Shard::Direct(3), Some(Role::Primary));
        recorder.record_plugin_override("test_plugin", Some(Shard::Direct(9)), Some(true));

        let trace = recorder.finalize(ExplainSummary {
            shard: Shard::Direct(9),
            read: true,
        });

        let descriptions: Vec<&str> = trace
            .steps()
            .iter()
            .map(|entry| entry.description.as_str())
            .collect();

        assert_eq!(descriptions[0], "manual override to shard=3 role=primary");
        assert_eq!(descriptions[1], "matched sharding key");
        assert_eq!(
            descriptions[2],
            "plugin test_plugin adjusted routing shard=9 role=replica"
        );
    }

    #[test]
    fn finalize_injects_fallback_when_no_entries() {
        let trace = ExplainRecorder::new().finalize(ExplainSummary {
            shard: Shard::All,
            read: false,
        });

        assert_eq!(trace.steps().len(), 1);
        assert_eq!(
            trace.steps()[0].description,
            "no sharding key matched; broadcasting"
        );
        assert!(trace.steps()[0].shard.is_none());
    }

    #[test]
    fn finalize_reports_multiple_shards() {
        let trace = ExplainRecorder::new().finalize(ExplainSummary {
            shard: Shard::Multi(vec![1, 5]),
            read: true,
        });

        assert_eq!(
            trace.steps()[0].description,
            "multiple shards matched: [1, 5]"
        );
    }
}
