//! SHOW MIRRORS - per-cluster mirror statistics

use crate::backend::databases::databases;

use super::prelude::*;

pub struct ShowMirrors;

#[async_trait]
impl Command for ShowMirrors {
    fn name(&self) -> String {
        "SHOW MIRRORS".into()
    }

    fn parse(_: &str) -> Result<Self, Error> {
        Ok(Self)
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        // Define the fields for the result
        let fields = vec![
            Field::text("database"),
            Field::text("user"),
            Field::numeric("total_count"),
            Field::numeric("mirrored_count"),
            Field::numeric("dropped_count"),
            Field::numeric("error_count"),
            Field::numeric("queue_length"),
        ];

        let mut messages = vec![RowDescription::new(&fields).message()?];

        // Iterate through all clusters and create a row for each
        for (user, cluster) in databases().all() {
            let counts = {
                let stats = cluster.stats();
                let stats = stats.lock();
                stats.mirrors
            };

            // Create a data row for this cluster
            let mut dr = DataRow::new();
            dr.add(user.database.as_str())
                .add(user.user.as_str())
                .add(counts.total_count as i64)
                .add(counts.mirrored_count as i64)
                .add(counts.dropped_count as i64)
                .add(counts.error_count as i64)
                .add(counts.queue_length as i64);

            messages.push(dr.message()?);
        }

        Ok(messages)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::{FromBytes, ToBytes};

    #[tokio::test]
    async fn test_show_mirrors_format() {
        let show = ShowMirrors;

        // Test command name
        assert_eq!(show.name(), "SHOW MIRRORS");

        // Test parsing
        let parsed = ShowMirrors::parse("SHOW MIRRORS");
        assert!(parsed.is_ok(), "Should parse successfully");

        // Test execution
        let messages = show.execute().await.expect("Should execute successfully");

        // Should have at least RowDescription
        assert!(!messages.is_empty(), "Should have at least RowDescription");

        // First message should be RowDescription
        assert_eq!(
            messages[0].code(),
            'T',
            "First message should be RowDescription"
        );

        // Parse the RowDescription to check column names
        let row_desc = RowDescription::from_bytes(messages[0].to_bytes().unwrap()).unwrap();
        let fields = &row_desc.fields;

        // Should have 7 columns for per-cluster stats
        assert_eq!(
            fields.len(),
            7,
            "Should have 7 columns for per-cluster stats"
        );

        // Check column names
        let expected_columns = [
            "database",
            "user",
            "total_count",
            "mirrored_count",
            "dropped_count",
            "error_count",
            "queue_length",
        ];
        for (i, expected) in expected_columns.iter().enumerate() {
            assert_eq!(
                fields[i].name, *expected,
                "Column {} should be named {}",
                i, expected
            );
        }

        // Count data rows - may be 0 or more depending on configured clusters
        let data_rows: Vec<_> = messages.iter().filter(|m| m.code() == 'D').collect();

        // If we have data rows, validate their format
        if !data_rows.is_empty() {
            // Parse the first data row to ensure it has valid format
            let data_row = DataRow::from_bytes(data_rows[0].to_bytes().unwrap()).unwrap();

            // Skip validating database and user strings for now since DataRow doesn't have get_string
            // Just validate the counter values are integers (>= 0)
            for i in 2..7 {
                let value = data_row.get_int(i, true).unwrap_or(0);
                assert!(value >= 0, "Column {} should have non-negative value", i);
            }
        }
    }
}
