//! SHOW STATS.
use super::prelude::*;

pub struct ShowStats;

#[async_trait]
impl Command for ShowStats {
    fn name(&self) -> String {
        "SHOW STATS".into()
    }

    fn parse(_: &str) -> Result<Self, Error> {
        Ok(Self)
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let mut fields = vec![Field::text("database"), Field::numeric("shard")];
        fields.extend(
            ["total", "avg"]
                .into_iter()
                .flat_map(|prefix| {
                    [
                        Field::numeric(&format!("{}_xact_count", prefix)),
                        Field::numeric(&format!("{}_server_assignment_count", prefix)),
                        Field::numeric(&format!("{}_received", prefix)),
                        Field::numeric(&format!("{}_sent", prefix)),
                        Field::numeric(&format!("{}_xact_time", prefix)),
                        Field::numeric(&format!("{}_query_time", prefix)),
                        Field::numeric(&format!("{}_wait_time", prefix)),
                        Field::numeric(&format!("{}_client_parse_count", prefix)),
                        Field::numeric(&format!("{}_server_parse_count", prefix)),
                        Field::numeric(&format!("{}_bind_count", prefix)),
                    ]
                })
                .collect::<Vec<Field>>(),
        );

        let rd = RowDescription::new(&fields);
        Ok(vec![rd.message()?])
    }
}
