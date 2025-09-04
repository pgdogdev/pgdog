use crate::util::pgdog_version;

use super::{
    prelude::{DataRow, Field, Protocol, RowDescription},
    *,
};

pub(crate) struct ShowVersion;

#[async_trait]
impl Command for ShowVersion {
    fn name(&self) -> String {
        "SHOW VERSION".into()
    }

    fn parse(_: &str) -> Result<Self, Error> {
        Ok(Self)
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let mut dr = DataRow::new();
        dr.add(format!("PgDog {}", pgdog_version()));

        Ok(vec![
            RowDescription::new(&[Field::text("version")]).message()?,
            dr.message()?,
        ])
    }
}
