use super::{
    prelude::{DataRow, Field, Protocol, RowDescription},
    *,
};

pub struct ShowVersion;

#[async_trait]
impl Command for ShowVersion {
    fn name(&self) -> String {
        "SHOW VERSION".into()
    }

    fn parse(_: &str) -> Result<Self, Error> {
        Ok(Self)
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let version = env!("GIT_HASH");

        let mut dr = DataRow::new();
        dr.add(format!("PgDog v{}", version));

        Ok(vec![
            RowDescription::new(&[Field::text("version")]).message()?,
            dr.message()?,
        ])
    }
}
