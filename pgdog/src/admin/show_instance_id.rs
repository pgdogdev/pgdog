use super::{
    prelude::{DataRow, Field, Protocol, RowDescription},
    *,
};
use crate::util::instance_id;

pub struct ShowInstanceId;

#[async_trait]
impl Command for ShowInstanceId {
    fn name(&self) -> String {
        "SHOW INSTANCE_ID".into()
    }

    fn parse(_: &str) -> Result<Self, Error> {
        Ok(Self)
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let mut dr = DataRow::new();
        dr.add(instance_id());

        Ok(vec![
            RowDescription::new(&[Field::text("instance_id")]).message()?,
            dr.message()?,
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse() {
        assert!(ShowInstanceId::parse("show instance_id").is_ok());
    }

    #[tokio::test]
    async fn test_execute() {
        let cmd = ShowInstanceId;
        let result = cmd.execute().await.unwrap();
        assert_eq!(result.len(), 2);

        // Verify first message is RowDescription
        assert_eq!(result[0].code(), 'T');
        // Verify second message is DataRow
        assert_eq!(result[1].code(), 'D');
    }

    #[test]
    fn test_name() {
        let cmd = ShowInstanceId;
        assert_eq!(cmd.name(), "SHOW INSTANCE_ID");
    }
}
