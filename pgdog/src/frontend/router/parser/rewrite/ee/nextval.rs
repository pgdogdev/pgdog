use super::Error;

pub(crate) async fn nextval(_name: &str) -> Result<i64, Error> {
    Err(Error::EERequired)
}
