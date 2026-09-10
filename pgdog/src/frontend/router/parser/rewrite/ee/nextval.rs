use super::Error;

pub(crate) async fn nextval(_name: &str) -> Result<i64, Error> {
    Err(Error::EERequired)
}

pub(crate) async fn currval(_name: &str) -> Result<i64, Error> {
    Err(Error::EERequired)
}

pub(crate) async fn setval(_name: &str, _value: i64, _is_called: bool) -> Result<i64, Error> {
    Err(Error::EERequired)
}
