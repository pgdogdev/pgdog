//! Request to use a prepared statement.

#[derive(Debug, Clone)]
pub struct Request {
    pub name: String,
    pub new: bool,
}

impl Request {
    pub fn new(name: &str, new: bool) -> Self {
        Self {
            name: name.to_string(),
            new,
        }
    }
}
