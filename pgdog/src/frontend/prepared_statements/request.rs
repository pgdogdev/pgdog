//! Request to use a prepared statement.

#[derive(Debug, Clone, Eq)]
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

impl Ord for Request {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.name.cmp(&other.name)
    }
}

impl PartialOrd for Request {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Request {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}
