use crate::net::Message;

#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    Intercept(Vec<Message>),
    Forward,
}
