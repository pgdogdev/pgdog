pub struct Schema<'a> {
    pub name: &'a str,
}

impl<'a> From<&'a str> for Schema<'a> {
    fn from(value: &'a str) -> Self {
        Schema { name: value }
    }
}
