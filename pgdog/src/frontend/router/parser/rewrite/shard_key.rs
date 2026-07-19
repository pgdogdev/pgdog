#[derive(Debug, Clone, PartialEq)]
pub enum AssignmentValue {
    Parameter(i32),
    Integer(i64),
    Float(String),
    String(String),
    Boolean(bool),
    Null,
    Column(String),
}

impl AssignmentValue {
    pub fn as_parameter(&self) -> Option<i32> {
        if let Self::Parameter(param) = self {
            Some(*param)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    column: String,
    value: AssignmentValue,
}

impl Assignment {
    pub fn new(column: String, value: AssignmentValue) -> Self {
        Self { column, value }
    }

    pub fn column(&self) -> &str {
        &self.column
    }

    pub fn value(&self) -> &AssignmentValue {
        &self.value
    }
}
