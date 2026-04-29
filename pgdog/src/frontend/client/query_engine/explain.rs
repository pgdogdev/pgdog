use crate::{frontend::router::parser::explain_trace::ExplainTrace, net::RowDescription};

#[derive(Debug, Default, Clone)]
pub(super) struct ExplainResponseState {
    pub(super) lines: Vec<String>,
    pub(super) row_description: Option<RowDescription>,
    pub(super) annotated: bool,
    pub(super) supported: bool,
}

impl ExplainResponseState {
    pub fn new(trace: ExplainTrace) -> Self {
        Self {
            lines: trace.render_lines(),
            row_description: None,
            annotated: false,
            supported: false,
        }
    }

    pub fn capture_row_description(&mut self, row_description: RowDescription) {
        self.supported = row_description.fields.len() == 1
            && matches!(row_description.field(0).map(|f| f.type_oid), Some(25));
        if self.supported {
            self.row_description = Some(row_description);
        } else {
            self.annotated = true;
        }
    }

    pub fn should_emit(&self) -> bool {
        self.supported && !self.annotated
    }
}
