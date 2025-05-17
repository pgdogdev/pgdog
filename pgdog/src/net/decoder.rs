use crate::frontend::PreparedStatements;

use super::{Bind, Format, RowDescription};

impl From<&Bind> for Decoder {
    fn from(value: &Bind) -> Self {
        let mut decoder = Decoder::new();
        decoder.bind(value);
        decoder
    }
}

impl From<&RowDescription> for Decoder {
    fn from(value: &RowDescription) -> Self {
        let mut decoder = Decoder::new();
        decoder.row_description(value);
        decoder
    }
}

#[derive(Debug, Clone, Default)]
pub struct Decoder {
    formats: Vec<Format>,
    rd: RowDescription,
}

impl Decoder {
    /// New column decoder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Infer types from Bind, if any provided.
    pub fn bind(&mut self, bind: &Bind) {
        // Only override RowDescription formats if
        // Bind specifies formats.
        if !bind.codes().is_empty() {
            self.formats = bind.codes().to_vec();
        }

        if self.rd.is_empty() {
            if let Some(rd) = PreparedStatements::global()
                .lock()
                .row_description(bind.statement())
            {
                self.rd = rd;
            }
        }
    }

    /// Infer types from RowDescription, if any.
    pub fn row_description(&mut self, rd: &RowDescription) {
        let formats = rd.fields.iter().map(|f| f.format()).collect();
        self.formats = formats;
        self.rd = rd.clone();
    }

    /// Get format used for column at position.
    pub fn format(&self, position: usize) -> Format {
        match self.formats.len() {
            0 => Format::Text,
            1 => self.formats[0],
            _ => self.formats.get(position).copied().unwrap_or(Format::Text),
        }
    }

    pub fn rd(&self) -> &RowDescription {
        &self.rd
    }
}
