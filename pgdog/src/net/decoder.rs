use crate::frontend::PreparedStatements;

use super::{Bind, Format, RowDescription};

/// Decodes result columns.
///
/// The server owns the column names and types, the client's Bind owns the
/// wire formats. Neither may overwrite the other.
#[derive(Debug, Clone, Default)]
pub struct Decoder {
    /// Formats requested by the client's Bind.
    formats: Vec<Format>,
    rd: RowDescription,
}

impl Decoder {
    /// New column decoder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Describe the rows a Bind is about to produce. Only a statement the
    /// cache knows replaces the columns, because clearing them would leave
    /// the sorter with no types at all.
    pub fn bind(&mut self, bind: &Bind) {
        self.formats.clear();
        self.formats.extend(bind.result_formats());

        if !bind.anonymous()
            && let Some(rd) = PreparedStatements::global()
                .read()
                .row_description(bind.statement())
        {
            self.rd = rd;
        }
    }

    /// Describe the rows the server just announced.
    pub fn set_row_description(&mut self, rd: RowDescription) {
        self.rd = rd;
    }

    /// Get format used for column at position.
    pub fn format(&self, position: usize) -> Format {
        match self.formats.len() {
            0 => self
                .rd
                .field(position)
                .map(|field| field.format())
                .unwrap_or(Format::Text),
            1 => self.formats[0],
            _ => self.formats.get(position).copied().unwrap_or(Format::Text),
        }
    }

    pub fn rd(&self) -> &RowDescription {
        &self.rd
    }
}

#[cfg(test)]
mod test_impls {
    use super::*;

    impl From<RowDescription> for Decoder {
        fn from(value: RowDescription) -> Self {
            let mut decoder = Decoder::new();
            decoder.set_row_description(value);
            decoder
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::net::messages::Field;

    fn text_rd() -> RowDescription {
        RowDescription::new(&[Field::bigint("id"), Field::text("name")])
    }

    #[test]
    fn test_row_description_decides_without_bind() {
        let mut decoder = Decoder::new();
        decoder.set_row_description(text_rd());

        assert_eq!(decoder.format(0), Format::Text);
        assert_eq!(decoder.format(1), Format::Text);
    }

    #[test]
    fn test_bind_result_formats_survive_row_description() {
        let mut decoder = Decoder::new();
        let bind = Bind::new_params_codes_results("s1", &[], &[], &[1, 0]);

        decoder.bind(&bind);
        decoder.set_row_description(text_rd());

        assert_eq!(decoder.format(0), Format::Binary);
        assert_eq!(decoder.format(1), Format::Text);
    }

    #[test]
    fn test_row_description_before_bind_gives_the_same_answer() {
        let mut decoder = Decoder::new();
        let bind = Bind::new_params_codes_results("s1", &[], &[], &[1, 0]);

        decoder.set_row_description(text_rd());
        decoder.bind(&bind);

        assert_eq!(decoder.format(0), Format::Binary);
        assert_eq!(decoder.format(1), Format::Text);
    }

    #[test]
    fn test_one_result_format_applies_to_every_column() {
        let mut decoder = Decoder::new();
        decoder.set_row_description(text_rd());
        decoder.bind(&Bind::new_params_codes_results("s1", &[], &[], &[1]));

        assert_eq!(decoder.format(0), Format::Binary);
        assert_eq!(decoder.format(1), Format::Binary);
    }

    #[test]
    fn test_bind_keeps_the_server_description_when_the_cache_is_empty() {
        let mut decoder = Decoder::new();
        decoder.set_row_description(text_rd());
        decoder.bind(&Bind::new_params_codes_results("s1", &[], &[], &[1]));

        // Nothing described s1, so the only columns we have are the ones the
        // server announced. The formats still come from the Bind.
        assert_eq!(decoder.rd().fields.len(), 2);
        assert_eq!(decoder.format(0), Format::Binary);

        decoder.bind(&Bind::new_statement("s2"));

        assert_eq!(decoder.format(0), Format::Text);
    }
}
