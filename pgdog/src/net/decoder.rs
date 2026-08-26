use crate::frontend::PreparedStatements;

use super::{Bind, Format, RowDescription};

/// Decodes columns returned by Postgres.
///
/// This is just a helpful interface on top of [`Bind`] and [`RowDescription`]. The
/// decoding logic in those messages are doing all the work.
///
#[derive(Debug, Clone, Default)]
pub(crate) struct Decoder {
    /// Expected result column formats, as requested by [`Bind`] sent by client.
    /// For queries using the simple protocol, the format will be text.
    formats: Vec<Format>,
    /// Row description returned by Postgres.
    row_description: Option<RowDescription>,
}

impl Decoder {
    /// Set the format the client specified for the request.
    pub(crate) fn set_formats(&mut self, bind: &Bind) {
        self.formats.clear();
        self.formats.extend(bind.result_formats());

        // Unnamed statements will cause the server to return `RowDescription`. Will will
        // see it and set it. Named statements will often request it separately with `Describe`
        // as part of a previous request, so we get it from the prepared statements cache.
        if !bind.anonymous()
            && let Some(rd) = PreparedStatements::global()
                .read()
                .row_description(bind.statement())
        {
            self.row_description = Some(rd);
        }
    }

    /// Set the [`RowDescription`] returned by the server.
    /// This will be used to identify column names and types.
    pub(crate) fn set_row_description(&mut self, rd: RowDescription) {
        self.row_description = Some(rd);
    }

    /// Get format used for column at position. Uses 0-indexed positioning.
    ///
    /// BUG(lev): Always returns a format, defaulting to text if the column format is not known.
    ///
    pub(crate) fn get_format(&self, position: usize) -> Format {
        match self.formats.len() {
            0 => self
                .row_description()
                .field(position)
                .map(|field| field.format())
                .unwrap_or(Format::Text),
            1 => self.formats[0],
            _ => self.formats.get(position).copied().unwrap_or(Format::Text),
        }
    }

    /// Get a reference to the [`RowDescription`] the server sent
    /// for the request.
    pub(crate) fn row_description(&self) -> &RowDescription {
        self.row_description
            .as_ref()
            .expect("decoder has no row description set")
    }
}

#[cfg(test)]
mod test_impls {
    use super::*;

    impl From<RowDescription> for Decoder {
        fn from(value: RowDescription) -> Self {
            let mut decoder = Decoder::default();
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
        let mut decoder = Decoder::default();
        decoder.set_row_description(text_rd());

        assert_eq!(decoder.get_format(0), Format::Text);
        assert_eq!(decoder.get_format(1), Format::Text);
    }

    #[test]
    fn test_bind_result_formats_survive_row_description() {
        let mut decoder = Decoder::default();
        let bind = Bind::new_params_codes_results("s1", &[], &[], &[1, 0]);

        decoder.set_formats(&bind);
        decoder.set_row_description(text_rd());

        assert_eq!(decoder.get_format(0), Format::Binary);
        assert_eq!(decoder.get_format(1), Format::Text);
    }

    #[test]
    fn test_row_description_before_bind_gives_the_same_answer() {
        let mut decoder = Decoder::default();
        let bind = Bind::new_params_codes_results("s1", &[], &[], &[1, 0]);

        decoder.set_row_description(text_rd());
        decoder.set_formats(&bind);

        assert_eq!(decoder.get_format(0), Format::Binary);
        assert_eq!(decoder.get_format(1), Format::Text);
    }

    #[test]
    fn test_one_result_format_applies_to_every_column() {
        let mut decoder = Decoder::default();
        decoder.set_row_description(text_rd());
        decoder.set_formats(&Bind::new_params_codes_results("s1", &[], &[], &[1]));

        assert_eq!(decoder.get_format(0), Format::Binary);
        assert_eq!(decoder.get_format(1), Format::Binary);
    }

    #[test]
    fn test_bind_keeps_the_server_description_when_the_cache_is_empty() {
        let mut decoder = Decoder::default();
        decoder.set_row_description(text_rd());
        decoder.set_formats(&Bind::new_params_codes_results("s1", &[], &[], &[1]));

        // Nothing described s1, so the only columns we have are the ones the
        // server announced. The formats still come from the Bind.
        assert_eq!(decoder.row_description().fields.len(), 2);
        assert_eq!(decoder.get_format(0), Format::Binary);

        decoder.set_formats(&Bind::new_statement("s2"));

        assert_eq!(decoder.get_format(0), Format::Text);
    }
}
