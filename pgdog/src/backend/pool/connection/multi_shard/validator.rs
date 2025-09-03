//! Row consistency validator for multi-shard queries.

use crate::net::messages::{DataRow, RowDescription};

use super::Error;

/// Validates consistency of rows and row descriptions across multiple shards.
#[derive(Debug, Default)]
pub(super) struct Validator {
    /// First row description received for consistency validation
    first_row_description: Option<RowDescription>,
    /// Expected column count from first data row
    expected_column_count: Option<usize>,
}

impl Validator {
    /// Reset the validator state.
    pub(super) fn reset(&mut self) {
        self.first_row_description = None;
        self.expected_column_count = None;
    }

    /// Set the row description.
    pub(super) fn set_row_description(&mut self, rd: &RowDescription) {
        self.first_row_description = Some(rd.clone());
    }

    /// Validate a row description against the first one received.
    /// Returns true if this is the first row description, false if it's a duplicate that matches.
    pub(super) fn validate_row_description(&mut self, rd: &RowDescription) -> Result<bool, Error> {
        match &self.first_row_description {
            None => {
                // First row description - store it for comparison
                self.first_row_description = Some(rd.clone());
                Ok(true)
            }
            Some(first_rd) => {
                // Check column count
                if first_rd.fields.len() != rd.fields.len() {
                    return Err(Error::InconsistentRowDescription {
                        expected: first_rd.fields.len(),
                        actual: rd.fields.len(),
                    });
                }

                // Check column names (but not OIDs - they can vary between shards)
                for (index, (first_field, field)) in
                    first_rd.fields.iter().zip(rd.fields.iter()).enumerate()
                {
                    if first_field.name != field.name {
                        return Err(Error::InconsistentColumnNames {
                            column_index: index,
                            expected: first_field.name.clone(),
                            actual: field.name.clone(),
                        });
                    }
                }

                Ok(false)
            }
        }
    }

    /// Validate a data row's column count against expected count.
    pub(super) fn validate_data_row(&mut self, data_row: &DataRow) -> Result<(), Error> {
        let column_count = data_row.len();

        match self.expected_column_count {
            None => {
                // First data row - store column count
                self.expected_column_count = Some(column_count);
                Ok(())
            }
            Some(expected) => {
                if column_count != expected {
                    Err(Error::InconsistentDataRowCount {
                        expected,
                        actual: column_count,
                    })
                } else {
                    Ok(())
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::net::messages::Field;

    #[test]
    fn test_row_description_validation() {
        let mut validator = Validator::default();

        let rd1 = RowDescription::new(&[Field::text("name"), Field::bigint("id")]);

        let rd2 = RowDescription::new(&[Field::text("name"), Field::bigint("id")]);

        // First row description should be accepted
        assert!(validator.validate_row_description(&rd1).unwrap());

        // Identical second row description should be accepted but not marked as first
        assert!(!validator.validate_row_description(&rd2).unwrap());
    }

    #[test]
    fn test_inconsistent_column_count() {
        let mut validator = Validator::default();

        let rd1 = RowDescription::new(&[Field::text("name")]);
        let rd2 = RowDescription::new(&[Field::text("name"), Field::bigint("id")]);

        validator.validate_row_description(&rd1).unwrap();

        let result = validator.validate_row_description(&rd2);
        assert!(matches!(
            result,
            Err(Error::InconsistentRowDescription {
                expected: 1,
                actual: 2
            })
        ));
    }

    #[test]
    fn test_inconsistent_column_names() {
        let mut validator = Validator::default();

        let rd1 = RowDescription::new(&[Field::text("name")]);
        let rd2 = RowDescription::new(&[Field::text("username")]); // Different name

        validator.validate_row_description(&rd1).unwrap();

        let result = validator.validate_row_description(&rd2);
        assert!(matches!(
            result,
            Err(Error::InconsistentColumnNames {
                column_index: 0,
                expected,
                actual
            }) if expected == "name" && actual == "username"
        ));
    }

    #[test]
    fn test_same_column_names_different_types_allowed() {
        let mut validator = Validator::default();

        let rd1 = RowDescription::new(&[Field::text("name")]);
        let rd2 = RowDescription::new(&[Field::bigint("name")]); // Same name, different type

        validator.validate_row_description(&rd1).unwrap();

        // Should be accepted since we only compare column names, not types
        let result = validator.validate_row_description(&rd2);
        assert!(result.is_ok());
        assert!(!result.unwrap()); // Not the first, so should return false
    }

    #[test]
    fn test_data_row_validation() {
        let mut validator = Validator::default();

        let mut dr1 = DataRow::new();
        dr1.add("test").add(123_i64);

        let mut dr2 = DataRow::new();
        dr2.add("another").add(456_i64);

        let mut dr3 = DataRow::new();
        dr3.add("only_one_column");

        // First data row sets the expected column count
        validator.validate_data_row(&dr1).unwrap();

        // Second data row with same column count should be fine
        validator.validate_data_row(&dr2).unwrap();

        // Third data row with different column count should fail
        let result = validator.validate_data_row(&dr3);
        assert!(matches!(
            result,
            Err(Error::InconsistentDataRowCount {
                expected: 2,
                actual: 1
            })
        ));
    }
}
