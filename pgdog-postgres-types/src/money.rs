use std::{
    cmp::Ordering,
    hash::{Hash, Hasher},
    str::from_utf8,
};

use bytes::{Buf, Bytes};

use crate::{Data, DataType, Error, Format, FromDataType, ToDataRowColumn, original::Original};

/// MONEY, ordered by its value in the currency's minor units.
#[derive(Debug, Clone)]
pub struct Money {
    units: i64,
    original: Original,
}

impl Money {
    /// Parse text output for any `lc_monetary`.
    ///
    /// Postgres prints every value in a session with the same number of
    /// fractional digits, so the digits alone give the amount in minor units
    /// regardless of currency symbol, grouping or decimal separator.
    /// Negative amounts carry a `-` or are wrapped in parentheses.
    fn parse_text(text: &str) -> Result<i64, Error> {
        let mut units: i128 = 0;
        let mut digits = false;

        for digit in text.bytes().filter(u8::is_ascii_digit) {
            units = units * 10 + i128::from(digit - b'0');
            digits = true;
            if units > i128::from(i64::MAX) + 1 {
                return Err(Error::NumericOutOfRange(DataType::Money));
            }
        }

        if !digits {
            return Err(Error::UnexpectedPayload);
        }

        if text.contains(['-', '(']) {
            units = -units;
        }

        i64::try_from(units).map_err(|_| Error::NumericOutOfRange(DataType::Money))
    }
}

impl PartialEq for Money {
    fn eq(&self, other: &Self) -> bool {
        self.units == other.units
    }
}

impl Eq for Money {}

impl PartialOrd for Money {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Money {
    fn cmp(&self, other: &Self) -> Ordering {
        self.units.cmp(&other.units)
    }
}

impl Hash for Money {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.units.hash(state);
    }
}

impl FromDataType for Money {
    fn decode(bytes: &[u8], encoding: Format) -> Result<Self, Error> {
        let units = match encoding {
            Format::Text => Self::parse_text(from_utf8(bytes)?)?,
            Format::Binary => {
                if bytes.len() != 8 {
                    return Err(Error::WrongSizeBinary(bytes.len()));
                }
                let mut buf = bytes;
                buf.get_i64()
            }
        };

        Ok(Self {
            units,
            original: Original::new(bytes, encoding),
        })
    }

    fn encode(&self, encoding: Format) -> Result<Bytes, Error> {
        if let Some(bytes) = self.original.get(encoding) {
            return Ok(bytes);
        }

        match encoding {
            Format::Binary => Ok(Bytes::copy_from_slice(&self.units.to_be_bytes())),
            // Formatting depends on the server's lc_monetary.
            Format::Text => Err(Error::SessionDependentText(DataType::Money)),
        }
    }
}

impl ToDataRowColumn for Money {
    fn to_data_row_column(&self) -> Data {
        self.original.bytes().clone().into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(value: &str) -> Money {
        Money::decode(value.as_bytes(), Format::Text).expect(value)
    }

    #[test]
    fn test_text_units_ignore_locale_formatting() {
        for (input, units) in [
            ("$5.00", 500),
            ("₹1,000.00", 100_000),
            ("₹1,00,000.00", 10_000_000),
            ("1.000,00 €", 100_000),
            ("¥1,000", 1_000),
            ("-$5.00", -500),
            ("($5.00)", -500),
            ("-₹0.01", -1),
            ("$0.00", 0),
            ("$92,233,720,368,547,758.07", i64::MAX),
            ("-$92,233,720,368,547,758.08", i64::MIN),
        ] {
            assert_eq!(text(input).units, units, "{input}");
        }
    }

    #[test]
    fn test_text_rejects_out_of_range_and_garbage() {
        for input in [
            "$92,233,720,368,547,758.08",
            "$999,999,999,999,999,999,999.00",
        ] {
            assert!(matches!(
                Money::decode(input.as_bytes(), Format::Text),
                Err(Error::NumericOutOfRange(DataType::Money))
            ));
        }
        assert!(Money::decode(b"$", Format::Text).is_err());
    }

    #[test]
    fn test_binary_decode() {
        let decoded = Money::decode(&100_000_i64.to_be_bytes(), Format::Binary).unwrap();
        assert_eq!(decoded, text("₹1,000.00"));
        assert!(Money::decode(&[0; 4], Format::Binary).is_err());
    }

    #[test]
    fn test_encode_returns_original_bytes() {
        let money = text("₹1,00,000.00");
        assert_eq!(
            money.encode(Format::Text).unwrap(),
            Bytes::from("₹1,00,000.00")
        );
        assert_eq!(
            money.encode(Format::Binary).unwrap(),
            Bytes::copy_from_slice(&10_000_000_i64.to_be_bytes())
        );

        let binary = Money::decode(&500_i64.to_be_bytes(), Format::Binary).unwrap();
        assert!(matches!(
            binary.encode(Format::Text),
            Err(Error::SessionDependentText(DataType::Money))
        ));
    }
}
