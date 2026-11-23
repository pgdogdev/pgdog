use std::{
    cmp::Ordering,
    hash::{Hash, Hasher},
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    str::{FromStr, from_utf8},
};

use bytes::Bytes;

use crate::{Data, DataType, Error, Format, FromDataType, ToDataRowColumn, original::Original};

/// Address family codes used by INET/CIDR binary format.
const PGSQL_AF_INET: u8 = 2;
const PGSQL_AF_INET6: u8 = 3;

/// INET or CIDR, ordered the way Postgres' `network_cmp` orders them.
#[derive(Debug, Clone)]
pub struct Inet {
    addr: IpAddr,
    bits: u8,
    cidr: bool,
    original: Original,
}

impl Inet {
    /// Mark the value as CIDR. Text output doesn't say which type it is.
    pub(crate) fn into_cidr(mut self) -> Self {
        self.cidr = true;
        self
    }

    pub(crate) fn data_type(&self) -> DataType {
        if self.cidr {
            DataType::Cidr
        } else {
            DataType::Inet
        }
    }

    fn max_bits(&self) -> u8 {
        max_bits(&self.addr)
    }

    fn text(&self) -> Bytes {
        if let Some(bytes) = self.original.get(Format::Text) {
            return bytes;
        }
        let text = if self.cidr || self.bits != self.max_bits() {
            format!("{}/{}", self.addr, self.bits)
        } else {
            self.addr.to_string()
        };
        Bytes::from(text)
    }

    fn decode_text(text: &str) -> Result<(IpAddr, u8), Error> {
        let (addr, bits) = match text.split_once('/') {
            Some((addr, bits)) => (addr, Some(bits)),
            None => (text, None),
        };
        let addr = IpAddr::from_str(addr).map_err(|_| Error::UnexpectedPayload)?;
        let bits = match bits {
            Some(bits) => bits.parse::<u8>()?,
            None => max_bits(&addr),
        };
        if bits > max_bits(&addr) {
            return Err(Error::UnexpectedPayload);
        }
        Ok((addr, bits))
    }

    fn decode_binary(bytes: &[u8]) -> Result<(IpAddr, u8, bool), Error> {
        let [family, bits, is_cidr, len, addr @ ..] = bytes else {
            return Err(Error::WrongSizeBinary(bytes.len()));
        };
        if usize::from(*len) != addr.len() {
            return Err(Error::WrongSizeBinary(bytes.len()));
        }
        let addr = match *family {
            PGSQL_AF_INET => IpAddr::V4(Ipv4Addr::from(<[u8; 4]>::try_from(addr)?)),
            PGSQL_AF_INET6 => IpAddr::V6(Ipv6Addr::from(<[u8; 16]>::try_from(addr)?)),
            _ => return Err(Error::UnexpectedPayload),
        };
        if *bits > max_bits(&addr) {
            return Err(Error::UnexpectedPayload);
        }
        Ok((addr, *bits, *is_cidr != 0))
    }
}

fn max_bits(addr: &IpAddr) -> u8 {
    match addr {
        IpAddr::V4(_) => 32,
        IpAddr::V6(_) => 128,
    }
}

/// Compare the leading `bits` of two addresses, like Postgres' `bitncmp`.
fn leading_bits(addr: u128, bits: u8, max_bits: u8) -> u128 {
    addr.checked_shr(u32::from(max_bits - bits)).unwrap_or(0)
}

/// Postgres' `network_cmp_internal` for addresses of the same family.
fn network_cmp(left: (u128, u8), right: (u128, u8), max_bits: u8) -> Ordering {
    let prefix = left.1.min(right.1);
    leading_bits(left.0, prefix, max_bits)
        .cmp(&leading_bits(right.0, prefix, max_bits))
        .then(left.1.cmp(&right.1))
        .then(left.0.cmp(&right.0))
}

impl PartialEq for Inet {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Inet {}

impl PartialOrd for Inet {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Inet {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.addr, other.addr) {
            (IpAddr::V4(left), IpAddr::V4(right)) => network_cmp(
                (u32::from(left).into(), self.bits),
                (u32::from(right).into(), other.bits),
                32,
            ),
            (IpAddr::V6(left), IpAddr::V6(right)) => network_cmp(
                (u128::from(left), self.bits),
                (u128::from(right), other.bits),
                128,
            ),
            (IpAddr::V4(_), IpAddr::V6(_)) => Ordering::Less,
            (IpAddr::V6(_), IpAddr::V4(_)) => Ordering::Greater,
        }
    }
}

impl Hash for Inet {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.addr.hash(state);
        self.bits.hash(state);
    }
}

impl FromDataType for Inet {
    fn decode(bytes: &[u8], encoding: Format) -> Result<Self, Error> {
        let (addr, bits, cidr) = match encoding {
            Format::Text => {
                let (addr, bits) = Self::decode_text(from_utf8(bytes)?)?;
                (addr, bits, false)
            }
            Format::Binary => Self::decode_binary(bytes)?,
        };

        Ok(Self {
            addr,
            bits,
            cidr,
            original: Original::new(bytes, encoding),
        })
    }

    fn encode(&self, encoding: Format) -> Result<Bytes, Error> {
        if let Some(bytes) = self.original.get(encoding) {
            return Ok(bytes);
        }

        match encoding {
            Format::Text => Ok(self.text()),
            Format::Binary => {
                let (family, addr) = match self.addr {
                    IpAddr::V4(addr) => (PGSQL_AF_INET, addr.octets().to_vec()),
                    IpAddr::V6(addr) => (PGSQL_AF_INET6, addr.octets().to_vec()),
                };
                let mut bytes = vec![family, self.bits, u8::from(self.cidr), addr.len() as u8];
                bytes.extend(addr);
                Ok(Bytes::from(bytes))
            }
        }
    }
}

impl ToDataRowColumn for Inet {
    fn to_data_row_column(&self) -> Data {
        self.text().into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(value: &str) -> Inet {
        Inet::decode(value.as_bytes(), Format::Text).expect(value)
    }

    #[test]
    fn test_ordering_matches_network_cmp() {
        // Ascending order according to Postgres' btree inet_ops.
        let ordered = [
            "0.0.0.0/0",
            "9.0.0.1",
            "9.255.255.255",
            "10.0.0.0/8",
            "10.0.0.5/8",
            "10.0.0.0/16",
            "10.0.0.0",
            "10.0.0.5",
            "10.0.0.10",
            "10.1.0.0/16",
            "192.168.1.1",
            "255.255.255.255",
            "::/0",
            "::1",
            "::2",
            "::ffff:1.2.3.4",
            "2001:db8::/32",
            "2001:db8::1",
            "fe80::1",
        ];

        for (left_index, left) in ordered.iter().enumerate() {
            for (right_index, right) in ordered.iter().enumerate() {
                assert_eq!(
                    text(left).cmp(&text(right)),
                    left_index.cmp(&right_index),
                    "{left} vs {right}"
                );
            }
        }
    }

    #[test]
    fn test_explicit_full_mask_equals_host() {
        assert_eq!(text("10.0.0.1/32"), text("10.0.0.1"));
        assert_eq!(text("::1/128"), text("::1"));
    }

    #[test]
    fn test_text_rejects_invalid() {
        for input in [
            "10.0.0.1/33",
            "::1/129",
            "10.0.0",
            "not an ip",
            "10.0.0.1/x",
        ] {
            assert!(
                Inet::decode(input.as_bytes(), Format::Text).is_err(),
                "{input}"
            );
        }
    }

    #[test]
    fn test_binary_roundtrip() {
        for input in ["9.0.0.1", "10.0.0.0/8", "2001:db8::/32", "fe80::1"] {
            let value = text(input);
            let binary = value.encode(Format::Binary).unwrap();
            let decoded = Inet::decode(&binary, Format::Binary).unwrap();
            assert_eq!(decoded, value, "{input}");
            assert_eq!(decoded.encode(Format::Text).unwrap(), Bytes::from(input));
        }
    }

    #[test]
    fn test_binary_rejects_malformed() {
        for payload in [
            &[2, 32, 0, 4, 10, 0, 0][..],
            &[2, 33, 0, 4, 10, 0, 0, 1],
            &[4, 32, 0, 4, 10, 0, 0, 1],
            &[2, 32, 0],
        ] {
            assert!(
                Inet::decode(payload, Format::Binary).is_err(),
                "{payload:?}"
            );
        }
    }

    #[test]
    fn test_cidr_text_and_binary() {
        let cidr = Inet::decode(&[2, 8, 1, 4, 10, 0, 0, 0], Format::Binary).unwrap();
        assert_eq!(cidr.data_type(), DataType::Cidr);
        assert_eq!(
            cidr.encode(Format::Text).unwrap(),
            Bytes::from("10.0.0.0/8")
        );

        let host = text("10.0.0.0/32").into_cidr();
        assert_eq!(
            host.encode(Format::Text).unwrap(),
            Bytes::from("10.0.0.0/32")
        );
        assert_eq!(
            host.encode(Format::Binary).unwrap(),
            Bytes::from_static(&[2, 32, 1, 4, 10, 0, 0, 0])
        );
    }
}
