//! Rewrite type OIDs embedded in binary-format values.
//!
//! Arrays carry their element type OID and composites (including anonymous
//! records) carry the OID of every field. Clients check those against the
//! type information they cached, so they have to be canonicalized too.

use std::collections::HashMap;

use bytes::{Buf, BufMut};

use super::TypeKind;

/// `record`, the anonymous composite type.
const RECORD_OID: u32 = 2249;
/// `record[]`.
const RECORD_ARRAY_OID: u32 = 2287;

/// How deep nested arrays/composites are followed before giving up.
const MAX_DEPTH: usize = 16;

/// Malformed binary value; leave it alone.
#[derive(Debug, PartialEq)]
pub(crate) struct Malformed;

/// Rewrites embedded OIDs of one direction (shard to canonical, or the reverse).
pub(crate) struct PayloadRewriter<'a> {
    /// Kinds of the types on the source side, keyed by their OID.
    kinds: &'a HashMap<u32, TypeKind>,
    /// Source to destination OIDs.
    mapping: &'a HashMap<u32, u32>,
}

impl<'a> PayloadRewriter<'a> {
    pub(crate) fn new(kinds: &'a HashMap<u32, TypeKind>, mapping: &'a HashMap<u32, u32>) -> Self {
        Self { kinds, mapping }
    }

    /// The kind of a type, looking through domains.
    fn kind(&self, mut oid: u32) -> TypeKind {
        for _ in 0..MAX_DEPTH {
            match oid {
                RECORD_OID => return TypeKind::Composite,
                RECORD_ARRAY_OID => {
                    return TypeKind::Array {
                        element: RECORD_OID,
                    };
                }
                _ => (),
            }
            match self.kinds.get(&oid) {
                Some(TypeKind::Domain { base }) => oid = *base,
                Some(kind) => return *kind,
                None => return TypeKind::Other,
            }
        }
        TypeKind::Other
    }

    /// A binary value of this type may embed type OIDs.
    pub(crate) fn needs_rewrite(&self, oid: u32) -> bool {
        matches!(self.kind(oid), TypeKind::Array { .. } | TypeKind::Composite)
    }

    /// Rewrite the OIDs embedded in a binary value of the given type, in place.
    /// Returns whether anything changed.
    pub(crate) fn rewrite(&self, oid: u32, data: &mut [u8]) -> Result<bool, Malformed> {
        self.rewrite_at(oid, data, 0).map(|(changed, _)| changed)
    }

    /// Rewrite a value and return how many bytes it occupied.
    fn rewrite_at(
        &self,
        oid: u32,
        data: &mut [u8],
        depth: usize,
    ) -> Result<(bool, usize), Malformed> {
        if depth > MAX_DEPTH {
            return Err(Malformed);
        }

        match self.kind(oid) {
            TypeKind::Array { .. } => self.rewrite_array(data, depth),
            TypeKind::Composite => self.rewrite_composite(data, depth),
            TypeKind::Domain { .. } | TypeKind::Other => Ok((false, data.len())),
        }
    }

    /// Big-endian `i32` at `pos`.
    fn read_i32(data: &[u8], pos: usize) -> Result<i32, Malformed> {
        data.get(pos..pos + 4)
            .map(|mut bytes| bytes.get_i32())
            .ok_or(Malformed)
    }

    /// Replace the OID stored at `pos` with its mapping, if any.
    fn map_oid(&self, data: &mut [u8], pos: usize) -> Result<(u32, bool), Malformed> {
        let slot = data.get_mut(pos..pos + 4).ok_or(Malformed)?;
        let oid = (&slot[..]).get_u32();
        match self.mapping.get(&oid) {
            Some(&mapped) => {
                (&mut slot[..]).put_u32(mapped);
                Ok((oid, true))
            }
            None => Ok((oid, false)),
        }
    }

    fn rewrite_array(&self, data: &mut [u8], depth: usize) -> Result<(bool, usize), Malformed> {
        if data.len() < 12 {
            return Err(Malformed);
        }
        let ndim = Self::read_i32(data, 0)?;
        if !(0..=6).contains(&ndim) {
            return Err(Malformed);
        }
        let (element, mut changed) = self.map_oid(data, 8)?;

        let mut pos = 12;
        let mut elements: usize = 1;
        for _ in 0..ndim {
            let size = Self::read_i32(data, pos)?;
            if size < 0 {
                return Err(Malformed);
            }
            elements = elements.checked_mul(size as usize).ok_or(Malformed)?;
            pos += 8;
        }
        if ndim == 0 {
            elements = 0;
        }

        let recurse = self.needs_rewrite(element);
        for _ in 0..elements {
            let len = Self::read_i32(data, pos)?;
            pos += 4;
            if len < 0 {
                continue;
            }
            let len = len as usize;
            let value = data.get_mut(pos..pos + len).ok_or(Malformed)?;
            if recurse {
                changed |= self.rewrite_at(element, value, depth + 1)?.0;
            }
            pos += len;
        }

        Ok((changed, pos))
    }

    fn rewrite_composite(&self, data: &mut [u8], depth: usize) -> Result<(bool, usize), Malformed> {
        let fields = Self::read_i32(data, 0)?;
        if fields < 0 {
            return Err(Malformed);
        }

        let mut changed = false;
        let mut pos = 4;
        for _ in 0..fields {
            let (field_oid, mapped) = self.map_oid(data, pos)?;
            changed |= mapped;
            pos += 4;
            let len = Self::read_i32(data, pos)?;
            pos += 4;
            if len < 0 {
                continue;
            }
            let len = len as usize;
            let value = data.get_mut(pos..pos + len).ok_or(Malformed)?;
            if self.needs_rewrite(field_oid) {
                changed |= self.rewrite_at(field_oid, value, depth + 1)?.0;
            }
            pos += len;
        }

        Ok((changed, pos))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use bytes::BytesMut;

    const MOOD: u32 = 17000;
    const MOOD_ARRAY: u32 = 17001;
    const PAIR: u32 = 17002;
    const PAIR_ARRAY: u32 = 17003;
    const POSINT: u32 = 17004;

    fn kinds() -> HashMap<u32, TypeKind> {
        [
            (MOOD, TypeKind::Other),
            (MOOD_ARRAY, TypeKind::Array { element: MOOD }),
            (PAIR, TypeKind::Composite),
            (PAIR_ARRAY, TypeKind::Array { element: PAIR }),
            (POSINT, TypeKind::Domain { base: MOOD_ARRAY }),
        ]
        .into_iter()
        .collect()
    }

    fn mapping() -> HashMap<u32, u32> {
        [
            (MOOD, 16400),
            (MOOD_ARRAY, 16401),
            (PAIR, 16402),
            (PAIR_ARRAY, 16403),
        ]
        .into_iter()
        .collect()
    }

    fn array(element: u32, values: &[Option<&[u8]>]) -> Vec<u8> {
        let mut buf = BytesMut::new();
        buf.put_i32(1);
        buf.put_i32(values.iter().any(Option::is_none) as i32);
        buf.put_u32(element);
        buf.put_i32(values.len() as i32);
        buf.put_i32(1);
        for value in values {
            match value {
                Some(value) => {
                    buf.put_i32(value.len() as i32);
                    buf.put_slice(value);
                }
                None => buf.put_i32(-1),
            }
        }
        buf.to_vec()
    }

    fn composite(fields: &[(u32, Option<&[u8]>)]) -> Vec<u8> {
        let mut buf = BytesMut::new();
        buf.put_i32(fields.len() as i32);
        for (oid, value) in fields {
            buf.put_u32(*oid);
            match value {
                Some(value) => {
                    buf.put_i32(value.len() as i32);
                    buf.put_slice(value);
                }
                None => buf.put_i32(-1),
            }
        }
        buf.to_vec()
    }

    #[test]
    fn test_needs_rewrite() {
        let (kinds, mapping) = (kinds(), mapping());
        let rewriter = PayloadRewriter::new(&kinds, &mapping);
        assert!(!rewriter.needs_rewrite(MOOD));
        assert!(!rewriter.needs_rewrite(25));
        assert!(rewriter.needs_rewrite(MOOD_ARRAY));
        assert!(rewriter.needs_rewrite(PAIR));
        assert!(rewriter.needs_rewrite(POSINT), "domain over an array");
        assert!(rewriter.needs_rewrite(RECORD_OID));
        assert!(rewriter.needs_rewrite(RECORD_ARRAY_OID));
    }

    #[test]
    fn test_array_of_enum() {
        let (kinds, mapping) = (kinds(), mapping());
        let rewriter = PayloadRewriter::new(&kinds, &mapping);

        let mut data = array(MOOD, &[Some(b"sad"), None, Some(b"happy")]);
        assert_eq!(rewriter.rewrite(MOOD_ARRAY, &mut data), Ok(true));
        assert_eq!(data, array(16400, &[Some(b"sad"), None, Some(b"happy")]));

        // Through a domain.
        let mut data = array(MOOD, &[Some(b"ok")]);
        assert_eq!(rewriter.rewrite(POSINT, &mut data), Ok(true));
        assert_eq!(data, array(16400, &[Some(b"ok")]));

        // Builtin elements are left alone.
        let mut data = array(25, &[Some(b"text")]);
        assert_eq!(rewriter.rewrite(MOOD_ARRAY, &mut data), Ok(false));
        assert_eq!(data, array(25, &[Some(b"text")]));
    }

    #[test]
    fn test_empty_array() {
        let (kinds, mapping) = (kinds(), mapping());
        let rewriter = PayloadRewriter::new(&kinds, &mapping);

        let mut buf = BytesMut::new();
        buf.put_i32(0);
        buf.put_i32(0);
        buf.put_u32(MOOD);
        let mut data = buf.to_vec();
        assert_eq!(rewriter.rewrite(MOOD_ARRAY, &mut data), Ok(true));
        assert_eq!((&data[8..12]).get_u32(), 16400);
    }

    #[test]
    fn test_composite_with_nested_array() {
        let (kinds, mapping) = (kinds(), mapping());
        let rewriter = PayloadRewriter::new(&kinds, &mapping);

        let moods = array(MOOD, &[Some(b"sad")]);
        let mut data = composite(&[
            (25, Some(b"name")),
            (MOOD, None),
            (MOOD_ARRAY, Some(&moods)),
        ]);
        assert_eq!(rewriter.rewrite(PAIR, &mut data), Ok(true));

        let expected_moods = array(16400, &[Some(b"sad")]);
        assert_eq!(
            data,
            composite(&[
                (25, Some(b"name")),
                (16400, None),
                (16401, Some(&expected_moods))
            ])
        );
    }

    #[test]
    fn test_array_of_composites_and_records() {
        let (kinds, mapping) = (kinds(), mapping());
        let rewriter = PayloadRewriter::new(&kinds, &mapping);

        let pair = composite(&[(MOOD, Some(b"ok")), (20, Some(&1i64.to_be_bytes()))]);
        let mut data = array(PAIR, &[Some(&pair), Some(&pair)]);
        assert_eq!(rewriter.rewrite(PAIR_ARRAY, &mut data), Ok(true));

        let expected_pair = composite(&[(16400, Some(b"ok")), (20, Some(&1i64.to_be_bytes()))]);
        assert_eq!(
            data,
            array(16402, &[Some(&expected_pair), Some(&expected_pair)])
        );

        // Anonymous records carry user types too.
        let mut data = composite(&[(MOOD, Some(b"ok"))]);
        assert_eq!(rewriter.rewrite(RECORD_OID, &mut data), Ok(true));
        assert_eq!(data, composite(&[(16400, Some(b"ok"))]));
    }

    #[test]
    fn test_malformed_leaves_no_partial_writes_visible() {
        let (kinds, mapping) = (kinds(), mapping());
        let rewriter = PayloadRewriter::new(&kinds, &mapping);

        let mut data = vec![0, 0, 0, 1, 0, 0];
        assert_eq!(rewriter.rewrite(MOOD_ARRAY, &mut data), Err(Malformed));

        // Element length runs past the end.
        let mut data = array(MOOD, &[Some(b"sad")]);
        let len = data.len();
        data[len - 4 - 3..len - 3].copy_from_slice(&100i32.to_be_bytes());
        assert_eq!(rewriter.rewrite(MOOD_ARRAY, &mut data), Err(Malformed));

        let mut data = composite(&[(MOOD, Some(b"ok"))]);
        data.truncate(10);
        assert_eq!(rewriter.rewrite(PAIR, &mut data), Err(Malformed));
    }
}
