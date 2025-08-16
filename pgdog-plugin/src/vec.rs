// use std::{ffi::c_void, marker::PhantomData, ops::Deref, slice::from_raw_parts};

// use crate::bindings::RustVecString;

// pub struct PdVecStr<'a> {
//     vec: RustVecString,
//     __marker: PhantomData<&'a ()>,
// }

// impl<'a> Deref for PdVecStr<'a> {
//     type Target = [&'a str];

//     fn deref(&self) -> &Self::Target {
//         unsafe { from_raw_parts(self.vec.data as *const &str, self.vec.len) }
//     }
// }

// impl From<RustVecString> for PdVecStr<'_> {
//     fn from(value: RustVecString) -> Self {
//         Self {
//             vec: value,
//             __marker: PhantomData,
//         }
//     }
// }

// impl From<&[&str]> for RustVecString {
//     fn from(value: &[&str]) -> Self {
//         RustVecString {
//             len: value.len(),
//             data: value.as_ptr() as *mut c_void,
//         }
//     }
// }

// #[cfg(test)]
// mod test {
//     use super::*;

//     #[test]
//     fn test_vec_ffi() {
//         let v_str = vec!["test", "hello"];
//         let rs_vstr: RustVecString = v_str.as_slice().into();
//         let pd_vstr: PdVecStr = rs_vstr.into();

//         assert_eq!(pd_vstr[0], "test");
//     }
// }
