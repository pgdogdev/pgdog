use crate::bindings::PdStr;
use std::{ops::Deref, os::raw::c_void, slice::from_raw_parts, str::from_utf8_unchecked};

impl From<&str> for PdStr {
    fn from(value: &str) -> Self {
        PdStr {
            data: value.as_ptr() as *mut c_void,
            len: value.len(),
        }
    }
}

impl From<&String> for PdStr {
    fn from(value: &String) -> Self {
        PdStr {
            data: value.as_ptr() as *mut c_void,
            len: value.len(),
        }
    }
}

impl Deref for PdStr {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        unsafe {
            let slice = from_raw_parts::<u8>(self.data as *mut u8, self.len);
            from_utf8_unchecked(slice)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_pd_str() {
        let s = "one_two_three";
        let pd = PdStr::from(s);
        assert_eq!(pd.deref(), "one_two_three");

        let s = String::from("one_two");
        let pd = PdStr::from(&s);
        assert_eq!(pd.deref(), "one_two");
        assert_eq!(&*pd, "one_two");
    }
}
