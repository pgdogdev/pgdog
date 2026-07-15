use crate::PdStr;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Config<'a> {
    pub log_level: PdStr<'a>,
    pub log_json: bool,
    pub plugin_config: PdStr<'a>,
}
