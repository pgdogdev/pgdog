use std::fs::File;
use std::io::{Error, Read, Result};
use std::mem::size_of;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

const PERF_TYPE_HARDWARE: u32 = 0;
const PERF_COUNT_HW_INSTRUCTIONS: u64 = 1;

const DISABLED: u64 = 1 << 0;
const EXCLUDE_KERNEL: u64 = 1 << 5;
const EXCLUDE_HV: u64 = 1 << 6;

const PERF_EVENT_IOC_ENABLE: libc::Ioctl = 0x2400;
const PERF_EVENT_IOC_DISABLE: libc::Ioctl = 0x2401;

#[repr(C)]
#[derive(Default)]
struct PerfEventAttr {
    kind: u32,
    size: u32,
    config: u64,
    sample_period: u64,
    sample_type: u64,
    read_format: u64,
    flags: u64,
    wakeup_events: u32,
    bp_type: u32,
    config1: u64,
}

pub(super) struct Instructions {
    file: File,
}

impl Instructions {
    pub(super) fn new() -> Result<Self> {
        let attr = PerfEventAttr {
            kind: PERF_TYPE_HARDWARE,
            size: size_of::<PerfEventAttr>() as u32,
            config: PERF_COUNT_HW_INSTRUCTIONS,
            flags: DISABLED | EXCLUDE_KERNEL | EXCLUDE_HV,
            ..Default::default()
        };

        let fd = unsafe { libc::syscall(libc::SYS_perf_event_open, &attr, 0, -1, -1, 0) };

        if fd < 0 {
            return Err(Error::last_os_error());
        }

        let fd = unsafe { OwnedFd::from_raw_fd(fd as i32) };

        Ok(Self {
            file: File::from(fd),
        })
    }

    pub(super) fn enable(&self) -> Result<()> {
        self.ioctl(PERF_EVENT_IOC_ENABLE)
    }

    pub(super) fn disable(&self) -> Result<()> {
        self.ioctl(PERF_EVENT_IOC_DISABLE)
    }

    pub(super) fn read(&mut self) -> Result<u64> {
        let mut count = [0; 8];
        self.file.read_exact(&mut count)?;

        Ok(u64::from_ne_bytes(count))
    }

    fn ioctl(&self, request: libc::Ioctl) -> Result<()> {
        if unsafe { libc::ioctl(self.file.as_raw_fd(), request, 0) } < 0 {
            return Err(Error::last_os_error());
        }

        Ok(())
    }
}
