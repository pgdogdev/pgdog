//! Process limits.

/// Raise the NOFILE soft limit to the hard limit and return the new value.
///
/// Some container runtimes (e.g. containerd v2) set a low soft limit
/// while keeping a high hard limit. This causes "Too many open files"
/// errors under load. Raising the soft limit on startup avoids this.
#[allow(clippy::unnecessary_cast)]
#[cfg(unix)]
pub(crate) fn raise_nofile_limit() -> u64 {
    use libc::{RLIMIT_NOFILE, getrlimit, rlimit, setrlimit};
    use tracing::warn;

    let mut rlim = rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };

    unsafe {
        if getrlimit(RLIMIT_NOFILE, &mut rlim) != 0 {
            warn!("failed to get NOFILE limit");
            return 0;
        }
    }

    if rlim.rlim_cur < rlim.rlim_max {
        let prev = rlim.rlim_cur;
        rlim.rlim_cur = rlim.rlim_max;

        unsafe {
            if setrlimit(RLIMIT_NOFILE, &rlim) != 0 {
                warn!(
                    "failed to raise NOFILE soft limit from {} to {}",
                    prev, rlim.rlim_max
                );
                return prev as u64;
            }
        }
    }

    rlim.rlim_cur as u64
}

#[cfg(not(unix))]
pub(crate) fn raise_nofile_limit() -> u64 {
    0
}
