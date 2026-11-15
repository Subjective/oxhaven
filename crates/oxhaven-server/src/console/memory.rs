//! The memory figures of `ConsoleService/GetHeapInfo`.
//!
//! Java reports the heap of its JVM: `max_memory` is the most the heap may grow to
//! (`-Xmx`), `total_memory` what the JVM has taken from the system for it, and
//! `free_memory` the part of that which holds no objects. The web IDE's memory widget
//! shows `total - free` as in use, out of `max`. This process has no managed heap, so the
//! three are the nearest quantities it has:
//!
//! | Field | Here |
//! | --- | --- |
//! | `max_memory` | the memory the process may use: the smallest limit of its control group and that group's ancestors (`memory.max`, or `memory.limit_in_bytes` of the first version), and the physical memory of the machine when no group limits it |
//! | `total_memory` | the resident set of the process (`VmRSS`): what it holds in physical memory, whoever allocated it, the engine, the interpreter or a native library |
//! | `free_memory` | what the C allocator holds and has not handed out (`mallinfo2`, `fordblks`), never more than `total_memory`; 0 where the C library is not glibc |
//!
//! So the widget's "in use" is the resident memory less what the allocator could give out
//! again without asking the system, and it fills as the process nears the point where the
//! kernel would end it. The figures are read from `/proc` and `/sys/fs/cgroup`; on a
//! system without them the call answers `UNIMPLEMENTED`.
//!
//! No Java counterpart beyond the three field names:
//! `io.deephaven.engine.table.impl.util.RuntimeMemory`.

use std::path::{Path, PathBuf};

/// What `GetHeapInfo` answers, in bytes.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Memory {
    pub max: i64,
    pub total: i64,
    pub free: i64,
}

/// The value of the line of `/proc/self/status` or `/proc/meminfo` that starts with `key`,
/// in bytes.
fn kilobytes(text: &str, key: &str) -> Option<i64> {
    let line = text.lines().find(|line| line.starts_with(key))?;
    let number = line[key.len()..].trim().trim_end_matches("kB").trim();
    number.parse::<i64>().ok()?.checked_mul(1024)
}

/// The files that may hold a memory limit for a process whose `/proc/self/cgroup` is
/// `membership`: the limit of its group and of every group above it, under `root`.
///
/// A line is `<id>:<controllers>:<path>`. The unified hierarchy (version 2) has no
/// controllers and the limit in `memory.max`; the first version has a hierarchy for the
/// `memory` controller, mounted in a directory of that name, and the limit in
/// `memory.limit_in_bytes`.
fn limit_files(membership: &str, root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for line in membership.lines() {
        let mut parts = line.splitn(3, ':');
        let (Some(_), Some(controllers), Some(path)) = (parts.next(), parts.next(), parts.next())
        else {
            continue;
        };
        let (base, file) = if controllers.is_empty() {
            (root.to_path_buf(), "memory.max")
        } else if controllers.split(',').any(|name| name == "memory") {
            (root.join("memory"), "memory.limit_in_bytes")
        } else {
            continue;
        };
        let mut group = base.clone();
        files.push(group.join(file));
        for segment in path.split('/').filter(|segment| !segment.is_empty()) {
            if segment == ".." {
                // a group outside the namespace's root: its files are not visible
                break;
            }
            group.push(segment);
            files.push(group.join(file));
        }
    }
    files
}

/// The limit a limit file states: `None` for `max`, which is no limit, and for a file that
/// holds no number.
fn stated_limit(text: &str) -> Option<i64> {
    // the first version states no limit as a number near the largest, which is no smaller
    // than any amount of physical memory
    text.trim().parse::<u64>().ok()?.try_into().ok()
}

/// The most memory the process may use: the smallest of `physical` and the limits read.
fn max_memory(physical: i64, limits: impl IntoIterator<Item = i64>) -> i64 {
    limits.into_iter().fold(physical, i64::min)
}

/// The bytes the C allocator holds from the system and has not handed out.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn allocator_free() -> i64 {
    // SAFETY: `mallinfo2` takes no arguments, reads the allocator's own statistics under
    // its locks and returns them by value.
    let info = unsafe { libc::mallinfo2() };
    i64::try_from(info.fordblks).unwrap_or(i64::MAX)
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn allocator_free() -> i64 {
    0
}

/// The memory of this process; `None` on a system without `/proc`.
pub fn process_memory() -> Option<Memory> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    let total = kilobytes(&status, "VmRSS:")?;
    let physical = kilobytes(&meminfo, "MemTotal:")?;
    let membership = std::fs::read_to_string("/proc/self/cgroup").unwrap_or_default();
    let limits = limit_files(&membership, Path::new("/sys/fs/cgroup"))
        .into_iter()
        .filter_map(|file| stated_limit(&std::fs::read_to_string(file).ok()?));
    Some(Memory {
        // a limit below what the process already holds is not one it is held to
        max: max_memory(physical, limits).max(total),
        total,
        free: allocator_free().clamp(0, total),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proc_lines_are_kilobytes() {
        let status = "Name:\tserver\nVmRSS:\t   12345 kB\nVmSwap:\t 0 kB\n";
        assert_eq!(kilobytes(status, "VmRSS:"), Some(12345 * 1024));
        assert_eq!(kilobytes(status, "VmHWM:"), None);
        assert_eq!(kilobytes("VmRSS:\tmany kB\n", "VmRSS:"), None);
    }

    #[test]
    fn the_limit_files_are_those_of_the_group_and_of_the_groups_above_it() {
        let root = Path::new("/sys/fs/cgroup");
        let files = |membership: &str| -> Vec<String> {
            limit_files(membership, root)
                .iter()
                .map(|file| file.display().to_string())
                .collect()
        };
        assert_eq!(
            files("0::/system.slice/server.service\n"),
            [
                "/sys/fs/cgroup/memory.max",
                "/sys/fs/cgroup/system.slice/memory.max",
                "/sys/fs/cgroup/system.slice/server.service/memory.max",
            ]
        );
        // a container sees its own group as the root
        assert_eq!(files("0::/\n"), ["/sys/fs/cgroup/memory.max"]);
        // the first version: only the hierarchy of the memory controller counts
        assert_eq!(
            files("12:cpu,cpuacct:/a\n7:memory:/docker/abc\n"),
            [
                "/sys/fs/cgroup/memory/memory.limit_in_bytes",
                "/sys/fs/cgroup/memory/docker/memory.limit_in_bytes",
                "/sys/fs/cgroup/memory/docker/abc/memory.limit_in_bytes",
            ]
        );
        assert_eq!(files("0::/../other\n"), ["/sys/fs/cgroup/memory.max"]);
        assert!(files("no colons\n").is_empty());
    }

    #[test]
    fn the_maximum_is_the_smallest_limit_or_the_physical_memory() {
        assert_eq!(stated_limit("max\n"), None);
        assert_eq!(stated_limit("1073741824\n"), Some(1 << 30));
        // what the first version states for no limit
        assert_eq!(
            stated_limit("9223372036854771712\n"),
            Some(9223372036854771712)
        );
        assert_eq!(stated_limit("18446744073709551615\n"), None);

        let physical = 8 << 30;
        assert_eq!(max_memory(physical, []), physical);
        assert_eq!(max_memory(physical, [4 << 30, 2 << 30]), 2 << 30);
        assert_eq!(max_memory(physical, [9223372036854771712]), physical);
    }

    #[test]
    fn the_figures_of_this_process_fit_together() {
        let Some(memory) = process_memory() else {
            return; // no /proc here
        };
        assert!(memory.total > 0);
        assert!(memory.max >= memory.total);
        assert!((0..=memory.total).contains(&memory.free));
        let meminfo = std::fs::read_to_string("/proc/meminfo").unwrap();
        assert!(memory.max <= kilobytes(&meminfo, "MemTotal:").unwrap().max(memory.total));
    }
}
