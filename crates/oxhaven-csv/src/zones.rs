//! The time zone names a date-time cell may end in.
//!
//! Java looks a name up by a number computed from its characters, not by the name, so
//! names that differ only in case are one zone, and so are a few names that are not zones
//! at all. That lookup is reproduced here over the names the reference server knows:
//! `zones.txt` lists the zone ids of its JVM (OpenJDK 21, tz database 2026b) and the
//! aliases of Deephaven's `default_time_zone_aliases.csv`, each zone without transitions
//! followed by its offset from UTC in seconds.

use std::collections::HashMap;
use std::sync::OnceLock;

/// A zone a name stands for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Zone {
    /// A zone that is always this many seconds ahead of UTC.
    Fixed(i32),
    /// A zone with rules of the tz database, by its name.
    Region(&'static str),
}

struct Zones {
    by_key: HashMap<i32, Zone>,
    longest_name: usize,
}

static ZONES: OnceLock<Zones> = OnceLock::new();

fn zones() -> &'static Zones {
    ZONES.get_or_init(|| {
        let mut by_key = HashMap::new();
        let mut longest_name = 0;
        for line in include_str!("zones.txt").lines() {
            let (name, offset) = match line.split_once(' ') {
                Some((name, offset)) => (name, offset.parse().ok()),
                None => (line, None),
            };
            longest_name = longest_name.max(name.len());
            let zone = offset.map_or(Zone::Region(name), Zone::Fixed);
            let key = key(name.as_bytes(), usize::MAX);
            by_key.insert(key.unwrap_or(-1), zone);
        }
        Zones {
            by_key,
            longest_name,
        }
    })
}

/// The number Java files a zone name under, or `None` for a text that has none: an empty
/// one, or one more than a character longer than the longest name.
///
/// Behavior: `io.deephaven.csv.DeephavenTimeZoneParser#tryParseTzKey`
fn key(name: &[u8], longest_name: usize) -> Option<i32> {
    if name.is_empty() || name.len() - 1 > longest_name {
        return None;
    }
    let mut key: i32 = 0;
    for &byte in name {
        // a byte is a signed Java byte widened to a char, and only ASCII letters fold
        let unit = if byte < 0x80 {
            i32::from(byte.to_ascii_uppercase())
        } else {
            0xFF00 | i32::from(byte)
        };
        key = key.wrapping_mul(26).wrapping_add(unit - i32::from(b'A'));
    }
    let key = key.wrapping_abs();
    (key >= 0).then_some(key)
}

/// Whether the zone with this id is UTC at every instant.
pub(crate) fn is_utc(id: &str) -> bool {
    let mut lines = include_str!("zones.txt").lines();
    lines.any(|line| line.split_once(' ') == Some((id, "0")))
}

/// The zone that the text after the space of a date-time cell names, if it names one.
///
/// Behavior: `io.deephaven.csv.DeephavenTimeZoneParser#tryParse`
pub(crate) fn lookup(name: &[u8]) -> Option<Zone> {
    let zones = zones();
    zones.by_key.get(&key(name, zones.longest_name)?).copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_found_by_their_key_whatever_their_case() {
        assert_eq!(lookup(b"UTC"), Some(Zone::Fixed(0)));
        assert_eq!(lookup(b"utc"), Some(Zone::Fixed(0)));
        assert_eq!(lookup(b"Etc/GMT+5"), Some(Zone::Fixed(-18_000)));
        assert_eq!(lookup(b"ET"), Some(Zone::Region("ET")));
        assert_eq!(
            lookup(b"america/new_york"),
            Some(Zone::Region("America/New_York"))
        );
        // not an alias of the reference, and `Z` is a zone only without the space
        assert_eq!(lookup(b"NY"), None);
        assert_eq!(lookup(b"Z"), None);
        assert_eq!(lookup(b""), None);
    }

    #[test]
    fn a_text_with_the_key_of_a_name_is_that_zone() {
        // `V` is one past `U` and `:` is 26 before `T`, so `V:C` has the key of `UTC`
        assert_eq!(key(b"V:C", 40), key(b"UTC", 40));
        assert_eq!(lookup(b"V:C"), Some(Zone::Fixed(0)));
        // one character longer than the longest name is still looked up, two is not
        assert_eq!(key(&[b'A'; 33], 32), Some(0));
        assert_eq!(key(&[b'A'; 34], 32), None);
    }

    #[test]
    fn no_two_names_share_a_key() {
        let names = include_str!("zones.txt").lines().count();
        assert_eq!(zones().by_key.len(), names);
        assert!(!zones().by_key.contains_key(&-1));
        assert_eq!(zones().longest_name, 32);
    }
}
