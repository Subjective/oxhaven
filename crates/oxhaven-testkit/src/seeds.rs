//! How many seeds a randomized test runs: a quick share on every pull request, all of them
//! nightly.

use std::ops::Range;
use std::sync::OnceLock;

/// The environment variable that chooses the seed budget of randomized tests: `quick`
/// (also when unset) or `full`.
pub const SEED_BUDGET_VARIABLE: &str = "OXHAVEN_TEST_SEEDS";

/// The quick budget runs one seed in this many, counted from the start of the range.
const QUICK_DIVISOR: u64 = 5;

/// The quick budget never runs fewer seeds of a range than this, or than the range has.
const QUICK_MINIMUM: u64 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Budget {
    Quick,
    Full,
}

impl Budget {
    fn parse(value: Option<&str>) -> Result<Budget, String> {
        match value {
            None | Some("" | "quick") => Ok(Budget::Quick),
            Some("full") => Ok(Budget::Full),
            Some(other) => Err(format!(
                "{SEED_BUDGET_VARIABLE} is `{other}`: expected `quick` or `full`"
            )),
        }
    }

    /// The budget of this process. A value that is neither budget fails the test that
    /// asks, so that a misspelled `full` does not pass as a quick run.
    fn current() -> Budget {
        static BUDGET: OnceLock<Result<Budget, String>> = OnceLock::new();
        let budget = BUDGET.get_or_init(|| {
            let value = std::env::var(SEED_BUDGET_VARIABLE).ok();
            Budget::parse(value.as_deref())
        });
        match budget {
            Ok(budget) => *budget,
            Err(message) => panic!("{message}"),
        }
    }

    fn seeds(self, full: Range<u64>) -> Range<u64> {
        let count = full.end.saturating_sub(full.start);
        let run = match self {
            Budget::Full => count,
            Budget::Quick => count.div_ceil(QUICK_DIVISOR).max(QUICK_MINIMUM).min(count),
        };
        full.start..full.start + run
    }

    fn at_least(self, floor: u64, full: Range<u64>) -> u64 {
        let count = full.end.saturating_sub(full.start);
        if count == 0 {
            return floor;
        }
        let run = self.seeds(full).count() as u64;
        // u128: a floor times a count must not overflow.
        (u128::from(floor) * u128::from(run) / u128::from(count)) as u64
    }
}

/// The seeds of `full` that a randomized test runs under the seed budget of this process.
///
/// With `OXHAVEN_TEST_SEEDS=full` they are all of `full`. Otherwise, which is what a pull
/// request runs, they are its first fifth, rounded up, and at least 2 seeds; a range of 2
/// seeds or fewer runs whole. The seeds a quick run leaves out run in the nightly workflow.
///
/// Write the full range at the call site and let this function cut it:
///
/// ```
/// use oxhaven_testkit::test_seeds;
///
/// for seed in test_seeds(0..40) {
///     // 40 seeds with OXHAVEN_TEST_SEEDS=full, the first 8 otherwise
///     # let _ = seed;
/// }
/// ```
///
/// # Panics
/// If the variable is set to anything but `quick` or `full`.
pub fn test_seeds(full: Range<u64>) -> Range<u64> {
    Budget::current().seeds(full)
}

/// A lower bound that was written for all the seeds of `full`, scaled to the share of them
/// that [`test_seeds`] runs, rounded down. With the full budget it is `floor` itself.
///
/// For a test that asserts how much its seeds exercised:
///
/// ```
/// use oxhaven_testkit::{at_least_for, test_seeds};
///
/// let mut updates = 0;
/// for seed in test_seeds(0..40) {
///     updates += 30; // one run of the seed
///     # let _ = seed;
/// }
/// assert!(updates > at_least_for(1000, 0..40));
/// ```
///
/// # Panics
/// As [`test_seeds`].
pub fn at_least_for(floor: u64, full: Range<u64>) -> u64 {
    Budget::current().at_least(floor, full)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_budget_runs_every_seed_and_keeps_every_floor() {
        for range in [0..0, 0..1, 0..8, 0..9, 0..150, 1000..1080, 7..7] {
            assert_eq!(Budget::Full.seeds(range.clone()), range);
            assert_eq!(Budget::Full.at_least(6000, range), 6000);
        }
    }

    #[test]
    fn quick_budget_runs_the_first_fifth_and_at_least_two() {
        let quick = |range| Budget::Quick.seeds(range);
        assert_eq!(quick(0..150), 0..30);
        assert_eq!(quick(1000..1080), 1000..1016);
        assert_eq!(quick(0..40), 0..8);
        assert_eq!(quick(0..24), 0..5);
        assert_eq!(quick(0..11), 0..3);
        assert_eq!(quick(0..10), 0..2);
        assert_eq!(quick(100..104), 100..102);
        assert_eq!(quick(0..2), 0..2);
        assert_eq!(quick(7..8), 7..8);
        assert_eq!(quick(5..5), 5..5);
    }

    #[test]
    fn quick_budget_scales_a_floor_by_the_share_of_seeds_it_runs() {
        assert_eq!(Budget::Quick.at_least(6000, 0..150), 1200);
        assert_eq!(Budget::Quick.at_least(1000, 0..40), 200);
        assert_eq!(Budget::Quick.at_least(500, 0..24), 500 * 5 / 24);
        assert_eq!(Budget::Quick.at_least(500, 100..104), 250);
        assert_eq!(Budget::Quick.at_least(500, 0..2), 500);
        assert_eq!(Budget::Quick.at_least(500, 3..3), 500);
        assert_eq!(Budget::Quick.at_least(u64::MAX, 0..2), u64::MAX);
    }

    #[test]
    fn budget_names_are_exact() {
        assert_eq!(Budget::parse(None), Ok(Budget::Quick));
        assert_eq!(Budget::parse(Some("")), Ok(Budget::Quick));
        assert_eq!(Budget::parse(Some("quick")), Ok(Budget::Quick));
        assert_eq!(Budget::parse(Some("full")), Ok(Budget::Full));
        for other in ["Full", "FULL", "1", "all", " full"] {
            let error = Budget::parse(Some(other)).unwrap_err();
            assert!(error.contains(SEED_BUDGET_VARIABLE), "{error}");
        }
    }
}
