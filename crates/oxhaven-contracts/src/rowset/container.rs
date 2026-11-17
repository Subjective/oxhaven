//! The keys of one block: 16-bit offsets held as a list of runs or as a bitmap.

use std::borrow::Cow;
use std::sync::Arc;

/// Keys per block.
pub(super) const BLOCK_KEYS: u64 = 1 << 16;
/// Words of a block's bitmap.
pub(super) const WORDS: usize = (BLOCK_KEYS / 64) as usize;
/// A block with more runs than this is a bitmap. A run costs 4 bytes and a bitmap 8192, so
/// this is the largest run list that is smaller than the bitmap.
pub(super) const MAX_RUNS: usize = 2047;

pub(super) type Words = [u64; WORDS];

/// The offsets `first..=last` within a block.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(super) struct Run {
    pub first: u16,
    pub last: u16,
}

impl Run {
    pub const WHOLE: Run = Run {
        first: 0,
        last: u16::MAX,
    };

    pub fn len(self) -> u64 {
        u64::from(self.last - self.first) + 1
    }
}

/// What a span holds. Each set of keys has exactly one form, so equal sets are equal
/// structurally:
///
/// - whole blocks are `Full`, and neighbouring `Full` spans are one span;
/// - a block with one run is `One`;
/// - a block with 2 to [`MAX_RUNS`] runs is `Runs`;
/// - a block with more runs is `Bits`.
///
/// `Runs` and `Bits` are shared between row sets and never changed in place.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(super) enum Body {
    /// This many whole blocks.
    Full(u64),
    One(Run),
    Runs(Arc<[Run]>),
    Bits(Arc<Words>),
}

/// The keys of one block, borrowed, in one of the two forms the block operations work on.
#[derive(Clone, Copy)]
pub(super) enum View<'a> {
    Runs(&'a [Run]),
    Bits(&'a Words),
}

impl Body {
    /// The keys of one block of this span.
    pub fn view(&self) -> View<'_> {
        match self {
            Body::Full(_) => View::Runs(std::slice::from_ref(&Run::WHOLE)),
            Body::One(run) => View::Runs(std::slice::from_ref(run)),
            Body::Runs(runs) => View::Runs(runs),
            Body::Bits(words) => View::Bits(words),
        }
    }

    /// Bytes on the heap behind this body, counting a shared container in full.
    pub fn heap_bytes(&self) -> usize {
        // Two reference counts precede the data of an `Arc`.
        let counts = 2 * std::mem::size_of::<usize>();
        match self {
            Body::Full(_) | Body::One(_) => 0,
            Body::Runs(runs) => counts + std::mem::size_of_val::<[Run]>(runs),
            Body::Bits(_) => counts + std::mem::size_of::<Words>(),
        }
    }
}

impl View<'_> {
    pub fn first(self) -> u16 {
        match self {
            View::Runs(runs) => runs[0].first,
            View::Bits(words) => match next_bit(words, 0, false) {
                Some(bit) => bit as u16,
                None => panic!("a stored bitmap is not empty"),
            },
        }
    }

    pub fn last(self) -> u16 {
        match self {
            View::Runs(runs) => runs[runs.len() - 1].last,
            View::Bits(words) => {
                let w = match words.iter().rposition(|&w| w != 0) {
                    Some(w) => w,
                    None => panic!("a stored bitmap is not empty"),
                };
                (w as u32 * 64 + 63 - words[w].leading_zeros()) as u16
            }
        }
    }

    pub fn contains(self, low: u16) -> bool {
        match self {
            View::Runs(runs) => {
                let i = runs.partition_point(|r| r.last < low);
                runs.get(i).is_some_and(|r| r.first <= low)
            }
            View::Bits(words) => words[usize::from(low >> 6)] & (1 << (low & 63)) != 0,
        }
    }

    /// The offset at position `pos` among the keys of this block; `pos` is below their number.
    pub fn select(self, pos: u64) -> u16 {
        let mut left = pos;
        match self {
            View::Runs(runs) => {
                for run in runs {
                    if left < run.len() {
                        return run.first + left as u16;
                    }
                    left -= run.len();
                }
            }
            View::Bits(words) => {
                for (w, &word) in words.iter().enumerate() {
                    let ones = u64::from(word.count_ones());
                    if left < ones {
                        let mut word = word;
                        for _ in 0..left {
                            word &= word - 1;
                        }
                        return (w as u32 * 64 + word.trailing_zeros()) as u16;
                    }
                    left -= ones;
                }
            }
        }
        panic!("position {pos} is beyond the keys of the block");
    }
}

/// The bitmap of a view: borrowed when it is one already.
fn to_words(view: View<'_>) -> Cow<'_, Words> {
    match view {
        View::Bits(words) => Cow::Borrowed(words),
        View::Runs(runs) => {
            let mut words = [0u64; WORDS];
            for run in runs {
                set_run(&mut words, *run);
            }
            Cow::Owned(words)
        }
    }
}

fn set_run(words: &mut Words, run: Run) {
    let (wf, wl) = (usize::from(run.first >> 6), usize::from(run.last >> 6));
    let from_first = !0u64 << (run.first & 63);
    let to_last = !0u64 >> (63 - (run.last & 63));
    if wf == wl {
        words[wf] |= from_first & to_last;
    } else {
        words[wf] |= from_first;
        words[wf + 1..wl].fill(!0);
        words[wl] |= to_last;
    }
}

/// The first offset at or after `from` whose bit is set, or with `clear` is not set.
fn next_bit(words: &Words, from: u32, clear: bool) -> Option<u32> {
    let flip = if clear { !0u64 } else { 0 };
    let mut w = (from >> 6) as usize;
    if w >= WORDS {
        return None;
    }
    let mut word = (words[w] ^ flip) & (!0u64 << (from & 63));
    loop {
        if word != 0 {
            return Some(w as u32 * 64 + word.trailing_zeros());
        }
        w += 1;
        if w == WORDS {
            return None;
        }
        word = words[w] ^ flip;
    }
}

/// The runs of a bitmap, in order.
pub(super) struct BitRuns<'a> {
    words: &'a Words,
    /// The next offset to look at; `BLOCK_KEYS` when done.
    at: u32,
}

impl<'a> BitRuns<'a> {
    pub fn new(words: &'a Words) -> Self {
        Self { words, at: 0 }
    }
}

impl Iterator for BitRuns<'_> {
    type Item = Run;

    fn next(&mut self) -> Option<Run> {
        let first = next_bit(self.words, self.at, false)?;
        let end = next_bit(self.words, first + 1, true).unwrap_or(BLOCK_KEYS as u32);
        self.at = end;
        Some(Run {
            first: first as u16,
            last: (end - 1) as u16,
        })
    }
}

pub(super) fn count_runs(words: &Words) -> usize {
    let mut runs = 0;
    let mut carry = 0;
    for &word in words {
        // A run starts at each set bit whose lower neighbour is clear.
        runs += (word & !((word << 1) | carry)).count_ones() as usize;
        carry = word >> 63;
    }
    runs
}

/// The keys of one block as an operation produced them, before they take their stored form.
pub(super) enum Piece {
    /// Sorted, neither overlapping nor adjacent.
    Runs(Vec<Run>),
    Bits(Box<Words>),
}

impl Piece {
    /// The stored form and the number of keys, or `None` when there is no key.
    pub fn seal(self) -> Option<(Body, u64)> {
        match self {
            Piece::Runs(runs) => seal_runs(&runs),
            Piece::Bits(words) => {
                if count_runs(&words) > MAX_RUNS {
                    let keys = words.iter().map(|w| u64::from(w.count_ones())).sum();
                    Some((Body::Bits(Arc::from(words)), keys))
                } else {
                    seal_runs(&BitRuns::new(&words).collect::<Vec<Run>>())
                }
            }
        }
    }
}

/// The stored form of the keys whose bits are set in `words`, and their number; `None` when
/// no bit is set. What [`Piece::seal`] gives for the same bitmap, without a bitmap on the
/// heap for a block that is not stored as one.
pub(super) fn seal_words(words: &Words) -> Option<(Body, u64)> {
    if count_runs(words) > MAX_RUNS {
        let keys = words.iter().map(|w| u64::from(w.count_ones())).sum();
        Some((Body::Bits(Arc::new(*words)), keys))
    } else {
        seal_runs(&BitRuns::new(words).collect::<Vec<Run>>())
    }
}

fn seal_runs(runs: &[Run]) -> Option<(Body, u64)> {
    let keys = runs.iter().map(|r| r.len()).sum();
    let body = match runs {
        [] => return None,
        [run] if *run == Run::WHOLE => Body::Full(1),
        [run] => Body::One(*run),
        _ if runs.len() <= MAX_RUNS => Body::Runs(Arc::from(runs)),
        _ => {
            let mut words: Box<Words> = Box::new([0; WORDS]);
            for run in runs {
                set_run(&mut words, *run);
            }
            Body::Bits(Arc::from(words))
        }
    };
    Some((body, keys))
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Op {
    Union,
    Intersect,
    Minus,
}

/// `a op b` for the keys of one block.
pub(super) fn combine(op: Op, a: View<'_>, b: View<'_>) -> Piece {
    if let (View::Runs(a), View::Runs(b)) = (a, b) {
        return Piece::Runs(match op {
            Op::Union => union_runs(a, b),
            Op::Intersect => intersect_runs(a, b),
            Op::Minus => minus_runs(a, b),
        });
    }
    let (a, b) = (to_words(a), to_words(b));
    let mut out: Box<Words> = Box::new([0; WORDS]);
    for ((o, &x), &y) in out.iter_mut().zip(a.iter()).zip(b.iter()) {
        *o = match op {
            Op::Union => x | y,
            Op::Intersect => x & y,
            Op::Minus => x & !y,
        };
    }
    Piece::Bits(out)
}

fn union_runs(a: &[Run], b: &[Run]) -> Vec<Run> {
    let mut out: Vec<Run> = Vec::with_capacity(a.len() + b.len());
    let (mut i, mut j) = (0, 0);
    loop {
        let next = match (a.get(i), b.get(j)) {
            (Some(x), Some(y)) if x.first <= y.first => {
                i += 1;
                *x
            }
            (_, Some(y)) => {
                j += 1;
                *y
            }
            (Some(x), None) => {
                i += 1;
                *x
            }
            (None, None) => break,
        };
        match out.last_mut() {
            Some(prev) if u32::from(next.first) <= u32::from(prev.last) + 1 => {
                prev.last = prev.last.max(next.last);
            }
            _ => out.push(next),
        }
    }
    out
}

fn intersect_runs(a: &[Run], b: &[Run]) -> Vec<Run> {
    let mut out = Vec::with_capacity(a.len().max(b.len()));
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        let (x, y) = (a[i], b[j]);
        let (first, last) = (x.first.max(y.first), x.last.min(y.last));
        if first <= last {
            out.push(Run { first, last });
        }
        if x.last < y.last {
            i += 1;
        } else {
            j += 1;
        }
    }
    out
}

fn minus_runs(a: &[Run], b: &[Run]) -> Vec<Run> {
    let mut out = Vec::with_capacity(a.len() + b.len());
    let mut j = 0;
    for x in a {
        // The first offset of `x` not yet decided; one past `x.last` when all of it is.
        let mut start = u32::from(x.first);
        while j < b.len() && u32::from(b[j].last) < start {
            j += 1;
        }
        let mut k = j;
        while k < b.len() && b[k].first <= x.last && start <= u32::from(x.last) {
            let y = b[k];
            if u32::from(y.first) > start {
                out.push(Run {
                    first: start as u16,
                    last: y.first - 1,
                });
            }
            start = start.max(u32::from(y.last) + 1);
            k += 1;
        }
        if start <= u32::from(x.last) {
            out.push(Run {
                first: start as u16,
                last: x.last,
            });
        }
    }
    out
}

/// Collects the runs of one block in increasing order. Runs may touch but not overlap.
#[derive(Default)]
pub(super) struct BlockRuns(Vec<Run>);

impl BlockRuns {
    pub fn push(&mut self, run: Run) {
        match self.0.last_mut() {
            Some(prev) if u32::from(prev.last) + 1 == u32::from(run.first) => prev.last = run.last,
            _ => self.0.push(run),
        }
    }

    /// The stored form of the runs collected so far, and their number of keys; `None` when
    /// there are none. Starts a new block.
    pub fn seal(&mut self) -> Option<(Body, u64)> {
        let sealed = seal_runs(&self.0);
        self.0.clear();
        sealed
    }
}
