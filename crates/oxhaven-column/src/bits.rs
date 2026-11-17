//! Bitmaps made from and read as slices: whether each value of a slice passes a test, as
//! the bits of a [`BooleanBuffer`], and the runs of set bits of one.
//!
//! No Java counterpart: Java's filters write the kept row keys of a chunk into a
//! `WritableLongChunk`. Here a filter and a comparison of a formula give one bit per row,
//! which costs an eighth of a byte per row and combines with `&` and `|`.

use arrow::buffer::{BooleanBuffer, Buffer};

/// The eight bytes of `bytes`, each 0 or 1, as the eight bits of a byte, the first lowest.
fn bits_of_bytes(bytes: &[u8]) -> u64 {
    let mut eight = [0_u8; 8];
    eight.copy_from_slice(bytes);
    // Byte `i` of the factor is `0x80 >> i`, so the top byte of the product is the sum of
    // `byte i << i`; no lower byte's sum reaches 256, so nothing carries into it.
    u64::from_le_bytes(eight).wrapping_mul(0x0102_0408_1020_4080) >> 56
}

/// Bit `row` is `test(values[row])`.
///
/// The values are judged 64 at a time into bytes, a loop with no branch and no bounds
/// check that the compiler turns into vector comparisons, and the bytes are then packed
/// into a word. Judging into bits directly costs a shift and an `or` for every row.
pub fn pack<T: Copy>(values: &[T], test: impl Fn(T) -> bool) -> BooleanBuffer {
    let mut words: Vec<u64> = Vec::with_capacity(values.len().div_ceil(64));
    let mut chunks = values.chunks_exact(64);
    for chunk in &mut chunks {
        let mut bytes = [0_u8; 64];
        for (byte, value) in bytes.iter_mut().zip(chunk) {
            *byte = u8::from(test(*value));
        }
        let mut word = 0_u64;
        for (index, eight) in bytes.chunks_exact(8).enumerate() {
            word |= bits_of_bytes(eight) << (8 * index);
        }
        words.push(word);
    }
    let rest = chunks.remainder();
    if !rest.is_empty() {
        let mut word = 0_u64;
        for (bit, value) in rest.iter().enumerate() {
            word |= u64::from(test(*value)) << bit;
        }
        words.push(word);
    }
    BooleanBuffer::new(Buffer::from_vec(words), 0, values.len())
}

/// Calls `run(start, end)` for every stretch `start..end` of set bits of `mask` among the
/// bits `from..to`, in order. Stretches that touch are one.
///
/// # Panics
/// If `to` is past the end of `mask`.
pub fn for_each_run(
    mask: &BooleanBuffer,
    from: usize,
    to: usize,
    mut run: impl FnMut(usize, usize),
) {
    assert!(
        to <= mask.len(),
        "bits {from}..{to} of a mask of {}",
        mask.len()
    );
    if from >= to {
        return;
    }
    let bytes = mask.values();
    let offset = mask.offset();
    let (from, to) = (from + offset, to + offset);
    // The word of 64 bits that starts at bit `64 * index`, zero past the end of `bytes`.
    let word = |index: usize| -> u64 {
        let mut word = [0_u8; 8];
        let rest = bytes.get(index * 8..).unwrap_or(&[]);
        let length = rest.len().min(8);
        word[..length].copy_from_slice(&rest[..length]);
        u64::from_le_bytes(word)
    };
    let (first_word, last_word) = (from / 64, (to - 1) / 64);
    // The start of a run that is open at the end of the word before.
    let mut open: Option<usize> = None;
    for index in first_word..=last_word {
        let mut bits = word(index);
        if index == first_word {
            bits &= u64::MAX << (from % 64);
        }
        if index == last_word && !to.is_multiple_of(64) {
            bits &= u64::MAX >> (64 - to % 64);
        }
        let base = index * 64;
        // The first bit of every run that starts in this word, and the last bit of every
        // run that ends before the word's last bit. Taking the two a bit at a time pairs
        // them up in a loop whose only branch is its end, where a walk from run to run
        // would branch on the length of each: with short runs that is a wrong guess per
        // run.
        let mut starts = bits & !(bits << 1);
        let mut ends = bits & !(bits >> 1) & !(1 << 63);
        if let Some(start) = open {
            if bits & 1 == 0 {
                // The run ended with the word before.
                run(start - offset, base - offset);
                open = None;
            } else {
                // Bit 0 continues the run. With no end in this word it fills the word.
                starts &= !1;
                if ends != 0 {
                    let end = ends.trailing_zeros() as usize;
                    run(start - offset, base + end + 1 - offset);
                    ends &= ends - 1;
                    open = None;
                }
            }
        }
        while ends != 0 {
            let start = starts.trailing_zeros() as usize;
            let end = ends.trailing_zeros() as usize;
            run(base + start - offset, base + end + 1 - offset);
            starts &= starts - 1;
            ends &= ends - 1;
        }
        // A start left over is that of a run that reaches the word's last bit.
        if starts != 0 {
            open = Some(base + starts.trailing_zeros() as usize);
        }
    }
    if let Some(start) = open {
        run(start - offset, to - offset);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fixed stream of numbers (SplitMix64).
    fn stream(seed: u64) -> impl FnMut() -> u64 {
        let mut state = seed;
        move || {
            state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }
    }

    #[test]
    fn packed_bits_are_the_test_of_each_value() {
        let mut next = stream(7);
        for length in [0, 1, 7, 8, 63, 64, 65, 127, 128, 129, 1000, 4096 + 13] {
            let values: Vec<i32> = (0..length).map(|_| (next() % 100) as i32).collect();
            for threshold in [0, 1, 37, 50, 99, 100] {
                let packed = pack(&values, |value| value < threshold);
                assert_eq!(packed.len(), length);
                let expected: Vec<bool> = values.iter().map(|value| *value < threshold).collect();
                assert_eq!(
                    packed.iter().collect::<Vec<_>>(),
                    expected,
                    "{length} {threshold}"
                );
            }
        }
        // Every pattern of eight neighbouring rows, at every place in a word.
        for pattern in 0..=255_u8 {
            for place in 0..8 {
                let mut values = [false; 64];
                for bit in 0..8 {
                    values[place * 8 + bit] = pattern >> bit & 1 == 1;
                }
                let packed = pack(&values, |value| value);
                assert_eq!(
                    packed.iter().collect::<Vec<_>>(),
                    values,
                    "{pattern} {place}"
                );
            }
        }
    }

    /// The runs of set bits among `from..to`, one bit at a time.
    fn runs_one_by_one(mask: &BooleanBuffer, from: usize, to: usize) -> Vec<(usize, usize)> {
        let mut runs: Vec<(usize, usize)> = Vec::new();
        for bit in from..to {
            if !mask.value(bit) {
                continue;
            }
            match runs.last_mut() {
                Some((_, end)) if *end == bit => *end = bit + 1,
                _ => runs.push((bit, bit + 1)),
            }
        }
        runs
    }

    #[test]
    fn runs_are_the_stretches_of_set_bits_within_the_bounds() {
        let mut next = stream(11);
        // Sparse, half and dense masks, all set and all clear, whole and sliced.
        for (length, one_in) in [(300, 2), (300, 7), (1000, 1), (200, 0), (140, 3), (131, 2)] {
            let bits: Vec<bool> = (0..length)
                .map(|_| match one_in {
                    0 => false,
                    1 => true,
                    n => !next().is_multiple_of(n),
                })
                .collect();
            let whole = BooleanBuffer::from(bits);
            for offset in [0, 1, 5, 9, 61] {
                let mask = whole.slice(offset, length - offset - 3);
                let bounds = [
                    (0, mask.len()),
                    (0, 0),
                    (3, 3),
                    (1, 60),
                    (0, 60),
                    (0, 64.min(mask.len())),
                    (59, 60),
                    (17, mask.len() - 1),
                    (mask.len() - 1, mask.len()),
                ];
                for (from, to) in bounds {
                    let mut runs = Vec::new();
                    for_each_run(&mask, from, to, |start, end| runs.push((start, end)));
                    assert_eq!(
                        runs,
                        runs_one_by_one(&mask, from, to),
                        "{length} rows, one in {one_in}, offset {offset}, bits {from}..{to}"
                    );
                }
            }
        }
    }
}
