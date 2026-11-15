//! The hash a file's `etag` is: Murmur3, the 128 bit variant for 64 bit machines, with seed
//! 0, written as Guava writes a hash code.

const C1: u64 = 0x87c3_7b91_1142_53d5;
const C2: u64 = 0x4cf5_ad43_2745_937f;

fn mix_k1(k1: u64) -> u64 {
    k1.wrapping_mul(C1).rotate_left(31).wrapping_mul(C2)
}

fn mix_k2(k2: u64) -> u64 {
    k2.wrapping_mul(C2).rotate_left(33).wrapping_mul(C1)
}

fn fmix(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    k ^ (k >> 33)
}

/// The value of at most eight bytes read as a little-endian number.
fn little_endian(bytes: &[u8]) -> u64 {
    bytes
        .iter()
        .rev()
        .fold(0, |value, byte| (value << 8) | u64::from(*byte))
}

/// The etag of a file with these contents: 32 hexadecimal digits, the two halves of the
/// hash with their lowest byte first.
///
/// Behavior: `FilesystemStorageServiceGrpcImpl#HASH_FUNCTION`,
/// `com.google.common.hash.Murmur3_128HashFunction`, `com.google.common.hash.HashCode#toString`
pub(super) fn etag(contents: &[u8]) -> String {
    let (mut h1, mut h2) = (0u64, 0u64);
    let mut blocks = contents.chunks_exact(16);
    for block in &mut blocks {
        h1 ^= mix_k1(little_endian(&block[..8]));
        h1 = h1
            .rotate_left(27)
            .wrapping_add(h2)
            .wrapping_mul(5)
            .wrapping_add(0x52dc_e729);
        h2 ^= mix_k2(little_endian(&block[8..]));
        h2 = h2
            .rotate_left(31)
            .wrapping_add(h1)
            .wrapping_mul(5)
            .wrapping_add(0x3849_5ab5);
    }
    let tail = blocks.remainder();
    if tail.len() > 8 {
        h2 ^= mix_k2(little_endian(&tail[8..]));
    }
    if !tail.is_empty() {
        h1 ^= mix_k1(little_endian(&tail[..tail.len().min(8)]));
    }
    let length = contents.len() as u64;
    h1 ^= length;
    h2 ^= length;
    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);
    h1 = fmix(h1);
    h2 = fmix(h2);
    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);
    h1.to_le_bytes()
        .iter()
        .chain(h2.to_le_bytes().iter())
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
