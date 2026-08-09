//! Minimal multiply-xor hasher for `u64` order ids.
//!
//! `std::collections::HashMap` defaults to SipHash, which is DoS-resistant
//! but costs ~2x on small integer keys. Order ids are engine-internal, so a
//! fast non-cryptographic mix (same construction as FxHash/SplitMix64
//! finalizers) is the right trade-off on the hot path.

use std::hash::{BuildHasherDefault, Hasher};

#[derive(Default)]
pub struct IdHasher {
    state: u64,
}

impl Hasher for IdHasher {
    #[inline]
    fn write_u64(&mut self, n: u64) {
        // SplitMix64 finalizer: full avalanche in three multiply-xor rounds.
        let mut z = n.wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        self.state = z ^ (z >> 31);
    }

    fn write(&mut self, bytes: &[u8]) {
        // Only u64 keys are expected; fall back to a simple fold for anything else.
        for &b in bytes {
            self.state = self.state.rotate_left(8) ^ u64::from(b);
        }
        let s = self.state;
        self.write_u64(s);
    }

    #[inline]
    fn finish(&self) -> u64 {
        self.state
    }
}

pub type IdMap<V> = std::collections::HashMap<u64, V, BuildHasherDefault<IdHasher>>;
