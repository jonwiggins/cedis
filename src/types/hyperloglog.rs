// HyperLogLog probabilistic cardinality estimator.
//
// Uses 2^14 = 16384 registers, matching Redis's HLL implementation.
// Each register stores the maximum number of leading zeros + 1 observed
// for elements hashing to that register.

const HLL_P: usize = 14; // Number of bits used for register index
const HLL_REGISTERS: usize = 1 << HLL_P; // 16384
const HLL_P_MASK: u64 = (HLL_REGISTERS as u64) - 1;

/// Threshold below which we use linear counting for small cardinalities.
const HLL_ALPHA: f64 = 0.7213 / (1.0 + 1.079 / HLL_REGISTERS as f64);

#[derive(Debug, Clone)]
pub struct HyperLogLog {
    registers: Vec<u8>,
}

impl Default for HyperLogLog {
    fn default() -> Self {
        Self::new()
    }
}

impl HyperLogLog {
    pub fn new() -> Self {
        HyperLogLog {
            registers: vec![0; HLL_REGISTERS],
        }
    }

    /// Add an element to the HyperLogLog. Returns true if the internal
    /// state changed (i.e., the cardinality estimate may have changed).
    pub fn add(&mut self, element: &[u8]) -> bool {
        let hash = fnv1a_hash(element);

        // Use the lower P bits to select the register
        let index = (hash & HLL_P_MASK) as usize;

        // Use the remaining bits to count leading zeros
        let remaining = hash >> HLL_P;
        // Count leading zeros in the remaining 50 bits (64 - 14 = 50)
        // We add 1 because HLL uses "position of first 1-bit" which is leading_zeros + 1
        let run = count_leading_zeros_50(remaining) + 1;

        let old = self.registers[index];
        if run > old {
            self.registers[index] = run;
            true
        } else {
            false
        }
    }

    /// Estimate the cardinality using the HyperLogLog algorithm.
    pub fn count(&self) -> u64 {
        // Compute the harmonic mean of 2^(-register[i])
        let mut sum = 0.0f64;
        let mut zero_registers = 0u32;

        for &val in &self.registers {
            sum += 2.0f64.powi(-(val as i32));
            if val == 0 {
                zero_registers += 1;
            }
        }

        let m = HLL_REGISTERS as f64;
        let raw_estimate = HLL_ALPHA * m * m / sum;

        // Apply corrections
        if raw_estimate <= 2.5 * m {
            // Small range correction: use linear counting if there are zero registers
            if zero_registers > 0 {
                // Linear counting
                let lc = m * (m / zero_registers as f64).ln();
                lc as u64
            } else {
                raw_estimate as u64
            }
        } else if raw_estimate > (1u64 << 32) as f64 / 30.0 {
            // Large range correction
            let two_32 = (1u64 << 32) as f64;
            let corrected = -two_32 * (1.0 - raw_estimate / two_32).ln();
            corrected as u64
        } else {
            raw_estimate as u64
        }
    }

    /// Merge another HyperLogLog into this one by taking the max of each register.
    pub fn merge(&mut self, other: &Self) {
        for i in 0..HLL_REGISTERS {
            if other.registers[i] > self.registers[i] {
                self.registers[i] = other.registers[i];
            }
        }
    }
}

/// FNV-1a 64-bit hash function.
fn fnv1a_hash(data: &[u8]) -> u64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x00000100000001B3;

    let mut hash = FNV_OFFSET_BASIS;
    for &byte in data {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// Count leading zeros in the lower 50 bits of a u64.
/// We only consider 50 bits because 14 bits are used for the register index.
fn count_leading_zeros_50(value: u64) -> u8 {
    // We examine bits from position 49 down to 0
    for i in 0..50 {
        if (value >> (49 - i)) & 1 == 1 {
            return i as u8;
        }
    }
    50 // All 50 bits are zero
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_hll_is_empty() {
        let hll = HyperLogLog::new();
        assert_eq!(hll.count(), 0);
    }

    #[test]
    fn test_add_single_element() {
        let mut hll = HyperLogLog::new();
        assert!(hll.add(b"hello"));
        assert!(hll.count() > 0);
    }

    #[test]
    fn test_add_returns_false_for_duplicate() {
        let mut hll = HyperLogLog::new();
        hll.add(b"hello");
        // Adding the same element again should not change state
        assert!(!hll.add(b"hello"));
    }

    #[test]
    fn test_cardinality_estimate() {
        let mut hll = HyperLogLog::new();
        let n = 10000;
        for i in 0..n {
            hll.add(format!("element-{i}").as_bytes());
        }
        let estimate = hll.count();
        // HLL should be within ~2% for this many elements
        let error = (estimate as f64 - n as f64).abs() / n as f64;
        assert!(
            error < 0.05,
            "Estimate {estimate} too far from {n} (error: {:.2}%)",
            error * 100.0
        );
    }

    #[test]
    fn test_merge() {
        let mut hll1 = HyperLogLog::new();
        let mut hll2 = HyperLogLog::new();

        for i in 0..5000 {
            hll1.add(format!("a-{i}").as_bytes());
        }
        for i in 0..5000 {
            hll2.add(format!("b-{i}").as_bytes());
        }

        let count1 = hll1.count();
        let count2 = hll2.count();

        hll1.merge(&hll2);
        let merged_count = hll1.count();

        // Merged count should be roughly count1 + count2 since they are disjoint
        assert!(merged_count > count1);
        assert!(merged_count > count2);
    }

    #[test]
    fn test_fnv1a_hash_deterministic() {
        let h1 = fnv1a_hash(b"test");
        let h2 = fnv1a_hash(b"test");
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_fnv1a_hash_different_inputs() {
        let h1 = fnv1a_hash(b"hello");
        let h2 = fnv1a_hash(b"world");
        assert_ne!(h1, h2);
    }
}
