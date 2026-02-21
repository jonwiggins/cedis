/// A bitmap type backed by a Vec<u8>.
/// Each byte stores 8 bits, with the most significant bit first (bit 0 of byte 0
/// is the highest-order bit), matching Redis bit ordering.
#[derive(Debug, Clone, Default)]
pub struct Bitmap {
    data: Vec<u8>,
}

#[allow(clippy::needless_range_loop)]
impl Bitmap {
    pub fn new() -> Self {
        Bitmap { data: Vec::new() }
    }

    /// Set the bit at `offset` to `value`. Returns the old bit value.
    /// The bitmap grows automatically if `offset` is beyond the current size.
    pub fn setbit(&mut self, offset: usize, value: bool) -> bool {
        let byte_index = offset / 8;
        let bit_index = 7 - (offset % 8); // MSB first, matching Redis

        // Grow if needed
        if byte_index >= self.data.len() {
            self.data.resize(byte_index + 1, 0);
        }

        let old = (self.data[byte_index] >> bit_index) & 1 == 1;

        if value {
            self.data[byte_index] |= 1 << bit_index;
        } else {
            self.data[byte_index] &= !(1 << bit_index);
        }

        old
    }

    /// Get the bit at `offset`. Returns false if the offset is beyond the current size.
    pub fn getbit(&self, offset: usize) -> bool {
        let byte_index = offset / 8;
        let bit_index = 7 - (offset % 8);

        if byte_index >= self.data.len() {
            return false;
        }

        (self.data[byte_index] >> bit_index) & 1 == 1
    }

    /// Count all set bits in the entire bitmap.
    pub fn bitcount(&self) -> usize {
        self.data.iter().map(|b| b.count_ones() as usize).sum()
    }

    /// Count set bits in a byte range [start, end] (inclusive).
    /// Negative indices count from the end, like Redis.
    pub fn bitcount_range(&self, start: i64, end: i64) -> usize {
        let len = self.data.len() as i64;
        if len == 0 {
            return 0;
        }

        // Normalize negative indices without clamping to detect start > end
        let start_norm = if start < 0 { len + start } else { start };
        let end_norm = if end < 0 { len + end } else { end };

        if start_norm > end_norm {
            return 0;
        }

        let start = start_norm.max(0) as usize;
        let end = end_norm.max(0) as usize;

        if start >= len as usize {
            return 0;
        }

        let end = end.min(self.data.len() - 1);

        self.data[start..=end]
            .iter()
            .map(|b| b.count_ones() as usize)
            .sum()
    }

    /// Count set bits in a bit range [start, end] (inclusive).
    /// start and end are bit offsets. Negative indices count from the end.
    pub fn bitcount_bit_range(&self, start: i64, end: i64) -> usize {
        let total_bits = (self.data.len() * 8) as i64;
        if total_bits == 0 {
            return 0;
        }

        // Normalize without clamping to detect start > end
        let start_norm = if start < 0 { total_bits + start } else { start };
        let end_norm = if end < 0 { total_bits + end } else { end };

        if start_norm > end_norm {
            return 0;
        }

        let start = start_norm.max(0) as usize;
        let end = end_norm.max(0) as usize;

        if start >= total_bits as usize {
            return 0;
        }
        let end = end.min(total_bits as usize - 1);

        let mut count = 0;
        for bit in start..=end {
            let byte_idx = bit / 8;
            let bit_idx = 7 - (bit % 8); // Redis uses MSB-first bit ordering
            if byte_idx < self.data.len() && (self.data[byte_idx] >> bit_idx) & 1 == 1 {
                count += 1;
            }
        }
        count
    }

    /// Bitwise AND of two bitmaps.
    pub fn bitop_and(&self, other: &Self) -> Self {
        let len = self.data.len().max(other.data.len());
        let mut result = vec![0u8; len];
        for (r, (a, b)) in result
            .iter_mut()
            .zip(self.data.iter().zip(other.data.iter()))
        {
            *r = a & b;
        }
        // Bytes beyond the shorter bitmap AND with 0 = 0, already zero-initialized.
        Bitmap { data: result }
    }

    /// Bitwise OR of two bitmaps.
    pub fn bitop_or(&self, other: &Self) -> Self {
        let len = self.data.len().max(other.data.len());
        let mut result = vec![0u8; len];
        for i in 0..len {
            let a = if i < self.data.len() { self.data[i] } else { 0 };
            let b = if i < other.data.len() {
                other.data[i]
            } else {
                0
            };
            result[i] = a | b;
        }
        Bitmap { data: result }
    }

    /// Bitwise XOR of two bitmaps.
    pub fn bitop_xor(&self, other: &Self) -> Self {
        let len = self.data.len().max(other.data.len());
        let mut result = vec![0u8; len];
        for i in 0..len {
            let a = if i < self.data.len() { self.data[i] } else { 0 };
            let b = if i < other.data.len() {
                other.data[i]
            } else {
                0
            };
            result[i] = a ^ b;
        }
        Bitmap { data: result }
    }

    /// Bitwise NOT of this bitmap.
    pub fn bitop_not(&self) -> Self {
        let result: Vec<u8> = self.data.iter().map(|b| !b).collect();
        Bitmap { data: result }
    }

    /// DIFF: bits set in first bitmap but not in the union of the rest.
    /// result = first & ~(OR of rest)
    pub fn bitop_diff(bitmaps: &[Bitmap]) -> Bitmap {
        if bitmaps.is_empty() {
            return Bitmap::new();
        }
        let first = &bitmaps[0];
        if bitmaps.len() == 1 {
            return first.clone();
        }
        // Compute OR of bitmaps[1..]
        let mut union = bitmaps[1].clone();
        for bm in &bitmaps[2..] {
            union = union.bitop_or(bm);
        }
        // result = first AND NOT union
        let len = first.data.len().max(union.data.len());
        let mut result = vec![0u8; len];
        for i in 0..len {
            let a = if i < first.data.len() {
                first.data[i]
            } else {
                0
            };
            let b = if i < union.data.len() {
                union.data[i]
            } else {
                0
            };
            result[i] = a & !b;
        }
        Bitmap { data: result }
    }

    /// DIFF1: bits NOT set in first bitmap but set in the union of the rest.
    /// result = ~first & (OR of rest)
    pub fn bitop_diff1(bitmaps: &[Bitmap]) -> Bitmap {
        if bitmaps.is_empty() {
            return Bitmap::new();
        }
        let first = &bitmaps[0];
        if bitmaps.len() == 1 {
            return first.clone();
        }
        let mut union = bitmaps[1].clone();
        for bm in &bitmaps[2..] {
            union = union.bitop_or(bm);
        }
        let len = first.data.len().max(union.data.len());
        let mut result = vec![0u8; len];
        for i in 0..len {
            let a = if i < first.data.len() {
                first.data[i]
            } else {
                0
            };
            let b = if i < union.data.len() {
                union.data[i]
            } else {
                0
            };
            result[i] = !a & b;
        }
        Bitmap { data: result }
    }

    /// ANDOR: bits set in first bitmap AND in the union of the rest.
    /// result = first & (OR of rest)
    pub fn bitop_andor(bitmaps: &[Bitmap]) -> Bitmap {
        if bitmaps.is_empty() {
            return Bitmap::new();
        }
        let first = &bitmaps[0];
        if bitmaps.len() == 1 {
            return first.clone();
        }
        let mut union = bitmaps[1].clone();
        for bm in &bitmaps[2..] {
            union = union.bitop_or(bm);
        }
        let len = first.data.len().max(union.data.len());
        let mut result = vec![0u8; len];
        for i in 0..len {
            let a = if i < first.data.len() {
                first.data[i]
            } else {
                0
            };
            let b = if i < union.data.len() {
                union.data[i]
            } else {
                0
            };
            result[i] = a & b;
        }
        Bitmap { data: result }
    }

    /// ONE: bits set in exactly one of the input bitmaps.
    pub fn bitop_one(bitmaps: &[Bitmap]) -> Bitmap {
        if bitmaps.is_empty() {
            return Bitmap::new();
        }
        let len = bitmaps.iter().map(|b| b.data.len()).max().unwrap_or(0);
        let mut result = vec![0u8; len];
        for i in 0..len {
            // Track bits that appear exactly once
            let mut seen_once = 0u8;
            let mut seen_multi = 0u8;
            for bm in bitmaps {
                let b = if i < bm.data.len() { bm.data[i] } else { 0 };
                seen_multi |= seen_once & b;
                seen_once ^= b;
            }
            result[i] = seen_once & !seen_multi;
        }
        Bitmap { data: result }
    }

    /// Find the position of the first bit set to `bit` (true=1, false=0).
    /// Optionally restrict the search to a byte range [start, end].
    /// Returns -1 if the bit is not found.
    pub fn bitpos(&self, bit: bool, start: Option<i64>, end: Option<i64>) -> i64 {
        let len = self.data.len() as i64;

        // If bitmap is empty
        if len == 0 {
            // Looking for 0 in empty string: return 0 (Redis behavior)
            // Looking for 1 in empty string: return -1
            return if bit { -1 } else { 0 };
        }

        let start_byte = match start {
            Some(s) => normalize_index(s, len),
            None => 0,
        };

        let end_specified = end.is_some();
        let end_byte = match end {
            Some(e) => normalize_index(e, len).min(self.data.len() - 1),
            None => self.data.len() - 1,
        };

        if start_byte > end_byte || start_byte >= self.data.len() {
            return -1;
        }

        // Search through the specified byte range
        for byte_idx in start_byte..=end_byte {
            let byte = self.data[byte_idx];
            for bit_idx in 0..8 {
                let shift = 7 - bit_idx;
                let b = (byte >> shift) & 1 == 1;
                if b == bit {
                    return (byte_idx * 8 + bit_idx) as i64;
                }
            }
        }

        // If looking for 0 and we didn't find it in the specified range:
        // - If no end was specified, the first 0 bit is right after the last byte
        // - If end was specified, return -1
        if !bit && !end_specified {
            return (end_byte as i64 + 1) * 8;
        }

        -1
    }

    /// Find the position of the first bit set to `bit` using BIT offsets.
    /// start and end are bit offsets, not byte offsets. Negative indices count from end.
    pub fn bitpos_bit(&self, bit: bool, start: i64, end: i64) -> i64 {
        let total_bits = (self.data.len() * 8) as i64;
        if total_bits == 0 {
            return if bit { -1 } else { 0 };
        }

        let start = if start < 0 {
            (total_bits + start).max(0) as usize
        } else {
            start as usize
        };
        let end = if end < 0 {
            (total_bits + end).max(0) as usize
        } else {
            end as usize
        };

        if start > end || start >= total_bits as usize {
            return -1;
        }
        let end = end.min(total_bits as usize - 1);

        for bit_offset in start..=end {
            let byte_idx = bit_offset / 8;
            let bit_idx = 7 - (bit_offset % 8);
            if byte_idx < self.data.len() {
                let b = (self.data[byte_idx] >> bit_idx) & 1 == 1;
                if b == bit {
                    return bit_offset as i64;
                }
            }
        }

        -1
    }

    /// Get the raw bytes of the bitmap.
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Create a bitmap from raw bytes.
    pub fn from_bytes(data: Vec<u8>) -> Self {
        Bitmap { data }
    }

    /// Get the number of bytes in the bitmap.
    pub fn byte_len(&self) -> usize {
        self.data.len()
    }
}

/// Normalize a possibly-negative index into a non-negative index.
/// Negative indices count from `len` (e.g., -1 => len-1).
fn normalize_index(idx: i64, len: i64) -> usize {
    if idx < 0 {
        let normalized = len + idx;
        if normalized < 0 {
            0
        } else {
            normalized as usize
        }
    } else {
        idx as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_setbit_getbit() {
        let mut bm = Bitmap::new();
        assert!(!bm.setbit(0, true));
        assert!(bm.getbit(0));
        assert!(!bm.getbit(1));

        assert!(!bm.setbit(7, true));
        assert!(bm.getbit(7));

        // Set bit far out, bitmap should grow
        assert!(!bm.setbit(100, true));
        assert!(bm.getbit(100));
        assert!(!bm.getbit(99));
    }

    #[test]
    fn test_setbit_returns_old_value() {
        let mut bm = Bitmap::new();
        assert!(!bm.setbit(5, true));
        assert!(bm.setbit(5, true));
        assert!(bm.setbit(5, false));
        assert!(!bm.setbit(5, false));
    }

    #[test]
    fn test_bitcount() {
        let mut bm = Bitmap::new();
        assert_eq!(bm.bitcount(), 0);
        bm.setbit(0, true);
        bm.setbit(1, true);
        bm.setbit(7, true);
        assert_eq!(bm.bitcount(), 3);
    }

    #[test]
    fn test_bitcount_range() {
        let mut bm = Bitmap::new();
        // Set bits in byte 0 and byte 1
        bm.setbit(0, true); // byte 0
        bm.setbit(8, true); // byte 1
        bm.setbit(16, true); // byte 2
        assert_eq!(bm.bitcount_range(0, 0), 1);
        assert_eq!(bm.bitcount_range(0, 1), 2);
        assert_eq!(bm.bitcount_range(1, 2), 2);
        assert_eq!(bm.bitcount_range(-1, -1), 1);
    }

    #[test]
    fn test_bitop_and() {
        let mut a = Bitmap::new();
        a.setbit(0, true);
        a.setbit(1, true);
        let mut b = Bitmap::new();
        b.setbit(0, true);
        b.setbit(2, true);
        let result = a.bitop_and(&b);
        assert!(result.getbit(0));
        assert!(!result.getbit(1));
        assert!(!result.getbit(2));
    }

    #[test]
    fn test_bitop_or() {
        let mut a = Bitmap::new();
        a.setbit(0, true);
        let mut b = Bitmap::new();
        b.setbit(1, true);
        let result = a.bitop_or(&b);
        assert!(result.getbit(0));
        assert!(result.getbit(1));
    }

    #[test]
    fn test_bitop_xor() {
        let mut a = Bitmap::new();
        a.setbit(0, true);
        a.setbit(1, true);
        let mut b = Bitmap::new();
        b.setbit(0, true);
        b.setbit(2, true);
        let result = a.bitop_xor(&b);
        assert!(!result.getbit(0));
        assert!(result.getbit(1));
        assert!(result.getbit(2));
    }

    #[test]
    fn test_bitop_not() {
        let mut bm = Bitmap::new();
        bm.setbit(0, true);
        bm.setbit(1, false);
        let result = bm.bitop_not();
        assert!(!result.getbit(0));
        assert!(result.getbit(1));
    }

    #[test]
    fn test_bitpos() {
        let mut bm = Bitmap::new();
        // All zeros initially
        bm.setbit(10, true);
        // First set bit is at position 10
        assert_eq!(bm.bitpos(true, None, None), 10);
        // First zero bit is at position 0
        assert_eq!(bm.bitpos(false, None, None), 0);
    }

    #[test]
    fn test_from_bytes() {
        let bm = Bitmap::from_bytes(vec![0xFF, 0x00]);
        assert_eq!(bm.bitcount(), 8);
        assert!(bm.getbit(0));
        assert!(!bm.getbit(8));
    }
}
