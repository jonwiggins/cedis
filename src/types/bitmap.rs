/// A bitmap type backed by a Vec<u8>.
/// Each byte stores 8 bits, with the most significant bit first (bit 0 of byte 0
/// is the highest-order bit), matching Redis bit ordering.
#[derive(Debug, Clone)]
pub struct Bitmap {
    data: Vec<u8>,
}

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
        self.data
            .iter()
            .map(|b| b.count_ones() as usize)
            .sum()
    }

    /// Count set bits in a byte range [start, end] (inclusive).
    /// Negative indices count from the end, like Redis.
    pub fn bitcount_range(&self, start: i64, end: i64) -> usize {
        let len = self.data.len() as i64;
        if len == 0 {
            return 0;
        }

        let start = normalize_index(start, len);
        let end = normalize_index(end, len);

        if start > end || start >= len as usize {
            return 0;
        }

        let end = end.min(self.data.len() - 1);

        self.data[start..=end]
            .iter()
            .map(|b| b.count_ones() as usize)
            .sum()
    }

    /// Bitwise AND of two bitmaps.
    pub fn bitop_and(&self, other: &Self) -> Self {
        let len = self.data.len().max(other.data.len());
        let mut result = vec![0u8; len];
        let min_len = self.data.len().min(other.data.len());
        for i in 0..min_len {
            result[i] = self.data[i] & other.data[i];
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
            let b = if i < other.data.len() { other.data[i] } else { 0 };
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
            let b = if i < other.data.len() { other.data[i] } else { 0 };
            result[i] = a ^ b;
        }
        Bitmap { data: result }
    }

    /// Bitwise NOT of this bitmap.
    pub fn bitop_not(&self) -> Self {
        let result: Vec<u8> = self.data.iter().map(|b| !b).collect();
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
            return if bit { -1 } else { -1 };
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
        if normalized < 0 { 0 } else { normalized as usize }
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
