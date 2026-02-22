/// A circular buffer storing raw RESP-serialized write commands
/// for partial replication resynchronization.
#[derive(Debug)]
pub struct ReplicationBacklog {
    buf: Vec<u8>,
    capacity: usize,
    /// Write position in the circular buffer.
    write_pos: usize,
    /// How many bytes have been written total (may exceed capacity).
    total_written: usize,
    /// The replication offset corresponding to the start of valid data in the buffer.
    start_offset: i64,
    /// The replication offset of the most recently written byte.
    end_offset: i64,
}

impl ReplicationBacklog {
    pub fn new(capacity: usize) -> Self {
        ReplicationBacklog {
            buf: vec![0u8; capacity],
            capacity,
            write_pos: 0,
            total_written: 0,
            start_offset: 0,
            end_offset: 0,
        }
    }

    /// Append data to the backlog. `repl_offset` is the replication offset
    /// after this data has been applied.
    pub fn append(&mut self, data: &[u8], repl_offset: i64) {
        for &b in data {
            self.buf[self.write_pos] = b;
            self.write_pos = (self.write_pos + 1) % self.capacity;
        }
        self.total_written += data.len();
        self.end_offset = repl_offset;

        // Update start_offset: if we've wrapped around, the oldest valid offset
        // is end_offset - capacity
        if self.total_written > self.capacity {
            self.start_offset = self.end_offset - self.capacity as i64;
        }
    }

    /// Check if a given replication offset is still available in the backlog.
    pub fn is_valid_offset(&self, offset: i64) -> bool {
        if self.total_written == 0 {
            return false;
        }
        offset >= self.start_offset && offset <= self.end_offset
    }

    /// Read all data from `offset` to the current end.
    /// Returns None if the offset is no longer in the backlog.
    pub fn read_from(&self, offset: i64) -> Option<Vec<u8>> {
        if !self.is_valid_offset(offset) {
            return None;
        }

        let bytes_to_read = (self.end_offset - offset) as usize;
        if bytes_to_read == 0 {
            return Some(Vec::new());
        }

        // Calculate read position in circular buffer
        let available = std::cmp::min(self.total_written, self.capacity);
        let read_start_from_end = (self.end_offset - offset) as usize;
        if read_start_from_end > available {
            return None;
        }

        let read_pos = if self.write_pos >= read_start_from_end {
            self.write_pos - read_start_from_end
        } else {
            self.capacity - (read_start_from_end - self.write_pos)
        };

        let mut result = Vec::with_capacity(bytes_to_read);
        let mut pos = read_pos;
        for _ in 0..bytes_to_read {
            result.push(self.buf[pos]);
            pos = (pos + 1) % self.capacity;
        }

        Some(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backlog_basic() {
        let mut bl = ReplicationBacklog::new(100);
        let data = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        bl.append(data, data.len() as i64);

        assert!(bl.is_valid_offset(0));
        assert!(bl.is_valid_offset(data.len() as i64));
        assert!(!bl.is_valid_offset(data.len() as i64 + 1));

        let read = bl.read_from(0).unwrap();
        assert_eq!(read, data);
    }

    #[test]
    fn test_backlog_wraparound() {
        let mut bl = ReplicationBacklog::new(10);
        let data1 = b"12345678"; // 8 bytes
        bl.append(data1, 8);

        let data2 = b"abcd"; // 4 bytes, will wrap
        bl.append(data2, 12);

        // Oldest 2 bytes should be gone (total 12, capacity 10)
        assert!(!bl.is_valid_offset(0));
        assert!(!bl.is_valid_offset(1));
        assert!(bl.is_valid_offset(2));
        assert!(bl.is_valid_offset(12));

        let read = bl.read_from(2).unwrap();
        assert_eq!(read, b"345678abcd");
    }
}
