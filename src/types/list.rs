use std::collections::VecDeque;

/// Redis list type — implemented using VecDeque for efficient push/pop from both ends.
#[derive(Debug, Clone, Default)]
pub struct RedisList {
    data: VecDeque<Vec<u8>>,
}

impl RedisList {
    pub fn new() -> Self {
        RedisList {
            data: VecDeque::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn lpush(&mut self, value: Vec<u8>) {
        self.data.push_front(value);
    }

    pub fn rpush(&mut self, value: Vec<u8>) {
        self.data.push_back(value);
    }

    pub fn lpop(&mut self) -> Option<Vec<u8>> {
        self.data.pop_front()
    }

    pub fn rpop(&mut self) -> Option<Vec<u8>> {
        self.data.pop_back()
    }

    pub fn lindex(&self, index: i64) -> Option<&Vec<u8>> {
        let idx = self.resolve_index(index)?;
        self.data.get(idx)
    }

    pub fn lset(&mut self, index: i64, value: Vec<u8>) -> bool {
        if let Some(idx) = self.resolve_index(index)
            && idx < self.data.len()
        {
            self.data[idx] = value;
            return true;
        }
        false
    }

    pub fn lrange(&self, start: i64, stop: i64) -> Vec<&Vec<u8>> {
        let len = self.data.len() as i64;
        let start = if start < 0 {
            (len + start).max(0)
        } else {
            start
        } as usize;
        let stop = if stop < 0 { (len + stop).max(0) } else { stop } as usize;

        if start > stop || start >= self.data.len() {
            return vec![];
        }

        let stop = stop.min(self.data.len() - 1);
        (start..=stop).filter_map(|i| self.data.get(i)).collect()
    }

    pub fn lrem(&mut self, count: i64, value: &[u8]) -> i64 {
        let mut removed = 0i64;
        if count > 0 {
            let mut i = 0;
            while i < self.data.len() && removed < count {
                if self.data[i] == value {
                    self.data.remove(i);
                    removed += 1;
                } else {
                    i += 1;
                }
            }
        } else if count < 0 {
            let target = -count;
            let mut i = self.data.len();
            while i > 0 && removed < target {
                i -= 1;
                if self.data[i] == value {
                    self.data.remove(i);
                    removed += 1;
                }
            }
        } else {
            self.data.retain(|v| {
                if v == value {
                    removed += 1;
                    false
                } else {
                    true
                }
            });
        }
        removed
    }

    pub fn ltrim(&mut self, start: i64, stop: i64) {
        let len = self.data.len() as i64;
        let start = if start < 0 {
            (len + start).max(0)
        } else {
            start
        } as usize;
        let stop = if stop < 0 { (len + stop).max(0) } else { stop } as usize;

        if start > stop || start >= self.data.len() {
            self.data.clear();
            return;
        }

        let stop = stop.min(self.data.len() - 1);
        let new_data: VecDeque<Vec<u8>> = self.data.drain(start..=stop).collect();
        self.data = new_data;
    }

    pub fn linsert_before(&mut self, pivot: &[u8], value: Vec<u8>) -> Option<usize> {
        if let Some(pos) = self.data.iter().position(|v| v.as_slice() == pivot) {
            self.data.insert(pos, value);
            Some(self.data.len())
        } else {
            None
        }
    }

    pub fn linsert_after(&mut self, pivot: &[u8], value: Vec<u8>) -> Option<usize> {
        if let Some(pos) = self.data.iter().position(|v| v.as_slice() == pivot) {
            self.data.insert(pos + 1, value);
            Some(self.data.len())
        } else {
            None
        }
    }

    pub fn lpos(&self, value: &[u8], rank: i64, count: Option<i64>, maxlen: usize) -> Vec<usize> {
        let len = self.data.len();
        let limit = if maxlen == 0 { len } else { maxlen.min(len) };

        let mut results = Vec::new();
        let max_count = count.unwrap_or(1);
        // If count is 0, return ALL matches
        let unlimited = max_count == 0;

        if rank >= 0 {
            // Forward scan, skip (rank-1) matches
            let skip = if rank == 0 { 0 } else { (rank - 1) as usize };
            let mut skipped = 0;
            for (scanned, (i, v)) in self.data.iter().enumerate().enumerate() {
                if scanned >= limit {
                    break;
                }
                if v.as_slice() == value {
                    if skipped < skip {
                        skipped += 1;
                        continue;
                    }
                    results.push(i);
                    if !unlimited && results.len() >= max_count as usize {
                        break;
                    }
                }
            }
        } else {
            // Reverse scan
            let skip = ((-rank) - 1) as usize;
            let mut skipped = 0;
            for (scanned, i) in (0..len).rev().enumerate() {
                if scanned >= limit {
                    break;
                }
                if self.data[i].as_slice() == value {
                    if skipped < skip {
                        skipped += 1;
                        continue;
                    }
                    results.push(i);
                    if !unlimited && results.len() >= max_count as usize {
                        break;
                    }
                }
            }
        }

        results
    }

    pub fn iter(&self) -> impl Iterator<Item = &Vec<u8>> {
        self.data.iter()
    }

    fn resolve_index(&self, index: i64) -> Option<usize> {
        let len = self.data.len() as i64;
        if index < 0 {
            let idx = len + index;
            if idx < 0 { None } else { Some(idx as usize) }
        } else {
            Some(index as usize)
        }
    }
}
