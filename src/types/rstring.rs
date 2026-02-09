/// Redis string type — binary-safe, stored as raw bytes.
/// When the value is a valid integer, we also cache the integer form
/// for efficient INCR/DECR operations.
#[derive(Debug, Clone)]
pub struct RedisString {
    data: Vec<u8>,
}

impl RedisString {
    pub fn new(data: Vec<u8>) -> Self {
        RedisString { data }
    }

    pub fn from_str(s: &str) -> Self {
        RedisString {
            data: s.as_bytes().to_vec(),
        }
    }

    pub fn from_i64(n: i64) -> Self {
        RedisString {
            data: n.to_string().into_bytes(),
        }
    }

    pub fn from_f64(n: f64) -> Self {
        // Use ryu or manual formatting to match Redis's float output
        let s = format_float(n);
        RedisString {
            data: s.into_bytes(),
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.data
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Try to parse the value as an i64.
    pub fn as_i64(&self) -> Option<i64> {
        std::str::from_utf8(&self.data)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
    }

    /// Try to parse the value as an f64.
    pub fn as_f64(&self) -> Option<f64> {
        std::str::from_utf8(&self.data)
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
    }

    /// Set the data.
    pub fn set(&mut self, data: Vec<u8>) {
        self.data = data;
    }

    /// Append data and return new length.
    pub fn append(&mut self, data: &[u8]) -> usize {
        self.data.extend_from_slice(data);
        self.data.len()
    }

    /// Get a range of bytes (GETRANGE).
    pub fn getrange(&self, start: i64, end: i64) -> &[u8] {
        let len = self.data.len() as i64;
        if len == 0 {
            return &[];
        }

        let start = if start < 0 {
            (len + start).max(0) as usize
        } else {
            (start as usize).min(self.data.len())
        };

        let end = if end < 0 {
            let e = len + end;
            if e < 0 {
                return &[];
            }
            (e + 1) as usize
        } else {
            ((end + 1) as usize).min(self.data.len())
        };

        if start >= end || start >= self.data.len() {
            return &[];
        }

        &self.data[start..end]
    }

    /// Set a range of bytes (SETRANGE). Pads with zeros if needed.
    pub fn setrange(&mut self, offset: usize, data: &[u8]) -> usize {
        let needed = offset + data.len();
        if needed > self.data.len() {
            self.data.resize(needed, 0);
        }
        self.data[offset..offset + data.len()].copy_from_slice(data);
        self.data.len()
    }

    /// Increment by i64, returning new value.
    pub fn incr_by(&mut self, delta: i64) -> Result<i64, &'static str> {
        let current = self
            .as_i64()
            .ok_or("value is not an integer or out of range")?;
        let new_val = current
            .checked_add(delta)
            .ok_or("increment or decrement would overflow")?;
        self.data = new_val.to_string().into_bytes();
        Ok(new_val)
    }

    /// Increment by f64, returning new value.
    pub fn incr_by_float(&mut self, delta: f64) -> Result<f64, &'static str> {
        let current = self.as_f64().ok_or("value is not a valid float")?;
        let new_val = current + delta;
        if new_val.is_nan() || new_val.is_infinite() {
            return Err("increment would produce NaN or Infinity");
        }
        self.data = format_float(new_val).into_bytes();
        Ok(new_val)
    }
}

/// Format a float like Redis does.
fn format_float(n: f64) -> String {
    if n == 0.0 && n.is_sign_negative() {
        return "0".to_string();
    }
    // Try to use the simplest representation that round-trips correctly
    let simple = format!("{n}");
    // Verify round-trip
    if simple.parse::<f64>().ok() == Some(n) {
        // Make sure there's a decimal point if it's not an integer representation
        simple
    } else {
        format!("{n:.17}")
    }
}
