use std::collections::HashMap;

/// Redis hash type.
#[derive(Debug, Clone, Default)]
pub struct RedisHash {
    data: HashMap<String, Vec<u8>>,
}

impl RedisHash {
    pub fn new() -> Self {
        RedisHash {
            data: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn get(&self, field: &str) -> Option<&Vec<u8>> {
        self.data.get(field)
    }

    /// Set a field. Returns true if the field is new (didn't exist before).
    pub fn set(&mut self, field: String, value: Vec<u8>) -> bool {
        self.data.insert(field, value).is_none()
    }

    pub fn del(&mut self, field: &str) -> bool {
        self.data.remove(field).is_some()
    }

    pub fn exists(&self, field: &str) -> bool {
        self.data.contains_key(field)
    }

    pub fn keys(&self) -> Vec<&String> {
        self.data.keys().collect()
    }

    pub fn values(&self) -> Vec<&Vec<u8>> {
        self.data.values().collect()
    }

    pub fn entries(&self) -> Vec<(&String, &Vec<u8>)> {
        self.data.iter().collect()
    }

    pub fn incr_by(&mut self, field: &str, delta: i64) -> Result<i64, &'static str> {
        let current = match self.data.get(field) {
            Some(v) => {
                let s = std::str::from_utf8(v).map_err(|_| "hash value is not an integer")?;
                s.parse::<i64>()
                    .map_err(|_| "hash value is not an integer")?
            }
            None => 0,
        };
        let new_val = current
            .checked_add(delta)
            .ok_or("increment or decrement would overflow")?;
        self.data
            .insert(field.to_string(), new_val.to_string().into_bytes());
        Ok(new_val)
    }

    pub fn incr_by_float(&mut self, field: &str, delta: f64) -> Result<f64, &'static str> {
        let current = match self.data.get(field) {
            Some(v) => {
                let s = std::str::from_utf8(v).map_err(|_| "hash value is not a valid float")?;
                s.parse::<f64>()
                    .map_err(|_| "hash value is not a valid float")?
            }
            None => 0.0,
        };
        let new_val = current + delta;
        if new_val.is_nan() || new_val.is_infinite() {
            return Err("value is NaN or Infinity");
        }
        self.data
            .insert(field.to_string(), format!("{new_val}").into_bytes());
        Ok(new_val)
    }

    pub fn setnx(&mut self, field: String, value: Vec<u8>) -> bool {
        use std::collections::hash_map::Entry;
        match self.data.entry(field) {
            Entry::Occupied(_) => false,
            Entry::Vacant(e) => {
                e.insert(value);
                true
            }
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &Vec<u8>)> {
        self.data.iter()
    }

    /// Check if any field name or value exceeds the given byte length.
    pub fn has_long_entry(&self, max_bytes: usize) -> bool {
        self.data
            .iter()
            .any(|(k, v)| k.len() > max_bytes || v.len() > max_bytes)
    }
}
