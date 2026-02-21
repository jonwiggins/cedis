use std::collections::HashMap;

/// Earth's radius in meters.
const EARTH_RADIUS_M: f64 = 6372797.560856;

/// Mercator projection limits (used by Redis geohash).
const MERCATOR_MAX: f64 = 20037726.37;
const MERCATOR_MIN: f64 = -20037726.37;

/// Number of bits used for the geohash score (26 bits lat + 26 bits lon = 52 bits).
const GEO_STEP: u32 = 26;

/// A geographic data set storing members with longitude/latitude coordinates.
/// Under the hood, Redis stores geo data in a sorted set using geohash scores.
#[derive(Debug, Clone, Default)]
pub struct GeoSet {
    /// member -> (longitude, latitude)
    members: HashMap<Vec<u8>, (f64, f64)>,
}

/// A search result entry with distance information.
#[derive(Debug, Clone)]
pub struct GeoResult {
    pub member: Vec<u8>,
    pub longitude: f64,
    pub latitude: f64,
    pub distance: f64,
}

impl GeoSet {
    pub fn new() -> Self {
        GeoSet {
            members: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// Add or update a member with the given longitude and latitude.
    /// Returns true if the member is new, false if updated.
    pub fn add(&mut self, member: Vec<u8>, longitude: f64, latitude: f64) -> bool {
        let is_new = !self.members.contains_key(&member);
        self.members.insert(member, (longitude, latitude));
        is_new
    }

    /// Get the position of a member.
    pub fn pos(&self, member: &[u8]) -> Option<(f64, f64)> {
        self.members.get(member).copied()
    }

    /// Calculate the distance in meters between two members.
    pub fn dist(&self, member1: &[u8], member2: &[u8]) -> Option<f64> {
        let &(lon1, lat1) = self.members.get(member1)?;
        let &(lon2, lat2) = self.members.get(member2)?;
        Some(haversine_distance(lat1, lon1, lat2, lon2))
    }

    /// Get all members.
    pub fn all_members(&self) -> Vec<(&[u8], f64, f64)> {
        self.members
            .iter()
            .map(|(m, &(lon, lat))| (m.as_slice(), lon, lat))
            .collect()
    }

    /// Search for members within a given radius (in meters) of a center point.
    /// Returns results sorted by distance ascending by default.
    pub fn search_within_radius(
        &self,
        center_lon: f64,
        center_lat: f64,
        radius_m: f64,
        ascending: bool,
        count: Option<usize>,
    ) -> Vec<GeoResult> {
        let mut results: Vec<GeoResult> = self
            .members
            .iter()
            .filter_map(|(member, &(lon, lat))| {
                let d = haversine_distance(center_lat, center_lon, lat, lon);
                if d <= radius_m {
                    Some(GeoResult {
                        member: member.clone(),
                        longitude: lon,
                        latitude: lat,
                        distance: d,
                    })
                } else {
                    None
                }
            })
            .collect();

        if ascending {
            results.sort_by(|a, b| {
                a.distance
                    .partial_cmp(&b.distance)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        } else {
            results.sort_by(|a, b| {
                b.distance
                    .partial_cmp(&a.distance)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        }

        if let Some(c) = count {
            results.truncate(c);
        }

        results
    }

    /// Search for members within a bounding box (width x height in meters) centered on a point.
    pub fn search_within_box(
        &self,
        center_lon: f64,
        center_lat: f64,
        width_m: f64,
        height_m: f64,
        ascending: bool,
        count: Option<usize>,
    ) -> Vec<GeoResult> {
        // Convert box dimensions to approximate lat/lon deltas
        let half_width = width_m / 2.0;
        let half_height = height_m / 2.0;

        let mut results: Vec<GeoResult> = self
            .members
            .iter()
            .filter_map(|(member, &(lon, lat))| {
                let d = haversine_distance(center_lat, center_lon, lat, lon);
                // Check if within the bounding box
                let dx = haversine_distance(center_lat, center_lon, center_lat, lon);
                let dy = haversine_distance(center_lat, center_lon, lat, center_lon);
                if dx <= half_width && dy <= half_height {
                    Some(GeoResult {
                        member: member.clone(),
                        longitude: lon,
                        latitude: lat,
                        distance: d,
                    })
                } else {
                    None
                }
            })
            .collect();

        if ascending {
            results.sort_by(|a, b| {
                a.distance
                    .partial_cmp(&b.distance)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        } else {
            results.sort_by(|a, b| {
                b.distance
                    .partial_cmp(&a.distance)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        }

        if let Some(c) = count {
            results.truncate(c);
        }

        results
    }

    /// Estimate memory usage.
    pub fn estimated_memory(&self) -> usize {
        let member_bytes: usize = self.members.keys().map(|k| k.len() + 16).sum();
        64 * self.members.len() + member_bytes
    }
}

/// Haversine distance between two points in meters.
fn haversine_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let lat1_rad = lat1.to_radians();
    let lat2_rad = lat2.to_radians();
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();

    let a =
        (dlat / 2.0).sin().powi(2) + lat1_rad.cos() * lat2_rad.cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();
    EARTH_RADIUS_M * c
}

/// Encode longitude/latitude into a 52-bit geohash score (as used by Redis).
/// The encoding interleaves latitude and longitude bits scaled to the Mercator projection range.
pub fn geohash_encode(longitude: f64, latitude: f64) -> u64 {
    // Scale longitude and latitude to Mercator ranges [0, 1]
    let lon_offset = (longitude - MERCATOR_MIN) / (MERCATOR_MAX - MERCATOR_MIN);
    let lat_offset = (latitude - MERCATOR_MIN) / (MERCATOR_MAX - MERCATOR_MIN);

    // Quantize to GEO_STEP bits each
    let max_val = (1u64 << GEO_STEP) - 1;
    let lon_bits = (lon_offset * max_val as f64) as u64;
    let lat_bits = (lat_offset * max_val as f64) as u64;

    // Interleave: longitude in even bits, latitude in odd bits
    interleave(lon_bits, lat_bits)
}

/// Interleave two 26-bit values into a 52-bit value.
fn interleave(x: u64, y: u64) -> u64 {
    let mut result = 0u64;
    for i in 0..GEO_STEP {
        result |= ((x >> i) & 1) << (2 * i);
        result |= ((y >> i) & 1) << (2 * i + 1);
    }
    result
}

/// Convert a unit string to a conversion factor from meters.
pub fn unit_to_meters(unit: &str) -> Option<f64> {
    match unit.to_lowercase().as_str() {
        "m" => Some(1.0),
        "km" => Some(1000.0),
        "ft" => Some(0.3048),
        "mi" => Some(1609.34),
        _ => None,
    }
}
