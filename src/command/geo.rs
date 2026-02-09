use crate::command::{arg_to_bytes, arg_to_f64, arg_to_i64, arg_to_string, wrong_arg_count, wrong_type_error};
use crate::connection::ClientState;
use crate::resp::RespValue;
use crate::store::SharedStore;
use crate::store::entry::Entry;
use crate::types::RedisValue;
use crate::types::geo::{GeoSet, unit_to_meters};

fn get_or_create_geo<'a>(
    db: &'a mut crate::store::Database,
    key: &str,
) -> Result<&'a mut GeoSet, RespValue> {
    if !db.exists(key) {
        db.set(key.to_string(), Entry::new(RedisValue::Geo(GeoSet::new())));
    }
    match db.get_mut(key) {
        Some(entry) => match &mut entry.value {
            RedisValue::Geo(g) => Ok(g),
            _ => Err(wrong_type_error()),
        },
        None => unreachable!(),
    }
}

/// GEOADD key [NX|XX] [CH] longitude latitude member [longitude latitude member ...]
pub async fn cmd_geoadd(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() < 4 {
        return wrong_arg_count("geoadd");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };

    // Parse flags
    let mut nx = false;
    let mut xx = false;
    let mut ch = false;
    let mut i = 1;

    while i < args.len() {
        let opt = match arg_to_string(&args[i]) {
            Some(s) => s.to_uppercase(),
            None => break,
        };
        match opt.as_str() {
            "NX" => { nx = true; i += 1; }
            "XX" => { xx = true; i += 1; }
            "CH" => { ch = true; i += 1; }
            _ => break,
        }
    }

    // Remaining args are longitude latitude member triples
    let triples = &args[i..];
    if triples.is_empty() || triples.len() % 3 != 0 {
        return wrong_arg_count("geoadd");
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    let geo = match get_or_create_geo(db, &key) {
        Ok(g) => g,
        Err(e) => return e,
    };

    let mut added = 0i64;
    let mut changed = 0i64;

    for triple in triples.chunks(3) {
        let longitude = match arg_to_f64(&triple[0]) {
            Some(v) => v,
            None => return RespValue::error("ERR value is not a valid float"),
        };
        let latitude = match arg_to_f64(&triple[1]) {
            Some(v) => v,
            None => return RespValue::error("ERR value is not a valid float"),
        };
        let member = match arg_to_bytes(&triple[2]) {
            Some(m) => m.to_vec(),
            None => continue,
        };

        // Validate ranges
        if !(-180.0..=180.0).contains(&longitude) || !(-85.05112878..=85.05112878).contains(&latitude) {
            return RespValue::error("ERR invalid longitude,latitude pair");
        }

        let exists = geo.pos(&member).is_some();

        if nx && exists { continue; }
        if xx && !exists { continue; }

        let old_pos = geo.pos(&member);
        let is_new = geo.add(member, longitude, latitude);
        if is_new {
            added += 1;
        } else if old_pos != Some((longitude, latitude)) {
            changed += 1;
        }
    }

    RespValue::integer(if ch { added + changed } else { added })
}

/// GEODIST key member1 member2 [m|km|ft|mi]
pub async fn cmd_geodist(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() < 3 || args.len() > 4 {
        return wrong_arg_count("geodist");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::null_bulk_string(),
    };
    let member1 = match arg_to_bytes(&args[1]) {
        Some(m) => m,
        None => return RespValue::null_bulk_string(),
    };
    let member2 = match arg_to_bytes(&args[2]) {
        Some(m) => m,
        None => return RespValue::null_bulk_string(),
    };

    let unit_factor = if args.len() == 4 {
        let unit_str = match arg_to_string(&args[3]) {
            Some(s) => s,
            None => return RespValue::error("ERR unsupported unit provided"),
        };
        match unit_to_meters(&unit_str) {
            Some(f) => f,
            None => return RespValue::error("ERR unsupported unit provided"),
        }
    } else {
        1.0 // default is meters
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::Geo(geo) => {
                match geo.dist(member1, member2) {
                    Some(d) => {
                        let converted = d / unit_factor;
                        RespValue::bulk_string(format!("{:.4}", converted).into_bytes())
                    }
                    None => RespValue::null_bulk_string(),
                }
            }
            _ => wrong_type_error(),
        },
        None => RespValue::null_bulk_string(),
    }
}

/// GEOPOS key member [member ...]
pub async fn cmd_geopos(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() < 2 {
        return wrong_arg_count("geopos");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => {
            let nulls: Vec<RespValue> = args[1..].iter().map(|_| RespValue::null_array()).collect();
            return RespValue::array(nulls);
        }
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::Geo(geo) => {
                let results: Vec<RespValue> = args[1..]
                    .iter()
                    .map(|arg| {
                        if let Some(member) = arg_to_bytes(arg) {
                            match geo.pos(member) {
                                Some((lon, lat)) => RespValue::array(vec![
                                    RespValue::bulk_string(format!("{}", lon).into_bytes()),
                                    RespValue::bulk_string(format!("{}", lat).into_bytes()),
                                ]),
                                None => RespValue::null_array(),
                            }
                        } else {
                            RespValue::null_array()
                        }
                    })
                    .collect();
                RespValue::array(results)
            }
            _ => wrong_type_error(),
        },
        None => {
            let nulls: Vec<RespValue> = args[1..].iter().map(|_| RespValue::null_array()).collect();
            RespValue::array(nulls)
        }
    }
}

/// GEOSEARCH key FROMMEMBER member|FROMLONLAT lon lat BYRADIUS radius m|km|ft|mi|BYBOX width height m|km|ft|mi [ASC|DESC] [COUNT count]
pub async fn cmd_geosearch(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() < 4 {
        return wrong_arg_count("geosearch");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::array(vec![]),
    };

    let mut i = 1;
    let mut center_lon: Option<f64> = None;
    let mut center_lat: Option<f64> = None;
    let mut from_member: Option<Vec<u8>> = None;
    let mut by_radius: Option<(f64, f64)> = None; // (radius_m, unit_factor)
    let mut by_box: Option<(f64, f64, f64)> = None; // (width_m, height_m, unit_factor)
    let mut ascending = true;
    let mut count: Option<usize> = None;
    let mut withcoord = false;
    let mut withdist = false;

    while i < args.len() {
        let opt = match arg_to_string(&args[i]) {
            Some(s) => s.to_uppercase(),
            None => { i += 1; continue; }
        };
        match opt.as_str() {
            "FROMMEMBER" => {
                i += 1;
                if i >= args.len() {
                    return wrong_arg_count("geosearch");
                }
                from_member = arg_to_bytes(&args[i]).map(|b| b.to_vec());
                i += 1;
            }
            "FROMLONLAT" => {
                i += 1;
                if i + 1 >= args.len() {
                    return wrong_arg_count("geosearch");
                }
                center_lon = arg_to_f64(&args[i]);
                center_lat = arg_to_f64(&args[i + 1]);
                if center_lon.is_none() || center_lat.is_none() {
                    return RespValue::error("ERR value is not a valid float");
                }
                i += 2;
            }
            "BYRADIUS" => {
                i += 1;
                if i + 1 >= args.len() {
                    return wrong_arg_count("geosearch");
                }
                let radius = match arg_to_f64(&args[i]) {
                    Some(r) => r,
                    None => return RespValue::error("ERR value is not a valid float"),
                };
                i += 1;
                let unit_str = match arg_to_string(&args[i]) {
                    Some(s) => s,
                    None => return RespValue::error("ERR unsupported unit provided"),
                };
                let factor = match unit_to_meters(&unit_str) {
                    Some(f) => f,
                    None => return RespValue::error("ERR unsupported unit provided"),
                };
                by_radius = Some((radius * factor, factor));
                i += 1;
            }
            "BYBOX" => {
                i += 1;
                if i + 2 >= args.len() {
                    return wrong_arg_count("geosearch");
                }
                let width = match arg_to_f64(&args[i]) {
                    Some(w) => w,
                    None => return RespValue::error("ERR value is not a valid float"),
                };
                i += 1;
                let height = match arg_to_f64(&args[i]) {
                    Some(h) => h,
                    None => return RespValue::error("ERR value is not a valid float"),
                };
                i += 1;
                let unit_str = match arg_to_string(&args[i]) {
                    Some(s) => s,
                    None => return RespValue::error("ERR unsupported unit provided"),
                };
                let factor = match unit_to_meters(&unit_str) {
                    Some(f) => f,
                    None => return RespValue::error("ERR unsupported unit provided"),
                };
                by_box = Some((width * factor, height * factor, factor));
                i += 1;
            }
            "ASC" => { ascending = true; i += 1; }
            "DESC" => { ascending = false; i += 1; }
            "COUNT" => {
                i += 1;
                if i >= args.len() {
                    return wrong_arg_count("geosearch");
                }
                count = arg_to_i64(&args[i]).map(|n| n.max(0) as usize);
                i += 1;
                // Skip optional ANY flag
                if i < args.len() {
                    if let Some(s) = arg_to_string(&args[i]) {
                        if s.to_uppercase() == "ANY" {
                            i += 1;
                        }
                    }
                }
            }
            "WITHCOORD" => { withcoord = true; i += 1; }
            "WITHDIST" => { withdist = true; i += 1; }
            _ => { i += 1; }
        }
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    // Resolve center point
    let (cx, cy) = if let Some(member) = from_member {
        match db.get(&key) {
            Some(entry) => match &entry.value {
                RedisValue::Geo(geo) => {
                    match geo.pos(&member) {
                        Some((lon, lat)) => (lon, lat),
                        None => return RespValue::array(vec![]),
                    }
                }
                _ => return wrong_type_error(),
            },
            None => return RespValue::array(vec![]),
        }
    } else if let (Some(lon), Some(lat)) = (center_lon, center_lat) {
        (lon, lat)
    } else {
        return RespValue::error("ERR exactly one of FROMMEMBER or FROMLONLAT must be provided");
    };

    let results = match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::Geo(geo) => {
                if let Some((radius_m, _factor)) = by_radius {
                    geo.search_within_radius(cx, cy, radius_m, ascending, count)
                } else if let Some((width_m, height_m, _factor)) = by_box {
                    geo.search_within_box(cx, cy, width_m, height_m, ascending, count)
                } else {
                    return RespValue::error("ERR exactly one of BYRADIUS or BYBOX must be provided");
                }
            }
            _ => return wrong_type_error(),
        },
        None => return RespValue::array(vec![]),
    };

    // Determine the unit factor for distance display
    let display_factor = if let Some((_, factor)) = by_radius {
        factor
    } else if let Some((_, _, factor)) = by_box {
        factor
    } else {
        1.0
    };

    let resp: Vec<RespValue> = results
        .iter()
        .map(|r| {
            if withcoord || withdist {
                let mut items = vec![RespValue::bulk_string(r.member.clone())];
                if withdist {
                    items.push(RespValue::bulk_string(
                        format!("{:.4}", r.distance / display_factor).into_bytes(),
                    ));
                }
                if withcoord {
                    items.push(RespValue::array(vec![
                        RespValue::bulk_string(format!("{}", r.longitude).into_bytes()),
                        RespValue::bulk_string(format!("{}", r.latitude).into_bytes()),
                    ]));
                }
                RespValue::array(items)
            } else {
                RespValue::bulk_string(r.member.clone())
            }
        })
        .collect();

    RespValue::array(resp)
}

/// GEOMEMBERS key — return all members in the geo set (non-standard, utility command).
pub async fn cmd_geomembers(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() != 1 {
        return wrong_arg_count("geomembers");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::array(vec![]),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::Geo(geo) => {
                let members = geo.all_members();
                let resp: Vec<RespValue> = members
                    .iter()
                    .map(|(m, _, _)| RespValue::bulk_string(m.to_vec()))
                    .collect();
                RespValue::array(resp)
            }
            _ => wrong_type_error(),
        },
        None => RespValue::array(vec![]),
    }
}
