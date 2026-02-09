/// Redis-style glob pattern matching.
/// Supports: * (any sequence), ? (any single char), [abc], [^abc], [a-z], \ (escape)
pub fn glob_match(pattern: &str, string: &str) -> bool {
    glob_match_bytes(pattern.as_bytes(), string.as_bytes())
}

fn glob_match_bytes(pattern: &[u8], string: &[u8]) -> bool {
    let mut pi = 0;
    let mut si = 0;
    let mut star_pi = usize::MAX;
    let mut star_si = usize::MAX;

    while si < string.len() {
        if pi < pattern.len() {
            match pattern[pi] {
                b'*' => {
                    star_pi = pi;
                    star_si = si;
                    pi += 1;
                    continue;
                }
                b'?' => {
                    pi += 1;
                    si += 1;
                    continue;
                }
                b'[' => {
                    if let Some((matched, new_pi)) = match_bracket(&pattern[pi..], string[si]) {
                        if matched {
                            pi += new_pi;
                            si += 1;
                            continue;
                        }
                    }
                    // Fall through to star backtrack
                }
                b'\\' => {
                    pi += 1;
                    if pi < pattern.len() && pattern[pi] == string[si] {
                        pi += 1;
                        si += 1;
                        continue;
                    }
                    // Fall through to star backtrack
                }
                c => {
                    if c == string[si] {
                        pi += 1;
                        si += 1;
                        continue;
                    }
                    // Fall through to star backtrack
                }
            }
        }

        // Try to backtrack to the last *
        if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_si += 1;
            si = star_si;
            continue;
        }

        return false;
    }

    // Consume remaining * in pattern
    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }

    pi == pattern.len()
}

/// Match a bracket expression [abc] or [^abc] or [a-z].
/// Returns (matched, bytes_consumed_from_pattern) or None if malformed.
fn match_bracket(pattern: &[u8], ch: u8) -> Option<(bool, usize)> {
    if pattern.is_empty() || pattern[0] != b'[' {
        return None;
    }

    let mut i = 1;
    let negate = if i < pattern.len() && pattern[i] == b'^' {
        i += 1;
        true
    } else {
        false
    };

    let mut matched = false;

    while i < pattern.len() && pattern[i] != b']' {
        if i + 2 < pattern.len() && pattern[i + 1] == b'-' && pattern[i + 2] != b']' {
            // Range: a-z
            let lo = pattern[i];
            let hi = pattern[i + 2];
            if ch >= lo && ch <= hi {
                matched = true;
            }
            i += 3;
        } else {
            if pattern[i] == ch {
                matched = true;
            }
            i += 1;
        }
    }

    if i >= pattern.len() {
        // Malformed: no closing ]
        return None;
    }

    // Skip the closing ]
    i += 1;

    if negate {
        matched = !matched;
    }

    Some((matched, i))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_star() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("*", ""));
        assert!(glob_match("h*o", "hello"));
        assert!(glob_match("h*o", "ho"));
        assert!(!glob_match("h*o", "help"));
    }

    #[test]
    fn test_question() {
        assert!(glob_match("h?llo", "hello"));
        assert!(glob_match("h?llo", "hallo"));
        assert!(!glob_match("h?llo", "hllo"));
    }

    #[test]
    fn test_bracket() {
        assert!(glob_match("h[ae]llo", "hello"));
        assert!(glob_match("h[ae]llo", "hallo"));
        assert!(!glob_match("h[ae]llo", "hillo"));
    }

    #[test]
    fn test_bracket_range() {
        assert!(glob_match("h[a-e]llo", "hello"));
        assert!(!glob_match("h[a-d]llo", "hello"));
    }

    #[test]
    fn test_bracket_negate() {
        assert!(!glob_match("h[^e]llo", "hello"));
        assert!(glob_match("h[^e]llo", "hallo"));
    }

    #[test]
    fn test_escape() {
        assert!(glob_match(r"h\*llo", "h*llo"));
        assert!(!glob_match(r"h\*llo", "hello"));
    }

    #[test]
    fn test_exact() {
        assert!(glob_match("hello", "hello"));
        assert!(!glob_match("hello", "world"));
    }

    #[test]
    fn test_complex() {
        assert!(glob_match("user:*:name", "user:123:name"));
        assert!(glob_match("user:*:name", "user::name"));
        assert!(!glob_match("user:*:name", "user:123:age"));
    }
}
