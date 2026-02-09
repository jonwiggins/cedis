use bytes::{Buf, BytesMut};
use std::io;

/// A RESP2 value.
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    /// +OK\r\n
    SimpleString(String),
    /// -ERR message\r\n
    Error(String),
    /// :1000\r\n
    Integer(i64),
    /// $6\r\nfoobar\r\n  or  $-1\r\n (null)
    BulkString(Option<Vec<u8>>),
    /// *2\r\n...  or  *-1\r\n (null)
    Array(Option<Vec<RespValue>>),
}

impl RespValue {
    pub fn ok() -> Self {
        RespValue::SimpleString("OK".to_string())
    }

    pub fn simple_string(s: impl Into<String>) -> Self {
        RespValue::SimpleString(s.into())
    }

    pub fn error(s: impl Into<String>) -> Self {
        RespValue::Error(s.into())
    }

    pub fn integer(n: i64) -> Self {
        RespValue::Integer(n)
    }

    pub fn bulk_string(data: impl Into<Vec<u8>>) -> Self {
        RespValue::BulkString(Some(data.into()))
    }

    pub fn null_bulk_string() -> Self {
        RespValue::BulkString(None)
    }

    pub fn null_array() -> Self {
        RespValue::Array(None)
    }

    pub fn array(items: Vec<RespValue>) -> Self {
        RespValue::Array(Some(items))
    }

    /// Serialize this value to RESP bytes.
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.write_to(&mut buf);
        buf
    }

    /// Write RESP bytes into the given buffer.
    pub fn write_to(&self, buf: &mut Vec<u8>) {
        match self {
            RespValue::SimpleString(s) => {
                buf.push(b'+');
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            RespValue::Error(s) => {
                buf.push(b'-');
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            RespValue::Integer(n) => {
                buf.push(b':');
                buf.extend_from_slice(n.to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            RespValue::BulkString(None) => {
                buf.extend_from_slice(b"$-1\r\n");
            }
            RespValue::BulkString(Some(data)) => {
                buf.push(b'$');
                buf.extend_from_slice(data.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(data);
                buf.extend_from_slice(b"\r\n");
            }
            RespValue::Array(None) => {
                buf.extend_from_slice(b"*-1\r\n");
            }
            RespValue::Array(Some(items)) => {
                buf.push(b'*');
                buf.extend_from_slice(items.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for item in items {
                    item.write_to(buf);
                }
            }
        }
    }

    /// Try to interpret this value as a string (for command parsing).
    pub fn as_str(&self) -> Option<&[u8]> {
        match self {
            RespValue::BulkString(Some(data)) => Some(data),
            RespValue::SimpleString(s) => Some(s.as_bytes()),
            _ => None,
        }
    }

    /// Convert to a UTF-8 string, if possible.
    pub fn to_string_lossy(&self) -> Option<String> {
        self.as_str()
            .map(|b| String::from_utf8_lossy(b).into_owned())
    }
}

/// Streaming RESP parser.
///
/// Handles partial reads — call `parse()` repeatedly as data arrives.
/// Returns `Ok(Some(value))` when a complete value is parsed,
/// `Ok(None)` when more data is needed.
pub struct RespParser;

impl RespParser {
    /// Try to parse a complete RESP value from the buffer.
    /// On success, consumes the parsed bytes from `buf` and returns the value.
    /// Returns `Ok(None)` if the buffer doesn't contain a complete value yet.
    pub fn parse(buf: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
        if buf.is_empty() {
            return Ok(None);
        }

        // Check if this is an inline command (doesn't start with a RESP type byte)
        match buf[0] {
            b'+' | b'-' | b':' | b'$' | b'*' => Self::parse_value(buf),
            _ => Self::parse_inline(buf),
        }
    }

    fn parse_value(buf: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
        if buf.is_empty() {
            return Ok(None);
        }

        match buf[0] {
            b'+' => Self::parse_simple_string(buf),
            b'-' => Self::parse_error(buf),
            b':' => Self::parse_integer(buf),
            b'$' => Self::parse_bulk_string(buf),
            b'*' => Self::parse_array(buf),
            other => Err(RespError::InvalidByte(other)),
        }
    }

    /// Parse an inline command (plain text terminated by \r\n).
    /// Converts it to a RESP array of bulk strings.
    fn parse_inline(buf: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
        let crlf = find_crlf(buf);
        let crlf = match crlf {
            Some(pos) => pos,
            None => {
                // Check for buffer that's too large without CRLF
                if buf.len() > 64 * 1024 {
                    return Err(RespError::InvalidData("Inline command too long".into()));
                }
                return Ok(None);
            }
        };

        let line = &buf[..crlf];
        let line_str = String::from_utf8_lossy(line).to_string();
        buf.advance(crlf + 2); // consume line + \r\n

        // Split by whitespace, treating quoted strings as single tokens
        let parts = split_inline_command(&line_str)?;
        if parts.is_empty() {
            // Empty line (e.g. bare \r\n) — return empty array so the caller
            // silently ignores it and continues parsing instead of blocking.
            return Ok(Some(RespValue::Array(Some(Vec::new()))));
        }

        let items: Vec<RespValue> = parts
            .into_iter()
            .map(|s| RespValue::BulkString(Some(s.into_bytes())))
            .collect();

        Ok(Some(RespValue::Array(Some(items))))
    }

    fn parse_simple_string(buf: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
        if let Some(end) = find_crlf_from(buf, 1) {
            let s = String::from_utf8_lossy(&buf[1..end]).to_string();
            buf.advance(end + 2);
            Ok(Some(RespValue::SimpleString(s)))
        } else {
            Ok(None)
        }
    }

    fn parse_error(buf: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
        if let Some(end) = find_crlf_from(buf, 1) {
            let s = String::from_utf8_lossy(&buf[1..end]).to_string();
            buf.advance(end + 2);
            Ok(Some(RespValue::Error(s)))
        } else {
            Ok(None)
        }
    }

    fn parse_integer(buf: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
        if let Some(end) = find_crlf_from(buf, 1) {
            let s = std::str::from_utf8(&buf[1..end])
                .map_err(|_| RespError::InvalidData("Invalid integer encoding".into()))?;
            let n: i64 = s
                .parse()
                .map_err(|_| RespError::InvalidData(format!("Invalid integer: {s}")))?;
            buf.advance(end + 2);
            Ok(Some(RespValue::Integer(n)))
        } else {
            Ok(None)
        }
    }

    fn parse_bulk_string(buf: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
        let crlf = match find_crlf_from(buf, 1) {
            Some(pos) => pos,
            None => return Ok(None),
        };

        let len_str = std::str::from_utf8(&buf[1..crlf])
            .map_err(|_| RespError::InvalidData("invalid bulk length".into()))?;
        let len: i64 = len_str
            .parse()
            .map_err(|_| RespError::InvalidData("invalid bulk length".into()))?;

        if len == -1 {
            buf.advance(crlf + 2);
            return Ok(Some(RespValue::BulkString(None)));
        }

        if len < -1 {
            return Err(RespError::InvalidData("invalid bulk length".into()));
        }

        if len > 512 * 1024 * 1024 {
            // 512MB max bulk string length
            return Err(RespError::InvalidData("invalid bulk length".into()));
        }
        let len = len as usize;
        let total_needed = crlf + 2 + len + 2; // header + data + trailing \r\n

        if buf.len() < total_needed {
            return Ok(None);
        }

        let data = buf[crlf + 2..crlf + 2 + len].to_vec();

        // Verify trailing \r\n
        if buf[crlf + 2 + len] != b'\r' || buf[crlf + 2 + len + 1] != b'\n' {
            return Err(RespError::InvalidData(
                "Missing trailing CRLF after bulk string".into(),
            ));
        }

        buf.advance(total_needed);
        Ok(Some(RespValue::BulkString(Some(data))))
    }

    fn parse_array(buf: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
        let crlf = match find_crlf_from(buf, 1) {
            Some(pos) => pos,
            None => return Ok(None),
        };

        let len_str = std::str::from_utf8(&buf[1..crlf])
            .map_err(|_| RespError::InvalidData("Invalid array length encoding".into()))?;
        let len: i64 = len_str
            .parse()
            .map_err(|_| RespError::InvalidData("invalid multibulk length".into()))?;

        if len == -1 {
            buf.advance(crlf + 2);
            return Ok(Some(RespValue::Array(None)));
        }

        if len < -1 {
            // Negative multibulk length other than -1: treat as null array
            buf.advance(crlf + 2);
            return Ok(Some(RespValue::Array(None)));
        }

        if len > 1024 * 1024 {
            return Err(RespError::InvalidData("invalid multibulk length".into()));
        }

        let len = len as usize;

        // We need to try parsing all elements.
        // Save the current position so we can restore if we don't have enough data.
        let saved = buf.clone();
        buf.advance(crlf + 2);

        let mut items = Vec::with_capacity(len);
        for _ in 0..len {
            match Self::parse_value(buf) {
                Ok(Some(val)) => items.push(val),
                Ok(None) => {
                    // Not enough data — restore buffer
                    *buf = saved;
                    return Ok(None);
                }
                Err(e) => return Err(e),
            }
        }

        Ok(Some(RespValue::Array(Some(items))))
    }
}

/// Find \r\n starting from position 0.
fn find_crlf(buf: &[u8]) -> Option<usize> {
    find_crlf_from(buf, 0)
}

/// Find \r\n starting from the given position.
fn find_crlf_from(buf: &[u8], start: usize) -> Option<usize> {
    if buf.len() < start + 2 {
        return None;
    }
    for i in start..buf.len() - 1 {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some(i);
        }
    }
    None
}

/// Split an inline command into tokens, respecting double-quoted strings.
fn split_inline_command(line: &str) -> Result<Vec<String>, RespError> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();

    while let Some(ch) = chars.next() {
        if in_quotes {
            if ch == '"' {
                in_quotes = false;
            } else if ch == '\\' {
                if let Some(&next) = chars.peek() {
                    match next {
                        '"' | '\\' => {
                            current.push(next);
                            chars.next();
                        }
                        'n' => {
                            current.push('\n');
                            chars.next();
                        }
                        'r' => {
                            current.push('\r');
                            chars.next();
                        }
                        't' => {
                            current.push('\t');
                            chars.next();
                        }
                        _ => current.push(ch),
                    }
                }
            } else {
                current.push(ch);
            }
        } else if ch == '"' {
            in_quotes = true;
        } else if ch == '\'' {
            // Single-quoted string — no escape processing
            while let Some(ch) = chars.next() {
                if ch == '\'' {
                    break;
                }
                current.push(ch);
            }
        } else if ch.is_whitespace() {
            if !current.is_empty() {
                tokens.push(std::mem::take(&mut current));
            }
        } else {
            current.push(ch);
        }
    }

    if in_quotes {
        return Err(RespError::InvalidData("unbalanced quotes in request".into()));
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    Ok(tokens)
}

#[derive(Debug, thiserror::Error)]
pub enum RespError {
    #[error("expected '$', got '{}'", *.0 as char)]
    InvalidByte(u8),

    #[error("Invalid data: {0}")]
    InvalidData(String),

    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_string() {
        let mut buf = BytesMut::from("+OK\r\n");
        let result = RespParser::parse(&mut buf).unwrap().unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
        assert!(buf.is_empty());
    }

    #[test]
    fn test_parse_error() {
        let mut buf = BytesMut::from("-ERR unknown command\r\n");
        let result = RespParser::parse(&mut buf).unwrap().unwrap();
        assert_eq!(
            result,
            RespValue::Error("ERR unknown command".to_string())
        );
    }

    #[test]
    fn test_parse_integer() {
        let mut buf = BytesMut::from(":1000\r\n");
        let result = RespParser::parse(&mut buf).unwrap().unwrap();
        assert_eq!(result, RespValue::Integer(1000));

        let mut buf = BytesMut::from(":-42\r\n");
        let result = RespParser::parse(&mut buf).unwrap().unwrap();
        assert_eq!(result, RespValue::Integer(-42));
    }

    #[test]
    fn test_parse_bulk_string() {
        let mut buf = BytesMut::from("$6\r\nfoobar\r\n");
        let result = RespParser::parse(&mut buf).unwrap().unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"foobar".to_vec())));
    }

    #[test]
    fn test_parse_null_bulk_string() {
        let mut buf = BytesMut::from("$-1\r\n");
        let result = RespParser::parse(&mut buf).unwrap().unwrap();
        assert_eq!(result, RespValue::BulkString(None));
    }

    #[test]
    fn test_parse_empty_bulk_string() {
        let mut buf = BytesMut::from("$0\r\n\r\n");
        let result = RespParser::parse(&mut buf).unwrap().unwrap();
        assert_eq!(result, RespValue::BulkString(Some(vec![])));
    }

    #[test]
    fn test_parse_array() {
        let mut buf = BytesMut::from("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        let result = RespParser::parse(&mut buf).unwrap().unwrap();
        assert_eq!(
            result,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"foo".to_vec())),
                RespValue::BulkString(Some(b"bar".to_vec())),
            ]))
        );
    }

    #[test]
    fn test_parse_null_array() {
        let mut buf = BytesMut::from("*-1\r\n");
        let result = RespParser::parse(&mut buf).unwrap().unwrap();
        assert_eq!(result, RespValue::Array(None));
    }

    #[test]
    fn test_parse_empty_array() {
        let mut buf = BytesMut::from("*0\r\n");
        let result = RespParser::parse(&mut buf).unwrap().unwrap();
        assert_eq!(result, RespValue::Array(Some(vec![])));
    }

    #[test]
    fn test_parse_nested_array() {
        let mut buf = BytesMut::from("*2\r\n*1\r\n:1\r\n*1\r\n:2\r\n");
        let result = RespParser::parse(&mut buf).unwrap().unwrap();
        assert_eq!(
            result,
            RespValue::Array(Some(vec![
                RespValue::Array(Some(vec![RespValue::Integer(1)])),
                RespValue::Array(Some(vec![RespValue::Integer(2)])),
            ]))
        );
    }

    #[test]
    fn test_parse_partial_data() {
        let mut buf = BytesMut::from("$6\r\nfoo");
        let result = RespParser::parse(&mut buf).unwrap();
        assert!(result.is_none());
        // Buffer should not be consumed
        assert_eq!(&buf[..], b"$6\r\nfoo");
    }

    #[test]
    fn test_parse_inline_command() {
        let mut buf = BytesMut::from("PING\r\n");
        let result = RespParser::parse(&mut buf).unwrap().unwrap();
        assert_eq!(
            result,
            RespValue::Array(Some(vec![RespValue::BulkString(Some(
                b"PING".to_vec()
            ))]))
        );
    }

    #[test]
    fn test_parse_inline_command_with_args() {
        let mut buf = BytesMut::from("SET key value\r\n");
        let result = RespParser::parse(&mut buf).unwrap().unwrap();
        assert_eq!(
            result,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"SET".to_vec())),
                RespValue::BulkString(Some(b"key".to_vec())),
                RespValue::BulkString(Some(b"value".to_vec())),
            ]))
        );
    }

    #[test]
    fn test_serialize_simple_string() {
        let val = RespValue::SimpleString("OK".to_string());
        assert_eq!(val.serialize(), b"+OK\r\n");
    }

    #[test]
    fn test_serialize_error() {
        let val = RespValue::Error("ERR bad".to_string());
        assert_eq!(val.serialize(), b"-ERR bad\r\n");
    }

    #[test]
    fn test_serialize_integer() {
        let val = RespValue::Integer(42);
        assert_eq!(val.serialize(), b":42\r\n");
    }

    #[test]
    fn test_serialize_bulk_string() {
        let val = RespValue::BulkString(Some(b"hello".to_vec()));
        assert_eq!(val.serialize(), b"$5\r\nhello\r\n");
    }

    #[test]
    fn test_serialize_null_bulk_string() {
        let val = RespValue::BulkString(None);
        assert_eq!(val.serialize(), b"$-1\r\n");
    }

    #[test]
    fn test_serialize_array() {
        let val = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"foo".to_vec())),
            RespValue::Integer(42),
        ]));
        assert_eq!(val.serialize(), b"*2\r\n$3\r\nfoo\r\n:42\r\n");
    }

    #[test]
    fn test_serialize_null_array() {
        let val = RespValue::Array(None);
        assert_eq!(val.serialize(), b"*-1\r\n");
    }

    #[test]
    fn test_multiple_values_in_buffer() {
        let mut buf = BytesMut::from("+OK\r\n+PONG\r\n");
        let r1 = RespParser::parse(&mut buf).unwrap().unwrap();
        assert_eq!(r1, RespValue::SimpleString("OK".to_string()));
        let r2 = RespParser::parse(&mut buf).unwrap().unwrap();
        assert_eq!(r2, RespValue::SimpleString("PONG".to_string()));
        assert!(buf.is_empty());
    }

    #[test]
    fn test_split_inline_quoted() {
        let parts = split_inline_command(r#"SET key "hello world""#).unwrap();
        assert_eq!(parts, vec!["SET", "key", "hello world"]);
    }
}
