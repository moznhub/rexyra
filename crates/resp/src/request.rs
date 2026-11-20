use bytes::{Buf, Bytes, BytesMut};

use crate::util::{read_line, Line};
use crate::{parse_i64, ProtocolError, MAX_AGGREGATE_LEN, MAX_BULK_LEN, MAX_LINE_LEN};

/// Parses one client command from the front of `buf`.
///
/// Accepts both forms Redis accepts:
///
/// * multibulk — `*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n`
/// * inline — `ECHO hi\r\n` (what `telnet` or `nc` users type)
///
/// Returns `Ok(None)` when more bytes are needed; `buf` is left untouched in
/// that case. Empty commands (`*0`, blank lines) are skipped. Arguments of a
/// multibulk command share the input allocation rather than being copied.
pub fn parse_request(buf: &mut BytesMut) -> Result<Option<Vec<Bytes>>, ProtocolError> {
    loop {
        let Some(&first) = buf.first() else {
            return Ok(None);
        };
        if first == b'*' {
            match scan_multibulk(&buf[..])? {
                None => return Ok(None),
                Some((consumed, ranges)) => {
                    let chunk = buf.split_to(consumed).freeze();
                    if ranges.is_empty() {
                        continue;
                    }
                    return Ok(Some(
                        ranges
                            .into_iter()
                            .map(|(start, len)| chunk.slice(start..start + len))
                            .collect(),
                    ));
                }
            }
        } else {
            match scan_inline(&buf[..])? {
                None => return Ok(None),
                Some((consumed, args)) => {
                    buf.advance(consumed);
                    if args.is_empty() {
                        continue;
                    }
                    return Ok(Some(args));
                }
            }
        }
    }
}

/// Locates every argument of a multibulk command without copying.
/// Returns the total bytes consumed and each argument's `(offset, len)`.
#[allow(clippy::type_complexity)]
fn scan_multibulk(src: &[u8]) -> Result<Option<(usize, Vec<(usize, usize)>)>, ProtocolError> {
    let mut pos = 1; // skip '*'
    let header = match read_line(src, &mut pos, "Protocol error: too big mbulk count string")? {
        Line::Complete(body) => body,
        Line::Incomplete => return Ok(None),
    };
    let count = match parse_i64(header) {
        Some(n) if n <= MAX_AGGREGATE_LEN as i64 => n,
        _ => {
            return Err(ProtocolError::new(
                "Protocol error: invalid multibulk length",
            ))
        }
    };
    if count <= 0 {
        return Ok(Some((pos, Vec::new())));
    }
    let count = count as usize;

    let mut ranges = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        let Some(&tag) = src.get(pos) else {
            return Ok(None);
        };
        if tag != b'$' {
            return Err(ProtocolError::new(format!(
                "Protocol error: expected '$', got '{}'",
                char::from(tag).escape_default()
            )));
        }
        pos += 1;
        let header = match read_line(src, &mut pos, "Protocol error: too big bulk count string")? {
            Line::Complete(body) => body,
            Line::Incomplete => return Ok(None),
        };
        let len = match parse_i64(header) {
            Some(n) if n >= 0 && n as u64 <= MAX_BULK_LEN as u64 => n as usize,
            _ => return Err(ProtocolError::new("Protocol error: invalid bulk length")),
        };
        let end = pos + len;
        if src.len() < end + 2 {
            return Ok(None);
        }
        if &src[end..end + 2] != b"\r\n" {
            return Err(ProtocolError::new(
                "Protocol error: expected CRLF after bulk payload",
            ));
        }
        ranges.push((pos, len));
        pos = end + 2;
    }
    Ok(Some((pos, ranges)))
}

fn scan_inline(src: &[u8]) -> Result<Option<(usize, Vec<Bytes>)>, ProtocolError> {
    let Some(nl) = src.iter().position(|&b| b == b'\n') else {
        if src.len() > MAX_LINE_LEN {
            return Err(ProtocolError::new("Protocol error: too big inline request"));
        }
        return Ok(None);
    };
    if nl > MAX_LINE_LEN {
        return Err(ProtocolError::new("Protocol error: too big inline request"));
    }
    let line = src[..nl].strip_suffix(b"\r").unwrap_or(&src[..nl]);
    let args = split_inline(line)
        .ok_or_else(|| ProtocolError::new("Protocol error: unbalanced quotes in request"))?;
    Ok(Some((nl + 1, args)))
}

fn is_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)
}

fn hex_value(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Splits an inline command line into arguments, honouring `"double"` quotes
/// (with `\n`, `\r`, `\t`, `\b`, `\a`, `\xHH` escapes) and `'single'` quotes
/// (with `\'`). Returns `None` on unbalanced quotes or a closing quote that is
/// not followed by whitespace.
fn split_inline(line: &[u8]) -> Option<Vec<Bytes>> {
    let n = line.len();
    let mut args = Vec::new();
    let mut i = 0;

    loop {
        while i < n && is_space(line[i]) {
            i += 1;
        }
        if i >= n {
            return Some(args);
        }

        let mut current = Vec::new();
        let mut in_double = false;
        let mut in_single = false;
        loop {
            if in_double {
                let c = *line.get(i)?; // unterminated quote
                if c == b'\\' && i + 3 < n && line[i + 1] == b'x' {
                    if let (Some(hi), Some(lo)) = (hex_value(line[i + 2]), hex_value(line[i + 3])) {
                        current.push(hi * 16 + lo);
                        i += 4;
                        continue;
                    }
                }
                if c == b'\\' && i + 1 < n {
                    current.push(match line[i + 1] {
                        b'n' => b'\n',
                        b'r' => b'\r',
                        b't' => b'\t',
                        b'b' => 0x08,
                        b'a' => 0x07,
                        other => other,
                    });
                    i += 2;
                } else if c == b'"' {
                    if i + 1 < n && !is_space(line[i + 1]) {
                        return None;
                    }
                    i += 1;
                    break;
                } else {
                    current.push(c);
                    i += 1;
                }
            } else if in_single {
                let c = *line.get(i)?; // unterminated quote
                if c == b'\\' && i + 1 < n && line[i + 1] == b'\'' {
                    current.push(b'\'');
                    i += 2;
                } else if c == b'\'' {
                    if i + 1 < n && !is_space(line[i + 1]) {
                        return None;
                    }
                    i += 1;
                    break;
                } else {
                    current.push(c);
                    i += 1;
                }
            } else {
                let Some(&c) = line.get(i) else { break };
                i += 1;
                match c {
                    c if is_space(c) => break,
                    b'"' => in_double = true,
                    b'\'' => in_single = true,
                    c => current.push(c),
                }
            }
        }
        args.push(Bytes::from(current));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn args(list: &[&str]) -> Vec<Bytes> {
        list.iter()
            .map(|s| Bytes::copy_from_slice(s.as_bytes()))
            .collect()
    }

    fn parse(input: &[u8]) -> Result<Option<Vec<Bytes>>, ProtocolError> {
        parse_request(&mut BytesMut::from(input))
    }

    #[test]
    fn multibulk() {
        assert_eq!(
            parse(b"*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n").unwrap(),
            Some(args(&["ECHO", "hi"]))
        );
    }

    #[test]
    fn multibulk_is_binary_safe() {
        let parsed = parse(b"*2\r\n$3\r\nSET\r\n$4\r\n\r\n\x00\xff\r\n")
            .unwrap()
            .unwrap();
        assert_eq!(&parsed[1][..], b"\r\n\x00\xff");
    }

    #[test]
    fn pipelined_commands_are_returned_one_at_a_time() {
        let mut buf = BytesMut::from(&b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nQUIT\r\n"[..]);
        assert_eq!(parse_request(&mut buf).unwrap(), Some(args(&["PING"])));
        assert_eq!(parse_request(&mut buf).unwrap(), Some(args(&["QUIT"])));
        assert_eq!(parse_request(&mut buf).unwrap(), None);
        assert!(buf.is_empty());
    }

    #[test]
    fn incomplete_input_consumes_nothing() {
        let full = b"*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n";
        for cut in 0..full.len() {
            let mut buf = BytesMut::from(&full[..cut]);
            assert_eq!(parse_request(&mut buf).unwrap(), None, "cut at {cut}");
            assert_eq!(buf.len(), cut);
        }
    }

    #[test]
    fn empty_commands_are_skipped() {
        assert_eq!(
            parse(b"*0\r\n\r\n*-1\r\n*1\r\n$4\r\nPING\r\n").unwrap(),
            Some(args(&["PING"]))
        );
        assert_eq!(parse(b"\r\n\r\n").unwrap(), None);
    }

    #[test]
    fn inline() {
        assert_eq!(parse(b"PING\r\n").unwrap(), Some(args(&["PING"])));
        assert_eq!(
            parse(b"  SET   key  value \n").unwrap(),
            Some(args(&["SET", "key", "value"]))
        );
    }

    #[test]
    fn inline_quoting() {
        assert_eq!(
            parse(b"SET k \"a b\\n\\x41\"\r\n").unwrap(),
            Some(args(&["SET", "k", "a b\nA"]))
        );
        assert_eq!(
            parse(b"SET k 'it\\'s'\r\n").unwrap(),
            Some(args(&["SET", "k", "it's"]))
        );
        assert_eq!(parse(b"ECHO \"\"\r\n").unwrap(), Some(args(&["ECHO", ""])));
    }

    #[test]
    fn inline_unbalanced_quotes() {
        assert!(parse(b"SET k \"abc\r\n").is_err());
        assert!(parse(b"SET k 'abc\r\n").is_err());
        assert!(parse(b"SET k \"abc\"def\r\n").is_err());
    }

    #[test]
    fn protocol_errors() {
        let cases: [(&[u8], &str); 5] = [
            (b"*abc\r\n", "Protocol error: invalid multibulk length"),
            (b"*99999999\r\n", "Protocol error: invalid multibulk length"),
            (b"*1\r\n:1\r\n", "Protocol error: expected '$', got ':'"),
            (b"*1\r\n$-1\r\n", "Protocol error: invalid bulk length"),
            (
                b"*1\r\n$999999999999\r\n",
                "Protocol error: invalid bulk length",
            ),
        ];
        for (input, expected) in cases {
            assert_eq!(parse(input).unwrap_err().message(), expected);
        }
    }

    #[test]
    fn oversized_lines_are_rejected() {
        let long = vec![b'a'; MAX_LINE_LEN + 2];
        assert_eq!(
            parse(&long).unwrap_err().message(),
            "Protocol error: too big inline request"
        );
        let mut mbulk = vec![b'*'];
        mbulk.extend(vec![b'1'; MAX_LINE_LEN + 2]);
        assert_eq!(
            parse(&mbulk).unwrap_err().message(),
            "Protocol error: too big mbulk count string"
        );
    }

    proptest! {
        #[test]
        fn multibulk_round_trip(
            cmd in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..64), 1..12),
            split in any::<prop::sample::Index>(),
        ) {
            let mut wire = Vec::new();
            wire.extend(format!("*{}\r\n", cmd.len()).bytes());
            for arg in &cmd {
                wire.extend(format!("${}\r\n", arg.len()).bytes());
                wire.extend(arg);
                wire.extend(b"\r\n");
            }

            // Deliver in two pieces to exercise the incomplete path.
            let split = split.index(wire.len());
            let mut buf = BytesMut::from(&wire[..split]);
            prop_assert_eq!(parse_request(&mut buf).unwrap(), None);
            buf.extend_from_slice(&wire[split..]);

            let parsed = parse_request(&mut buf).unwrap().unwrap();
            let parsed: Vec<Vec<u8>> = parsed.iter().map(|b| b.to_vec()).collect();
            prop_assert_eq!(parsed, cmd);
            prop_assert!(buf.is_empty());
        }

        #[test]
        fn arbitrary_bytes_never_panic(data in prop::collection::vec(any::<u8>(), 0..256)) {
            let mut buf = BytesMut::from(&data[..]);
            // Drain until the parser stops making progress.
            while let Ok(Some(_)) = parse_request(&mut buf) {}
        }
    }
}
